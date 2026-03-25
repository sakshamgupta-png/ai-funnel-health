from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any
from zoneinfo import ZoneInfo

from app.funnels.models import FunnelConfig


def _save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _parse_bucket_dt(bucket_start: str) -> datetime:
    return datetime.fromisoformat(bucket_start)


def _build_event_series(normalized_report: dict[str, Any]) -> dict[str, list[tuple[datetime, float]]]:
    event_series: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for bucket in normalized_report.get("buckets", []):
        dt = _parse_bucket_dt(bucket["bucket_start"])
        values = bucket.get("values", {})
        for event_name, value in values.items():
            event_series[event_name].append((dt, float(value)))
    return event_series


def _determine_target_hour(
    normalized_report: dict[str, Any],
    run_dt: datetime,
    timezone_name: str,
) -> datetime:
    tz = ZoneInfo(timezone_name)
    run_dt = run_dt.astimezone(tz)
    target_dt = run_dt.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    target_naive = target_dt.replace(tzinfo=None)

    available = {
        _parse_bucket_dt(bucket["bucket_start"])
        for bucket in normalized_report.get("buckets", [])
    }

    if target_naive not in available:
        available_str = sorted(dt.strftime("%Y-%m-%dT%H:00:00") for dt in available)
        raise ValueError(
            f"Target hour {target_naive.strftime('%Y-%m-%dT%H:00:00')} not found in normalized report. "
            f"Latest available buckets: {available_str[-5:]}"
        )

    return target_naive


def _config_for_event(config: dict[str, Any], event_name: str) -> dict[str, Any]:
    defaults = {
        "drop_threshold_pct": config.get("default_drop_threshold_pct", 5),
        "min_baseline_count": config.get("default_min_baseline_count", 1),
        "min_history_points": config.get("default_min_history_points", 7),
    }
    defaults.update(config.get("events", {}).get(event_name, {}))
    return defaults


def _config_for_ratio(config: dict[str, Any], from_event: str, to_event: str) -> dict[str, Any]:
    key = f"{from_event}->{to_event}"
    defaults = {
        "drop_threshold_pct": config.get("default_ratio_drop_threshold_pct", 10),
        "min_history_points": config.get("default_min_ratio_history_points", 7),
    }
    defaults.update(config.get("ratios", {}).get(key, {}))
    return defaults


def _same_hour_history(
    series: list[tuple[datetime, float]],
    target_dt: datetime,
    comparison_days: int,
) -> list[float]:
    history: list[float] = []
    for dt, value in series:
        day_gap = (target_dt.date() - dt.date()).days
        if 1 <= day_gap <= comparison_days and dt.hour == target_dt.hour:
            history.append(value)
    return history


def _pct_drop(current: float, baseline: float | None) -> float | None:
    if baseline is None or baseline <= 0:
        return None
    return ((baseline - current) / baseline) * 100.0


def _safe_ratio(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den


def _classify_overall(
    event_results: list[dict[str, Any]],
    ratio_results: list[dict[str, Any]],
) -> str:
    alerted_events = [e for e in event_results if e.get("alert")]
    alerted_ratios = [r for r in ratio_results if r.get("alert")]

    if not alerted_events and not alerted_ratios:
        return "healthy"

    if alerted_events:
        max_event_drop = max((e.get("drop_pct_vs_mean") or 0) for e in alerted_events)
        if max_event_drop >= 25:
            return "critical"

    return "watch"


def analyze_and_store_health(
    funnel: FunnelConfig,
    normalized_report: dict[str, Any],
    run_dt: datetime,
    timezone_name: str,
) -> dict[str, Any]:
    config = funnel.health_config
    comparison_days = int(config.get("comparison_days", 10))
    ordered_events = funnel.step_names()

    event_series = _build_event_series(normalized_report)
    target_dt = _determine_target_hour(normalized_report, run_dt, timezone_name)

    target_values: dict[str, float] = {}
    for name in ordered_events:
        series_by_dt = {dt: value for dt, value in event_series.get(name, [])}
        if target_dt in series_by_dt:
            target_values[name] = series_by_dt[target_dt]

    event_results: list[dict[str, Any]] = []
    for event_name in ordered_events:
        series = event_series.get(event_name, [])
        if not series:
            continue

        series_by_dt = {dt: value for dt, value in series}
        if target_dt not in series_by_dt:
            continue

        settings = _config_for_event(config, event_name)
        current_value = series_by_dt[target_dt]
        history_values = _same_hour_history(series, target_dt, comparison_days)

        baseline_mean = mean(history_values) if history_values else None
        baseline_median = median(history_values) if history_values else None
        baseline_stddev = pstdev(history_values) if len(history_values) > 1 else 0.0
        drop_pct_vs_mean = _pct_drop(current_value, baseline_mean)

        alert = (
            baseline_mean is not None
            and len(history_values) >= int(settings["min_history_points"])
            and baseline_mean >= float(settings["min_baseline_count"])
            and drop_pct_vs_mean is not None
            and drop_pct_vs_mean >= float(settings["drop_threshold_pct"])
        )

        event_results.append(
            {
                "event_name": event_name,
                "current_value": current_value,
                "latest_complete_hour": target_dt.strftime("%Y-%m-%dT%H:00:00"),
                "history_same_hour_last_n_days": history_values,
                "history_points": len(history_values),
                "baseline_mean": baseline_mean,
                "baseline_median": baseline_median,
                "baseline_stddev": baseline_stddev,
                "drop_pct_vs_mean": drop_pct_vs_mean,
                "threshold_pct": settings["drop_threshold_pct"],
                "min_baseline_count": settings["min_baseline_count"],
                "min_history_points": settings["min_history_points"],
                "alert": alert,
            }
        )

    ratio_results: list[dict[str, Any]] = []

    bucket_presence = {}
    for bucket in normalized_report.get("buckets", []):
        dt = _parse_bucket_dt(bucket["bucket_start"])
        bucket_presence[dt] = set(bucket.get("present_steps", []))

    for idx in range(len(ordered_events) - 1):
        from_event = ordered_events[idx]
        to_event = ordered_events[idx + 1]

        current_ratio = None
        current_present = bucket_presence.get(target_dt, set())
        if from_event in current_present and to_event in current_present:
            current_ratio = _safe_ratio(target_values.get(to_event, 0), target_values.get(from_event, 0))

        ratio_history: list[float] = []

        from_by_dt = {dt: value for dt, value in event_series.get(from_event, [])}
        to_by_dt = {dt: value for dt, value in event_series.get(to_event, [])}

        for dt in sorted(from_by_dt.keys()):
            day_gap = (target_dt.date() - dt.date()).days
            if not (1 <= day_gap <= comparison_days and dt.hour == target_dt.hour):
                continue

            present_steps = bucket_presence.get(dt, set())
            if from_event not in present_steps or to_event not in present_steps:
                continue

            ratio = _safe_ratio(to_by_dt.get(dt, 0), from_by_dt.get(dt, 0))
            if ratio is not None:
                ratio_history.append(ratio)

        baseline_ratio_mean = mean(ratio_history) if ratio_history else None
        ratio_drop_pct = _pct_drop(current_ratio, baseline_ratio_mean) if current_ratio is not None else None

        ratio_settings = _config_for_ratio(config, from_event, to_event)
        ratio_alert = (
            baseline_ratio_mean is not None
            and len(ratio_history) >= int(ratio_settings["min_history_points"])
            and ratio_drop_pct is not None
            and ratio_drop_pct >= float(ratio_settings["drop_threshold_pct"])
        )

        ratio_results.append(
            {
                "from_event": from_event,
                "to_event": to_event,
                "current_ratio": current_ratio,
                "baseline_ratio_mean": baseline_ratio_mean,
                "ratio_drop_pct_vs_mean": ratio_drop_pct,
                "history_points": len(ratio_history),
                "history_ratios": ratio_history,
                "threshold_pct": ratio_settings["drop_threshold_pct"],
                "min_history_points": ratio_settings["min_history_points"],
                "alert": ratio_alert,
            }
        )

    health_report = {
        "latest_complete_hour": target_dt.strftime("%Y-%m-%dT%H:00:00"),
        "comparison_days": comparison_days,
        "overall_status": _classify_overall(event_results, ratio_results),
        "source_meta": normalized_report.get("source_meta", {}),
        "events": event_results,
        "ratios": ratio_results,
        "alerted_events": [e["event_name"] for e in event_results if e["alert"]],
        "alerted_ratios": [
            f"{r['from_event']}->{r['to_event']}" for r in ratio_results if r["alert"]
        ],
    }

    _save_json(funnel.output_file("health_report.json"), health_report)
    return health_report