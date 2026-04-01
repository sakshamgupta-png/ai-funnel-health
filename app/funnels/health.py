from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
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
    event_cfg = config.get("events", {}).get(event_name, {})
    if "benchmark_value" not in event_cfg:
        raise ValueError(f"Missing benchmark_value for event: {event_name}")
    return {
        "benchmark_value": float(event_cfg["benchmark_value"]),
        "drop_threshold_pct": float(event_cfg.get("drop_threshold_pct", 5)),
    }


def _config_for_ratio(config: dict[str, Any], from_event: str, to_event: str) -> dict[str, Any]:
    key = f"{from_event}->{to_event}"
    ratio_cfg = config.get("ratios", {}).get(key, {})
    if "benchmark_pct" not in ratio_cfg:
        raise ValueError(f"Missing benchmark_pct for ratio: {key}")
    return {
        "benchmark_pct": float(ratio_cfg["benchmark_pct"]),
        "drop_threshold_pct": float(ratio_cfg.get("drop_threshold_pct", 0)),
    }


def _pct_drop(current: float, benchmark: float | None) -> float | None:
    if benchmark is None or benchmark <= 0:
        return None
    return ((benchmark - current) / benchmark) * 100.0


def _safe_ratio(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den


def _classify_overall(
    alert_mode: str,
    event_results: list[dict[str, Any]],
    ratio_results: list[dict[str, Any]],
) -> str:
    alerted_events = [e for e in event_results if e.get("alert")]
    alerted_ratios = [r for r in ratio_results if r.get("alert")]

    if alert_mode == "ratios_only":
        return "critical" if alerted_ratios else "healthy"

    if not alerted_events and not alerted_ratios:
        return "healthy"

    event_max_drop = max((e.get("drop_pct_vs_benchmark") or 0) for e in alerted_events) if alerted_events else 0
    ratio_max_drop = max((r.get("ratio_drop_pct_vs_benchmark") or 0) for r in alerted_ratios) if alerted_ratios else 0

    if event_max_drop >= 25 or ratio_max_drop >= 25 or (len(alerted_events) + len(alerted_ratios) >= 2):
        return "critical"

    return "watch"


def analyze_and_store_health(
    funnel: FunnelConfig,
    normalized_report: dict[str, Any],
    run_dt: datetime,
    timezone_name: str,
) -> dict[str, Any]:
    config = funnel.health_config
    alert_mode = (config.get("alert_mode") or "events_and_ratios").lower()
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
        benchmark_value = settings["benchmark_value"]
        drop_pct_vs_benchmark = _pct_drop(current_value, benchmark_value)

        # In ratios_only mode, event alerts are disabled
        event_alert = False
        if alert_mode != "ratios_only":
            event_alert = (
                drop_pct_vs_benchmark is not None
                and drop_pct_vs_benchmark >= settings["drop_threshold_pct"]
            )

        event_results.append(
            {
                "event_name": event_name,
                "current_value": current_value,
                "latest_complete_hour": target_dt.strftime("%Y-%m-%dT%H:00:00"),
                "benchmark_value": benchmark_value,
                "drop_pct_vs_benchmark": drop_pct_vs_benchmark,
                "threshold_pct": settings["drop_threshold_pct"],
                "alert": event_alert,
            }
        )

    ratio_results: list[dict[str, Any]] = []
    for idx in range(len(ordered_events) - 1):
        from_event = ordered_events[idx]
        to_event = ordered_events[idx + 1]

        current_ratio = _safe_ratio(target_values.get(to_event, 0), target_values.get(from_event, 0))

        ratio_settings = _config_for_ratio(config, from_event, to_event)
        benchmark_ratio = ratio_settings["benchmark_pct"] / 100.0
        ratio_drop_pct = _pct_drop(current_ratio, benchmark_ratio) if current_ratio is not None else None

        ratio_alert = (
            ratio_drop_pct is not None
            and ratio_drop_pct >= ratio_settings["drop_threshold_pct"]
        )

        ratio_results.append(
            {
                "from_event": from_event,
                "to_event": to_event,
                "current_ratio": current_ratio,
                "benchmark_ratio": benchmark_ratio,
                "benchmark_pct": ratio_settings["benchmark_pct"],
                "ratio_drop_pct_vs_benchmark": ratio_drop_pct,
                "threshold_pct": ratio_settings["drop_threshold_pct"],
                "alert": ratio_alert,
            }
        )

    health_report = {
        "latest_complete_hour": target_dt.strftime("%Y-%m-%dT%H:00:00"),
        "overall_status": _classify_overall(alert_mode, event_results, ratio_results),
        "alert_mode": alert_mode,
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