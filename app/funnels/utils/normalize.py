from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.funnels.utils.models import FunnelConfig

WEBENGAGE_BUCKET_FORMAT = "%Y-%m-%d-%H"


def _coerce_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if cleaned == "":
            return 0.0
        return float(cleaned)
    return float(value)


def _parse_bucket_datetime(value: Any, timezone_name: str) -> datetime:
    tz = ZoneInfo(timezone_name)

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            parsed = None
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
            ]
            for fmt in formats:
                try:
                    parsed = datetime.strptime(raw, fmt)
                    break
                except Exception:
                    continue
            if parsed is None:
                raise ValueError(f"Could not parse Redash time bucket: {value}")
            dt = parsed
    else:
        raise ValueError(f"Unsupported time bucket type: {type(value).__name__}")

    if dt.tzinfo is not None:
        dt = dt.astimezone(tz)
    else:
        dt = dt.replace(tzinfo=tz)

    return dt.replace(minute=0, second=0, microsecond=0, tzinfo=None)


def _normalize_webengage(
    funnel: FunnelConfig,
    report_json: dict[str, Any] | None,
    bucket_map: dict[datetime, dict[str, Any]],
    source_meta: dict[str, Any],
) -> None:
    if not report_json:
        return

    step_map = {
        step.source_key: step.name
        for step in funnel.steps_for_source("webengage")
    }

    data = report_json.get("response", {}).get("data", [])
    if not data:
        return

    dimensions = data[0].get("dimensions", [])
    for row in dimensions:
        bucket_dt = datetime.strptime(row["value"], WEBENGAGE_BUCKET_FORMAT)
        bucket_values = bucket_map.setdefault(bucket_dt, {"values": {}, "present_steps": set()})
        for stat in row.get("stats", []):
            source_event_name = stat.get("event_name") or stat.get("series")
            if source_event_name in step_map:
                step_name = step_map[source_event_name]
                bucket_values["values"][step_name] = _coerce_float(stat.get("value", 0))
                bucket_values["present_steps"].add(step_name)

    source_meta["webengage"] = {
        "bucket_count": len(dimensions),
    }


def _extract_redash_rows_and_meta(report_json: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not report_json:
        return [], {}

    query_result = report_json.get("query_result", report_json)
    data = query_result.get("data", {})

    if isinstance(data, str):
        data = json.loads(data)

    rows = data.get("rows", []) if isinstance(data, dict) else []
    meta = {
        "retrieved_at": query_result.get("retrieved_at"),
        "query_result_id": query_result.get("id"),
    }
    return rows, meta


def _normalize_redash(
    funnel: FunnelConfig,
    report_json: dict[str, Any] | None,
    bucket_map: dict[datetime, dict[str, Any]],
    source_meta: dict[str, Any],
    timezone_name: str,
) -> None:
    if not report_json:
        return

    source_cfg = funnel.source("redash")
    time_column = source_cfg["time_column"]

    step_map = {
        step.source_key: step.name
        for step in funnel.steps_for_source("redash")
    }

    rows, meta = _extract_redash_rows_and_meta(report_json)

    for row in rows:
        if time_column not in row:
            continue

        bucket_dt = _parse_bucket_datetime(row[time_column], timezone_name)
        bucket_values = bucket_map.setdefault(bucket_dt, {"values": {}, "present_steps": set()})

        for source_column, step_name in step_map.items():
            if source_column in row:
                bucket_values["values"][step_name] = _coerce_float(row.get(source_column, 0))
                bucket_values["present_steps"].add(step_name)

    source_meta["redash"] = {
        **meta,
        "row_count": len(rows),
        "time_column": time_column,
    }


def normalize_report(
    funnel: FunnelConfig,
    run_dt: datetime,
    timezone_name: str,
    webengage_report: dict[str, Any] | None,
    redash_report: dict[str, Any] | None,
) -> dict[str, Any]:
    bucket_map: dict[datetime, dict[str, Any]] = {}
    source_meta: dict[str, Any] = {}

    _normalize_webengage(funnel, webengage_report, bucket_map, source_meta)
    _normalize_redash(funnel, redash_report, bucket_map, source_meta, timezone_name)

    comparison_days = int(funnel.health_config.get("comparison_days", 10))
    target_dt = run_dt.replace(minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(hours=1)
    window_start = (target_dt - timedelta(days=comparison_days + 1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    all_step_names = funnel.step_names()
    buckets: list[dict[str, Any]] = []

    for dt in sorted(bucket_map.keys()):
        if dt < window_start or dt > target_dt:
            continue

        values = {step_name: bucket_map[dt]["values"].get(step_name, 0.0) for step_name in all_step_names}
        present_steps = sorted(bucket_map[dt]["present_steps"])

        buckets.append(
            {
                "bucket_start": dt.strftime("%Y-%m-%dT%H:00:00"),
                "values": values,
                "present_steps": present_steps,
            }
        )

    return {
        "funnel_id": funnel.funnel_id,
        "funnel_name": funnel.name,
        "generated_at": run_dt.isoformat(),
        "timezone": timezone_name,
        "window_start": window_start.strftime("%Y-%m-%dT%H:00:00"),
        "target_hour": target_dt.strftime("%Y-%m-%dT%H:00:00"),
        "comparison_days": comparison_days,
        "steps": all_step_names,
        "source_meta": source_meta,
        "buckets": buckets,
    }


# Backward-compatible alias
def normalize_and_store_report(
    funnel: FunnelConfig,
    run_dt: datetime,
    timezone_name: str,
    webengage_report: dict[str, Any] | None,
    redash_report: dict[str, Any] | None,
) -> dict[str, Any]:
    return normalize_report(
        funnel=funnel,
        run_dt=run_dt,
        timezone_name=timezone_name,
        webengage_report=webengage_report,
        redash_report=redash_report,
    )