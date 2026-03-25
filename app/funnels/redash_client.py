from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from app.funnels.models import FunnelConfig


def _save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _get_redash_user_api_key(source_cfg: dict[str, Any]) -> str:
    explicit_key = source_cfg.get("user_api_key")
    if explicit_key:
        return explicit_key

    env_name = source_cfg.get("user_api_key_env")
    if env_name:
        value = os.getenv(env_name)
        if value:
            return value

    raise RuntimeError(
        "Redash User API key not found. Set user_api_key_env in funnel.json and the value in .env"
    )


def _auth_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Key {api_key}",
        "Content-Type": "application/json",
    }


def _get_query_result_from_id(
    base_url: str,
    api_key: str,
    query_result_id: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    url = f"{base_url}/api/query_results/{query_result_id}.json"
    response = requests.get(url, headers=_auth_headers(api_key), timeout=timeout_seconds)
    response.raise_for_status()
    return response.json()


def _poll_redash_job(
    base_url: str,
    api_key: str,
    job_id: str | int,
    timeout_seconds: int,
) -> dict[str, Any]:
    url = f"{base_url}/api/jobs/{job_id}"

    for _ in range(25):
        response = requests.get(url, headers=_auth_headers(api_key), timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()
        job = data.get("job", data)

        status = job.get("status")
        # 1=PENDING, 2=STARTED, 3=SUCCESS, 4=FAILURE, 5=CANCELLED
        if status == 3:
            query_result_id = job.get("query_result_id")
            if not query_result_id:
                raise RuntimeError(f"Redash job {job_id} succeeded but query_result_id missing")
            return _get_query_result_from_id(base_url, api_key, query_result_id, timeout_seconds)

        if status in {4, 5}:
            raise RuntimeError(f"Redash job {job_id} failed/cancelled: {data}")

        time.sleep(3)

    raise RuntimeError(f"Timed out waiting for Redash job {job_id}")


def _run_results_request(
    base_url: str,
    api_key: str,
    query_id: int,
    max_age_seconds: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    """
    Redash semantics:
    - POST /api/queries/<id>/results with max_age>0 may return cached result
    - if cache too old, Redash starts a new execution job
    - max_age=0 guarantees a new execution
    """
    url = f"{base_url}/api/queries/{query_id}/results"
    payload = {"max_age": max_age_seconds}

    response = requests.post(
        url,
        headers=_auth_headers(api_key),
        json=payload,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    data = response.json()

    if "query_result" in data:
        return data

    if "job" in data:
        return _poll_redash_job(base_url, api_key, data["job"]["id"], timeout_seconds)

    # fallback for variant shapes
    if isinstance(data, dict) and data.get("status") in {1, 2, 3, 4, 5} and data.get("id"):
        return _poll_redash_job(base_url, api_key, data["id"], timeout_seconds)

    return data


def _extract_rows(report_json: dict[str, Any]) -> list[dict[str, Any]]:
    query_result = report_json.get("query_result", report_json)
    data = query_result.get("data", {})

    if isinstance(data, str):
        data = json.loads(data)

    if isinstance(data, dict):
        return data.get("rows", []) or []

    return []


def _parse_retrieved_at(report_json: dict[str, Any]) -> datetime | None:
    query_result = report_json.get("query_result", report_json)
    retrieved_at = query_result.get("retrieved_at")
    if not retrieved_at:
        return None

    try:
        return datetime.fromisoformat(retrieved_at.replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_bucket_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(minute=0, second=0, microsecond=0, tzinfo=None)

    if not isinstance(value, str):
        raise ValueError(f"Unsupported bucket value type: {type(value).__name__}")

    raw = value.strip()

    # try ISO first
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt.replace(minute=0, second=0, microsecond=0)
    except Exception:
        pass

    # common Redash string formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(minute=0, second=0, microsecond=0)
        except Exception:
            continue

    raise ValueError(f"Could not parse Redash time bucket: {value}")


def _latest_bucket_from_rows(rows: list[dict[str, Any]], time_column: str) -> datetime | None:
    latest: datetime | None = None
    for row in rows:
        if time_column not in row or row[time_column] in (None, ""):
            continue
        dt = _parse_bucket_datetime(row[time_column])
        if latest is None or dt > latest:
            latest = dt
    return latest


def fetch_redash_report(
    funnel: FunnelConfig,
    run_dt: datetime,
) -> dict[str, Any] | None:
    source_cfg = funnel.source("redash")
    if not source_cfg or not source_cfg.get("enabled", True):
        return None

    base_url = source_cfg["base_url"].rstrip("/")
    query_id = int(source_cfg["query_id"])
    api_key = _get_redash_user_api_key(source_cfg)
    timeout_seconds = int(source_cfg.get("timeout_seconds", 90))
    max_age_seconds = int(source_cfg.get("max_age_seconds", 900))
    freshness_tolerance_minutes = int(source_cfg.get("freshness_tolerance_minutes", 120))
    time_column = source_cfg["time_column"]

    # previous-hour bucket we want aligned with WebEngage
    target_dt = run_dt.replace(minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(hours=1)

    # 1) normal fetch: cached if fresh enough, otherwise Redash runs query
    result = _run_results_request(
        base_url=base_url,
        api_key=api_key,
        query_id=query_id,
        max_age_seconds=max_age_seconds,
        timeout_seconds=timeout_seconds,
    )

    rows = _extract_rows(result)
    latest_bucket = _latest_bucket_from_rows(rows, time_column)
    retrieved_at = _parse_retrieved_at(result)

    is_stale_by_retrieved_at = False
    if retrieved_at is not None:
        same_tz_run = run_dt.astimezone(retrieved_at.tzinfo)
        oldest_allowed = same_tz_run - timedelta(minutes=freshness_tolerance_minutes)
        is_stale_by_retrieved_at = retrieved_at < oldest_allowed

    missing_target_bucket = latest_bucket is None or latest_bucket < target_dt

    # 2) if stale or missing required bucket, force fresh execution
    if is_stale_by_retrieved_at or missing_target_bucket:
        result = _run_results_request(
            base_url=base_url,
            api_key=api_key,
            query_id=query_id,
            max_age_seconds=0,   # guarantees fresh execution
            timeout_seconds=timeout_seconds,
        )

        rows = _extract_rows(result)
        latest_bucket = _latest_bucket_from_rows(rows, time_column)

    # 3) final validation: after forced refresh, the target bucket must exist
    if latest_bucket is None:
        raise RuntimeError("Redash returned no usable rows after refresh")

    if latest_bucket < target_dt:
        raise RuntimeError(
            f"Redash data still does not include the required bucket. "
            f"target_hour={target_dt.isoformat()} latest_redash_bucket={latest_bucket.isoformat()}"
        )

    _save_json(funnel.output_file("redash_last_report.json"), result)
    return result