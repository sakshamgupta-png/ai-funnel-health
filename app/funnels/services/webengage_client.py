from __future__ import annotations

import copy
import time
from datetime import datetime, timedelta
from typing import Any

import requests

from app.auth.playwright_auth import build_cookie_header_from_auth_state
from app.configs.settings import Settings
from app.funnels.utils.models import FunnelConfig


def build_webengage_runtime_payload(
    funnel: FunnelConfig,
    run_dt: datetime,
) -> dict[str, Any]:
    source_cfg = funnel.source("webengage")
    payload = copy.deepcopy(source_cfg["payload_template"])

    comparison_days = int(funnel.health_config.get("comparison_days", 10))
    target_dt = run_dt.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    start_dt = (target_dt - timedelta(days=comparison_days + 1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    payload["startTime"] = start_dt.isoformat(timespec="milliseconds")
    payload["endTime"] = run_dt.isoformat(timespec="milliseconds")
    return payload


def _extract_status_and_job_id(result: dict[str, Any]) -> tuple[str | None, str | None]:
    response_obj = result.get("response", {})
    status = response_obj.get("status")
    data = response_obj.get("data")

    if isinstance(data, list):
        return status, None

    if isinstance(data, dict):
        return status, data.get("_jobId")

    return status, None


def fetch_webengage_report(
    funnel: FunnelConfig,
    settings: Settings,
    run_dt: datetime,
) -> dict[str, Any] | None:
    source_cfg = funnel.source("webengage")
    if not source_cfg or not source_cfg.get("enabled", True):
        return None

    if not settings.webengage_account_id:
        raise RuntimeError("WEBENGAGE_ACCOUNT_ID is missing")

    runtime_payload = build_webengage_runtime_payload(funnel, run_dt)

    endpoint = (
        f"{settings.webengage_base_url}/api/v2/accounts/"
        f"{settings.webengage_account_id}/event-analytics/report"
    )
    cookie_header = build_cookie_header_from_auth_state(
        settings.webengage_auth_state,
        settings.webengage_base_url,
    )

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": settings.webengage_base_url,
        "Referer": f"{settings.webengage_base_url}/accounts/{settings.webengage_account_id}/analytics/events",
        "Cookie": cookie_header,
        "x-async": "false",
    }

    with requests.Session() as session:
        response = session.post(
            endpoint,
            headers=headers,
            json=runtime_payload,
            timeout=settings.webengage_timeout_seconds,
        )
        response.raise_for_status()
        result = response.json()

        status, job_id = _extract_status_and_job_id(result)

        poll_attempts = 10
        while status == "running" and job_id and poll_attempts > 0:
            poll_attempts -= 1
            time.sleep(3)

            poll_headers = dict(headers)
            poll_headers["x-async"] = "true"
            poll_headers["x-job-id"] = job_id

            poll_response = session.post(
                endpoint,
                headers=poll_headers,
                json=runtime_payload,
                timeout=settings.webengage_timeout_seconds,
            )
            poll_response.raise_for_status()
            result = poll_response.json()
            status, job_id = _extract_status_and_job_id(result)

    return {
        "runtime_payload": runtime_payload,
        "report": result,
    }