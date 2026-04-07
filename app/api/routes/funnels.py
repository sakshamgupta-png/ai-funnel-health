from __future__ import annotations

import traceback

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from app.funnels.services.service import run_all_enabled_funnels, run_funnel_monitor
from app.funnels.utils.registry import get_funnel, list_funnels
from app.repositories.funnel_run_repo import (
    get_latest_health_report,
    get_latest_summary,
    get_run_state,
)

router = APIRouter()


@router.get("")
def get_funnels() -> list[dict]:
    return [
        {
            "funnel_id": funnel.funnel_id,
            "name": funnel.name,
            "enabled": funnel.enabled,
        }
        for funnel in list_funnels()
    ]


@router.post("/run-all")
async def run_enabled_funnels_route() -> list[dict]:
    try:
        return await run_in_threadpool(run_all_enabled_funnels)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc


@router.post("/{funnel_id}/run")
async def run_single_funnel(
    funnel_id: str,
    force_notification: bool = False,
) -> dict:
    try:
        return await run_in_threadpool(
            run_funnel_monitor,
            funnel_id,
            None,
            force_notification,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc


@router.get("/{funnel_id}/health-report")
def get_health_report_route(funnel_id: str) -> dict:
    try:
        get_funnel(funnel_id)  # validates funnel exists
        return get_latest_health_report(funnel_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc


@router.get("/{funnel_id}/summary")
def get_summary_route(funnel_id: str) -> dict:
    try:
        get_funnel(funnel_id)
        return get_latest_summary(funnel_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc


@router.get("/{funnel_id}/run-state")
def get_run_state_route(funnel_id: str) -> dict:
    try:
        get_funnel(funnel_id)
        return get_run_state(funnel_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc