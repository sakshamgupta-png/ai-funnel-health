from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import traceback

from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool

from app.configs.settings import get_settings
from app.cron.scheduler import start_scheduler, stop_scheduler
from app.funnels.registry import get_funnel, list_funnels
from app.funnels.service import run_all_enabled_funnels, run_funnel_monitor


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    if settings.enable_scheduler:
        start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(
    title="AI Funnel Health",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    settings = get_settings()
    return {
        "ok": True,
        "app_name": settings.app_name,
        "scheduler_enabled": settings.enable_scheduler,
        "timezone": settings.app_timezone,
    }


@app.get("/funnels")
def funnels() -> list[dict]:
    return [
        {
            "funnel_id": funnel.funnel_id,
            "name": funnel.name,
            "enabled": funnel.enabled,
            "outputs_dir": str(funnel.outputs_dir),
        }
        for funnel in list_funnels()
    ]


@app.post("/funnels/{funnel_id}/run")
async def run_single_funnel(funnel_id: str) -> dict:
    try:
        return await run_in_threadpool(run_funnel_monitor, funnel_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc


@app.post("/funnels/run-all")
async def run_enabled_funnels() -> list[dict]:
    try:
        return await run_in_threadpool(run_all_enabled_funnels)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc


@app.get("/funnels/{funnel_id}/health-report")
def get_health_report(funnel_id: str) -> dict:
    try:
        funnel = get_funnel(funnel_id)
        path = funnel.output_file("health_report.json")
        if not path.exists():
            raise FileNotFoundError("health_report.json not found")
        return __import__("json").load(path.open("r", encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc


@app.get("/funnels/{funnel_id}/summary")
def get_summary(funnel_id: str) -> dict:
    try:
        funnel = get_funnel(funnel_id)
        path = funnel.output_file("summary.json")
        if not path.exists():
            raise FileNotFoundError("summary.json not found")
        return __import__("json").load(path.open("r", encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=traceback.format_exc()) from exc