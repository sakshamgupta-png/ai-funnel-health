from __future__ import annotations

import asyncio
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.configs.settings import get_settings
from app.funnels.registry import list_funnels
from app.funnels.service import run_funnel_monitor

_scheduler: Optional[AsyncIOScheduler] = None


async def _run_funnel_job(funnel_id: str) -> None:
    await asyncio.to_thread(run_funnel_monitor, funnel_id)


def start_scheduler() -> None:
    global _scheduler

    settings = get_settings()
    if not settings.enable_scheduler:
        return

    if _scheduler and _scheduler.running:
        return

    _scheduler = AsyncIOScheduler(timezone=settings.scheduler_timezone)

    for funnel in list_funnels():
        if not funnel.enabled:
            continue

        _scheduler.add_job(
            _run_funnel_job,
            trigger="cron",
            hour="*",
            minute=settings.scheduler_minute,
            args=[funnel.funnel_id],
            id=f"funnel-monitor:{funnel.funnel_id}",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        #use this for testing with 1 minute interval
        _scheduler.add_job(
            _run_funnel_job,
            trigger="interval",
            minutes=1,
            args=[funnel.funnel_id],
            id=f"funnel-monitor:{funnel.funnel_id}",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )

    _scheduler.start()


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    _scheduler = None