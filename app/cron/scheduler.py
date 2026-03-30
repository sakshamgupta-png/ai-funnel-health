from __future__ import annotations

import asyncio
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.configs.settings import get_settings
from app.funnels.registry import list_funnels
from app.funnels.service import run_funnel_monitor

_scheduler: Optional[AsyncIOScheduler] = None


async def _run_funnel_job(funnel_id: str) -> None:
    print(f"[SCHEDULER] Triggered job for funnel={funnel_id}")
    await asyncio.to_thread(run_funnel_monitor, funnel_id)


def start_scheduler() -> None:
    global _scheduler

    settings = get_settings()
    print(
        f"[SCHEDULER] start_scheduler called | "
        f"enable_scheduler={settings.enable_scheduler} | "
        f"timezone={settings.scheduler_timezone} | "
        f"minute={settings.scheduler_minute}"
    )

    if not settings.enable_scheduler:
        print("[SCHEDULER] Scheduler disabled via ENABLE_SCHEDULER=false")
        return

    if _scheduler and _scheduler.running:
        print("[SCHEDULER] Scheduler already running")
        return

    _scheduler = AsyncIOScheduler(timezone=settings.scheduler_timezone)

    enabled_funnels = [funnel for funnel in list_funnels() if funnel.enabled]
    print(f"[SCHEDULER] Enabled funnels: {[f.funnel_id for f in enabled_funnels]}")

    for funnel in enabled_funnels:
        job = _scheduler.add_job(
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
        print(
            f"[SCHEDULER] Added job id={job.id} "
            f"for funnel={funnel.funnel_id} "
            f"trigger={job.trigger}"
        )

    _scheduler.start()
    print("[SCHEDULER] Scheduler started")

    for job in _scheduler.get_jobs():
        print(f"[SCHEDULER] Job {job.id} next_run_time={job.next_run_time}")


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        print("[SCHEDULER] Shutting down scheduler")
        _scheduler.shutdown(wait=False)
    _scheduler = None