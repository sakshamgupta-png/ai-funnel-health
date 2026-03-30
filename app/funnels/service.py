from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.configs.settings import get_settings
from app.funnels.ai_summary import generate_and_store_summary
from app.funnels.health import analyze_and_store_health
from app.funnels.normalize import normalize_and_store_report
from app.funnels.notification_service import maybe_send_notifications
from app.funnels.redash_client import fetch_redash_report
from app.funnels.registry import get_funnel, list_funnels
from app.funnels.webengage_client import fetch_webengage_report


def run_funnel_monitor(
    funnel_id: str,
    run_dt: datetime | None = None,
    force_notification: bool = False,
) -> dict:
    settings = get_settings()
    funnel = get_funnel(funnel_id)

    if not funnel.enabled:
        raise ValueError(f"Funnel is disabled: {funnel_id}")

    tz = ZoneInfo(settings.app_timezone)
    run_dt = run_dt.astimezone(tz) if run_dt else datetime.now(tz)

    print(f"[RUN] Starting funnel monitor for {funnel_id} at {run_dt.isoformat()}")

    webengage_report = fetch_webengage_report(funnel, settings, run_dt)
    redash_report = fetch_redash_report(funnel, run_dt)

    normalized_report = normalize_and_store_report(
        funnel=funnel,
        run_dt=run_dt,
        timezone_name=settings.app_timezone,
        webengage_report=webengage_report,
        redash_report=redash_report,
    )

    health_report = analyze_and_store_health(
        funnel=funnel,
        normalized_report=normalized_report,
        run_dt=run_dt,
        timezone_name=settings.app_timezone,
    )

    summary = generate_and_store_summary(funnel, health_report, settings)

    notification_results = maybe_send_notifications(
        funnel=funnel,
        health_report=health_report,
        summary=summary,
        settings=settings,
        force_notification=force_notification,
    )

    print(f"[DONE] Finished funnel monitor for {funnel_id} with status={health_report.get('overall_status')}")

    return {
        "funnel_id": funnel.funnel_id,
        "name": funnel.name,
        "run_time": run_dt.isoformat(),
        "target_hour": health_report.get("latest_complete_hour"),
        "status": health_report.get("overall_status"),
        "mail_sent": notification_results["mail_sent"],
        "chat_sent": notification_results["chat_sent"],
        "outputs_dir": str(funnel.outputs_dir),
    }


def run_all_enabled_funnels(run_dt: datetime | None = None) -> list[dict]:
    results: list[dict] = []
    for funnel in list_funnels():
        if not funnel.enabled:
            continue
        results.append(run_funnel_monitor(funnel.funnel_id, run_dt=run_dt))
    return results