from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.configs.settings import get_settings
from app.funnels.services.notification_service import maybe_send_notifications
from app.funnels.services.redash_client import fetch_redash_report
from app.funnels.services.webengage_client import fetch_webengage_report
from app.funnels.utils.ai_summary import generate_summary
from app.funnels.utils.health import analyze_health
from app.funnels.utils.normalize import normalize_report
from app.funnels.utils.registry import get_funnel, list_funnels
from app.repositories.funnel_run_repo import create_run, update_run_results


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

    run_id = create_run(
        funnel=funnel,
        run_dt=run_dt,
        force_notification=force_notification,
    )

    webengage_bundle: dict | None = None
    redash_report: dict | None = None

    try:
        webengage_bundle = fetch_webengage_report(funnel, settings, run_dt)
        redash_report = fetch_redash_report(funnel, run_dt)

        normalized_report = normalize_report(
            funnel=funnel,
            run_dt=run_dt,
            timezone_name=settings.app_timezone,
            webengage_report=(webengage_bundle or {}).get("report"),
            redash_report=redash_report,
        )

        health_report = analyze_health(
            funnel=funnel,
            normalized_report=normalized_report,
            run_dt=run_dt,
            timezone_name=settings.app_timezone,
        )

        summary = generate_summary(funnel, health_report, settings)

        update_run_results(
            run_id,
            target_hour=health_report.get("latest_complete_hour"),
            status=health_report.get("overall_status"),
            health_report=health_report,
            summary=summary,
        )

        notification_results = maybe_send_notifications(
            funnel=funnel,
            health_report=health_report,
            summary=summary,
            settings=settings,
            run_id=run_id,
            force_notification=force_notification,
        )

        print(f"[DONE] Finished funnel monitor for {funnel_id} with status={health_report.get('overall_status')}")

        return {
            "run_id": run_id,
            "funnel_id": funnel.funnel_id,
            "name": funnel.name,
            "run_time": run_dt.isoformat(),
            "target_hour": health_report.get("latest_complete_hour"),
            "status": health_report.get("overall_status"),
            "mail_sent": notification_results["mail_sent"],
            "chat_sent": notification_results["chat_sent"],
        }

    except Exception as exc:
        update_run_results(
            run_id,
            status="failed",
            error=str(exc)
        )
        raise


def run_all_enabled_funnels(run_dt: datetime | None = None) -> list[dict]:
    results: list[dict] = []
    for funnel in list_funnels():
        if not funnel.enabled:
            continue
        results.append(run_funnel_monitor(funnel.funnel_id, run_dt=run_dt))
    return results