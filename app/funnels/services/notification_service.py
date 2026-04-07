from __future__ import annotations

from app.configs.settings import Settings
from app.funnels.services.chat_service import send_google_chat_message
from app.funnels.services.email_service import maybe_send_email
from app.funnels.utils.models import FunnelConfig
from app.repositories.funnel_run_repo import (
    mark_notification_result,
    should_send_notification_for_hour,
)


def _has_alert(funnel: FunnelConfig, health_report: dict[str, object]) -> bool:
    alert_mode = (funnel.health_config.get("alert_mode") or "events_and_ratios").lower()

    if alert_mode == "ratios_only":
        return bool(health_report.get("alerted_ratios"))

    return bool(health_report.get("alerted_events")) or bool(health_report.get("alerted_ratios"))


def _should_notify(funnel: FunnelConfig, health_report: dict[str, object]) -> bool:
    mode = (funnel.email.notify_on or "alert_only").lower()

    if mode == "never":
        return False
    if mode == "always":
        return True
    if mode == "alert_only":
        return _has_alert(funnel, health_report)
    return False


def _parse_channels(value: str) -> list[str]:
    channels = []
    for item in value.split(","):
        item = item.strip().lower()
        if item in {"mail", "chat"} and item not in channels:
            channels.append(item)
    return channels


def maybe_send_notifications(
    funnel: FunnelConfig,
    health_report: dict,
    summary: dict,
    settings: Settings,
    run_id: str,
    force_notification: bool = False,
) -> dict[str, bool]:
    results = {
        "mail_sent": False,
        "chat_sent": False,
    }

    if not _should_notify(funnel, health_report):
        return results

    target_hour = health_report.get("latest_complete_hour")
    if not target_hour:
        return results

    channels = _parse_channels(settings.notification_channel)

    for channel in channels:
        if not should_send_notification_for_hour(
            funnel_id=funnel.funnel_id,
            target_hour=target_hour,
            channel=channel,
            force_notification=force_notification,
        ):
            print(f"[SKIP] {channel} already sent for {funnel.funnel_id} target_hour={target_hour}")
            continue

        try:
            if channel == "mail":
                sent = maybe_send_email(funnel, health_report, summary, settings)
                mark_notification_result(run_id, "mail", attempted=True, sent=bool(sent), error=None)
                results["mail_sent"] = bool(sent)

            elif channel == "chat":
                sent = send_google_chat_message(funnel, health_report, summary, settings)
                mark_notification_result(run_id, "chat", attempted=True, sent=bool(sent), error=None)
                results["chat_sent"] = bool(sent)

        except Exception as exc:
            mark_notification_result(run_id, channel, attempted=True, sent=False, error=str(exc))
            raise

    return results