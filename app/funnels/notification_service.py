from __future__ import annotations

from typing import Any

from app.configs.settings import Settings
from app.funnels.chat_service import send_google_chat_message
from app.funnels.email_service import maybe_send_email
from app.funnels.models import FunnelConfig
from app.funnels.run_state import (
    mark_notification_sent,
    should_send_notification_for_hour,
)


def _has_alert(health_report: dict[str, Any]) -> bool:
    return bool(health_report.get("alerted_events")) or bool(health_report.get("alerted_ratios"))


def _should_notify(funnel: FunnelConfig, health_report: dict[str, Any]) -> bool:
    mode = (funnel.email.notify_on or "alert_only").lower()

    if mode == "never":
        return False
    if mode == "always":
        return True
    if mode == "alert_only":
        return _has_alert(health_report)
    return False


def _parse_channels(value: str) -> list[str]:
    value = (value or "mail").strip().lower()
    if value == "both":
        return ["mail", "chat"]
    if value in {"mail", "chat", "none"}:
        return [] if value == "none" else [value]

    # support comma-separated values too
    channels = []
    for item in value.split(","):
        item = item.strip().lower()
        if item in {"mail", "chat"} and item not in channels:
            channels.append(item)
    return channels


def maybe_send_notifications(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
    summary: dict[str, Any],
    settings: Settings,
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
            outputs_dir=funnel.outputs_dir,
            target_hour=target_hour,
            channel=channel,
            force_notification=force_notification,
        ):
            print(f"[SKIP] {channel} already sent for {funnel.funnel_id} target_hour={target_hour}")
            continue

        if channel == "mail":
            sent = maybe_send_email(funnel, health_report, summary, settings)
            if sent:
                mark_notification_sent(funnel.outputs_dir, target_hour, channel)
                results["mail_sent"] = True

        elif channel == "chat":
            sent = send_google_chat_message(funnel, health_report, summary, settings)
            if sent:
                mark_notification_sent(funnel.outputs_dir, target_hour, channel)
                results["chat_sent"] = True

    return results