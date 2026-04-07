from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import requests

from app.configs.settings import Settings
from app.funnels.utils.models import FunnelConfig


def _format_hour(dt: datetime) -> str:
    return dt.strftime("%I %p").lstrip("0")


def _format_bucket_range(hour_str: str | None) -> tuple[str, str]:
    if not hour_str:
        return "Unknown date", "Unknown time"

    dt = datetime.fromisoformat(hour_str)
    end_dt = dt + timedelta(hours=1)
    date_label = dt.strftime("%d %b %Y")
    time_range = f"{_format_hour(dt)} to {_format_hour(end_dt)}"
    return date_label, time_range


def _format_number(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.0f}"
    except Exception:
        return str(value)


def _format_pct(value: Any, decimals: int = 1) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.{decimals}f}%"
    except Exception:
        return str(value)


def _event_map(health_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {e.get("event_name"): e for e in health_report.get("events", [])}


def _ratio_map(health_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for r in health_report.get("ratios", []):
        result[f"{r.get('from_event')}->{r.get('to_event')}"] = r
    return result


def _display_change_from_drop_pct(drop_pct: Any) -> str:
    if drop_pct is None:
        return "—"

    try:
        value = float(drop_pct)
    except Exception:
        return str(drop_pct)

    display_change = -value
    if display_change > 0:
        return f"+{display_change:.1f}%"
    if display_change < 0:
        return f"{display_change:.1f}%"
    return "0.0%"


def _status_emoji(status: str) -> str:
    s = (status or "").lower()
    if s == "critical":
        return "🚨"
    if s == "watch":
        return "⚠️"
    return "✅"


def build_google_chat_text(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
    summary: dict[str, Any],
    settings: Settings,
) -> str:
    status = (health_report.get("overall_status") or "healthy").upper()
    status_emoji = _status_emoji(status)
    date_label, time_range = _format_bucket_range(health_report.get("latest_complete_hour"))

    events = _event_map(health_report)
    ratios = _ratio_map(health_report)

    # Individual events: only current values
    step_lines = []
    for step_name in funnel.step_names():
        event = events.get(step_name, {})
        step_lines.append(
            f"• *{step_name}*: `{_format_number(event.get('current_value'))}`"
        )

    # Ratios: current + benchmark + change
    ratio_lines = []
    step_names = funnel.step_names()
    for i in range(max(0, len(step_names) - 1)):
        from_event = step_names[i]
        to_event = step_names[i + 1]
        ratio = ratios.get(f"{from_event}->{to_event}", {})

        benchmark_pct = ratio.get("benchmark_pct")
        if benchmark_pct is None:
            baseline_ratio = ratio.get("baseline_ratio_mean")
            benchmark_pct = (baseline_ratio or 0) * 100

        ratio_change = _display_change_from_drop_pct(
            ratio.get("ratio_drop_pct_vs_benchmark", ratio.get("ratio_drop_pct_vs_mean"))
        )

        ratio_lines.append(
            f"• *{from_event} → {to_event}*: "
            f"`{_format_pct((ratio.get('current_ratio') or 0) * 100, 2)}` "
            f"(benchmark `{_format_pct(benchmark_pct, 2)}`, change *{ratio_change}*)"
        )

    alerted_events = health_report.get("alerted_events", []) or []
    alerted_ratios = health_report.get("alerted_ratios", []) or []

    alert_lines = []
    if alerted_events:
        alert_lines.append(f"• *Event alerts:* {', '.join(alerted_events)}")
    if alerted_ratios:
        alert_lines.append(f"• *Step alerts:* {', '.join(alerted_ratios)}")
    if not alert_lines:
        alert_lines.append("• *No active alerts*")

    action_lines = [
        f"• {item}" for item in (summary.get("what_to_check_now", []) or [])[:3]
    ]

    text = (
        f"{status_emoji} *{funnel.name}* — *{status}*\n"
        f"{summary.get('one_line_summary', '')}\n\n"
        f"*Date:* {date_label}\n"
        f"*Time Range:* {time_range}\n\n"
        f"*Alerts*\n" + "\n".join(alert_lines) + "\n\n"
        f"*Current event values*\n" + "\n".join(step_lines) + "\n\n"
        f"*Funnel ratios*\n" + "\n".join(ratio_lines) + "\n\n"
        f"*Main insight*\n{summary.get('main_insight', '')}\n\n"
        f"*What to check now*\n" + "\n".join(action_lines)
    )
    return text


def send_google_chat_message(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
    summary: dict[str, Any],
    settings: Settings,
) -> bool:
    if not settings.google_chat_webhook_url:
        return False

    text = build_google_chat_text(funnel, health_report, summary, settings)

    response = requests.post(
        settings.google_chat_webhook_url,
        json={"text": text},
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(f"Google Chat send failed: {response.status_code} {response.text}")

    response.raise_for_status()
    return True