from __future__ import annotations

from datetime import datetime, timedelta
from html import escape
from typing import Any

import requests

from app.configs.settings import Settings
from app.funnels.models import FunnelConfig


def build_subject(funnel: FunnelConfig, health_report: dict[str, Any]) -> str:
    status = (health_report.get("overall_status") or "healthy").upper()
    date_label, time_range = format_bucket_range(health_report.get("latest_complete_hour"))
    return f"[{status}] {funnel.email.subject_prefix} — {date_label} | {time_range}"


def format_bucket_range(hour_str: str | None) -> tuple[str, str]:
    if not hour_str:
        return "Unknown date", "Unknown time"

    dt = datetime.fromisoformat(hour_str)
    end_dt = dt + timedelta(hours=1)

    date_label = dt.strftime("%d %b %Y")
    time_range = f"{_format_hour(dt)} to {_format_hour(end_dt)}"
    return date_label, time_range


def _format_hour(dt: datetime) -> str:
    return dt.strftime("%I %p").lstrip("0")


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
    return {
        event.get("event_name", f"event_{idx}"): event
        for idx, event in enumerate(health_report.get("events", []))
    }


def _ratio_map(health_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for ratio in health_report.get("ratios", []):
        key = f"{ratio.get('from_event')}->{ratio.get('to_event')}"
        result[key] = ratio
    return result


def _emoji_for_step(step_name: str) -> str:
    name = step_name.lower()
    if "session" in name or "user" in name or "visit" in name:
        return "🚀"
    if "goal" in name or "page" in name:
        return "🎯"
    if "order" in name or "transaction" in name or "checkout" in name:
        return "🛒"
    if "paid" in name or "payment" in name or "success" in name:
        return "💳"
    return "📌"


def _verdict_colors(verdict: str) -> tuple[str, str]:
    v = verdict.lower()
    if v == "critical":
        return "#FEE2E2", "#B91C1C"
    if v == "watch":
        return "#FEF3C7", "#B45309"
    return "#DCFCE7", "#166534"


def _display_change_from_drop_pct(drop_pct: Any) -> tuple[str, str]:
    if drop_pct is None:
        return "—", "#6B7280"

    try:
        value = float(drop_pct)
    except Exception:
        return str(drop_pct), "#6B7280"

    display_change = -value

    if display_change > 0:
        return f"+{display_change:.1f}%", "#166534"
    if display_change < 0:
        return f"{display_change:.1f}%", "#B91C1C"
    return "0.0%", "#6B7280"


def _build_what_happened_bullets(
    health_report: dict[str, Any],
    summary: dict[str, Any],
) -> list[str]:
    bullets: list[str] = []

    status = (health_report.get("overall_status") or "healthy").title()
    alerted_events = health_report.get("alerted_events", []) or []
    alerted_ratios = health_report.get("alerted_ratios", []) or []

    bullets.append(f"Overall verdict for this hour: {status}.")

    if alerted_events:
        bullets.append(f"Event alerts fired for: {', '.join(alerted_events)}.")
    else:
        bullets.append("No event-count alert fired in this hour.")

    if alerted_ratios:
        bullets.append(f"Step-conversion alerts fired for: {', '.join(alerted_ratios)}.")
    else:
        bullets.append("No step-conversion alert fired in this hour.")

    what_happened = summary.get("what_happened")
    if what_happened:
        bullets.append(str(what_happened).strip())

    return bullets[:4]


def _render_alert_chips(health_report: dict[str, Any]) -> str:
    chips = []

    for item in health_report.get("alerted_events", []) or []:
        chips.append(
            f"""<span style="display:inline-block;margin:4px 6px 0 0;padding:6px 10px;border-radius:999px;
            background:#FEE2E2;color:#B91C1C;font-size:12px;font-weight:600;">⚠️ {escape(item)}</span>"""
        )

    for item in health_report.get("alerted_ratios", []) or []:
        chips.append(
            f"""<span style="display:inline-block;margin:4px 6px 0 0;padding:6px 10px;border-radius:999px;
            background:#FEF3C7;color:#B45309;font-size:12px;font-weight:600;">📉 {escape(item)}</span>"""
        )

    return "".join(chips) if chips else """
        <span style="display:inline-block;margin-top:4px;padding:6px 10px;border-radius:999px;
        background:#DCFCE7;color:#166534;font-size:12px;font-weight:600;">✅ No active alerts</span>
    """


def _render_step_highlight_cards(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
) -> str:
    events = _event_map(health_report)
    step_names = funnel.step_names()

    cards = []
    for step_name in step_names:
        event = events.get(step_name, {})
        change_text, change_color = _display_change_from_drop_pct(
            event.get("drop_pct_vs_benchmark", event.get("drop_pct_vs_mean"))
        )
        benchmark_value = event.get("benchmark_value", event.get("baseline_mean"))

        cards.append(
            f"""
            <td style="width:{100/max(1, len(step_names)):.2f}%;vertical-align:top;background:#fafafa;border:1px solid #e5e7eb;border-radius:14px;padding:16px;">
              <div style="font-size:12px;color:#6b7280;">{_emoji_for_step(step_name)} {escape(step_name)}</div>
              <div style="margin-top:8px;font-size:24px;font-weight:700;color:#111827;">{_format_number(event.get("current_value"))}</div>
              <div style="margin-top:6px;font-size:13px;color:#6b7280;">
                vs benchmark {_format_number(benchmark_value)}
              </div>
              <div style="margin-top:6px;font-size:13px;color:#6b7280;">
                Change:
                <span style="font-weight:600;color:{change_color};">{change_text}</span>
              </div>
            </td>
            """
        )
    return "".join(cards)


def _render_ratio_cards(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
) -> str:
    ratios = _ratio_map(health_report)
    cards = []

    step_names = funnel.step_names()
    for i in range(max(0, len(step_names) - 1)):
        from_event = step_names[i]
        to_event = step_names[i + 1]
        ratio = ratios.get(f"{from_event}->{to_event}", {})
        change_text, change_color = _display_change_from_drop_pct(
            ratio.get("ratio_drop_pct_vs_benchmark", ratio.get("ratio_drop_pct_vs_mean"))
        )

        benchmark_pct = ratio.get("benchmark_pct")
        if benchmark_pct is None:
            baseline_ratio = ratio.get("baseline_ratio_mean")
            benchmark_pct = (baseline_ratio or 0) * 100

        cards.append(
            f"""
            <td style="width:{100/max(1, len(step_names)-1):.2f}%;vertical-align:top;background:#fafafa;border:1px solid #e5e7eb;border-radius:14px;padding:16px;">
              <div style="font-size:12px;color:#6b7280;">📉 {escape(from_event)} → {escape(to_event)}</div>
              <div style="margin-top:8px;font-size:22px;font-weight:700;color:#111827;">
                {_format_pct((ratio.get("current_ratio") or 0) * 100, 2)}
              </div>
              <div style="margin-top:6px;font-size:13px;color:#6b7280;">
                Benchmark {_format_pct(benchmark_pct, 2)}
              </div>
              <div style="margin-top:6px;font-size:13px;color:#6b7280;">
                Change:
                <span style="font-weight:600;color:{change_color};">{change_text}</span>
              </div>
            </td>
            """
        )

    return "".join(cards)


def render_email_html(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
    summary: dict[str, Any],
) -> str:
    verdict = escape(summary.get("simple_verdict", "Healthy"))
    main_insight = escape(summary.get("main_insight", ""))
    one_line_summary = escape(summary.get("one_line_summary", ""))

    date_label, time_range = format_bucket_range(health_report.get("latest_complete_hour"))
    reasons = summary.get("likely_reasons", []) or []
    actions = summary.get("what_to_check_now", []) or []
    what_happened_bullets = _build_what_happened_bullets(health_report, summary)

    reasons_html = "".join(f"<li>{escape(str(item))}</li>" for item in reasons[:3])
    actions_html = "".join(f"<li>{escape(str(item))}</li>" for item in actions[:3])
    what_happened_html = "".join(f"<li>{escape(str(item))}</li>" for item in what_happened_bullets)

    step_cards_html = _render_step_highlight_cards(funnel, health_report)
    ratio_cards_html = _render_ratio_cards(funnel, health_report)
    alert_chips_html = _render_alert_chips(health_report)

    pill_bg, pill_fg = _verdict_colors(verdict)

    return f"""
    <html>
      <body style="margin:0;padding:0;background:#f6f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#111827;">
        <div style="max-width:760px;margin:32px auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:18px;overflow:hidden;">
          
          <div style="padding:28px 30px;border-bottom:1px solid #e5e7eb;background:#fcfcfd;">
            <div style="font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#6b7280;">
              Hourly funnel health alert
            </div>
            <h1 style="margin:10px 0 6px;font-size:26px;line-height:1.3;">
              {escape(funnel.name)}
            </h1>
            <div style="margin-top:12px;display:inline-block;padding:7px 12px;border-radius:999px;background:{pill_bg};font-size:12px;font-weight:700;color:{pill_fg};">
              {verdict}
            </div>
            <div style="margin-top:10px;font-size:16px;line-height:1.7;color:#111827;">
              {one_line_summary}
            </div>
            <div style="margin-top:14px;font-size:14px;color:#4b5563;">
              <strong>Date:</strong> {escape(date_label)}<br/>
              <strong>Time Range:</strong> {escape(time_range)}
            </div>
            <div style="margin-top:14px;">
              {alert_chips_html}
            </div>
          </div>

          <div style="padding:24px 30px;">
            <h2 style="margin:0 0 14px;font-size:16px;">✨ Quick highlights</h2>

            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:separate;border-spacing:12px 0;">
              <tr>
                {step_cards_html}
              </tr>
            </table>

            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:12px;border-collapse:separate;border-spacing:12px 0;">
              <tr>
                {ratio_cards_html}
              </tr>
            </table>

            <h2 style="margin:28px 0 10px;font-size:16px;">🧾 What happened</h2>
            <ul style="margin:0 0 22px 18px;padding:0;font-size:15px;line-height:1.8;color:#111827;">
              {what_happened_html}
            </ul>

            <h2 style="margin:0 0 10px;font-size:16px;">🔍 Main insight</h2>
            <div style="margin:0 0 22px;padding:14px 16px;background:#F9FAFB;border:1px solid #E5E7EB;border-radius:12px;font-size:15px;line-height:1.8;color:#111827;">
              {main_insight}
            </div>

            <h2 style="margin:0 0 10px;font-size:16px;">🤔 Likely reasons</h2>
            <ul style="margin:0 0 22px 18px;padding:0;font-size:15px;line-height:1.8;color:#111827;">
              {reasons_html}
            </ul>

            <h2 style="margin:0 0 10px;font-size:16px;">✅ What to check now</h2>
            <ul style="margin:0 0 22px 18px;padding:0;font-size:15px;line-height:1.8;color:#111827;">
              {actions_html}
            </ul>
          </div>
        </div>
      </body>
    </html>
    """.strip()


def render_email_text(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
    summary: dict[str, Any],
) -> str:
    date_label, time_range = format_bucket_range(health_report.get("latest_complete_hour"))
    bullets = _build_what_happened_bullets(health_report, summary)

    lines = [
        funnel.name,
        summary.get("one_line_summary", ""),
        f"Date: {date_label}",
        f"Time Range: {time_range}",
        f"Verdict: {summary.get('simple_verdict', '')}",
        "",
        "What happened:",
    ]

    for item in bullets:
        lines.append(f"- {item}")

    lines.extend([
        "",
        "Main insight:",
        str(summary.get("main_insight", "")),
        "",
        "Likely reasons:",
    ])

    for item in summary.get("likely_reasons", []) or []:
        lines.append(f"- {item}")

    lines.extend(["", "What to check now:"])
    for item in summary.get("what_to_check_now", []) or []:
        lines.append(f"- {item}")

    return "\n".join(lines)


def maybe_send_email(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
    summary: dict[str, Any],
    settings: Settings,
) -> bool:
    notify_on = (funnel.email.notify_on or "alert_only").lower()

    if notify_on == "never":
        return False

    has_alert = bool(health_report.get("alerted_events")) or bool(health_report.get("alerted_ratios"))
    if notify_on == "alert_only" and not has_alert:
        return False

    if not funnel.email.subscribers:
        return False

    if not settings.mailgun_api_key or not settings.mailgun_domain or not settings.mailgun_from_email:
        return False

    if not isinstance(summary, dict):
        raise ValueError(f"Expected summary to be dict, got {type(summary).__name__}")

    subject = build_subject(funnel, health_report)
    html = render_email_html(funnel, health_report, summary)
    text = render_email_text(funnel, health_report, summary)

    data = {
        "from": settings.mailgun_from_email,
        "to": funnel.email.subscribers,
        "subject": subject,
        "text": text,
        "html": html,
    }

    if settings.mailgun_test_mode:
        data["o:testmode"] = "yes"

    response = requests.post(
        f"https://api.mailgun.net/v3/{settings.mailgun_domain}/messages",
        auth=("api", settings.mailgun_api_key),
        data=data,
        timeout=60,
    )

    if not response.ok:
        raise RuntimeError(f"Mailgun send failed: {response.status_code} {response.text}")

    response.raise_for_status()
    return True