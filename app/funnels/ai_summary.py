from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from app.configs.settings import Settings
from app.funnels.models import FunnelConfig


OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
GEMINI_GENERATE_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"


def _save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _extract_openai_output_text(response_json: dict[str, Any]) -> str:
    if isinstance(response_json.get("output_text"), str) and response_json["output_text"].strip():
        return response_json["output_text"]

    texts: list[str] = []
    for item in response_json.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text" and content.get("text"):
                texts.append(content["text"])
    return "\n".join(texts).strip()


def _extract_gemini_output_text(response_json: dict[str, Any]) -> str:
    texts: list[str] = []
    for candidate in response_json.get("candidates", []):
        content = candidate.get("content", {})
        for part in content.get("parts", []):
            text = part.get("text")
            if text:
                texts.append(text)
    return "\n".join(texts).strip()


def _extract_json_block(text: str) -> dict[str, Any]:
    cleaned = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1:
        parsed = json.loads(cleaned)
    else:
        parsed = json.loads(cleaned[start:end + 1])

    if not isinstance(parsed, dict):
        raise ValueError(f"Expected JSON object from model, got {type(parsed).__name__}")
    return parsed


def _fallback_summary(health_report: dict[str, Any]) -> dict[str, Any]:
    status = health_report.get("overall_status", "healthy")
    hour = health_report.get("latest_complete_hour")
    alerted_events = health_report.get("alerted_events", [])
    alerted_ratios = health_report.get("alerted_ratios", [])

    if status == "healthy":
        return {
            "simple_verdict": "Healthy",
            "what_happened": f"For {hour}, no configured event or funnel-step alert fired.",
            "main_insight": "Traffic and funnel movement looked within expected range.",
            "likely_reasons": [],
            "what_to_check_now": [
                "No urgent action needed",
                "Continue monitoring the next few hours",
                "Review only if business context suggests an external issue",
            ],
            "one_line_summary": "Healthy hour: no configured funnel alert fired.",
        }

    reasons = []
    if alerted_ratios:
        reasons.append("Traffic was okay, but conversion between steps weakened.")
    if alerted_events:
        reasons.append("One or more event counts dropped beyond configured thresholds.")

    return {
        "simple_verdict": status.title(),
        "what_happened": f"For {hour}, alerts fired for: {', '.join(alerted_events + alerted_ratios)}.",
        "main_insight": "This looks like a funnel-performance issue worth checking.",
        "likely_reasons": reasons[:3],
        "what_to_check_now": [
            "Check traffic source mix",
            "Check page/app experience for the affected step",
            "Segment the affected step by channel, device, geo, or experiment",
        ],
        "one_line_summary": f"{status.title()} hour: alerts fired for {', '.join(alerted_events + alerted_ratios)}.",
    }


def _build_compact_payload(health_report: dict[str, Any]) -> dict[str, Any]:
    return {
        "analyzed_previous_hour": health_report.get("latest_complete_hour"),
        "overall_status": health_report.get("overall_status"),
        "alerted_events": health_report.get("alerted_events", []),
        "alerted_ratios": health_report.get("alerted_ratios", []),
        "events": health_report.get("events", []),
        "ratios": health_report.get("ratios", []),
    }


def _developer_prompt() -> str:
    return (
        "You are a business analyst writing for non-technical stakeholders. "
        "Return ONLY valid JSON with exactly these keys: "
        "simple_verdict, what_happened, main_insight, likely_reasons, what_to_check_now, one_line_summary. "
        "likely_reasons must be a JSON array with max 3 strings. "
        "what_to_check_now must be a JSON array with exactly 3 strings. "
        "Keep the language simple. Avoid jargon."
    )


def _user_prompt(compact_payload: dict[str, Any]) -> str:
    return (
        "Analyze this previous-hour funnel-health payload.\n"
        "If ratio alerts fired but event counts did not drop, clearly say: traffic was okay, but conversion got weaker.\n"
        f"Payload:\n{json.dumps(compact_payload, indent=2, ensure_ascii=False)}"
    )


def _generate_with_openai(compact_payload: dict[str, Any], settings: Settings) -> dict[str, Any]:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    payload = {
        "model": settings.openai_model,
        "reasoning": {"effort": "medium"},
        "text": {"verbosity": "low"},
        "input": [
            {
                "role": "developer",
                "content": [{"type": "input_text", "text": _developer_prompt()}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": _user_prompt(compact_payload)}],
            },
        ],
    }

    response = requests.post(
        OPENAI_RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )
    response.raise_for_status()
    raw_text = _extract_openai_output_text(response.json())
    return _extract_json_block(raw_text)


def _generate_with_gemini(compact_payload: dict[str, Any], settings: Settings) -> dict[str, Any]:
    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY / GOOGLE_API_KEY is missing")

    url = GEMINI_GENERATE_URL.format(model=settings.gemini_model)
    prompt_text = f"{_developer_prompt()}\n\n{_user_prompt(compact_payload)}"

    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "text": prompt_text
                    }
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json"
        }
    }

    response = requests.post(
        url,
        headers={
            "x-goog-api-key": settings.gemini_api_key,
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )
    response.raise_for_status()
    raw_text = _extract_gemini_output_text(response.json())
    return _extract_json_block(raw_text)


def generate_and_store_summary(
    funnel: FunnelConfig,
    health_report: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    compact_payload = _build_compact_payload(health_report)

    try:
        if settings.ai_provider == "gemini":
            summary = _generate_with_gemini(compact_payload, settings)
        else:
            summary = _generate_with_openai(compact_payload, settings)
    except Exception:
        summary = _fallback_summary(health_report)

    _save_json(funnel.output_file("summary.json"), summary)
    return summary