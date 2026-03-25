from __future__ import annotations

import json
from pathlib import Path

from app.funnels.models import EmailConfig, FunnelConfig, StepConfig

FUNNELS_DIR = Path(__file__).resolve().parent


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def list_funnels() -> list[FunnelConfig]:
    funnels: list[FunnelConfig] = []

    for child in FUNNELS_DIR.iterdir():
        if not child.is_dir():
            continue

        funnel_json = child / "funnel.json"
        if not funnel_json.exists():
            continue

        raw = _load_json(funnel_json)
        email_raw = raw.get("email", {})
        steps_raw = raw.get("steps", [])

        funnels.append(
            FunnelConfig(
                funnel_id=raw["funnel_id"],
                name=raw.get("name", raw["funnel_id"]),
                enabled=bool(raw.get("enabled", True)),
                funnel_dir=child,
                sources=raw.get("sources", {}),
                steps=[
                    StepConfig(
                        name=step["name"],
                        source=step["source"],
                        source_key=step["source_key"],
                    )
                    for step in steps_raw
                ],
                health_config=raw.get("health_config", {}),
                email=EmailConfig(
                    subject_prefix=email_raw.get("subject_prefix", raw.get("name", raw["funnel_id"])),
                    subscribers=email_raw.get("subscribers", []),
                    send_always_summary=bool(email_raw.get("send_always_summary", True)),
                ),
            )
        )

    return sorted(funnels, key=lambda f: f.funnel_id)


def get_funnel(funnel_id: str) -> FunnelConfig:
    for funnel in list_funnels():
        if funnel.funnel_id == funnel_id:
            return funnel
    raise ValueError(f"Unknown funnel_id: {funnel_id}")