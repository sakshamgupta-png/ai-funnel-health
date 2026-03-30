from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _state_file(outputs_dir: Path) -> Path:
    return outputs_dir / "run_state.json"


def load_run_state(outputs_dir: Path) -> dict[str, Any]:
    path = _state_file(outputs_dir)
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_run_state(outputs_dir: Path, state: dict[str, Any]) -> None:
    path = _state_file(outputs_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def should_send_notification_for_hour(
    outputs_dir: Path,
    target_hour: str,
    channel: str,
    force_notification: bool = False,
) -> bool:
    if force_notification:
        return True

    state = load_run_state(outputs_dir)
    notifications = state.get("notifications", {})
    last_sent_hour = notifications.get(channel, {}).get("last_sent_for_hour")

    return last_sent_hour != target_hour


def mark_notification_sent(
    outputs_dir: Path,
    target_hour: str,
    channel: str,
) -> None:
    state = load_run_state(outputs_dir)
    notifications = state.setdefault("notifications", {})
    channel_state = notifications.setdefault(channel, {})

    channel_state["last_sent_for_hour"] = target_hour
    channel_state["last_sent_at"] = datetime.utcnow().isoformat() + "Z"

    save_run_state(outputs_dir, state)