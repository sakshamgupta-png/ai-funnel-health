from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EmailConfig:
    subject_prefix: str
    subscribers: list[str]
    notify_on: str  # alert_only | always | never


@dataclass(frozen=True)
class StepConfig:
    name: str
    source: str
    source_key: str


@dataclass(frozen=True)
class FunnelConfig:
    funnel_id: str
    name: str
    enabled: bool
    sources: dict[str, Any]
    steps: list[StepConfig]
    health_config: dict[str, Any]
    email: EmailConfig

    def source(self, source_name: str) -> dict[str, Any]:
        return self.sources.get(source_name, {})

    def steps_for_source(self, source_name: str) -> list[StepConfig]:
        return [step for step in self.steps if step.source == source_name]

    def step_names(self) -> list[str]:
        return [step.name for step in self.steps]