from __future__ import annotations

from typing import Any

from app.db.mongo import get_database
from app.funnels.utils.models import EmailConfig, FunnelConfig, StepConfig

COLLECTION_NAME = "funnels"


def _collection():
    return get_database()[COLLECTION_NAME]


def ensure_indexes() -> None:
    _collection().create_index("funnel_id", unique=True)
    _collection().create_index("enabled")


def _doc_to_funnel(doc: dict[str, Any]) -> FunnelConfig:
    email_raw = doc.get("email", {}) or {}
    steps_raw = doc.get("steps", []) or []

    return FunnelConfig(
        funnel_id=doc["funnel_id"],
        name=doc.get("name", doc["funnel_id"]),
        enabled=bool(doc.get("enabled", True)),
        sources=doc.get("sources", {}) or {},
        steps=[
            StepConfig(
                name=step["name"],
                source=step["source"],
                source_key=step["source_key"],
            )
            for step in steps_raw
        ],
        health_config=doc.get("health_config", {}) or {},
        email=EmailConfig(
            subject_prefix=email_raw.get("subject_prefix", doc.get("name", doc["funnel_id"])),
            subscribers=email_raw.get("subscribers", []) or [],
            notify_on=email_raw.get("notify_on", "alert_only"),
        ),
    )


def list_funnels(enabled_only: bool | None = None) -> list[FunnelConfig]:
    query: dict[str, Any] = {}
    if enabled_only is True:
        query["enabled"] = True
    elif enabled_only is False:
        query["enabled"] = False

    docs = _collection().find(query)
    funnels = [_doc_to_funnel(doc) for doc in docs]
    return sorted(funnels, key=lambda f: f.funnel_id)


def get_funnel(funnel_id: str) -> FunnelConfig:
    doc = _collection().find_one({"funnel_id": funnel_id})
    if not doc:
        raise ValueError(f"Unknown funnel_id: {funnel_id}")
    return _doc_to_funnel(doc)


def upsert_funnel(raw_doc: dict[str, Any]) -> None:
    funnel_id = raw_doc.get("funnel_id")
    if not funnel_id:
        raise ValueError("funnel_id is required")

    _collection().update_one(
        {"funnel_id": funnel_id},
        {"$set": raw_doc},
        upsert=True,
    )