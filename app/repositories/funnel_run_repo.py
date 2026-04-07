from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bson import ObjectId

from app.db.mongo import get_database
from app.funnels.utils.models import FunnelConfig

COLLECTION_NAME = "funnel_runs"


def _collection():
    return get_database()[COLLECTION_NAME]


def ensure_indexes() -> None:
    coll = _collection()
    coll.create_index([("funnel_id", 1), ("run_time", -1)])
    coll.create_index([("funnel_id", 1), ("target_hour", -1)])
    coll.create_index([("funnel_id", 1), ("status", 1)])


def _to_object_id(run_id: str) -> ObjectId:
    return ObjectId(run_id)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _base_notification_state() -> dict[str, Any]:
    return {
        "attempted": False,
        "sent": False,
        "sent_at": None,
        "error": None,
    }


def create_run(
    funnel: FunnelConfig,
    run_dt: datetime,
    force_notification: bool = False,
) -> str:
    now = _utc_now()
    doc = {
        "funnel_id": funnel.funnel_id,
        "funnel_name": funnel.name,
        "run_time": run_dt.astimezone(timezone.utc),
        "force_notification": force_notification,
        "target_hour": None,
        "status": "running",
        "error": None,
        "health_report": None,
        "summary": None,
        "notifications": {
            "mail": _base_notification_state(),
            "chat": _base_notification_state(),
        },
        "created_at": now,
        "updated_at": now,
    }
    result = _collection().insert_one(doc)
    return str(result.inserted_id)


def update_run_results(
    run_id: str,
    *,
    target_hour: str | None = None,
    status: str | None = None,
    error: str | None = None,
    health_report: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
) -> None:
    set_fields: dict[str, Any] = {
        "updated_at": _utc_now(),
    }

    if target_hour is not None:
        set_fields["target_hour"] = target_hour
    if status is not None:
        set_fields["status"] = status
    if error is not None:
        set_fields["error"] = error
    if health_report is not None:
        set_fields["health_report"] = health_report
    if summary is not None:
        set_fields["summary"] = summary

    _collection().update_one(
        {"_id": _to_object_id(run_id)},
        {"$set": set_fields},
    )


def mark_notification_result(
    run_id: str,
    channel: str,
    *,
    attempted: bool,
    sent: bool,
    error: str | None = None,
) -> None:
    if channel not in {"mail", "chat"}:
        raise ValueError(f"Unsupported channel: {channel}")

    _collection().update_one(
        {"_id": _to_object_id(run_id)},
        {
            "$set": {
                f"notifications.{channel}.attempted": attempted,
                f"notifications.{channel}.sent": sent,
                f"notifications.{channel}.sent_at": _utc_now() if sent else None,
                f"notifications.{channel}.error": error,
                "updated_at": _utc_now(),
            }
        },
    )


def should_send_notification_for_hour(
    funnel_id: str,
    target_hour: str,
    channel: str,
    force_notification: bool = False,
) -> bool:
    if force_notification:
        return True

    if channel not in {"mail", "chat"}:
        raise ValueError(f"Unsupported channel: {channel}")

    existing = _collection().find_one(
        {
            "funnel_id": funnel_id,
            "target_hour": target_hour,
            f"notifications.{channel}.sent": True,
        },
        projection={"_id": 1},
    )
    return existing is None


def _get_latest_doc_with_field(funnel_id: str, field_name: str) -> dict[str, Any] | None:
    return _collection().find_one(
        {
            "funnel_id": funnel_id,
            field_name: {"$ne": None},
        },
        sort=[("run_time", -1)],
    )


def get_latest_health_report(funnel_id: str) -> dict[str, Any]:
    doc = _get_latest_doc_with_field(funnel_id, "health_report")
    if not doc or not doc.get("health_report"):
        raise FileNotFoundError("health report not found")
    return doc["health_report"]


def get_latest_summary(funnel_id: str) -> dict[str, Any]:
    doc = _get_latest_doc_with_field(funnel_id, "summary")
    if not doc or not doc.get("summary"):
        raise FileNotFoundError("summary not found")
    return doc["summary"]


def _get_latest_sent_notification_doc(funnel_id: str, channel: str) -> dict[str, Any] | None:
    return _collection().find_one(
        {
            "funnel_id": funnel_id,
            f"notifications.{channel}.sent": True,
        },
        sort=[("run_time", -1)],
    )


def get_run_state(funnel_id: str) -> dict[str, Any]:
    mail_doc = _get_latest_sent_notification_doc(funnel_id, "mail")
    chat_doc = _get_latest_sent_notification_doc(funnel_id, "chat")

    state: dict[str, Any] = {"notifications": {}}

    if mail_doc:
        state["notifications"]["mail"] = {
            "last_sent_for_hour": mail_doc.get("target_hour"),
            "last_sent_at": (
                mail_doc.get("notifications", {})
                .get("mail", {})
                .get("sent_at")
                .isoformat()
                if mail_doc.get("notifications", {}).get("mail", {}).get("sent_at")
                else None
            ),
        }

    if chat_doc:
        state["notifications"]["chat"] = {
            "last_sent_for_hour": chat_doc.get("target_hour"),
            "last_sent_at": (
                chat_doc.get("notifications", {})
                .get("chat", {})
                .get("sent_at")
                .isoformat()
                if mail_doc is not None and chat_doc.get("notifications", {}).get("chat", {}).get("sent_at")
                else (
                    chat_doc.get("notifications", {})
                    .get("chat", {})
                    .get("sent_at")
                    .isoformat()
                    if chat_doc.get("notifications", {}).get("chat", {}).get("sent_at")
                    else None
                )
            ),
        }

    return state