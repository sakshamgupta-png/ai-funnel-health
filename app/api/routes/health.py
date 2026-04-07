from fastapi import APIRouter

from app.configs.settings import get_settings

router = APIRouter()


@router.get("/health")
def health() -> dict:
    settings = get_settings()
    return {
        "ok": True,
        "app_name": settings.app_name,
        "scheduler_enabled": settings.enable_scheduler,
        "timezone": settings.app_timezone,
        "notification_channel": settings.notification_channel,
        "mongodb_db_name": settings.mongodb_db_name,
    }