from __future__ import annotations

from functools import lru_cache

from pymongo import MongoClient
from pymongo.database import Database

from app.configs.settings import get_settings


@lru_cache
def get_mongo_client() -> MongoClient:
    settings = get_settings()
    return MongoClient(
        settings.mongodb_uri,
        serverSelectionTimeoutMS=settings.mongodb_server_selection_timeout_ms,
        appname=settings.app_name,
    )


def get_database() -> Database:
    settings = get_settings()
    return get_mongo_client()[settings.mongodb_db_name]


def ping_mongo() -> None:
    get_mongo_client().admin.command("ping")


def close_mongo_connection() -> None:
    try:
        client = get_mongo_client()
    except Exception:
        return
    client.close()
    get_mongo_client.cache_clear()