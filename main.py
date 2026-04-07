from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.router import api_router
from app.configs.settings import get_settings
from app.cron.scheduler import start_scheduler, stop_scheduler
from app.db.mongo import close_mongo_connection, ping_mongo
from app.repositories import ensure_all_indexes


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    ping_mongo()
    ensure_all_indexes()

    if settings.enable_scheduler:
        start_scheduler()

    yield

    stop_scheduler()
    close_mongo_connection()


def create_app() -> FastAPI:
    app = FastAPI(
        title="AI Funnel Health",
        version="2.0.0",
        lifespan=lifespan,
    )
    app.include_router(api_router)
    return app


app = create_app()