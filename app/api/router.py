from fastapi import APIRouter

from app.api.routes.funnels import router as funnels_router
from app.api.routes.health import router as health_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(funnels_router, prefix="/funnels", tags=["funnels"])