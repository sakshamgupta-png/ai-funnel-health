from __future__ import annotations

from app.funnels.utils.models import FunnelConfig
from app.repositories.funnel_repo import get_funnel as repo_get_funnel
from app.repositories.funnel_repo import list_funnels as repo_list_funnels


def list_funnels() -> list[FunnelConfig]:
    return repo_list_funnels()


def get_funnel(funnel_id: str) -> FunnelConfig:
    return repo_get_funnel(funnel_id)