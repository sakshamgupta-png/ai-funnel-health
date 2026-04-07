from app.repositories.funnel_repo import ensure_indexes as ensure_funnel_indexes
from app.repositories.funnel_run_repo import ensure_indexes as ensure_funnel_run_indexes


def ensure_all_indexes() -> None:
    ensure_funnel_indexes()
    ensure_funnel_run_indexes()