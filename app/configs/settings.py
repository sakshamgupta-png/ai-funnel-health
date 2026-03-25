from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env", override=False)


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    root_dir: Path
    app_name: str
    app_timezone: str

    enable_scheduler: bool
    scheduler_minute: int
    scheduler_timezone: str

    webengage_base_url: str
    webengage_account_id: str
    webengage_auth_state: Path
    webengage_timeout_seconds: int

    ai_provider: str
    openai_api_key: str | None
    openai_model: str
    gemini_api_key: str | None
    gemini_model: str

    mailgun_api_key: str | None
    mailgun_domain: str | None
    mailgun_from_email: str | None
    mailgun_test_mode: bool


@lru_cache
def get_settings() -> Settings:
    return Settings(
        root_dir=ROOT_DIR,
        app_name=os.getenv("APP_NAME", "AI Funnel Health"),
        app_timezone=os.getenv("APP_TIMEZONE", "Asia/Kolkata"),
        enable_scheduler=_as_bool(os.getenv("ENABLE_SCHEDULER"), True),
        scheduler_minute=int(os.getenv("SCHEDULER_MINUTE", "5")),
        scheduler_timezone=os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata"),
        webengage_base_url=os.getenv("WEBENGAGE_BASE_URL", "https://dashboard.webengage.com").rstrip("/"),
        webengage_account_id=os.getenv("WEBENGAGE_ACCOUNT_ID", "").strip(),
        webengage_auth_state=Path(
            os.getenv("WEBENGAGE_AUTH_STATE", str(ROOT_DIR / "auth_state.json"))
        ),
        webengage_timeout_seconds=int(os.getenv("WEBENGAGE_TIMEOUT_SECONDS", "90")),
        ai_provider=os.getenv("AI_PROVIDER", "openai").strip().lower(),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5.4"),
        gemini_api_key=os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"),
        gemini_model=os.getenv("GEMINI_MODEL", "gemini-3-flash-preview"),
        mailgun_api_key=os.getenv("MAILGUN_API_KEY"),
        mailgun_domain=os.getenv("MAILGUN_DOMAIN"),
        mailgun_from_email=os.getenv("MAILGUN_FROM_EMAIL"),
        mailgun_test_mode=_as_bool(os.getenv("MAILGUN_TEST_MODE"), False),
    )