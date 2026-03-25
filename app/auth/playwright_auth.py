from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


def save_manual_login_state(
    login_url: str,
    auth_state_path: str | Path,
    headless: bool = False,
) -> Path:
    """
    Opens WebEngage. You manually enter email/password/OTP.
    After login is complete, press Enter in terminal to save auth state.
    """
    auth_state_path = Path(auth_state_path)
    auth_state_path.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.goto(login_url, wait_until="domcontentloaded")

        print("\nComplete login manually in the opened browser.")
        input("After login + OTP is complete and dashboard is visible, press Enter to save auth state... ")

        context.storage_state(path=str(auth_state_path))
        browser.close()

    return auth_state_path


def build_cookie_header_from_auth_state(
    auth_state_path: str | Path,
    base_url: str,
) -> str:
    auth_state_path = Path(auth_state_path)
    if not auth_state_path.exists():
        raise FileNotFoundError(f"Auth state not found: {auth_state_path}")

    with auth_state_path.open("r", encoding="utf-8") as f:
        state = json.load(f)

    hostname = urlparse(base_url).hostname or ""
    cookie_parts: list[str] = []

    for cookie in state.get("cookies", []):
        cookie_domain = cookie.get("domain", "").lstrip(".")
        if hostname.endswith(cookie_domain) or cookie_domain.endswith(hostname):
            cookie_parts.append(f"{cookie['name']}={cookie['value']}")

    if not cookie_parts:
        raise RuntimeError(
            f"No matching cookies found in auth state for host: {hostname}. "
            "Refresh auth_state.json by logging in again."
        )

    return "; ".join(cookie_parts)