from app.auth.playwright_auth import save_manual_login_state
from app.configs.settings import get_settings


def main() -> None:
    settings = get_settings()

    save_manual_login_state(
        login_url="https://dashboard.webengage.com/user/account.html?action=viewLogin",
        auth_state_path=settings.webengage_auth_state,
        headless=False,
    )

    print(f"\nSaved auth state to: {settings.webengage_auth_state}")


if __name__ == "__main__":
    main()