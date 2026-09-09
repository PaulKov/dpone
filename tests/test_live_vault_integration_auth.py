from __future__ import annotations

import pytest

from tests.integration.conftest import (
    ensure_vault_runtime_auth,
    read_google_sheets_live_integration_settings,
    read_mindbox_live_integration_settings,
    read_openexchangerates_live_integration_settings,
    read_similarweb_live_integration_settings,
    read_yandex_webmaster_live_integration_settings,
    require_vault_live_integration,
    vault_runtime_configured,
)


def test_vault_runtime_configured_accepts_jwt_auth_env() -> None:
    env = {
        "VAULT_ADDR": "https://vault.example.com",
        "VAULT_AUTH_METHOD": "jwt",
        "VAULT_ID_TOKEN": "gitlab-id-token",
        "VAULT_AUTH_ROLE_DEV": "dpone-viewer-dev",
    }

    assert vault_runtime_configured(env) is True


def test_ensure_vault_runtime_auth_resolves_dev_role_without_token_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = {
        "VAULT_ADDR": "https://vault.example.com",
        "VAULT_AUTH_METHOD": "jwt",
        "VAULT_ID_TOKEN": "gitlab-id-token",
        "VAULT_AUTH_ROLE_DEV": "dpone-viewer-dev",
    }
    captured: dict[str, str] = {}

    def fake_preflight(source):
        captured["role"] = source["VAULT_AUTH_ROLE"]
        return None

    monkeypatch.setattr("tests.integration.conftest.run_vault_jwt_preflight", fake_preflight)
    ensure_vault_runtime_auth(env)

    assert env["VAULT_AUTH_ROLE"] == "dpone-viewer-dev"
    assert "VAULT_TOKEN" not in env
    assert captured["role"] == "dpone-viewer-dev"


def test_ensure_vault_runtime_auth_resolves_prod_role(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        "VAULT_SERVER_URL": "https://vault.example.com",
        "VAULT_AUTH_METHOD": "jwt",
        "VAULT_ID_TOKEN": "gitlab-id-token",
        "ENV_CODE": "prod",
        "VAULT_AUTH_ROLE_PROD": "dpone-viewer-prod",
        "VAULT_AUTH_PATH": "jwt",
    }
    captured: dict[str, str] = {}

    def fake_preflight(source):
        captured["role"] = source["VAULT_AUTH_ROLE"]
        return None

    monkeypatch.setattr("tests.integration.conftest.run_vault_jwt_preflight", fake_preflight)
    ensure_vault_runtime_auth(env)

    assert env["VAULT_AUTH_ROLE"] == "dpone-viewer-prod"
    assert "VAULT_TOKEN" not in env
    assert captured["role"] == "dpone-viewer-prod"


def test_ensure_vault_runtime_auth_prefers_explicit_role(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        "VAULT_ADDR": "https://vault.example.com",
        "VAULT_AUTH_METHOD": "jwt",
        "VAULT_ID_TOKEN": "gitlab-id-token",
        "ENV_CODE": "prod",
        "VAULT_AUTH_ROLE": "dpone-explicit-role",
        "VAULT_AUTH_ROLE_PROD": "dpone-viewer-prod",
    }
    captured: dict[str, str] = {}

    def fake_preflight(source):
        captured["role"] = source["VAULT_AUTH_ROLE"]
        return None

    monkeypatch.setattr("tests.integration.conftest.run_vault_jwt_preflight", fake_preflight)
    ensure_vault_runtime_auth(env)

    assert env["VAULT_AUTH_ROLE"] == "dpone-explicit-role"
    assert "VAULT_TOKEN" not in env
    assert captured["role"] == "dpone-explicit-role"


def test_require_vault_live_integration_raises_when_jwt_auth_lacks_token_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DPONE_RUN_INTEGRATION", "1")
    monkeypatch.setenv("DPONE_RUN_INTEGRATION_LIVE", "1")
    monkeypatch.setenv("VAULT_ADDR", "https://vault.example.com")
    monkeypatch.setenv("VAULT_AUTH_METHOD", "jwt")
    monkeypatch.setenv("VAULT_AUTH_ROLE_DEV", "dpone-viewer-dev")
    monkeypatch.delenv("VAULT_TOKEN", raising=False)
    monkeypatch.delenv("VAULT_ROLE_ID", raising=False)
    monkeypatch.delenv("VAULT_SECRET_ID", raising=False)
    monkeypatch.delenv("VAULT_ID_TOKEN", raising=False)
    monkeypatch.delenv("VAULT_JWT", raising=False)
    monkeypatch.delenv("VAULT_JWT_FILE", raising=False)

    with pytest.raises(RuntimeError, match="no JWT source was found"):
        require_vault_live_integration()


def test_require_vault_live_integration_raises_with_preflight_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPONE_RUN_INTEGRATION", "1")
    monkeypatch.setenv("DPONE_RUN_INTEGRATION_LIVE", "1")
    monkeypatch.setenv("VAULT_ADDR", "https://vault.example.com")
    monkeypatch.setenv("VAULT_AUTH_METHOD", "jwt")
    monkeypatch.setenv("VAULT_ID_TOKEN", "gitlab-id-token")
    monkeypatch.setenv("VAULT_AUTH_ROLE_DEV", "dpone-viewer-dev")
    monkeypatch.delenv("VAULT_TOKEN", raising=False)
    monkeypatch.delenv("VAULT_ROLE_ID", raising=False)
    monkeypatch.delenv("VAULT_SECRET_ID", raising=False)

    def fake_preflight(source):
        del source
        raise RuntimeError("Vault JWT auth preflight failed: HTTP 403: permission denied")

    monkeypatch.setattr("tests.integration.conftest.run_vault_jwt_preflight", fake_preflight)

    with pytest.raises(RuntimeError, match="preflight failed: HTTP 403: permission denied"):
        require_vault_live_integration()


def test_require_vault_live_integration_skips_without_vault_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPONE_RUN_INTEGRATION", "1")
    monkeypatch.setenv("DPONE_RUN_INTEGRATION_LIVE", "1")
    for name in [
        "VAULT_ADDR",
        "VAULT_SERVER_URL",
        "VAULT_TOKEN",
        "VAULT_ROLE_ID",
        "VAULT_SECRET_ID",
        "VAULT_AUTH_METHOD",
        "VAULT_JWT",
        "VAULT_JWT_FILE",
        "VAULT_JWT_ENV_VAR",
        "VAULT_ID_TOKEN",
        "VAULT_AUTH_ROLE",
        "VAULT_AUTH_ROLE_DEV",
        "VAULT_AUTH_ROLE_PROD",
    ]:
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(pytest.skip.Exception, match="VAULT_ADDR or VAULT_SERVER_URL and one of:"):
        require_vault_live_integration()


def test_read_mindbox_live_integration_settings_uses_ci_safe_export_timeout_default() -> None:
    settings = read_mindbox_live_integration_settings(
        {
            "DPONE_IT_MB_VAULT_PATH": "api/mindbox",
            "DPONE_IT_MB_RESOURCE": "getactions",
        }
    )

    assert settings.export_timeout == 3000
    assert settings.days_back == 1
    assert settings.since_datetime_utc is not None
    assert settings.till_datetime_utc is not None
    assert settings.since_datetime_utc.endswith("T12:00:00Z")
    assert settings.till_datetime_utc.endswith("T13:00:00Z")


def test_read_mindbox_live_integration_settings_accepts_exact_datetime_window() -> None:
    settings = read_mindbox_live_integration_settings(
        {
            "DPONE_IT_MB_VAULT_PATH": "api/mindbox",
            "DPONE_IT_MB_RESOURCE": "getmailings",
            "DPONE_IT_MB_SINCE_DATETIME_UTC": "2026-03-10T09:00:00Z",
            "DPONE_IT_MB_TILL_DATETIME_UTC": "2026-03-10T10:00:00Z",
        }
    )

    assert settings.resource == "getmailings"
    assert settings.since_datetime_utc == "2026-03-10T09:00:00Z"
    assert settings.till_datetime_utc == "2026-03-10T10:00:00Z"


def test_read_similarweb_live_integration_settings_uses_low_live_defaults() -> None:
    settings = read_similarweb_live_integration_settings(
        {
            "DPONE_IT_SW_DOMAINS": "travel.example.com,travel-alt.example.com",
        }
    )

    assert settings.domains == ("travel.example.com", "travel-alt.example.com")
    assert settings.limit == 5
    assert settings.page_size == 5
    assert settings.min_keywords_count == 1


def test_read_google_sheets_live_integration_settings_uses_service_account_ci_defaults() -> None:
    settings = read_google_sheets_live_integration_settings(
        {
            "DPONE_IT_GS_SPREADSHEET_ID": "sheet-123",
        }
    )

    assert settings.vault_path == "api/google_sheets"
    assert settings.resource == "worksheet_rows"
    assert settings.selectors == ("app.worksheet_rows",)
    assert settings.spreadsheet_id == "sheet-123"
    assert settings.add_metadata_columns is True
    assert settings.timeout == 60
    assert settings.max_retries == 1
    assert settings.rate_limit_delay == 0.2


def test_read_google_sheets_live_integration_settings_requires_spreadsheet_target() -> None:
    with pytest.raises(ValueError, match="DPONE_IT_GS_SPREADSHEET_ID or DPONE_IT_GS_SPREADSHEET_URL"):
        read_google_sheets_live_integration_settings({})


def test_read_yandex_webmaster_live_integration_settings_uses_defaults() -> None:
    settings = read_yandex_webmaster_live_integration_settings({})

    assert settings.vault_path == "api/yandex_webmaster"
    assert settings.resource == "host_metrics_daily"
    assert settings.selectors == ("app.host_metrics_daily",)
    assert settings.host_id == "https:travel.example.com:443"
    assert settings.days_back == 3
    assert settings.pages_in_search_daily_agg == "last"
    assert settings.timeout == 60


def test_read_similarweb_live_integration_settings_uses_canonical_default_domain() -> None:
    settings = read_similarweb_live_integration_settings({})

    assert settings.domains == ("travel.example.com",)


def test_read_openexchangerates_live_integration_settings_uses_defaults() -> None:
    settings = read_openexchangerates_live_integration_settings({})

    assert settings.vault_path == "api/openexchangerates"
    assert settings.resource == "historical_rates_daily"
    assert settings.selectors == ("default.historical_rates_daily",)
    assert settings.symbols == ("ARS", "RUB", "EUR")
    assert settings.days_back == 1
    assert settings.retry_delay == 1.0
