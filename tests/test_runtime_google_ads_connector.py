from __future__ import annotations

from dpone.runtime.connectors.api.google_ads import GoogleAdsCredentials
from dpone.runtime.connectors.api.google_ads_resources import google_ads_table_name


class DummyVaultManager:
    def __init__(self, secret: dict[str, object]) -> None:
        self.secret = secret
        self.calls: list[tuple[str, str]] = []

    def get_secret(self, *, mount_point: str, path: str):
        self.calls.append((mount_point, path))
        return dict(self.secret)


def test_google_ads_credentials_from_dict_supports_oauth_and_service_account() -> None:
    oauth = GoogleAdsCredentials.from_dict(
        {
            "developer_token": "dev-token",
            "customer_ids": ["123-456-7890"],
            "client_id": "cid",
            "client_secret": "secret",
            "refresh_token": "refresh",
        }
    )
    assert oauth.auth_type == "oauth_refresh_token"
    assert oauth.customer_ids == ["1234567890"]
    assert oauth.client_id == "cid"

    service_account = GoogleAdsCredentials.from_dict(
        {
            "auth_type": "service_account",
            "developer_token": "dev-token",
            "customer_id": "123-456-7890",
            "credentials_json": '{"client_email":"bot@example.com"}',
        }
    )
    assert service_account.auth_type == "service_account"
    assert service_account.customer_ids == ["1234567890"]
    assert service_account.json_key == '{"client_email":"bot@example.com"}'


def test_google_ads_credentials_from_vault_reads_secret() -> None:
    manager = DummyVaultManager(
        {
            "developer_token": "dev-token",
            "customer_ids": ["1234567890"],
            "client_id": "cid",
            "client_secret": "secret",
            "refresh_token": "refresh",
        }
    )

    creds = GoogleAdsCredentials.from_vault("api/google_ads", vault_manager=manager, mount_point="prod")

    assert creds.developer_token == "dev-token"
    assert manager.calls == [("prod", "api/google_ads")]
    assert google_ads_table_name("ads_stats") == "app__ads_stats"
