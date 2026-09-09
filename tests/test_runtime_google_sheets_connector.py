from __future__ import annotations

from dpone.runtime.connectors.api.google_sheets import GoogleSheetsConnector, GoogleSheetsCredentials
from dpone.runtime.connectors.api.google_sheets_resources import google_sheets_table_name


class DummyVaultManager:
    def __init__(self, secret: dict[str, object]) -> None:
        self.secret = secret
        self.calls: list[tuple[str, str]] = []

    def get_secret(self, *, mount_point: str, path: str):
        self.calls.append((mount_point, path))
        return dict(self.secret)


def test_google_sheets_credentials_from_dict_supports_service_account_variants() -> None:
    creds = GoogleSheetsCredentials.from_dict(
        {
            "endpoint": "https://sheets.googleapis.com",
            "service_account_json": '{"project_id":"proj","private_key":"line1\\\\nline2","client_email":"bot@example.com"}',
        }
    )

    assert creds.auth_type == "service_account"
    assert creds.project_id == "proj"
    assert creds.client_email == "bot@example.com"
    assert "line1\nline2" == creds.private_key

    top_level = GoogleSheetsCredentials.from_dict(
        {
            "project_id": "proj2",
            "private_key": "pk1\\npk2",
            "client_email": "bot2@example.com",
        }
    )
    assert top_level.project_id == "proj2"
    assert top_level.private_key == "pk1\npk2"


def test_google_sheets_credentials_from_dict_supports_oauth_refresh_and_published_csv() -> None:
    oauth = GoogleSheetsCredentials.from_dict(
        {
            "auth_type": "oauth_refresh_token",
            "client_id": "cid",
            "client_secret": "secret",
            "refresh_token": "refresh",
        }
    )
    assert oauth.auth_type == "oauth_refresh_token"
    assert oauth.client_id == "cid"
    assert oauth.refresh_token == "refresh"

    published = GoogleSheetsCredentials.from_dict({"auth_type": "published_csv"})
    assert published.auth_type == "published_csv"
    assert published.endpoint == "https://sheets.googleapis.com"


def test_google_sheets_credentials_from_vault_reads_secret() -> None:
    manager = DummyVaultManager(
        {
            "auth_type": "oauth_refresh_token",
            "client_id": "cid",
            "client_secret": "secret",
            "refresh_token": "refresh",
        }
    )

    creds = GoogleSheetsCredentials.from_vault("api/google_sheets", vault_manager=manager, mount_point="dev")

    assert creds.auth_type == "oauth_refresh_token"
    assert manager.calls == [("dev", "api/google_sheets")]


def test_google_sheets_connector_helpers_normalize_headers_and_spreadsheet_id() -> None:
    connector = GoogleSheetsConnector(
        credentials=GoogleSheetsCredentials(
            auth_type="published_csv",
            endpoint="https://sheets.googleapis.com",
        )
    )

    assert (
        connector._resolve_spreadsheet_id(  # noqa: SLF001
            spreadsheet_id=None,
            spreadsheet_url="https://docs.google.com/spreadsheets/d/abc-123/edit#gid=0",
        )
        == "abc-123"
    )
    assert connector._normalize_headers([" Order ID ", "Order ID", "", "123 test"]) == [  # noqa: SLF001
        "order_id",
        "order_id_2",
        "column_3",
        "column_4_123_test",
    ]
    assert google_sheets_table_name("worksheet_rows") == "app__worksheet_rows"


def test_google_sheets_connector_applies_limit_rows_and_limit_columns_like_legacy_operator() -> None:
    connector = GoogleSheetsConnector(
        credentials=GoogleSheetsCredentials(
            auth_type="published_csv",
            endpoint="https://sheets.googleapis.com",
        )
    )

    records = connector._values_to_records(  # noqa: SLF001
        raw_values=[
            ["Order ID", "City", "Country", "Unused"],
            [1, "Paris", "France", "A"],
            [2, "Berlin", "Germany", "B"],
            [3, "Madrid", "Spain", "C"],
        ],
        spreadsheet_id="sheet-1",
        spreadsheet_title="example_travel",
        worksheet_title="Cities",
        range_name=None,
        header_row=1,
        skip_rows=0,
        limit_rows=2,
        limit_columns=3,
        add_metadata_columns=False,
    )

    assert records == [
        {"order_id": 1, "city": "Paris", "country": "France"},
        {"order_id": 2, "city": "Berlin", "country": "Germany"},
    ]
