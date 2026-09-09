from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.nightly]


def test_google_sheets_live_connector_builds_from_vault(google_sheets_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    pytest.importorskip("google.auth")
    from dpone.runtime.connectors.api.google_sheets import GoogleSheetsConnector

    connector = GoogleSheetsConnector.from_vault(
        vault_path=google_sheets_live_settings.vault_path,
        timeout=google_sheets_live_settings.timeout,
        max_retries=google_sheets_live_settings.max_retries,
        rate_limit_delay=google_sheets_live_settings.rate_limit_delay,
    )

    assert connector.credentials.auth_type == "service_account"
    assert (
        connector.health_check(
            spreadsheet_id=google_sheets_live_settings.spreadsheet_id,
            spreadsheet_url=google_sheets_live_settings.spreadsheet_url,
        )
        is True
    )


def test_google_sheets_live_fetch_worksheet_rows(google_sheets_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    pytest.importorskip("google.auth")
    from dpone.runtime.connectors.api.google_sheets import GoogleSheetsConnector

    connector = GoogleSheetsConnector.from_vault(
        vault_path=google_sheets_live_settings.vault_path,
        timeout=google_sheets_live_settings.timeout,
        max_retries=google_sheets_live_settings.max_retries,
        rate_limit_delay=google_sheets_live_settings.rate_limit_delay,
    )
    rows = connector.get_records(
        spreadsheet_id=google_sheets_live_settings.spreadsheet_id,
        spreadsheet_url=google_sheets_live_settings.spreadsheet_url,
        worksheet_title=google_sheets_live_settings.worksheet_title,
        worksheet_index=google_sheets_live_settings.worksheet_index,
        range_name=google_sheets_live_settings.range_name,
        header_row=google_sheets_live_settings.header_row,
        skip_rows=google_sheets_live_settings.skip_rows,
        add_metadata_columns=google_sheets_live_settings.add_metadata_columns,
    )

    assert isinstance(rows, list)
    if rows:
        first = rows[0]
        assert isinstance(first, dict)
        assert first
        if google_sheets_live_settings.add_metadata_columns:
            assert "_meta_spreadsheet_id" in first
