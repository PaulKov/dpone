from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "google_sheets_live_smoke.py"
SPEC = importlib.util.spec_from_file_location("google_sheets_live_smoke_tool", TOOL_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_args(**overrides):
    base = {
        "selector": [],
        "resource": None,
        "manifest": str(ROOT / "examples" / "batch" / "landing_google_sheets_api.batch.yaml"),
        "vault_path": None,
        "spreadsheet_id": None,
        "spreadsheet_url": None,
        "worksheet_title": None,
        "worksheet_index": None,
        "range_name": None,
        "header_row": None,
        "skip_rows": None,
        "add_metadata_columns": None,
        "timeout": None,
        "max_retries": None,
        "rate_limit_delay": None,
        "mode": "connector",
        "project_root": str(ROOT),
        "format": "json",
        "dry_run": True,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_read_settings_uses_env_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_IT_GS_SPREADSHEET_URL", "https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0")
    monkeypatch.setenv("DPONE_IT_GS_SELECTORS", "app.worksheet_rows")
    settings = MODULE.read_settings(_make_args())

    assert settings.spreadsheet_url == "https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0"
    assert settings.selectors == ("app.worksheet_rows",)
    assert settings.timeout == MODULE.DEFAULT_TIMEOUT


def test_patch_manifest_for_live_run_sets_sheet_options_and_filters_tables() -> None:
    settings = MODULE.GoogleSheetsSmokeSettings(
        vault_path="api/google_sheets",
        resource="worksheet_rows",
        selectors=("app.worksheet_rows",),
        manifest=str(ROOT / "examples" / "batch" / "landing_google_sheets_api.batch.yaml"),
        spreadsheet_id="sheet-123",
        spreadsheet_url=None,
        worksheet_title="Leads",
        worksheet_index=None,
        range_name="A1:C10",
        header_row=2,
        skip_rows=1,
        add_metadata_columns=False,
        timeout=44,
        max_retries=2,
        rate_limit_delay=0.6,
    )

    temp_manifest, selectors = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_google_sheets_api.batch.yaml",
        settings,
    )

    assert selectors == ("app.worksheet_rows",)
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    assert payload["defaults"]["source"]["vault_path"] == "api/google_sheets"
    options = payload["defaults"]["source"]["options"]
    assert options["spreadsheet_id"] == "sheet-123"
    assert "spreadsheet_url" not in options
    assert options["worksheet_title"] == "Leads"
    assert options["range_name"] == "A1:C10"
    assert options["header_row"] == 2
    assert options["skip_rows"] == 1
    assert options["add_metadata_columns"] is False
    assert payload["schemas"]["app"]["tables"] == ["worksheet_rows"]


def test_run_connector_smoke_uses_get_records(monkeypatch) -> None:
    class DummyConnector:
        credentials = type("Credentials", (), {"auth_type": "service_account"})()

        @classmethod
        def from_vault(cls, **kwargs):
            del kwargs
            return cls()

        def get_records(self, **kwargs):
            assert kwargs["spreadsheet_id"] == "sheet-123"
            return [{"order_id": 1, "_meta_spreadsheet_id": "sheet-123"}]

    monkeypatch.setattr("dpone.runtime.connectors.api.google_sheets.GoogleSheetsConnector", DummyConnector)
    settings = MODULE.GoogleSheetsSmokeSettings(
        vault_path="api/google_sheets",
        resource="worksheet_rows",
        selectors=("app.worksheet_rows",),
        manifest=str(ROOT / "examples" / "batch" / "landing_google_sheets_api.batch.yaml"),
        spreadsheet_id="sheet-123",
        spreadsheet_url=None,
        worksheet_title=None,
        worksheet_index=None,
        range_name=None,
        header_row=1,
        skip_rows=0,
        add_metadata_columns=True,
        timeout=60,
        max_retries=1,
        rate_limit_delay=0.2,
    )

    payload = MODULE.run_connector_smoke(settings)

    assert payload["auth_type"] == "service_account"
    assert payload["row_count"] == 1
