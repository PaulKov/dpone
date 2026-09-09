from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import yaml

from dpone.runtime.connectors.api.appsflyer import AppsflyerQuotaExceededError

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "appsflyer_live_smoke.py"
SPEC = importlib.util.spec_from_file_location("appsflyer_live_smoke_tool", TOOL_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_args(**overrides):
    base = {
        "app_id": [],
        "selector": [],
        "date_from": None,
        "date_to": None,
        "days_back": None,
        "resource": None,
        "manifest": str(ROOT / "examples" / "batch" / "landing_appsflyer_api.batch.yaml"),
        "vault_path": None,
        "timezone": None,
        "maximum_rows": None,
        "mode": "connector",
        "project_root": str(ROOT),
        "format": "json",
        "dry_run": True,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_read_settings_uses_env_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_IT_AF_APP_IDS", "com.example.travel,id1234567890")
    monkeypatch.setenv("DPONE_IT_AF_SELECTORS", "app.installs_report,app.in_app_events_report")
    monkeypatch.setenv("DPONE_IT_AF_DAYS_BACK", "3")
    settings = MODULE.read_settings(_make_args())
    assert settings.app_ids == ("com.example.travel", "id1234567890")
    assert settings.selectors == ("app.installs_report", "app.in_app_events_report")
    assert settings.maximum_rows == MODULE.DEFAULT_MAXIMUM_ROWS


def test_patch_manifest_for_live_run_filters_tables_and_sets_window(tmp_path: Path) -> None:
    settings = MODULE.AppsflyerSmokeSettings(
        vault_path="api/appsflyer",
        app_ids=("com.example.travel", "id1234567890"),
        resource="installs_report",
        selectors=("app.installs_report",),
        manifest=str(ROOT / "examples" / "batch" / "landing_appsflyer_api.batch.yaml"),
        timezone="Europe/Moscow",
        date_from="2026-03-01",
        date_to="2026-03-02",
        maximum_rows=777,
    )
    temp_manifest, selectors = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_appsflyer_api.batch.yaml",
        settings,
    )
    assert selectors == ("app.installs_report",)
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    options = payload["defaults"]["source"]["options"]
    assert options["app_ids"] == ["com.example.travel", "id1234567890"]
    assert options["date_from"] == "2026-03-01"
    assert options["date_to"] == "2026-03-02"
    assert options["maximum_rows"] == 777
    tables = payload["schemas"]["app"]["tables"]
    assert tables == ["installs_report"]


def test_run_connector_smoke_returns_green_skip_payload_on_quota(monkeypatch) -> None:
    class DummyConnector:
        @classmethod
        def from_vault(cls, **kwargs):
            del kwargs
            return cls()

        def fetch_resource_rows(self, **kwargs):
            del kwargs
            raise AppsflyerQuotaExceededError(
                "AppsFlyer daily install-report quota is exhausted for the current app/token."
            )

    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.AppsflyerConnector", DummyConnector)
    settings = MODULE.AppsflyerSmokeSettings(
        vault_path="api/appsflyer",
        app_ids=("com.example.travel",),
        resource="installs_report",
        selectors=("app.installs_report",),
        manifest=str(ROOT / "examples" / "batch" / "landing_appsflyer_api.batch.yaml"),
        timezone="Europe/Moscow",
        date_from="2026-03-01",
        date_to="2026-03-01",
        maximum_rows=1000,
    )

    payload = MODULE.run_connector_smoke(settings)

    assert payload["skipped"] is True
    assert "quota" in payload["skip_reason"].lower()
