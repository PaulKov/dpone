from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "mindbox_live_smoke.py"
SPEC = importlib.util.spec_from_file_location("mindbox_live_smoke_tool", TOOL_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_args(**overrides):
    base: dict[str, object] = {
        "selector": [],
        "resource": None,
        "manifest": str(ROOT / "examples" / "batch" / "landing_mindbox_api.batch.yaml"),
        "vault_path": None,
        "since_datetime_utc": None,
        "till_datetime_utc": None,
        "date_from": None,
        "date_to": None,
        "days_back": None,
        "poll_interval": None,
        "export_timeout": None,
        "batch_size": None,
        "utc_boundary_time": None,
        "mode": "connector",
        "project_root": str(ROOT),
        "format": "json",
        "dry_run": True,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_read_settings_uses_env_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_IT_MB_SELECTORS", "app.getactions,app.getmailings")
    monkeypatch.setenv("DPONE_IT_MB_DAYS_BACK", "5")
    settings = MODULE.read_settings(_make_args())
    assert settings.selectors == ("app.getactions", "app.getmailings")
    assert settings.resource == "getactions"
    assert settings.days_back == 5
    assert settings.export_timeout == 3000


def test_read_settings_defaults_to_one_hour_yesterday_noon_utc(monkeypatch) -> None:
    monkeypatch.delenv("DPONE_IT_MB_DAYS_BACK", raising=False)
    monkeypatch.delenv("DPONE_IT_MB_SINCE_DATETIME_UTC", raising=False)
    monkeypatch.delenv("DPONE_IT_MB_TILL_DATETIME_UTC", raising=False)
    monkeypatch.delenv("DPONE_IT_MB_DATE_FROM", raising=False)
    monkeypatch.delenv("DPONE_IT_MB_DATE_TO", raising=False)
    settings = MODULE.read_settings(_make_args())
    assert settings.days_back == 1
    assert settings.since_datetime_utc is not None
    assert settings.till_datetime_utc is not None
    assert settings.since_datetime_utc.endswith("T12:00:00Z")
    assert settings.till_datetime_utc.endswith("T13:00:00Z")


def test_read_settings_prefers_explicit_datetime_window(monkeypatch) -> None:
    monkeypatch.delenv("DPONE_IT_MB_DAYS_BACK", raising=False)
    monkeypatch.setenv("DPONE_IT_MB_SINCE_DATETIME_UTC", "2026-03-10T09:00:00Z")
    monkeypatch.setenv("DPONE_IT_MB_TILL_DATETIME_UTC", "2026-03-10T10:00:00Z")
    settings = MODULE.read_settings(_make_args())
    assert settings.since_datetime_utc == "2026-03-10T09:00:00Z"
    assert settings.till_datetime_utc == "2026-03-10T10:00:00Z"


def test_patch_manifest_for_live_run_filters_tables_and_sets_window() -> None:
    settings = MODULE.MindboxSmokeSettings(
        vault_path="api/mindbox",
        resource="getactions",
        selectors=("app.getactions",),
        manifest=str(ROOT / "examples" / "batch" / "landing_mindbox_api.batch.yaml"),
        since_datetime_utc=None,
        till_datetime_utc=None,
        date_from="2026-03-01",
        date_to="2026-03-03",
        days_back=1,
        poll_interval=15,
        export_timeout=600,
        batch_size=777,
        utc_boundary_time="21:00:00",
    )
    temp_manifest, selectors, effective_days_back = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_mindbox_api.batch.yaml",
        settings,
    )
    assert selectors == ("app.getactions",)
    assert effective_days_back == 3
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    options = payload["defaults"]["source"]["options"]
    assert options["poll_interval"] == 15
    assert options["export_timeout"] == 600
    assert options["batch_size"] == 777
    tables = payload["schemas"]["app"]["tables"]
    assert len(tables) == 1
    assert tables[0]["table"] == "getactions"
    assert tables[0]["overrides"]["source"]["options"]["lookback_days"] == 3


def test_patch_manifest_for_live_run_uses_exact_datetime_window_when_provided() -> None:
    settings = MODULE.MindboxSmokeSettings(
        vault_path="api/mindbox",
        resource="getmessagingreport",
        selectors=("app.getmessagingreport",),
        manifest=str(ROOT / "examples" / "batch" / "landing_mindbox_api.batch.yaml"),
        since_datetime_utc="2026-03-10T09:00:00Z",
        till_datetime_utc="2026-03-10T10:00:00Z",
        date_from=None,
        date_to=None,
        days_back=1,
        poll_interval=15,
        export_timeout=600,
        batch_size=777,
        utc_boundary_time="21:00:00",
    )
    temp_manifest, selectors, effective_days_back = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_mindbox_api.batch.yaml",
        settings,
    )
    assert selectors == ("app.getmessagingreport",)
    assert effective_days_back == 1
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    table = payload["schemas"]["app"]["tables"][0]
    options = table["overrides"]["source"]["options"]
    assert table["table"] == "getmessagingreport"
    assert options["since_datetime_utc"] == "2026-03-10T09:00:00Z"
    assert options["till_datetime_utc"] == "2026-03-10T10:00:00Z"
    assert "full_refresh_days" not in options
