from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "cbr_public_smoke.py"
SPEC = importlib.util.spec_from_file_location("cbr_public_smoke_tool", TOOL_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_args(**overrides):
    base = {
        "selector": [],
        "resource": None,
        "manifest": str(ROOT / "examples" / "batch" / "landing_cbr_api.batch.yaml"),
        "day": None,
        "date_from": None,
        "date_to": None,
        "days_back": None,
        "timeout": None,
        "retries": None,
        "retry_delay": None,
        "mode": "connector",
        "project_root": str(ROOT),
        "format": "json",
        "dry_run": True,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_read_settings_uses_env_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_IT_CBR_SELECTORS", "app.xml_daily_asp")
    monkeypatch.setenv("DPONE_IT_CBR_DAYS_BACK", "4")
    settings = MODULE.read_settings(_make_args())
    assert settings.selectors == ("app.xml_daily_asp",)
    assert settings.days_back == 4
    assert settings.resource == MODULE.DEFAULT_RESOURCE


def test_patch_manifest_for_live_run_filters_tables_and_sets_request() -> None:
    settings = MODULE.CbrSmokeSettings(
        resource="xml_daily_asp",
        selectors=("app.xml_daily_asp",),
        manifest=str(ROOT / "examples" / "batch" / "landing_cbr_api.batch.yaml"),
        day=None,
        date_from="2026-03-01",
        date_to="2026-03-02",
        days_back=3,
        timeout=15,
        retries=2,
        retry_delay=1.0,
    )
    temp_manifest, selectors, request = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_cbr_api.batch.yaml",
        settings,
    )
    assert selectors == ("app.xml_daily_asp",)
    assert request == {"start_date": "2026-03-01", "end_date": "2026-03-02"}
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    options = payload["defaults"]["source"]["options"]
    assert options["resource"] == "xml_daily_asp"
    assert options["start_date"] == "2026-03-01"
    assert options["end_date"] == "2026-03-02"
