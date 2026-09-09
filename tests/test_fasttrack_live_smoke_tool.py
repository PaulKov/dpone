from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "fasttrack_live_smoke.py"
SPEC = importlib.util.spec_from_file_location("fasttrack_live_smoke_tool", TOOL_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_args(**overrides):
    base = {
        "selector": [],
        "resource": None,
        "manifest": str(ROOT / "examples" / "batch" / "landing_fasttrack_api.batch.yaml"),
        "vault_path": None,
        "dashboard_uuid": None,
        "category": None,
        "limit": None,
        "offset": None,
        "page_size": None,
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
    monkeypatch.setenv("DPONE_IT_FT_SELECTORS", "default.flex_cms_ratings,default.chat_sessions")
    monkeypatch.setenv("DPONE_IT_FT_MAX_RETRIES", "5")
    settings = MODULE.read_settings(_make_args())
    assert settings.selectors == ("default.flex_cms_ratings", "default.chat_sessions")
    assert settings.resource == "flex_cms_ratings"
    assert settings.max_retries == 5


def test_patch_manifest_for_live_run_filters_tables_and_sets_options() -> None:
    settings = MODULE.FasttrackSmokeSettings(
        vault_path="api/fasttrack",
        resource="flex_cms_ratings",
        selectors=("default.flex_cms_ratings",),
        manifest=str(ROOT / "examples" / "batch" / "landing_fasttrack_api.batch.yaml"),
        timeout=33,
        max_retries=7,
        rate_limit_delay=0.5,
        dashboard_uuid="dashboard-uuid",
        category="cat-1",
        limit=11,
        offset=12,
        page_size=250,
    )
    temp_manifest, selectors, filters = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_fasttrack_api.batch.yaml",
        settings,
    )
    assert selectors == ("default.flex_cms_ratings",)
    assert filters == {
        "uuid": "dashboard-uuid",
        "category": "cat-1",
        "limit": 11,
        "offset": 12,
        "page_size": 250,
    }
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    options = payload["defaults"]["source"]["options"]
    assert options["resource"] == "flex_cms_ratings"
    assert options["timeout"] == 33
    assert options["max_retries"] == 7
    assert options["category"] == "cat-1"
    tables = payload["schemas"]["default"]["tables"]
    assert len(tables) == 1
    assert tables[0]["table"] == "flex_cms_ratings"
