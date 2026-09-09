from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "similarweb_live_smoke.py"
SPEC = importlib.util.spec_from_file_location("similarweb_live_smoke_tool", TOOL_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_args(**overrides):
    base = {
        "domain": [],
        "selector": [],
        "resource": None,
        "manifest": str(ROOT / "examples" / "batch" / "landing_similarweb_api.batch.yaml"),
        "vault_path": None,
        "snapshot_month": None,
        "limit": None,
        "page_size": None,
        "min_keywords_count": None,
        "traffic_source": None,
        "web_source": None,
        "branded_type": None,
        "country": None,
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
    monkeypatch.setenv("DPONE_IT_SW_DOMAINS", "travel.example.com,travel-alt.example.com")
    monkeypatch.setenv("DPONE_IT_SW_SELECTORS", "default.keywords")
    monkeypatch.setenv("DPONE_IT_SW_SNAPSHOT_MONTH", "2026-02")
    settings = MODULE.read_settings(_make_args())
    assert settings.domains == ("travel.example.com", "travel-alt.example.com")
    assert settings.selectors == ("default.keywords",)
    assert settings.snapshot_month == "2026-02-01"
    assert settings.limit == MODULE.DEFAULT_LIMIT


def test_read_settings_uses_canonical_default_domain_when_env_is_missing(monkeypatch) -> None:
    monkeypatch.delenv("DPONE_IT_SW_DOMAINS", raising=False)
    settings = MODULE.read_settings(_make_args())
    assert settings.domains == MODULE.DEFAULT_DOMAINS


def test_patch_manifest_for_live_run_filters_tables_and_sets_options() -> None:
    settings = MODULE.SimilarwebSmokeSettings(
        vault_path="api/similarweb",
        domains=("travel.example.com",),
        resource="keywords",
        selectors=("default.keywords",),
        manifest=str(ROOT / "examples" / "batch" / "landing_similarweb_api.batch.yaml"),
        snapshot_month="2026-02-01",
        limit=77,
        page_size=55,
        min_keywords_count=3,
        traffic_source="Organic",
        web_source="Total",
        branded_type="All",
        country="world",
        timeout=44,
        max_retries=2,
        rate_limit_delay=1.3,
    )
    temp_manifest, selectors, manifest_options = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_similarweb_api.batch.yaml",
        settings,
    )

    assert selectors == ("default.keywords",)
    assert manifest_options["snapshot_month"] == "2026-02-01"
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    options = payload["defaults"]["source"]["options"]
    assert options["domains"] == ["travel.example.com"]
    assert options["limit"] == 77
    assert options["page_size"] == 55
    assert options["min_keywords_count"] == 3
    tables = payload["schemas"]["default"]["tables"]
    assert tables == ["keywords"]


def test_run_connector_smoke_uses_default_domain(monkeypatch) -> None:
    class DummyConnector:
        @classmethod
        def from_vault(cls, **kwargs):
            del kwargs
            return cls()

        def fetch_resource_rows(self, **kwargs):
            assert kwargs["url"] == "travel.example.com"
            return [{"keyword": "example_travel paris", "top_url": "https://travel.example.com/paris"}]

    monkeypatch.setattr("dpone.runtime.connectors.api.similarweb.SimilarwebConnector", DummyConnector)
    settings = MODULE.SimilarwebSmokeSettings(
        vault_path="api/similarweb",
        domains=("travel.example.com", "travel-alt.example.com"),
        resource="keywords",
        selectors=("default.keywords",),
        manifest=str(ROOT / "examples" / "batch" / "landing_similarweb_api.batch.yaml"),
        snapshot_month="2026-02-01",
        limit=5,
        page_size=5,
        min_keywords_count=1,
        traffic_source="Organic",
        web_source="Total",
        branded_type="All",
        country="world",
        timeout=60,
        max_retries=1,
        rate_limit_delay=1.0,
    )

    payload = MODULE.run_connector_smoke(settings)

    assert payload["domain"] == "travel.example.com"
    assert payload["row_count"] == 1
