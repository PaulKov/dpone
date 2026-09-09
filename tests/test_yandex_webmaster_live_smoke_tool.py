from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "yandex_webmaster_live_smoke.py"
SPEC = importlib.util.spec_from_file_location("yandex_webmaster_live_smoke_tool", TOOL_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_args(**overrides):
    base = {
        "selector": [],
        "resource": None,
        "manifest": str(ROOT / "examples" / "batch" / "landing_yandex_webmaster_api.batch.yaml"),
        "vault_path": None,
        "user_id": None,
        "host_id": None,
        "host_url": None,
        "device_types": None,
        "region_ids": None,
        "day": None,
        "date_from": None,
        "date_to": None,
        "days_back": None,
        "timeout": None,
        "max_retries": None,
        "rate_limit_delay": None,
        "pages_in_search_daily_agg": None,
        "mode": "connector",
        "project_root": str(ROOT),
        "format": "json",
        "dry_run": True,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_read_settings_uses_env_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_IT_YANDEX_WEBMASTER_HOST_URL", "https://travel.example.com")
    monkeypatch.setenv("DPONE_IT_YANDEX_WEBMASTER_SELECTORS", "app.host_metrics_daily")
    settings = MODULE.read_settings(_make_args())

    assert settings.host_url == "https://travel.example.com"
    assert settings.selectors == ("app.host_metrics_daily",)
    assert settings.days_back == MODULE.DEFAULT_DAYS_BACK


def test_read_settings_uses_canonical_default_host_id_when_env_is_missing(monkeypatch) -> None:
    monkeypatch.delenv("DPONE_IT_YANDEX_WEBMASTER_HOST_ID", raising=False)
    monkeypatch.delenv("DPONE_IT_YANDEX_WEBMASTER_HOST_URL", raising=False)
    settings = MODULE.read_settings(_make_args())
    assert settings.host_id == MODULE.DEFAULT_HOST_ID


def test_patch_manifest_for_live_run_sets_window_and_filters_tables() -> None:
    settings = MODULE.YandexWebmasterSmokeSettings(
        vault_path="api/yandex_webmaster",
        resource="host_metrics_daily",
        selectors=("app.host_metrics_daily",),
        manifest=str(ROOT / "examples" / "batch" / "landing_yandex_webmaster_api.batch.yaml"),
        user_id=42,
        host_id=None,
        host_url="https://travel.example.com",
        device_types=(),
        region_ids=(),
        day="2026-03-10",
        date_from=None,
        date_to=None,
        days_back=3,
        timeout=44,
        max_retries=2,
        rate_limit_delay=0.8,
        pages_in_search_daily_agg="max",
    )

    temp_manifest, selectors, manifest_options = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_yandex_webmaster_api.batch.yaml",
        settings,
    )

    assert selectors == ("app.host_metrics_daily",)
    assert manifest_options["day"] == "2026-03-10"
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    assert payload["defaults"]["source"]["vault_path"] == "api/yandex_webmaster"
    options = payload["defaults"]["source"]["options"]
    assert options["host_url"] == "https://travel.example.com"
    assert options["day"] == "2026-03-10"
    assert options["pages_in_search_daily_agg"] == "max"
    table_entry = payload["schemas"]["app"]["tables"][0]
    assert table_entry["table"] == "host_metrics_daily"
    assert table_entry["overrides"]["source"]["options"]["resource"] == "host_metrics_daily"


def test_run_connector_smoke_uses_canonical_row_builder(monkeypatch) -> None:
    class DummyConnector:
        @classmethod
        def from_vault(cls, **kwargs):
            del kwargs
            return cls()

    def fake_build_rows(**kwargs):
        assert kwargs["host_url"] == "https://travel.example.com"
        return [{"date": "2026-03-10", "pages_in_search": 7}]

    monkeypatch.setattr("dpone.runtime.connectors.api.yandex_webmaster.YandexWebmasterConnector", DummyConnector)
    monkeypatch.setattr(
        "dpone.runtime.sources.strategies.api.yandex_webmaster.common.build_yandex_webmaster_rows",
        fake_build_rows,
    )
    settings = MODULE.YandexWebmasterSmokeSettings(
        vault_path="api/yandex_webmaster",
        resource="host_metrics_daily",
        selectors=("app.host_metrics_daily",),
        manifest=str(ROOT / "examples" / "batch" / "landing_yandex_webmaster_api.batch.yaml"),
        user_id=None,
        host_id=None,
        host_url="https://travel.example.com",
        device_types=(),
        region_ids=(),
        day="2026-03-10",
        date_from=None,
        date_to=None,
        days_back=3,
        timeout=60,
        max_retries=1,
        rate_limit_delay=0.5,
        pages_in_search_daily_agg="last",
    )

    payload = MODULE.run_connector_smoke(settings)

    assert payload["row_count"] == 1
    assert "date" in payload["sample_keys"]
