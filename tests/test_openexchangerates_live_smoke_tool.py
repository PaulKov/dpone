from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "openexchangerates_live_smoke.py"
SPEC = importlib.util.spec_from_file_location("openexchangerates_live_smoke_tool", TOOL_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_args(**overrides):
    base = {
        "selector": [],
        "symbol": [],
        "resource": None,
        "manifest": str(ROOT / "examples" / "batch" / "landing_openexchangerates_api.batch.yaml"),
        "vault_path": None,
        "day": None,
        "date_from": None,
        "date_to": None,
        "days_back": None,
        "timeout": None,
        "max_retries": None,
        "retry_delay": None,
        "rate_limit_delay": None,
        "mode": "connector",
        "project_root": str(ROOT),
        "format": "json",
        "dry_run": True,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_read_settings_uses_env_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_IT_OXR_SELECTORS", "default.historical_rates_daily")
    monkeypatch.setenv("DPONE_IT_OXR_SYMBOLS", "ARS,RUB,EUR")
    settings = MODULE.read_settings(_make_args())
    assert settings.selectors == ("default.historical_rates_daily",)
    assert settings.symbols == ("ARS", "RUB", "EUR")
    assert settings.days_back == 1


def test_patch_manifest_for_live_run_filters_tables_and_sets_options() -> None:
    settings = MODULE.OpenExchangeRatesSmokeSettings(
        vault_path="api/openexchangerates",
        resource="historical_rates_daily",
        selectors=("default.historical_rates_daily",),
        manifest=str(ROOT / "examples" / "batch" / "landing_openexchangerates_api.batch.yaml"),
        symbols=("ARS", "RUB"),
        day=None,
        date_from="2026-03-29",
        date_to="2026-03-30",
        days_back=1,
        timeout=44,
        max_retries=2,
        retry_delay=0.7,
        rate_limit_delay=1.3,
    )
    temp_manifest, selectors, details = MODULE._patch_manifest_for_live_run(
        ROOT / "examples" / "batch" / "landing_openexchangerates_api.batch.yaml",
        settings,
    )

    assert selectors == ("default.historical_rates_daily",)
    payload = yaml.safe_load(temp_manifest.read_text(encoding="utf-8"))
    options = payload["defaults"]["source"]["options"]
    assert options["symbols"] == ["ARS", "RUB"]
    assert options["start_date"] == "2026-03-29"
    assert options["end_date"] == "2026-03-30"
    assert details["symbols"] == ["ARS", "RUB"]
    tables = payload["schemas"]["default"]["tables"]
    assert len(tables) == 1


def test_run_connector_smoke_uses_vault_connector(monkeypatch) -> None:
    class DummyConnector:
        @classmethod
        def from_vault(cls, **kwargs):
            del kwargs
            return cls()

        def health_check(self):
            return True

        def get_resources(self, resource, filters):
            assert resource == "historical_rates_daily"
            assert filters["symbols"] == ("ARS", "RUB", "EUR")
            return iter(
                [
                    {
                        "as_of_date": "2026-03-30",
                        "base_currency": "USD",
                        "symbol": "ARS",
                        "rate": 123.45,
                    }
                ]
            )

    monkeypatch.setattr("dpone.runtime.connectors.api.openexchangerates.OpenExchangeRatesConnector", DummyConnector)
    settings = MODULE.OpenExchangeRatesSmokeSettings(
        vault_path="api/openexchangerates",
        resource="historical_rates_daily",
        selectors=("default.historical_rates_daily",),
        manifest=str(ROOT / "examples" / "batch" / "landing_openexchangerates_api.batch.yaml"),
        symbols=("ARS", "RUB", "EUR"),
        day="2026-03-30",
        date_from=None,
        date_to=None,
        days_back=1,
        timeout=60,
        max_retries=1,
        retry_delay=1.0,
        rate_limit_delay=1.0,
    )
    payload = MODULE.run_connector_smoke(settings)
    assert payload["health_check"] is True
    assert payload["row_count"] == 1
    assert payload["symbols"] == ["ARS", "RUB", "EUR"]
