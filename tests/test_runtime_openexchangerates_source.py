from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.sources.api.openexchangerates import OpenExchangeRatesSource
from dpone.runtime.sources.strategies.api.openexchangerates import (
    OpenExchangeRatesFullExtractStrategy,
    OpenExchangeRatesIncrementalMergeExtractStrategy,
)


class DummyLogger:
    def __init__(self) -> None:
        self.events = []

    def info(self, *args, **kwargs):
        self.events.append(("info", args, kwargs))

    def warning(self, *args, **kwargs):
        self.events.append(("warning", args, kwargs))

    def log_etl_progress(self, event, payload):
        self.events.append((event, payload))


class DummyConnector:
    def __init__(self) -> None:
        self.calls = []

    def get_resources(self, resource_type, filters=None, **kwargs):
        params = dict(filters or {})
        params.update(kwargs)
        self.calls.append((resource_type, params))
        rows = []
        current = params["start_date"]
        end_date = params["end_date"]
        while current <= end_date:
            for symbol in ("ARS", "RUB", "EUR"):
                rows.append(
                    {
                        "as_of_date": current.isoformat(),
                        "base_currency": "USD",
                        "symbol": symbol,
                        "rate": 1.0,
                        "provider_timestamp_utc": None,
                        "disclaimer": "terms",
                        "license": "license",
                    }
                )
            current = current.fromordinal(current.toordinal() + 1)
        return iter(rows)

    def health_check(self):
        return True


class DummySinkConnector:
    def __init__(self, max_value=None) -> None:
        self.max_value = max_value

    def get_max_column_value(self, schema, table, column):
        assert column == "as_of_date"
        return self.max_value


def _load_config(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__openexchangerates",
        target_conn_id="bigquery-dwh",
        source_schema="default",
        source_table=options.get("resource", "historical_rates_daily"),
        target_schema="landing__openexchangerates__api",
        target_table="default__historical_rates_daily",
        load_strategy=options.pop("load_strategy", LoadStrategy.INCREMENTAL_MERGE),
        options=dict(options),
        unique_key=["as_of_date", "symbol"],
    )


def test_openexchangerates_full_extract_strategy_supports_explicit_range() -> None:
    connector = DummyConnector()
    strategy = OpenExchangeRatesFullExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    result = strategy.extract(
        _load_config(
            load_strategy=LoadStrategy.FULL_REFRESH,
            resource="historical_rates_daily",
            start_date="2026-03-29",
            end_date="2026-03-30",
            symbols="ARS,RUB,EUR",
        ),
        None,
    )
    assert result.force_full_refresh is True
    assert len(result.artifact._rows) == 6
    assert connector.calls[0][1]["symbols"] == ("ARS", "RUB", "EUR")


def test_openexchangerates_incremental_merge_state_uses_sink_max() -> None:
    strategy = OpenExchangeRatesIncrementalMergeExtractStrategy(
        connector=DummyConnector(),
        sink_connector=DummySinkConnector(max_value="2026-03-29"),
        logger=DummyLogger(),
    )
    state = strategy.get_state(_load_config(resource="historical_rates_daily"))
    assert state == {"last_value": "2026-03-29"}


def test_openexchangerates_source_maps_health_check_and_extract() -> None:
    source = OpenExchangeRatesSource(
        connector=DummyConnector(),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    assert source.health_check() is True
    result = source.extract(
        _load_config(resource="historical_rates_daily", start_date="2026-03-30", end_date="2026-03-30"),
        None,
    )
    assert len(result.artifact._rows) == 3


def test_runtime_bootstrap_builds_openexchangerates_source_from_vault(monkeypatch) -> None:
    captured = {}

    class DummyRuntimeConnector:
        @classmethod
        def from_vault(cls, vault_path: str, **kwargs):
            captured["vault_path"] = vault_path
            captured["kwargs"] = kwargs
            return DummyConnector()

    monkeypatch.setattr(
        "dpone.runtime.connectors.api.openexchangerates.OpenExchangeRatesConnector", DummyRuntimeConnector
    )
    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "openexchangerates",
            "vault_path": "api/openexchangerates",
            "options": {
                "resource": "historical_rates_daily",
                "timeout": 15,
                "max_retries": 4,
                "retry_delay": 0.3,
                "rate_limit_delay": 0.7,
            },
        },
        vault_path="api/openexchangerates",
        sink_connector=None,
    )
    assert isinstance(source, OpenExchangeRatesSource)
    assert captured["vault_path"] == "api/openexchangerates"
    assert captured["kwargs"]["timeout"] == 15
    assert captured["kwargs"]["max_retries"] == 4
    assert captured["kwargs"]["retry_delay"] == 0.3
    assert captured["kwargs"]["rate_limit_delay"] == 0.7


def test_openexchangerates_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_openexchangerates_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)
    assert manifest is not None
    assert len(manifest.processes) == 1
    proc = manifest.processes[0].config.load_config
    assert proc.source_conn_id == "api__openexchangerates"
    assert proc.source_schema == "default"
    assert proc.source_table == "historical_rates_daily"
    assert proc.target_schema == "landing__openexchangerates__api"
    assert proc.target_table == "default__historical_rates_daily"
