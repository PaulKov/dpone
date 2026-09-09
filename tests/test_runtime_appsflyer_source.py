from __future__ import annotations

from datetime import date
from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.sources.api.appsflyer import AppsflyerSource
from dpone.runtime.sources.strategies.api.appsflyer import (
    AppsflyerFullExtractStrategy,
    AppsflyerIncrementalAppendExtractStrategy,
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


class DummySinkConnector:
    def __init__(self, max_value=None) -> None:
        self.max_value = max_value

    def get_max_column_value(self, schema, table, column):
        return self.max_value


class DummyConnector:
    DEFAULT_TIMEOUT = 60

    def __init__(self) -> None:
        self.calls = []
        self.default_app_id = "app.one"

    def iter_resource_rows(self, **kwargs):
        self.calls.append(kwargs)
        app_id = kwargs["app_id"]
        resource = kwargs["resource_name"]
        start = kwargs["from_value"].isoformat()
        yield {
            "event_time": f"{start} 10:00:00",
            "media_source": "googleadwords_int",
            "source_app_id": app_id,
            "resource_name": resource,
            "date": start,
        }

    def health_check(self):
        return True

    def _parse_possible_datetime(self, value):
        from dpone.runtime.connectors.api.appsflyer import AppsflyerConnector, AppsflyerCredentials

        creds = AppsflyerCredentials(endpoint="http://example", api_key="x")
        return AppsflyerConnector(credentials=creds)._parse_possible_datetime(value)


def _load_config(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="appsflyer-api",
        target_conn_id="bigquery-dwh",
        source_schema="appsflyer",
        source_table=options.get("resource", "installs_report"),
        target_schema="landing__appsflyer__api",
        target_table="app__installs_report",
        load_strategy=options.pop("load_strategy", LoadStrategy.INCREMENTAL_APPEND),
        options=dict(options),
    )


def test_appsflyer_incremental_strategy_builds_partitions_and_state() -> None:
    connector = DummyConnector()
    strategy = AppsflyerIncrementalAppendExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(max_value="2026-03-10"),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        resource="installs_report",
        app_ids=["app.one", "app.two"],
        lookback_days=2,
        date_to="2026-03-10",
        incremental_column="date",
        batch_size=10,
    )
    state = strategy.get_state(load_config)
    assert state == {"last_value": "2026-03-10", "column": "date"} or state == {
        "last_value": state["last_value"],
        "column": "date",
    }
    result = strategy.extract(load_config, {"last_value": "2026-03-10", "column": "date"})
    rows = list(result.artifact._iterator)
    assert len(rows) == 2
    assert {row["source_app_id"] for row in rows} == {"app.one", "app.two"}
    assert result.artifact.lookback_partitions == ("2026-03-08", "2026-03-09", "2026-03-10")
    assert result.state == {"last_value": "2026-03-10", "column": "date"}
    assert connector.calls[0]["resource_name"] == "installs_report"
    assert connector.calls[0]["timezone_name"] == "Europe/Moscow"


def test_appsflyer_full_extract_strategy_uses_full_refresh_window() -> None:
    connector = DummyConnector()
    strategy = AppsflyerFullExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        load_strategy=LoadStrategy.FULL_REFRESH,
        resource="organic_installs_report",
        app_ids=["app.one"],
        date_from="2026-03-01",
        date_to="2026-03-03",
        batch_size=10,
    )
    result = strategy.extract(load_config, None)
    rows = list(result.artifact._iterator)
    assert rows[0]["resource_name"] == "organic_installs_report"
    assert connector.calls[0]["from_value"] == date(2026, 3, 1)
    assert connector.calls[0]["to_value"] == date(2026, 3, 3)


def test_appsflyer_source_maps_health_check_and_extract() -> None:
    source = AppsflyerSource(connector=DummyConnector(), sink_connector=DummySinkConnector(), logger=DummyLogger())
    assert source.health_check() is True
    load_config = _load_config(
        resource="installs_report", app_ids=["app.one"], date_from="2026-03-01", date_to="2026-03-01"
    )
    result = source.extract(load_config, None)
    rows = list(result.artifact._iterator)
    assert rows[0]["source_app_id"] == "app.one"


def test_runtime_bootstrap_builds_appsflyer_source(monkeypatch) -> None:
    captured = {}

    class FakeConnector(DummyConnector):
        @classmethod
        def from_vault(cls, vault_path, vault_manager=None, **kwargs):
            captured["vault_path"] = vault_path
            captured["kwargs"] = kwargs
            inst = cls()
            inst.default_app_id = kwargs.get("default_app_id")
            return inst

    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.AppsflyerConnector", FakeConnector)
    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "appsflyer",
            "options": {
                "app_ids": ["app.one", "app.two"],
                "rate_limit_delay": 2.0,
                "max_retries": 7,
                "timeout": 15,
            },
        },
        vault_path="api/appsflyer",
        sink_connector=None,
    )
    assert isinstance(source, AppsflyerSource)
    assert captured["vault_path"] == "api/appsflyer"
    assert captured["kwargs"]["default_app_id"] == "app.one"


def test_appsflyer_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_appsflyer_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)
    assert manifest is not None
    assert len(manifest.processes) == 9
    names = {proc.config.load_config.target_table for proc in manifest.processes}
    assert "app__daily_report" in names
    assert "app__installs_report" in names
    assert "app__organic_uninstall_events_report" in names
