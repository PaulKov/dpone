from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.sources.api.mindbox import MindboxSource
from dpone.runtime.sources.strategies.api.mindbox import (
    MindboxFullExtractStrategy,
    MindboxIncrementalMergeExtractStrategy,
    MindboxReplaceExtractStrategy,
)


class DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[Any, ...]] = []

    def info(self, *args, **kwargs):
        self.events.append(("info", args, kwargs))

    def warning(self, *args, **kwargs):
        self.events.append(("warning", args, kwargs))

    def log_etl_progress(self, event, payload):
        self.events.append((event, payload))


class DummyConnector:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get_resources(self, resource_type, filters=None, **kwargs):
        filters = dict(filters or {})
        self.calls.append({"resource": resource_type, "filters": filters})
        if resource_type == "getmessagingreport":
            yield {"messageId": "1", "sentAt": "2026-03-04 10:00:00"}
            return
        if resource_type == "getactions":
            yield {"creationDateTimeUtc": "2026-03-04 10:00:00", "operationId": 1}
            return
        yield {"ids_mindboxId": 123, "createdAt": "2026-03-04T10:00:00Z", "email": "a@example.com"}

    def health_check(self):
        return True


class DummySinkConnector:
    pass


def _load_config(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__mindbox",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table=options.get("resource", "getclients"),
        target_schema="landing__mindbox__api",
        target_table=f"app__{options.get('resource', 'getclients')}",
        load_strategy=options.pop("load_strategy", LoadStrategy.INCREMENTAL_MERGE),
        options=dict(options),
    )


def _artifact_rows(result: Any) -> list[dict[str, Any]]:
    return list(cast(Any, result.artifact)._iterator)


def test_mindbox_incremental_merge_strategy_builds_lookback_window(monkeypatch) -> None:
    monkeypatch.setattr(
        MindboxIncrementalMergeExtractStrategy,
        "_mindbox_today",
        staticmethod(lambda utc_boundary_time="21:00:00": date(2026, 3, 5)),
    )
    connector = DummyConnector()
    strategy = MindboxIncrementalMergeExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(resource="getclients", lookback_days=2, unique_key=["ids_mindboxId"])
    result = strategy.extract(load_config, None)
    rows = _artifact_rows(result)
    assert len(rows) == 1
    assert rows[0]["ids_mindboxId"] == 123
    assert connector.calls[0]["filters"]["since"] == date(2026, 3, 2)
    assert connector.calls[0]["filters"]["till"] == date(2026, 3, 4)


def test_mindbox_full_refresh_strategy_uses_force_full_refresh(monkeypatch) -> None:
    monkeypatch.setattr(
        MindboxFullExtractStrategy,
        "_mindbox_today",
        staticmethod(lambda utc_boundary_time="21:00:00": date(2026, 3, 5)),
    )
    connector = DummyConnector()
    strategy = MindboxFullExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        load_strategy=LoadStrategy.FULL_REFRESH,
        resource="getmessagingreport",
        full_refresh_days=30,
    )
    result = strategy.extract(load_config, None)
    rows = _artifact_rows(result)
    assert result.force_full_refresh is True
    assert rows[0]["messageId"] == "1"
    assert connector.calls[0]["resource"] == "getmessagingreport"


def test_mindbox_replace_strategy_sets_custom_predicate(monkeypatch) -> None:
    monkeypatch.setattr(
        MindboxReplaceExtractStrategy,
        "_mindbox_today",
        staticmethod(lambda utc_boundary_time="21:00:00": date(2026, 3, 5)),
    )
    connector = DummyConnector()
    strategy = MindboxReplaceExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(load_strategy=LoadStrategy.REPLACE, resource="getactions", lookback_days=2)
    result = strategy.extract(load_config, None)
    rows = _artifact_rows(result)
    assert load_config.custom_predicate is not None
    assert rows[0]["operationId"] == 1
    assert "creationDateTimeUtc >= TIMESTAMP '2026-03-02 21:00:00'" in load_config.custom_predicate
    assert "< TIMESTAMP '2026-03-04 21:00:00'" in load_config.custom_predicate


def test_mindbox_incremental_merge_strategy_prefers_explicit_datetime_window(monkeypatch) -> None:
    monkeypatch.setattr(
        MindboxIncrementalMergeExtractStrategy,
        "_mindbox_today",
        staticmethod(lambda utc_boundary_time="21:00:00": date(2026, 3, 5)),
    )
    connector = DummyConnector()
    strategy = MindboxIncrementalMergeExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        resource="getclients",
        lookback_days=30,
        since_datetime_utc="2026-03-04T09:00:00Z",
        till_datetime_utc="2026-03-04T10:00:00Z",
    )
    strategy.extract(load_config, None)
    assert connector.calls[0]["filters"]["since"] == datetime(2026, 3, 4, 9, 0)
    assert connector.calls[0]["filters"]["till"] == datetime(2026, 3, 4, 10, 0)


def test_mindbox_full_refresh_strategy_supports_exact_datetime_window() -> None:
    connector = DummyConnector()
    strategy = MindboxFullExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        load_strategy=LoadStrategy.FULL_REFRESH,
        resource="getmessagingreport",
        since_datetime_utc="2026-03-04T18:00:00+00:00",
        till_datetime_utc="2026-03-04T19:00:00+00:00",
    )
    strategy.extract(load_config, None)
    assert connector.calls[0]["filters"]["since"] == datetime(2026, 3, 4, 18, 0)
    assert connector.calls[0]["filters"]["till"] == datetime(2026, 3, 4, 19, 0)


def test_mindbox_source_maps_health_check_and_extract() -> None:
    source = MindboxSource(connector=DummyConnector(), sink_connector=DummySinkConnector(), logger=DummyLogger())
    assert source.health_check() is True
    result = source.extract(_load_config(resource="getclients"), None)
    rows = _artifact_rows(result)
    assert rows[0]["email"] == "a@example.com"


def test_runtime_bootstrap_builds_mindbox_source(monkeypatch) -> None:
    captured = {}

    class FakeConnector(DummyConnector):
        @classmethod
        def from_vault(cls, vault_path, vault_manager=None, **kwargs):
            captured["vault_path"] = vault_path
            captured["kwargs"] = kwargs
            return cls()

    monkeypatch.setattr("dpone.runtime.connectors.api.mindbox.MindboxConnector", FakeConnector)
    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "mindbox",
            "options": {
                "rate_limit_delay": 0.5,
                "max_retries": 4,
                "timeout": 120,
            },
        },
        vault_path="api/mindbox",
        sink_connector=None,
    )
    assert isinstance(source, MindboxSource)
    assert captured["vault_path"] == "api/mindbox"
    assert captured["kwargs"]["timeout"] == 120


def test_mindbox_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_mindbox_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)
    assert manifest is not None
    assert len(manifest.processes) == 6
    tables = {proc.config.load_config.target_table for proc in manifest.processes}
    assert "app__getclients" in tables
    assert "app__operationslogs" in tables
