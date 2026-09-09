from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.clickhouse_incremental_cursor import CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.run_context import RunContext
from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService
from dpone.runtime.sinks.mssql_transaction_requirement import MSSQL_GENERIC_TRANSACTION_CAPABILITY
from dpone.runtime.sources.clickhouse import ClickHouseSource
from dpone.runtime.sources.strategies.clickhouse.clickhouse_incremental_extract import (
    ClickHouseIncrementalExtractStrategy,
)


class NoIoConnector:
    database = "source"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_records(self, *_args, **_kwargs):
        self.calls.append("get_records")
        raise AssertionError("unsafe route reached source I/O")

    def export_incremental_with_cleanup(self, *_args, **_kwargs):
        self.calls.append("export_incremental_with_cleanup")
        raise AssertionError("unsafe route reached GCS export")


class ExplicitMssqlSink:
    dialect = "mssql"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_max_column_value(self, *_args, **_kwargs):
        self.calls.append("get_max_column_value")
        raise AssertionError("unsafe route reached target MAX I/O")


class MssqlNamedButUndeclaredSink:
    """A class name is deliberately not an authority capability."""


class Logger:
    def __getattr__(self, _name: str):
        return lambda *_args, **_kwargs: None


class ClickHouseIdentityConnector:
    database = "analytics"

    def __init__(self) -> None:
        self.calls: list[tuple[object, object, bool]] = []

    def get_records(self, query, params=None, as_dict=False):
        self.calls.append((query, params, as_dict))
        if "system.columns" in str(query):
            return [
                {"name": "id", "type": "Int32"},
                {"name": "payload", "type": "Nullable(LowCardinality(String))"},
            ]
        return [
            {
                "cluster_identifier": "3b3c72b6-a8ce-4d8a-9c6c-b3d1950bcd8e",
                "database": "analytics",
                "principal": "etl_reader",
                "server_address": "clickhouse-01",
                "server_port": 9000,
            }
        ]


def config(
    *,
    load_strategy: LoadStrategy = LoadStrategy.INCREMENTAL_APPEND,
    sink_type: str = "mssql",
    export_to_gcs: bool = False,
) -> LoadConfig:
    return LoadConfig(
        source_conn_id="clickhouse",
        target_conn_id="mssql",
        source_schema="analytics",
        source_table="events",
        target_database="DWH",
        target_schema="landing",
        target_table="events",
        load_strategy=load_strategy,
        options={
            "source_type": "clickhouse",
            "sink_type": sink_type,
            "incremental_column": "event_at",
            "export_to_gcs": export_to_gcs,
        },
    )


@pytest.mark.parametrize("entrypoint", ("get_incremental_state", "extract"))
def test_clickhouse_source_rejects_cursor_before_source_or_target_io(entrypoint: str) -> None:
    source_connector = NoIoConnector()
    sink_connector = ExplicitMssqlSink()
    source = ClickHouseSource(source_connector, Logger(), sink_connector=sink_connector)
    load_config = config()

    with pytest.raises(ValueError, match=CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE):
        if entrypoint == "get_incremental_state":
            source.get_incremental_state(load_config)
        else:
            source.extract(load_config, {"column": "event_at", "last_value": "2026-08-16 10:00:00"})

    assert source_connector.calls == []
    assert sink_connector.calls == []


def test_clickhouse_incremental_checkpoint_is_typed_unsafe_without_io() -> None:
    source_connector = NoIoConnector()
    sink_connector = ExplicitMssqlSink()
    source = ClickHouseSource(source_connector, Logger(), sink_connector=sink_connector)

    assert (
        source.mssql_transaction_checkpoint_mode(config())
        is MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE
    )
    assert source_connector.calls == []
    assert sink_connector.calls == []


def test_clickhouse_source_uses_database_issued_physical_identity() -> None:
    connector = ClickHouseIdentityConnector()
    source = ClickHouseSource(connector, Logger())

    identity = source.mssql_transaction_source_physical_identity(config(load_strategy=LoadStrategy.FULL_REFRESH))

    assert identity.to_dict() == {
        "version": 1,
        "dialect": "clickhouse",
        "cluster_identifier": "3b3c72b6-a8ce-4d8a-9c6c-b3d1950bcd8e",
        "database": "analytics",
        "effective_principal": "etl_reader",
        "session_principal": "etl_reader",
        "topology_role": "standalone",
    }
    assert connector.calls[0][1:] == ({"database": "analytics"}, True)


def test_clickhouse_source_exposes_exact_schema_preflight_projection() -> None:
    connector = ClickHouseIdentityConnector()
    source = ClickHouseSource(connector, Logger())

    fetched = source.fetch_schema_projection(config(load_strategy=LoadStrategy.FULL_REFRESH))

    assert fetched.relation_schema == (
        ("id", "Int32"),
        ("payload", "Nullable(LowCardinality(String))"),
    )
    assert fetched.projected_schema == (
        ("id", "Int32", False),
        ("payload", "Nullable(LowCardinality(String))", True),
    )
    assert tuple(column.nullable for column in fetched.relation_metadata) == (False, True)
    assert connector.calls[0][1:] == (
        {"database": "analytics", "table": "events"},
        True,
    )


def test_generic_admission_rejects_clickhouse_cursor_before_target_authority() -> None:
    source_connector = NoIoConnector()
    sink_connector = ExplicitMssqlSink()
    source = ClickHouseSource(source_connector, Logger(), sink_connector=sink_connector)
    target_calls = 0

    def target_resolver(*_args, **_kwargs):
        nonlocal target_calls
        target_calls += 1
        raise AssertionError("unsafe route reached target authority")

    sink = type(
        "Sink",
        (),
        {
            "target_dialect": lambda self: "mssql",
            "mssql_transaction_governance_capability": (lambda self: MSSQL_GENERIC_TRANSACTION_CAPABILITY),
        },
    )()

    with pytest.raises(ValueError, match=CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE):
        MssqlTransactionAdmissionService(target_resolver=target_resolver).prepare(
            config(),
            source=source,
            sink=sink,
            run_context=RunContext("run", config={"process": "events"}),
            load_record=type("Load", (), {"load_id": "load"})(),
            dag_id="dag",
        )

    assert target_calls == 0
    assert source_connector.calls == []
    assert sink_connector.calls == []


def test_key_snapshot_cannot_exempt_clickhouse_cursor_before_any_io() -> None:
    source_connector = NoIoConnector()
    sink_connector = ExplicitMssqlSink()
    source = ClickHouseSource(source_connector, Logger(), sink_connector=sink_connector)
    load_config = config()
    load_config.options["reconciliation"] = {"enabled": True, "mode": "key_snapshot"}
    target_calls = 0

    def target_resolver(*_args, **_kwargs):
        nonlocal target_calls
        target_calls += 1
        raise AssertionError("wrong-source key_snapshot reached target authority")

    sink = type(
        "Sink",
        (),
        {
            "target_dialect": lambda self: "mssql",
            "mssql_transaction_governance_capability": (lambda self: MSSQL_GENERIC_TRANSACTION_CAPABILITY),
        },
    )()

    with pytest.raises(ValueError, match=CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE):
        MssqlTransactionAdmissionService(target_resolver=target_resolver).prepare(
            load_config,
            source=source,
            sink=sink,
            run_context=RunContext("run", config={"process": "events"}),
            load_record=type("Load", (), {"load_id": "load"})(),
            dag_id="dag",
        )

    assert target_calls == 0
    assert source_connector.calls == []
    assert sink_connector.calls == []


def test_missing_cursor_column_cannot_bypass_runtime_guard() -> None:
    source_connector = NoIoConnector()
    sink_connector = ExplicitMssqlSink()
    source = ClickHouseSource(source_connector, Logger(), sink_connector=sink_connector)
    load_config = config()
    load_config.options.pop("incremental_column")

    with pytest.raises(ValueError, match=CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE):
        source.extract(load_config, None)

    assert source_connector.calls == []
    assert sink_connector.calls == []


def test_unproven_lookback_and_unique_key_do_not_enable_cursor_route() -> None:
    source_connector = NoIoConnector()
    sink_connector = ExplicitMssqlSink()
    source = ClickHouseSource(source_connector, Logger(), sink_connector=sink_connector)
    load_config = config()
    load_config.options["lookback_days"] = 7
    load_config.unique_key = ["event_id"]

    with pytest.raises(ValueError, match=CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE):
        source.get_incremental_state(load_config)

    assert source_connector.calls == []
    assert sink_connector.calls == []


@pytest.mark.parametrize("export_to_gcs", (False, True), ids=("stream", "gcs"))
@pytest.mark.parametrize(
    "last_state",
    (
        None,
        {"column": "event_at", "last_value": "2026-08-16 10:00:00.123"},
    ),
    ids=("cold-start", "retry-or-next-run"),
)
def test_direct_strategy_cannot_bypass_guard_for_any_transport(
    export_to_gcs: bool,
    last_state: dict[str, str] | None,
) -> None:
    source_connector = NoIoConnector()
    sink_connector = ExplicitMssqlSink()
    strategy = ClickHouseIncrementalExtractStrategy(
        source_connector,
        sink_connector,
        Logger(),
    )

    with pytest.raises(ValueError, match=CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE):
        strategy.extract(config(export_to_gcs=export_to_gcs), last_state)

    assert source_connector.calls == []
    assert sink_connector.calls == []


def test_runtime_uses_declared_dialect_not_misleading_class_name() -> None:
    source = ClickHouseSource(NoIoConnector(), Logger(), sink_connector=MssqlNamedButUndeclaredSink())

    strategy = source._resolve_strategy(config(sink_type="postgres"))

    assert strategy is source._incremental_extract


def test_runtime_rejects_conflicting_config_and_connector_dialect_authorities() -> None:
    source = ClickHouseSource(NoIoConnector(), Logger(), sink_connector=ExplicitMssqlSink())

    with pytest.raises(ValueError, match="sink_dialect_authority_conflict"):
        source._resolve_strategy(config(sink_type="postgres"))


@pytest.mark.parametrize(
    "load_strategy",
    (
        LoadStrategy.FULL_REFRESH,
        LoadStrategy.REPLACE,
        LoadStrategy.PARTITION_REPLACE,
        LoadStrategy.BACKFILL,
    ),
)
def test_complete_clickhouse_scans_are_typed_stateless(load_strategy: LoadStrategy) -> None:
    source = ClickHouseSource(NoIoConnector(), Logger(), sink_connector=ExplicitMssqlSink())

    assert (
        source.mssql_transaction_checkpoint_mode(config(load_strategy=load_strategy))
        is MssqlTransactionCheckpointMode.STATELESS
    )


@pytest.mark.parametrize("arrival", ("before_snapshot", "after_snapshot", "retry"))
def test_equal_timestamp_is_not_selected_by_legacy_strict_boundary(arrival: str) -> None:
    target_max = datetime(2026, 8, 16, 10, 0, 0, 123000)
    distinct_row_timestamp = datetime(2026, 8, 16, 10, 0, 0, 123000)
    available_during_first_snapshot = arrival == "before_snapshot"

    selected_during_first_snapshot = available_during_first_snapshot and distinct_row_timestamp > target_max
    selected_after_snapshot_or_on_retry = distinct_row_timestamp > target_max

    assert selected_during_first_snapshot is False
    assert selected_after_snapshot_or_on_retry is False


@pytest.mark.parametrize(
    "candidate",
    (
        datetime(2026, 8, 15, 23, 59, 59),
        None,
    ),
    ids=("late-older", "null-unordered"),
)
def test_late_or_null_cursor_is_not_selected_by_legacy_strict_boundary(
    candidate: datetime | None,
) -> None:
    target_max = datetime(2026, 8, 16, 10, 0, 0)

    selected = candidate is not None and candidate > target_max

    assert selected is False


def test_clickhouse_precision_can_collapse_at_mssql_target_boundary() -> None:
    first = Decimal("0.12345671")
    distinct = Decimal("0.12345674")
    mssql_tick = Decimal("0.0000001")

    projected_first = first.quantize(mssql_tick, rounding=ROUND_HALF_UP)
    projected_distinct = distinct.quantize(mssql_tick, rounding=ROUND_HALF_UP)

    assert first != distinct
    assert projected_first == projected_distinct
    assert not projected_distinct > projected_first
