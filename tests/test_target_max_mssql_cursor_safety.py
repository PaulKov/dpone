from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.sink_dialect import (
    SinkDialectAuthorityConflictError,
    SinkDialectAuthorityMissingError,
    is_mssql_dialect,
)
from dpone.contracts.target_max_incremental_cursor import (
    MSSQL_MSSQL_POLICY,
    MYSQL_MSSQL_POLICY,
    UnsafeTargetMaxMssqlCursorError,
    assert_target_max_mssql_cursor_supported,
)
from dpone.runtime.sources.mssql import MSSQLSource
from dpone.runtime.sources.mysql import MySQLSource
from dpone.runtime.sources.strategies.mssql.mssql_incremental import MSSQLIncrementalExtractStrategy
from dpone.runtime.sources.strategies.mysql.mysql_incremental import MySQLIncrementalExtractStrategy


class _NoIoConnector:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str):
        def forbidden(*_args, **_kwargs):
            self.calls.append(name)
            raise AssertionError(f"unsafe route reached connector I/O: {name}")

        return forbidden


class _DeclaredSink:
    def __init__(self, dialect: str) -> None:
        self.dialect = dialect
        self.calls: list[str] = []

    def get_max_column_value(self, *_args, **_kwargs):
        self.calls.append("get_max_column_value")
        raise AssertionError("unsafe route reached target MAX I/O")


class _MssqlNamedButUndeclaredSink:
    """A class name is deliberately not a connector capability."""


@pytest.mark.parametrize(
    "alias",
    (
        "mssql",
        "MSSQL",
        "microsoft mssql",
        "microsoft_mssql",
        "odbc",
        "sqlserver",
        "sql_server",
        "sql-server",
    ),
)
def test_mssql_dialect_aliases_share_one_canonical_classifier(alias: str) -> None:
    assert is_mssql_dialect(alias)


def _config(
    *,
    source_type: str,
    sink_type: str | None = "mssql",
    strategy: LoadStrategy = LoadStrategy.INCREMENTAL_APPEND,
) -> LoadConfig:
    options: dict[str, object] = {
        "source_type": source_type,
        "incremental_column": "updated_at",
        "mssql_export_mode": "streaming",
    }
    if sink_type is not None:
        options["sink_type"] = sink_type
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="events",
        target_database="DWH",
        target_schema="landing",
        target_table="events",
        load_strategy=strategy,
        unique_key=["id"],
        options=options,
    )


@pytest.mark.parametrize(
    ("source_type", "source_cls", "runtime_code"),
    (
        ("mysql", MySQLSource, MYSQL_MSSQL_POLICY.runtime_error_code),
        ("mssql", MSSQLSource, MSSQL_MSSQL_POLICY.runtime_error_code),
    ),
)
@pytest.mark.parametrize("entrypoint", ("get_incremental_state", "extract"))
def test_source_facade_rejects_sibling_target_max_route_before_any_io(
    source_type: str,
    source_cls: type,
    runtime_code: str,
    entrypoint: str,
) -> None:
    source_connector = _NoIoConnector()
    sink_connector = _DeclaredSink("mssql")
    source = source_cls(source_connector, SimpleNamespace(), sink_connector=sink_connector)
    config = _config(source_type=source_type)

    with pytest.raises(UnsafeTargetMaxMssqlCursorError, match=runtime_code):
        if entrypoint == "get_incremental_state":
            source.get_incremental_state(config)
        else:
            source.extract(config, {"column": "updated_at", "last_value": "2026-08-16 10:00:00.123456"})

    assert source_connector.calls == []
    assert sink_connector.calls == []


@pytest.mark.parametrize(
    ("source_type", "strategy_cls", "runtime_code"),
    (
        ("mysql", MySQLIncrementalExtractStrategy, MYSQL_MSSQL_POLICY.runtime_error_code),
        ("mssql", MSSQLIncrementalExtractStrategy, MSSQL_MSSQL_POLICY.runtime_error_code),
    ),
)
@pytest.mark.parametrize("entrypoint", ("get_state", "cold_extract", "retry_extract"))
def test_direct_strategy_cannot_bypass_sibling_guard(
    source_type: str,
    strategy_cls: type,
    runtime_code: str,
    entrypoint: str,
) -> None:
    source_connector = _NoIoConnector()
    sink_connector = _DeclaredSink("mssql")
    strategy = strategy_cls(source_connector, SimpleNamespace(), sink_connector=sink_connector)
    config = _config(source_type=source_type)

    with pytest.raises(UnsafeTargetMaxMssqlCursorError, match=runtime_code):
        if entrypoint == "get_state":
            strategy.get_state(config)
        else:
            state = (
                None
                if entrypoint == "cold_extract"
                else {
                    "column": "updated_at",
                    "last_value": "2026-08-16 10:00:00.123456",
                }
            )
            strategy.extract(config, state)

    assert source_connector.calls == []
    assert sink_connector.calls == []


@pytest.mark.parametrize("source_type", ("clickhouse", "mysql", "mssql", "postgres"))
def test_direct_guard_requires_explicit_dialect_authority(source_type: str) -> None:
    with pytest.raises(SinkDialectAuthorityMissingError, match="sink_dialect_authority_required"):
        assert_target_max_mssql_cursor_supported(
            source_type=source_type,
            configured_sink=None,
            sink_connector=_MssqlNamedButUndeclaredSink(),
        )


@pytest.mark.parametrize("source_type", ("clickhouse", "mysql", "mssql", "postgres"))
def test_direct_guard_accepts_matching_non_mssql_authorities_and_ignores_class_names(source_type: str) -> None:
    assert (
        assert_target_max_mssql_cursor_supported(
            source_type=source_type,
            configured_sink="postgres",
            sink_connector=SimpleNamespace(dialect="postgres"),
        )
        == "postgres"
    )
    assert (
        assert_target_max_mssql_cursor_supported(
            source_type=source_type,
            configured_sink="postgres",
            sink_connector=_MssqlNamedButUndeclaredSink(),
        )
        == "postgres"
    )


@pytest.mark.parametrize("source_type", ("clickhouse", "mysql", "mssql", "postgres"))
def test_direct_guard_rejects_conflicting_dialect_authorities(source_type: str) -> None:
    with pytest.raises(SinkDialectAuthorityConflictError, match="sink_dialect_authority_conflict"):
        assert_target_max_mssql_cursor_supported(
            source_type=source_type,
            configured_sink="postgres",
            sink_connector=SimpleNamespace(dialect="mssql"),
        )


@pytest.mark.parametrize("arrival", ("before_snapshot", "after_snapshot", "retry"))
@pytest.mark.parametrize("source_type", ("clickhouse", "mysql", "mssql", "postgres"))
def test_strict_target_max_boundary_loses_equal_values_for_every_builtin_source(
    source_type: str,
    arrival: str,
) -> None:
    del source_type
    target_max = datetime(2026, 8, 16, 10, 0, 0, 123456)
    candidate = datetime(2026, 8, 16, 10, 0, 0, 123456)
    available_in_first_snapshot = arrival == "before_snapshot"

    assert (available_in_first_snapshot and candidate > target_max) is False
    assert (candidate > target_max) is False


@pytest.mark.parametrize(
    "candidate",
    (datetime(2026, 8, 15, 23, 59, 59), None),
    ids=("late-older", "null-unordered"),
)
def test_strict_target_max_boundary_loses_late_and_null_values(candidate: datetime | None) -> None:
    target_max = datetime(2026, 8, 16, 10, 0, 0)

    assert (candidate is not None and candidate > target_max) is False


def test_target_precision_can_collapse_distinct_source_values() -> None:
    first = Decimal("0.12345671")
    distinct = Decimal("0.12345674")
    target_tick = Decimal("0.0000001")

    assert first != distinct
    assert first.quantize(target_tick, rounding=ROUND_HALF_UP) == distinct.quantize(
        target_tick,
        rounding=ROUND_HALF_UP,
    )
