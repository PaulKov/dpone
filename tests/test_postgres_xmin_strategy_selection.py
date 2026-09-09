from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.postgres_incremental_cursor import (
    POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE,
    postgres_mssql_column_cursor_is_unsafe,
)
from dpone.runtime.sources.postgres import PostgresSource
from dpone.runtime.sources.strategies.postgres.postgres_incremental_extract import (
    PostgresIncrementalExtractStrategy,
)


class FakeConnector:
    pass


class FakeMssqlConnector:
    """Misleading class name without an authoritative dialect capability."""


class ExplicitMssqlWrapper:
    """Custom wrapper that forwards a typed sink dialect capability."""

    dialect = "mssql"

    def __init__(self) -> None:
        self.delegate = FakeConnector()


class FakeStateStorage:
    pass


class FakeLogger:
    def log_xmin_state_info(self, message, payload):
        del message, payload


def make_config(*, options: dict[str, object] | None = None) -> LoadConfig:
    return LoadConfig(
        source_conn_id="pg_source",
        target_conn_id="mssql_sink",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        options=options or {},
    )


def make_source(*, sink_connector=None) -> PostgresSource:
    return PostgresSource(  # type: ignore[arg-type]
        FakeConnector(),
        FakeStateStorage(),
        FakeLogger(),
        sink_connector=sink_connector or FakeConnector(),
    )


def test_postgres_source_can_select_xmin_incremental_strategy_explicitly() -> None:
    source = make_source()

    strategy = source._resolve_strategy(make_config(options={"incremental_strategy": "xmin"}))

    assert strategy is source._xmin_extract


def test_postgres_source_legacy_default_uses_xmin_when_incremental_column_is_omitted() -> None:
    source = make_source()

    strategy = source._resolve_strategy(make_config())

    assert strategy is source._xmin_extract


def test_postgres_source_can_select_column_cursor_incremental_strategy_explicitly() -> None:
    source = make_source()

    strategy = source._resolve_strategy(
        make_config(
            options={
                "incremental_strategy": "column",
                "incremental_column": "updated_at",
                "sink_type": "postgres",
            }
        )
    )

    assert strategy is source._incremental_extract


def test_postgres_source_rejects_xmin_with_incremental_column_conflict() -> None:
    source = make_source()

    with pytest.raises(ValueError, match="conflicts with source.options.incremental_column"):
        source._resolve_strategy(
            make_config(options={"incremental_strategy": "xmin", "incremental_column": "updated_at"})
        )


def test_postgres_source_rejects_column_strategy_without_incremental_column() -> None:
    source = make_source()

    with pytest.raises(ValueError, match="requires source.options.incremental_column"):
        source._resolve_strategy(make_config(options={"incremental_strategy": "column"}))


@pytest.mark.parametrize("load_strategy", (LoadStrategy.INCREMENTAL_APPEND, LoadStrategy.INCREMENTAL_MERGE))
@pytest.mark.parametrize(
    "options",
    (
        {"incremental_strategy": "column", "incremental_column": "updated_at", "sink_type": "mssql"},
        {"incremental_column": "updated_at", "sink_type": "mssql"},
    ),
    ids=("explicit-column", "legacy-incremental-column"),
)
def test_postgres_mssql_column_cursor_fails_before_strategy_or_connector_io(
    load_strategy: LoadStrategy,
    options: dict[str, object],
) -> None:
    source = make_source(sink_connector=FakeMssqlConnector())
    config = replace(make_config(options=options), load_strategy=load_strategy)

    assert (
        source.mssql_transaction_checkpoint_mode(config)
        is MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE
    )

    with pytest.raises(ValueError, match=POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE):
        source.get_incremental_state(config)

    assert source._incremental_extract_instance is None


def test_postgres_mssql_column_cursor_uses_declared_wrapper_dialect_without_route_hint() -> None:
    source = make_source(sink_connector=ExplicitMssqlWrapper())

    with pytest.raises(ValueError, match=POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE):
        source.get_incremental_state(make_config(options={"incremental_column": "updated_at"}))


def test_postgresql_alias_uses_the_same_column_cursor_policy() -> None:
    assert postgres_mssql_column_cursor_is_unsafe(
        source_type="postgresql",
        sink_type="mssql",
        strategy="column",
        incremental_column="updated_at",
        load_strategy="incremental_merge",
    )


def test_misleading_connector_class_name_is_not_dialect_authority() -> None:
    source = make_source(sink_connector=FakeMssqlConnector())

    strategy = source._resolve_strategy(
        make_config(options={"sink_type": "postgres", "incremental_column": "updated_at"})
    )

    assert strategy is source._incremental_extract


def test_configured_and_connector_dialect_authorities_must_agree() -> None:
    source = make_source(sink_connector=ExplicitMssqlWrapper())

    with pytest.raises(ValueError, match="sink_dialect_authority_conflict"):
        source._resolve_strategy(make_config(options={"sink_type": "postgres", "incremental_column": "updated_at"}))


def test_direct_postgres_column_strategy_cannot_bypass_mssql_safety_contract() -> None:
    strategy = PostgresIncrementalExtractStrategy(
        FakeConnector(),
        sink_connector=ExplicitMssqlWrapper(),
        logger=FakeLogger(),
    )

    with pytest.raises(ValueError, match=POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE):
        strategy.get_state(make_config(options={"incremental_column": "updated_at"}))


@pytest.mark.parametrize(
    ("transport_options", "last_state"),
    (
        ({"delta_size_threshold": 1_000_000}, {"last_value": "2026-08-16 10:00:00.123456"}),
        (
            {"delta_size_threshold": 0, "batch_commit_mode": "whole"},
            {"last_value": "2026-08-16 10:00:00.123456"},
        ),
        (
            {"delta_size_threshold": 0, "batch_commit_mode": "separate"},
            {"last_value": "2026-08-16 10:00:00.123456"},
        ),
        ({"delta_size_threshold": 0}, None),
    ),
    ids=("stream", "whole-file", "batched-file", "cold-start"),
)
def test_direct_mssql_gate_precedes_stream_file_and_retry_branches(
    transport_options: dict[str, object],
    last_state: dict[str, object] | None,
) -> None:
    strategy = PostgresIncrementalExtractStrategy(
        FakeConnector(),
        sink_connector=ExplicitMssqlWrapper(),
        logger=FakeLogger(),
    )
    options = {"incremental_column": "updated_at", **transport_options}

    with pytest.raises(ValueError, match=POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE):
        strategy.extract(make_config(options=options), last_state=last_state)


@pytest.mark.parametrize(
    "candidate",
    (
        datetime(2026, 8, 16, 10, 0, 0, 123456),
        datetime(2026, 8, 16, 10, 0, 0, 123455),
        None,
    ),
    ids=("concurrent-equal-or-retry", "late-older", "null-unordered"),
)
def test_legacy_single_column_max_counterexamples_are_not_selected(candidate: datetime | None) -> None:
    """Model the strict SQL predicate that justified removing the success claim."""

    durable_target_max = datetime(2026, 8, 16, 10, 0, 0, 123456)

    selected_by_legacy_strict_gt = candidate is not None and candidate > durable_target_max

    assert selected_by_legacy_strict_gt is False


def test_finite_precision_collision_is_equal_after_target_handoff() -> None:
    """Two distinct commits may legitimately share PostgreSQL microsecond precision."""

    first_commit = datetime(2026, 8, 16, 10, 0, 0, 123456)
    concurrent_commit = first_commit + timedelta(microseconds=0)

    assert concurrent_commit == first_commit
    assert not concurrent_commit > first_commit


@pytest.mark.parametrize("strategy", (LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2))
def test_postgres_complete_snapshot_strategies_use_one_full_extract(strategy: LoadStrategy) -> None:
    source = make_source()
    config = make_config()

    resolved = source._resolve_strategy(replace(config, load_strategy=strategy))

    assert resolved is source._full_extract
