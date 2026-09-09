from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.mssql_source_checkpoint import (
    MssqlTransactionCheckpointMode,
    require_generic_mssql_checkpoint_safety,
    require_snapshot_envelope_mssql_checkpoint_safety,
)
from dpone.contracts.run_context import RunContext
from dpone.contracts.target_max_incremental_cursor import MSSQL_MSSQL_POLICY, MYSQL_MSSQL_POLICY
from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService
from dpone.runtime.sinks.mssql_transaction_requirement import MSSQL_GENERIC_TRANSACTION_CAPABILITY
from dpone.runtime.sources.kafka import KafkaSource
from dpone.runtime.sources.mssql import MSSQLSource
from dpone.runtime.sources.mysql import MySQLSource
from dpone.runtime.sources.postgres import PostgresSource
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedPostgresSnapshotSource,
)


def config(strategy: LoadStrategy) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="events",
        target_database="DWH",
        target_schema="landing",
        target_table="events",
        load_strategy=strategy,
        options={
            "source_type": "mysql",
            "sink_type": "mssql",
            "incremental_column": "updated_at",
        },
    )


@pytest.mark.parametrize("source_type", (MySQLSource, MSSQLSource))
@pytest.mark.parametrize(
    "strategy",
    (LoadStrategy.INCREMENTAL_APPEND, LoadStrategy.INCREMENTAL_MERGE),
)
def test_target_max_sources_declare_incremental_checkpoint_unsafe(
    source_type: type,
    strategy: LoadStrategy,
) -> None:
    source = source_type(connector=SimpleNamespace(), logger=SimpleNamespace(), sink_connector=SimpleNamespace())

    assert (
        source.mssql_transaction_checkpoint_mode(config(strategy))
        is MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE
    )


@pytest.mark.parametrize(
    ("source_type", "runtime_code"),
    (
        (MySQLSource, MYSQL_MSSQL_POLICY.runtime_error_code),
        (MSSQLSource, MSSQL_MSSQL_POLICY.runtime_error_code),
    ),
)
def test_generic_admission_rejects_sibling_target_max_sources_before_target_io(
    source_type: type,
    runtime_code: str,
) -> None:
    source = source_type(connector=SimpleNamespace(), logger=SimpleNamespace(), sink_connector=SimpleNamespace())
    target_calls = 0

    def target_resolver(*_args, **_kwargs):
        nonlocal target_calls
        target_calls += 1
        raise AssertionError("unsafe route reached target authority")

    sink = SimpleNamespace(
        target_dialect=lambda: "mssql",
        mssql_transaction_governance_capability=lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY,
    )

    with pytest.raises(ValueError, match=runtime_code):
        MssqlTransactionAdmissionService(target_resolver=target_resolver).prepare(
            config(LoadStrategy.INCREMENTAL_APPEND),
            source=source,
            sink=sink,
            run_context=RunContext("run", config={"process": "events"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert target_calls == 0


@pytest.mark.parametrize("source_type", (MySQLSource, MSSQLSource))
@pytest.mark.parametrize(
    "strategy",
    (LoadStrategy.FULL_REFRESH, LoadStrategy.REPLACE, LoadStrategy.PARTITION_REPLACE, LoadStrategy.BACKFILL),
)
def test_target_max_sources_declare_complete_scans_stateless(
    source_type: type,
    strategy: LoadStrategy,
) -> None:
    source = source_type(connector=SimpleNamespace(), logger=SimpleNamespace(), sink_connector=SimpleNamespace())

    assert source.mssql_transaction_checkpoint_mode(config(strategy)) is MssqlTransactionCheckpointMode.STATELESS


@pytest.mark.parametrize("strategy", (LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2))
def test_mysql_complete_snapshot_modes_are_stateless(strategy: LoadStrategy) -> None:
    source = MySQLSource(
        connector=SimpleNamespace(),
        logger=SimpleNamespace(),
        sink_connector=SimpleNamespace(),
    )

    assert source.mssql_transaction_checkpoint_mode(config(strategy)) is MssqlTransactionCheckpointMode.STATELESS


@pytest.mark.parametrize(
    "mode",
    (
        MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE,
        MssqlTransactionCheckpointMode.SNAPSHOT_ENVELOPE_TARGET_ATOMIC,
        MssqlTransactionCheckpointMode.EXTERNAL_NONATOMIC,
        MssqlTransactionCheckpointMode.UNKNOWN,
        MssqlTransactionCheckpointMode.LEGACY_AMBIGUOUS,
        "invented",
    ),
)
def test_only_typed_stateless_mode_is_generic_receipt_safe(mode: object) -> None:
    with pytest.raises(RuntimeError, match="source_checkpoint_not_atomic"):
        require_generic_mssql_checkpoint_safety(mode)

    assert (
        require_generic_mssql_checkpoint_safety(MssqlTransactionCheckpointMode.STATELESS)
        is MssqlTransactionCheckpointMode.STATELESS
    )


def test_only_snapshot_envelope_mode_can_borrow_snapshot_finalizer_atomicity() -> None:
    assert (
        require_snapshot_envelope_mssql_checkpoint_safety(
            MssqlTransactionCheckpointMode.SNAPSHOT_ENVELOPE_TARGET_ATOMIC
        )
        is MssqlTransactionCheckpointMode.SNAPSHOT_ENVELOPE_TARGET_ATOMIC
    )
    for mode in (
        MssqlTransactionCheckpointMode.STATELESS,
        MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE,
        MssqlTransactionCheckpointMode.EXTERNAL_NONATOMIC,
        MssqlTransactionCheckpointMode.UNKNOWN,
    ):
        with pytest.raises(RuntimeError, match=f"snapshot_envelope_checkpoint_required:{mode.value}"):
            require_snapshot_envelope_mssql_checkpoint_safety(mode)


@pytest.mark.parametrize(
    "strategy",
    (LoadStrategy.INCREMENTAL_APPEND, LoadStrategy.INCREMENTAL_MERGE),
)
def test_governed_complete_snapshot_adapter_explicitly_declares_stateless(
    strategy: LoadStrategy,
) -> None:
    source = GovernedPostgresSnapshotSource(
        connector=SimpleNamespace(),
        state_storage=None,
        logger=SimpleNamespace(),
        sink_connector=SimpleNamespace(),
    )

    assert source.mssql_transaction_checkpoint_mode(config(strategy)) is MssqlTransactionCheckpointMode.STATELESS


@pytest.mark.parametrize(
    ("options", "expected"),
    (
        (
            {"source_type": "postgres", "sink_type": "mssql"},
            MssqlTransactionCheckpointMode.EXTERNAL_NONATOMIC,
        ),
        (
            {
                "source_type": "postgres",
                "sink_type": "clickhouse",
                "incremental_strategy": "column",
                "incremental_column": "updated_at",
            },
            MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE,
        ),
    ),
)
def test_production_postgres_incremental_checkpoint_modes_remain_atomicity_gated(
    options: dict[str, object],
    expected: MssqlTransactionCheckpointMode,
) -> None:
    load_config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_database="DWH",
        target_schema="landing",
        target_table="events",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
        options=options,
    )
    source = PostgresSource(
        connector=SimpleNamespace(),
        state_storage=None,
        logger=SimpleNamespace(),
        sink_connector=SimpleNamespace(),
    )

    mode = source.mssql_transaction_checkpoint_mode(load_config)

    assert mode is expected
    with pytest.raises(RuntimeError, match=f"source_checkpoint_not_atomic:{expected.value}"):
        require_generic_mssql_checkpoint_safety(mode)


def test_postgres_xmin_key_snapshot_declares_target_atomic_envelope_mode() -> None:
    load_config = config(LoadStrategy.INCREMENTAL_MERGE)
    load_config.options = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "incremental_strategy": "xmin",
        "reconciliation": {"enabled": True, "mode": "key_snapshot"},
    }
    source = PostgresSource(
        connector=SimpleNamespace(),
        state_storage=None,
        logger=SimpleNamespace(),
        sink_connector=SimpleNamespace(dialect="mssql"),
    )

    assert (
        source.mssql_transaction_checkpoint_mode(load_config)
        is MssqlTransactionCheckpointMode.SNAPSHOT_ENVELOPE_TARGET_ATOMIC
    )


@pytest.mark.parametrize(
    ("source_type", "runtime_code"),
    (
        (MySQLSource, MYSQL_MSSQL_POLICY.runtime_error_code),
        (MSSQLSource, MSSQL_MSSQL_POLICY.runtime_error_code),
    ),
)
def test_key_snapshot_config_cannot_exempt_wrong_target_max_source_before_io(
    source_type: type,
    runtime_code: str,
) -> None:
    load_config = config(LoadStrategy.INCREMENTAL_MERGE)
    load_config.options["reconciliation"] = {"enabled": True, "mode": "key_snapshot"}
    source = source_type(
        connector=SimpleNamespace(),
        logger=SimpleNamespace(),
        sink_connector=SimpleNamespace(dialect="mssql"),
    )
    target_calls = 0

    def target_resolver(*_args, **_kwargs):
        nonlocal target_calls
        target_calls += 1
        raise AssertionError("wrong-source key_snapshot reached target authority")

    sink = SimpleNamespace(
        target_dialect=lambda: "mssql",
        mssql_transaction_governance_capability=lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY,
    )

    with pytest.raises(ValueError, match=runtime_code):
        MssqlTransactionAdmissionService(target_resolver=target_resolver).prepare(
            load_config,
            source=source,
            sink=sink,
            run_context=RunContext("run", config={"process": "events"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert target_calls == 0


def test_valid_postgres_xmin_key_snapshot_exemption_is_typed_and_pre_io() -> None:
    load_config = config(LoadStrategy.INCREMENTAL_MERGE)
    load_config.options = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "incremental_strategy": "xmin",
        "reconciliation": {"enabled": True, "mode": "key_snapshot"},
    }
    source = PostgresSource(
        connector=SimpleNamespace(),
        state_storage=None,
        logger=SimpleNamespace(),
        sink_connector=SimpleNamespace(dialect="mssql"),
    )
    sink = SimpleNamespace(
        target_dialect=lambda: "mssql",
        mssql_transaction_governance_capability=lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY,
    )

    prepared = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: pytest.fail("snapshot envelope must not use generic target admission")
    ).prepare(
        load_config,
        source=source,
        sink=sink,
        run_context=RunContext("run", config={"process": "events"}),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )

    assert prepared is not load_config
    assert prepared.options["reconciliation"]["mode"] == "key_snapshot"


@pytest.mark.parametrize("target_dialect", ("mssql", "sqlserver", "sql_server"))
def test_key_snapshot_cannot_exempt_kafka_external_checkpoint_before_target_io(
    target_dialect: str,
) -> None:
    load_config = config(LoadStrategy.INCREMENTAL_APPEND)
    load_config.options["reconciliation"] = {"enabled": True, "mode": "key_snapshot"}
    source = KafkaSource(connector=SimpleNamespace())
    target_calls = 0

    def target_resolver(*_args, **_kwargs):
        nonlocal target_calls
        target_calls += 1
        raise AssertionError("Kafka key_snapshot reached target authority")

    sink = SimpleNamespace(
        target_dialect=lambda: target_dialect,
        mssql_transaction_governance_capability=lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY,
    )

    with pytest.raises(RuntimeError, match="snapshot_envelope_checkpoint_required:external_nonatomic"):
        MssqlTransactionAdmissionService(target_resolver=target_resolver).prepare(
            load_config,
            source=source,
            sink=sink,
            run_context=RunContext("run", config={"process": "events"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert target_calls == 0
