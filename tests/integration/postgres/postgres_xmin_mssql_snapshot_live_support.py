"""Disposable vendor-live environment for PostgreSQL XMin reconciliation.

The helpers in this module deliberately provision SQL Server objects with DDL
that is independent from the runtime stores.  The test can therefore prove
that ``provisioning=external`` validates and uses a separately managed state
contract instead of silently creating or falling back to local objects.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from psycopg import sql

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.postgres import PostgresSource
from dpone.runtime.state.mssql import (
    MSSQLLoadAuditStorage,
    MSSQLRunStateStorage,
    MSSQLXMinStateStorage,
)
from tests.integration.postgres.postgres_live_support import (
    mssql_connector,
    postgres_connector,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    bind_factual_mssql_database_authority,
    bind_factual_postgres_source_authority,
)

TARGET_SCHEMA = "sample_metrics"
STAGING_SCHEMA = "staging"
STATE_SCHEMA = "system"
SOURCE_TABLE = "metric_values"
TARGET_TABLE = "metric_values"
PROCESS_NAME = "integration.postgres_xmin_mssql_snapshot"
DAG_ID = "DAG__integration__postgres_xmin__mssql_snapshot"

SPECIAL_KEY = "metric\talpha\nmarker\x1dΩ"
SPECIAL_NOTE = "payload\tline-one\nline-two\rmarker\x1dunit\x1f"

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")


class QuietIntegrationLogger:
    """Runtime logger used by the live test without terminal noise."""

    def info(self, *_args: object, **_kwargs: object) -> None:
        return None

    def warning(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_etl_start(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_etl_end(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_etl_error(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_etl_progress(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_xmin_state_info(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_run_state_info(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_sql_query(self, *_args: object, **_kwargs: object) -> None:
        return None


@dataclass(slots=True)
class SnapshotRouteEnvironment:
    """All disposable vendor objects and the standard runtime processor."""

    postgres: Any
    target: Any
    state: Any
    master: Any
    checkpoint_storage: MSSQLXMinStateStorage
    processor: ETLProcessor
    load_config: LoadConfig
    source_schema: str
    target_database: str
    state_database: str

    def target_transaction_state(self) -> dict[str, int]:
        """Return the target session id and its exact open transaction count."""

        return _one(
            self.target.get_records(
                "SELECT @@SPID AS session_id, @@TRANCOUNT AS transaction_count",
                as_dict=True,
            ),
            "target transaction state",
        )

    def blocked_by_target_session(self, session_id: int) -> int:
        """Count requests blocked by one target session without exposing SQL text."""

        rows = self.master.get_records(
            "SELECT COUNT_BIG(*) AS blocked_requests FROM sys.dm_exec_requests WHERE blocking_session_id = ?",
            (session_id,),
            as_dict=True,
        )
        return int(_one(rows, "target blocker probe")["blocked_requests"])

    @property
    def target_name(self) -> str:
        return f"[{self.target_database}].[{TARGET_SCHEMA}].[{TARGET_TABLE}]"

    def run(self, execution_date: datetime) -> dict[str, Any]:
        """Run the normal ETLProcessor/PayloadLoadService lifecycle."""

        return self.processor.run(
            self.load_config,
            dag_id=DAG_ID,
            execution_date=execution_date,
        )

    def target_rows(self) -> list[dict[str, Any]]:
        return self.target.get_records(
            f"SELECT metric_code, metric_value, note, occurred_at, "
            f"__dpone__row_hash, __dpone__loaded_at, __dpone__deleted_at "
            f"FROM {self.target_name} ORDER BY metric_code",
            as_dict=True,
        )

    def evidence(self, load_id: str, execution_date: datetime) -> dict[str, Any]:
        """Read receipt, checkpoint, audit, and run evidence for one attempt."""

        receipt = _one(
            self.state.get_records(
                f"SELECT receipt_id, state_key, load_id, previous_xmin, candidate_xmin, "
                f"source_snapshot_token, committed_at "
                f"FROM [{self.state_database}].[{STATE_SCHEMA}].[dpone_commit_receipt] "
                "WHERE load_id = ?",
                (load_id,),
                as_dict=True,
            ),
            "commit receipt",
        )
        checkpoint = _one(
            self.state.get_records(
                f"SELECT state_key, xmin_value, is_initial, last_load_id, source_snapshot_token, "
                f"environment, process_name, target_database, target_schema, target_table "
                f"FROM [{self.state_database}].[{STATE_SCHEMA}].[dpone_source_state]",
                as_dict=True,
            ),
            "source checkpoint",
        )
        audit = _one(
            self.state.get_records(
                f"SELECT * FROM [{self.state_database}].[{STATE_SCHEMA}].[dpone_load_audit] WHERE load_id = ?",
                (load_id,),
                as_dict=True,
            ),
            "load audit",
        )
        run_state = _one(
            self.state.get_records(
                f"SELECT * FROM [{self.state_database}].[{STATE_SCHEMA}].[dpone_run_state] "
                "WHERE dag_id = ? AND process_name = ? AND execution_date = ?",
                (DAG_ID, PROCESS_NAME, execution_date),
                as_dict=True,
            ),
            "run state",
        )
        return {
            "receipt": receipt,
            "checkpoint": checkpoint,
            "audit": audit,
            "run_state": run_state,
        }


@contextmanager
def provision_snapshot_route(work_dir: Path) -> Iterator[SnapshotRouteEnvironment]:
    """Provision two disposable MSSQL databases plus one PostgreSQL table."""

    suffix = uuid.uuid4().hex[:10]
    source_schema = _safe(f"it_xmin_{suffix}")
    target_database = _safe(f"dpone_it_xmin_t_{suffix}")
    state_database = _safe(f"dpone_it_xmin_s_{suffix}")
    postgres = postgres_connector()
    master = mssql_connector(database="master")
    target = None
    state = None
    try:
        wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
        wait_until_ready("mssql", lambda: master.get_records("SELECT 1"))
        _create_database(master, target_database)
        _create_database(master, state_database)
        target = mssql_connector(database=target_database)
        state = mssql_connector(database=state_database)
        wait_until_ready("mssql target database", lambda: target.get_records("SELECT 1"))
        wait_until_ready("mssql state database", lambda: state.get_records("SELECT 1"))
        _require_same_instance(target, state)
        _provision_target(target, target_database)
        _provision_external_state(state, state_database)
        _provision_source(postgres, source_schema)
        load_config = _load_config(
            postgres=postgres,
            source_schema=source_schema,
            target_database=target_database,
            work_dir=work_dir,
        )
        logger = QuietIntegrationLogger()
        checkpoint_storage = MSSQLXMinStateStorage(
            state,
            database=state_database,
            schema=STATE_SCHEMA,
            table="dpone_source_state",
            receipt_table="dpone_commit_receipt",
            run_table="dpone_run_state",
            audit_table="dpone_load_audit",
            atomicity="target_atomic",
            provisioning="external",
        )
        sink = MSSQLSink(target, state_storage=checkpoint_storage, logger=logger)
        source = PostgresSource(
            postgres,
            checkpoint_storage,
            logger,
            sink_connector=target,
        )
        bind_factual_mssql_database_authority(
            checkpoint_storage,
            master,
            target_database=target_database,
            staging_database=target_database,
            state_database=state_database,
        )
        bind_factual_postgres_source_authority(source, postgres, load_config=load_config)
        run_state_storage = MSSQLRunStateStorage(
            state,
            database=state_database,
            schema=STATE_SCHEMA,
            table="dpone_run_state",
            provisioning="external",
        )
        audit_storage = MSSQLLoadAuditStorage(
            state,
            database=state_database,
            schema=STATE_SCHEMA,
            table="dpone_load_audit",
            provisioning="external",
        )
        processor = ETLProcessor(
            source,
            sink,
            etl_logger=logger,
            run_state_storage=run_state_storage,
            load_identity_service=LoadIdentityService(audit_storage=audit_storage),
        )
        yield SnapshotRouteEnvironment(
            postgres=postgres,
            target=target,
            state=state,
            master=master,
            checkpoint_storage=checkpoint_storage,
            processor=processor,
            load_config=load_config,
            source_schema=source_schema,
            target_database=target_database,
            state_database=state_database,
        )
    finally:
        with suppress(Exception):
            postgres.execute_query(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(source_schema)))
        with suppress(Exception):
            postgres.close()
        if target is not None:
            with suppress(Exception):
                target.close()
        if state is not None:
            with suppress(Exception):
                state.close()
        for database in (target_database, state_database):
            with suppress(Exception):
                _drop_database(master, database)
        with suppress(Exception):
            master.close()


def _load_config(
    *,
    postgres: Any,
    source_schema: str,
    target_database: str,
    work_dir: Path,
) -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_it_source",
        target_conn_id="mssql_it_target",
        source_database=str(postgres.database),
        source_schema=source_schema,
        source_table=SOURCE_TABLE,
        target_database=target_database,
        target_schema=TARGET_SCHEMA,
        target_table=TARGET_TABLE,
        staging_database=target_database,
        staging_schema=STAGING_SCHEMA,
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["metric_code"],
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "incremental_strategy": "xmin",
            "batch_commit_mode": "whole",
            # Every vendor command must fail inside the test's own diagnostic
            # budget instead of being killed only by the 45-minute suite cap.
            "bulk": {"mode": "bcp", "bcp": {"timeout_seconds": 120}},
            "work_dir": str(work_dir),
            "technical_columns": "required",
            "soft_delete": {"mode": "timestamp_only"},
            "physical_design": {
                "apply_runtime": False,
                "columns": {
                    "metric_code": {"target_type": {"mssql": "nvarchar(450)"}},
                },
                "storage": {"mssql": {"compression": "none"}},
            },
            "schema_contract": {
                "columns": {
                    "metric_code": {"nullable": False},
                    "metric_value": {"nullable": False},
                    "note": {"nullable": True},
                    "occurred_at": {"nullable": False},
                }
            },
            "reconciliation": {
                "enabled": True,
                "mode": "key_snapshot",
                "cadence": "every_run",
                "consistency": "same_source_snapshot",
                "delete_policy": "soft_delete",
                "empty_snapshot": {"policy": "fail"},
                "guards": {"max_delete_ratio": 1.0, "max_delete_rows": 10},
            },
            "state_identity": {
                "environment": "integration",
                "process": PROCESS_NAME,
            },
        },
    )


def _provision_source(postgres: Any, source_schema: str) -> None:
    postgres.execute_query(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(source_schema)))
    postgres.execute_query(
        sql.SQL(
            "CREATE TABLE {}.{} ("
            "metric_code text PRIMARY KEY, "
            "metric_value double precision NOT NULL, "
            "note text NULL, "
            "occurred_at timestamptz NOT NULL)"
        ).format(sql.Identifier(source_schema), sql.Identifier(SOURCE_TABLE))
    )
    insert = sql.SQL("INSERT INTO {}.{} (metric_code, metric_value, note, occurred_at) VALUES (%s, %s, %s, %s)").format(
        sql.Identifier(source_schema), sql.Identifier(SOURCE_TABLE)
    )
    rows = (
        # This row must stay well before the retired 2026-01-01 workload
        # predicate.  Otherwise a regression that silently restores the old
        # cutoff could still pass the live route certification.
        (SPECIAL_KEY, 1.23456789012345, SPECIAL_NOTE, "2024-06-15T03:04:05.123456+03:00"),
        ("empty", -42.125, "", "2026-02-02T04:05:06.654321-05:00"),
        ("nullable", 9007199254740991.0, None, "2026-03-03T00:00:00+00:00"),
    )
    with postgres.connection.cursor() as cursor:
        cursor.executemany(insert, rows)


def _provision_target(target: Any, database: str) -> None:
    target.execute_query(f"CREATE SCHEMA [{TARGET_SCHEMA}] AUTHORIZATION [dbo]")
    target.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
    _execute_sql_fixture(
        target,
        "postgres_xmin_mssql_target.sql",
        database=database,
    )


def _provision_external_state(state: Any, database: str) -> None:
    state.execute_query(f"CREATE SCHEMA [{STATE_SCHEMA}] AUTHORIZATION [dbo]")
    _execute_sql_fixture(
        state,
        "postgres_xmin_mssql_external_state.sql",
        database=database,
    )


def _execute_sql_fixture(connector: Any, filename: str, *, database: str) -> None:
    fixture = Path(__file__).with_name("sql") / filename
    rendered = fixture.read_text(encoding="utf-8").replace("__DATABASE__", _safe(database))
    for statement in rendered.split("-- dpone:statement"):
        if statement := statement.strip():
            connector.execute_query(statement)


def _create_database(master: Any, database: str) -> None:
    master.execute_query(f"CREATE DATABASE [{_safe(database)}]")


def _drop_database(master: Any, database: str) -> None:
    safe = _safe(database)
    master.execute_query(f"ALTER DATABASE [{safe}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
    master.execute_query(f"DROP DATABASE [{safe}]")


def _require_same_instance(target: Any, state: Any) -> None:
    dimensions = ("host", "port", "user")
    if any(str(getattr(target, name, "")).lower() != str(getattr(state, name, "")).lower() for name in dimensions):
        raise RuntimeError("Vendor-live target/state connectors must use the same SQL Server instance and principal")


def _safe(value: str) -> str:
    if not _SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe integration identifier: {value!r}")
    return value


def _one(rows: list[dict[str, Any]], label: str) -> dict[str, Any]:
    if len(rows) != 1:
        raise AssertionError(f"Expected one {label}, found {len(rows)}")
    return rows[0]


__all__ = [
    "DAG_ID",
    "PROCESS_NAME",
    "SOURCE_TABLE",
    "SPECIAL_KEY",
    "SPECIAL_NOTE",
    "SnapshotRouteEnvironment",
    "provision_snapshot_route",
]
