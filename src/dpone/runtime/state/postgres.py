"""PostgreSQL-backed runtime state storage."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg import sql

from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.state.load_audit_mapping import load_audit_insert_params
from dpone.runtime.state.models import RunState
from dpone.runtime.state.run_state_mapping import run_state_from_row
from dpone.runtime.state.xmin_storage import XMinState
from dpone.runtime.state_logging import etl_logger


class PostgresXMinStateStorage:
    """Stores PostgreSQL XMin checkpoints in PostgreSQL."""

    def __init__(self, connector, schema: str = "etl_state", table: str = "etl_xmin_state") -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._table_created = False

    @property
    def fq_table(self) -> sql.Composed:
        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(self.table))

    def create_state_table(self) -> None:
        if self._table_created:
            return
        self.connector.execute_query(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
        self.connector.execute_query(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    source_schema text NOT NULL,
                    source_table text NOT NULL,
                    xmin_value bigint NOT NULL,
                    is_initial boolean NOT NULL DEFAULT false,
                    wraparound_detected boolean NOT NULL DEFAULT false,
                    frozen_xid bigint NULL,
                    __dpone__loaded_at timestamp NOT NULL DEFAULT timezone('utc', now()),
                    __dpone__updated_at timestamp NOT NULL DEFAULT timezone('utc', now()),
                    CONSTRAINT {} PRIMARY KEY (source_schema, source_table)
                )
                """
            ).format(self.fq_table, sql.Identifier(f"pk_{self.table}_source"))
        )
        self._table_created = True

    def _ensure_table_exists(self) -> None:
        self.create_state_table()

    def save_state(self, source_schema: str, source_table: str, xmin_state: XMinState) -> None:
        self._ensure_table_exists()
        self.connector.execute_query(
            sql.SQL(
                """
                INSERT INTO {} (
                    source_schema, source_table, xmin_value, is_initial, wraparound_detected,
                    frozen_xid, __dpone__loaded_at, __dpone__updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, timezone('utc', now()), timezone('utc', now()))
                ON CONFLICT (source_schema, source_table) DO UPDATE SET
                    xmin_value = EXCLUDED.xmin_value,
                    is_initial = EXCLUDED.is_initial,
                    wraparound_detected = EXCLUDED.wraparound_detected,
                    frozen_xid = EXCLUDED.frozen_xid,
                    __dpone__updated_at = timezone('utc', now())
                """
            ).format(self.fq_table),
            (
                source_schema,
                source_table,
                xmin_state.xmin_value,
                xmin_state.is_initial,
                xmin_state.wraparound_detected,
                xmin_state.frozen_xid,
            ),
        )
        etl_logger.log_xmin_state_info("Postgres xmin state saved", {"Source": f"{source_schema}.{source_table}"})

    def load_state(self, source_schema: str, source_table: str) -> XMinState | None:
        self._ensure_table_exists()
        rows = self.connector.get_records(
            sql.SQL(
                """
                SELECT xmin_value, __dpone__loaded_at, is_initial, wraparound_detected, frozen_xid
                FROM {}
                WHERE source_schema = %s AND source_table = %s
                """
            ).format(self.fq_table),
            (source_schema, source_table),
            as_dict=True,
        )
        if not rows:
            return None
        row = rows[0]
        return XMinState(
            xmin_value=int(row["xmin_value"]),
            timestamp=row["__dpone__loaded_at"],
            is_initial=bool(row["is_initial"]),
            wraparound_detected=bool(row["wraparound_detected"]),
            frozen_xid=row.get("frozen_xid"),
        )

    def delete_state(self, source_schema: str, source_table: str) -> None:
        self._ensure_table_exists()
        self.connector.execute_query(
            sql.SQL("DELETE FROM {} WHERE source_schema = %s AND source_table = %s").format(self.fq_table),
            (source_schema, source_table),
        )


class PostgresRunStateStorage:
    """Stores ETL run state in PostgreSQL."""

    def __init__(self, connector, schema: str = "etl_state", table: str = "etl_run_state") -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._table_created = False

    @property
    def fq_table(self) -> sql.Composed:
        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(self.table))

    def create_state_table(self) -> None:
        if self._table_created:
            return
        self.connector.execute_query(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
        self.connector.execute_query(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    id bigserial NOT NULL,
                    dag_id text NOT NULL,
                    source_schema text NOT NULL,
                    source_table text NOT NULL,
                    target_schema text NOT NULL,
                    target_table text NOT NULL,
                    load_strategy text NOT NULL,
                    execution_date timestamp NOT NULL,
                    state text NOT NULL,
                    started_at timestamp NOT NULL,
                    ended_at timestamp NULL,
                    duration_min double precision NULL,
                    error_message text NULL,
                    rows_read bigint NULL,
                    rows_written bigint NULL,
                    rows_updated bigint NULL,
                    rows_deleted bigint NULL,
                    __dpone__loaded_at timestamp NOT NULL DEFAULT timezone('utc', now()),
                    __dpone__updated_at timestamp NOT NULL DEFAULT timezone('utc', now()),
                    CONSTRAINT {} PRIMARY KEY (dag_id, execution_date)
                )
                """
            ).format(self.fq_table, sql.Identifier(f"pk_{self.table}_dag_execution"))
        )
        self._table_created = True

    def _ensure_table_exists(self) -> None:
        self.create_state_table()

    def save_run_state(self, run_state: RunState) -> None:
        self._ensure_table_exists()
        self.connector.execute_query(
            sql.SQL(
                """
                INSERT INTO {} (
                    dag_id, source_schema, source_table, target_schema, target_table, load_strategy,
                    execution_date, state, started_at, ended_at, duration_min, error_message,
                    rows_read, rows_written, rows_updated, rows_deleted, __dpone__loaded_at, __dpone__updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, timezone('utc', now()), timezone('utc', now()))
                ON CONFLICT (dag_id, execution_date) DO UPDATE SET
                    state = EXCLUDED.state,
                    ended_at = EXCLUDED.ended_at,
                    duration_min = EXCLUDED.duration_min,
                    error_message = EXCLUDED.error_message,
                    rows_read = EXCLUDED.rows_read,
                    rows_written = EXCLUDED.rows_written,
                    rows_updated = EXCLUDED.rows_updated,
                    rows_deleted = EXCLUDED.rows_deleted,
                    load_strategy = EXCLUDED.load_strategy,
                    __dpone__updated_at = timezone('utc', now())
                """
            ).format(self.fq_table),
            self._params(run_state),
        )
        etl_logger.log_run_state_info("Postgres run state saved", {"DAG ID": run_state.dag_id})

    def update_run_state(self, run_state: RunState) -> None:
        self.save_run_state(run_state)

    def get_run_state(self, dag_id: str, execution_date: datetime) -> RunState | None:
        self._ensure_table_exists()
        rows = self.connector.get_records(
            sql.SQL("SELECT * FROM {} WHERE dag_id = %s AND execution_date = %s LIMIT 1").format(self.fq_table),
            (dag_id, execution_date),
            as_dict=True,
        )
        return run_state_from_row(rows[0]) if rows else None

    def get_run_states_by_dag(
        self, dag_id: str, execution_date: datetime | None = None, limit: int = 100
    ) -> list[RunState]:
        self._ensure_table_exists()
        if execution_date is None:
            rows = self.connector.get_records(
                sql.SQL("SELECT * FROM {} WHERE dag_id = %s ORDER BY execution_date DESC LIMIT %s").format(
                    self.fq_table
                ),
                (dag_id, limit),
                as_dict=True,
            )
        else:
            rows = self.connector.get_records(
                sql.SQL(
                    "SELECT * FROM {} WHERE dag_id = %s AND execution_date = %s ORDER BY execution_date DESC LIMIT %s"
                ).format(self.fq_table),
                (dag_id, execution_date, limit),
                as_dict=True,
            )
        return [run_state_from_row(row) for row in rows]

    def delete_old_states(self, days_to_keep: int = 30) -> int:
        self._ensure_table_exists()
        return self.connector.execute_query(
            sql.SQL("DELETE FROM {} WHERE execution_date < timezone('utc', now()) - (%s * interval '1 day')").format(
                self.fq_table
            ),
            (days_to_keep,),
        )

    def _params(self, run_state: RunState) -> tuple[Any, ...]:
        return (
            run_state.dag_id,
            run_state.source_schema,
            run_state.source_table,
            run_state.target_schema,
            run_state.target_table,
            run_state.load_strategy,
            run_state.execution_date,
            run_state.state.value,
            run_state.started_at,
            run_state.ended_at,
            run_state.duration_min,
            run_state.error_message,
            run_state.rows_read,
            run_state.rows_written,
            run_state.rows_updated,
            run_state.rows_deleted,
        )


class PostgresLoadAuditStorage:
    """Stores canonical dpone load lifecycle records in PostgreSQL."""

    def __init__(self, connector, schema: str = "etl_state", table: str = "__dpone__loads") -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._table_created = False

    @property
    def fq_table(self) -> sql.Composed:
        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(self.table))

    def create_load_table(self) -> None:
        if self._table_created:
            return
        self.connector.execute_query(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
        self.connector.execute_query(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    run_id varchar(26) NOT NULL,
                    load_id varchar(26) NOT NULL,
                    status text NOT NULL,
                    process_name text NULL,
                    source_schema text NOT NULL,
                    source_table text NOT NULL,
                    target_schema text NOT NULL,
                    target_table text NOT NULL,
                    strategy text NOT NULL,
                    started_at timestamp NOT NULL,
                    staged_at timestamp NULL,
                    committed_at timestamp NULL,
                    failed_at timestamp NULL,
                    extracted_rows bigint NULL,
                    staged_rows bigint NULL,
                    inserted_rows bigint NULL,
                    updated_rows bigint NULL,
                    loaded_rows bigint NULL,
                    error_message text NULL,
                    artifact_uri text NULL,
                    __dpone__loaded_at timestamp NOT NULL DEFAULT timezone('utc', now()),
                    CONSTRAINT {} PRIMARY KEY (load_id)
                )
                """
            ).format(self.fq_table, sql.Identifier(f"pk_{self.table}_load_id"))
        )
        self._table_created = True

    def record_load_started(self, record: LoadAuditRecord) -> None:
        self._upsert(record)

    def record_load_staged(self, record: LoadAuditRecord) -> None:
        self._upsert(record)

    def record_load_committed(self, record: LoadAuditRecord) -> None:
        self._upsert(record)

    def record_load_failed(self, record: LoadAuditRecord) -> None:
        self._upsert(record)

    def _upsert(self, record: LoadAuditRecord) -> None:
        self.create_load_table()
        self.connector.execute_query(
            sql.SQL(
                """
                INSERT INTO {} (
                    run_id, load_id, status, process_name, source_schema, source_table,
                    target_schema, target_table, strategy, started_at, staged_at, committed_at,
                    failed_at, extracted_rows, staged_rows, inserted_rows, updated_rows,
                    loaded_rows, error_message, artifact_uri, __dpone__loaded_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    timezone('utc', now())
                )
                ON CONFLICT (load_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    staged_at = EXCLUDED.staged_at,
                    committed_at = EXCLUDED.committed_at,
                    failed_at = EXCLUDED.failed_at,
                    extracted_rows = EXCLUDED.extracted_rows,
                    staged_rows = EXCLUDED.staged_rows,
                    inserted_rows = EXCLUDED.inserted_rows,
                    updated_rows = EXCLUDED.updated_rows,
                    loaded_rows = EXCLUDED.loaded_rows,
                    error_message = EXCLUDED.error_message,
                    artifact_uri = EXCLUDED.artifact_uri,
                    __dpone__loaded_at = timezone('utc', now())
                """
            ).format(self.fq_table),
            self._params(record),
        )

    def _params(self, record: LoadAuditRecord) -> tuple[Any, ...]:
        return load_audit_insert_params(record)
