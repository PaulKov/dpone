"""Import-compatible MSSQL run-state and load-audit stores."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_object_name import MSSQLObjectName, mssql_ensure_schema_statement
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.state.load_audit_mapping import (
    load_audit_exact_metric_params,
    load_audit_insert_params,
)
from dpone.runtime.state.mssql_contract import (
    LOAD_AUDIT_CONTRACT,
    RUN_STATE_CONTRACT,
    load_audit_additive_ddl,
    require_external_table_shape,
    runtime_table_columns,
)
from dpone.runtime.state.mssql_operational_ddl import render_load_audit_create_sql
from dpone.runtime.state.mssql_run_state_migration import (
    MssqlRunStateIdentityPolicy,
    MssqlRunStateTableCapability,
    render_run_state_v1_create_sql,
    render_run_state_v2_create_sql,
    resolve_run_state_table_capability,
)
from dpone.runtime.state.run_state_mapping import (
    canonical_run_execution_date,
    legacy_run_state_merge_params,
    legacy_run_state_merge_sql,
    run_process_name,
    run_state_from_row,
    run_state_key,
    run_state_merge_params,
)
from dpone.runtime.state_logging import etl_logger

if TYPE_CHECKING:
    from dpone.runtime.state.models import RunState


class MSSQLRunStateStorage:
    """Store ETL run state in a configurable SQL Server database."""

    def __init__(
        self,
        connector: Any,
        schema: str = "etl_state",
        table: str = "etl_run_state",
        *,
        database: str | None = None,
        provisioning: str = "runtime",
        identity_policy: MssqlRunStateIdentityPolicy | str = MssqlRunStateIdentityPolicy.LEGACY_COMPATIBLE,
    ) -> None:
        self.connector = connector
        self.database = database
        self.provisioning = provisioning
        self.schema = schema
        self.table = table
        self.identity_policy = MssqlRunStateIdentityPolicy(identity_policy)
        self._table_capability = (
            MssqlRunStateTableCapability.PROCESS_SCOPED_V2
            if self.identity_policy is MssqlRunStateIdentityPolicy.PROCESS_SCOPED_V2_REQUIRED
            else None
        )
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return _qualified_name(self.connector, self.database, self.schema, self.table)

    def create_state_table(self) -> None:
        if self._table_created:
            return
        if self.provisioning == "external":
            columns = runtime_table_columns(
                self.connector,
                database=self.database,
                schema=self.schema,
                table=self.table,
            )
            self._table_capability = resolve_run_state_table_capability(
                columns,
                identity_policy=self.identity_policy,
                table_label=self.fq_table,
            )
            if self._table_capability is MssqlRunStateTableCapability.PROCESS_SCOPED_V2:
                require_external_table_shape(
                    self.connector,
                    database=self.database,
                    schema=self.schema,
                    table=self.table,
                    contract=RUN_STATE_CONTRACT,
                )
            self._table_created = True
            return
        self.connector.execute_query(*_ensure_schema_statement(self.database, self.schema))
        renderer = (
            render_run_state_v2_create_sql
            if self.identity_policy is MssqlRunStateIdentityPolicy.PROCESS_SCOPED_V2_REQUIRED
            else render_run_state_v1_create_sql
        )
        create_sql = renderer(self.fq_table, constraint_token=self.table)
        self.connector.execute_query(
            f"IF OBJECT_ID(N'{_object_id(self.database, self.schema, self.table)}', N'U') IS NULL\n{create_sql}"
        )
        columns = runtime_table_columns(
            self.connector,
            database=self.database,
            schema=self.schema,
            table=self.table,
        )
        if not columns:
            raise RuntimeError(f"mssql_run_state_contract_unavailable:{self.fq_table}")
        self._table_capability = resolve_run_state_table_capability(
            columns,
            identity_policy=self.identity_policy,
            table_label=self.fq_table,
        )
        self._table_created = True

    def _ensure_table_exists(self) -> None:
        self.create_state_table()

    def save_run_state(self, run_state: RunState) -> None:
        self._ensure_table_exists()
        if self._table_capability is MssqlRunStateTableCapability.LEGACY_V1:
            self.connector.execute_query(
                legacy_run_state_merge_sql(self.fq_table),
                legacy_run_state_merge_params(run_state, run_state.execution_date),
            )
            etl_logger.log_run_state_info("MSSQL run state saved", {"DAG ID": run_state.dag_id})
            return
        process_name = run_process_name(run_state)
        execution_date = canonical_run_execution_date(run_state.execution_date)
        identity = run_state_key(run_state.dag_id, process_name, execution_date)
        self.connector.execute_query(
            f"""
            IF EXISTS (
                SELECT 1
                FROM {self.fq_table} WITH (UPDLOCK, HOLDLOCK)
                WHERE run_state_key = ?
                  AND (dag_id <> ? OR process_name <> ? OR execution_date <> ?)
            )
                THROW 51000, 'mssql_run_state_identity_collision', 1;

            MERGE {self.fq_table} AS target
            USING (
                SELECT ? AS run_state_key, ? AS dag_id, ? AS process_name, ? AS execution_date
            ) AS source
            ON target.run_state_key = source.run_state_key
               AND target.dag_id = source.dag_id
               AND target.process_name = source.process_name
               AND target.execution_date = source.execution_date
            WHEN MATCHED THEN UPDATE SET
                state = ?, ended_at = ?, duration_min = ?, error_message = ?, rows_read = ?, rows_written = ?,
                rows_updated = ?, rows_deleted = ?, load_strategy = ?, __dpone__updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                run_state_key, dag_id, process_name, source_schema, source_table,
                target_schema, target_table, load_strategy,
                execution_date, state, started_at, ended_at, duration_min, error_message,
                rows_read, rows_written, rows_updated, rows_deleted, __dpone__loaded_at, __dpone__updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
            run_state_merge_params(run_state, identity, process_name, execution_date),
        )
        etl_logger.log_run_state_info("MSSQL run state saved", {"DAG ID": run_state.dag_id})

    def update_run_state(self, run_state: RunState) -> None:
        self.save_run_state(run_state)

    def get_run_state(
        self,
        dag_id: str,
        execution_date: datetime,
        process_name: str | None = None,
    ) -> RunState | None:
        self._ensure_table_exists()
        if self._table_capability is MssqlRunStateTableCapability.LEGACY_V1:
            rows = self.connector.get_records(
                f"SELECT TOP (1) * FROM {self.fq_table} WHERE dag_id = ? AND execution_date = ?",
                (dag_id, execution_date),
                as_dict=True,
            )
            return run_state_from_row(rows[0]) if rows else None
        execution_date = canonical_run_execution_date(execution_date)
        if process_name is not None:
            identity = run_state_key(dag_id, process_name, execution_date)
            rows = self.connector.get_records(
                f"SELECT TOP (1) * FROM {self.fq_table} WHERE run_state_key = ? "
                "AND dag_id = ? AND process_name = ? AND execution_date = ?",
                (identity, dag_id, process_name, execution_date),
                as_dict=True,
            )
            return run_state_from_row(rows[0]) if rows else None
        rows = self.connector.get_records(
            f"SELECT TOP (2) * FROM {self.fq_table} WHERE dag_id = ? AND execution_date = ?",
            (dag_id, execution_date),
            as_dict=True,
        )
        if len(rows) > 1:
            raise RuntimeError("mssql_run_state_process_identity_required")
        return run_state_from_row(rows[0]) if rows else None

    def get_run_states_by_dag(
        self,
        dag_id: str,
        execution_date: datetime | None = None,
        limit: int = 100,
    ) -> list[RunState]:
        self._ensure_table_exists()
        if execution_date is None:
            rows = self.connector.get_records(
                f"SELECT TOP ({int(limit)}) * FROM {self.fq_table} WHERE dag_id = ? ORDER BY execution_date DESC",
                (dag_id,),
                as_dict=True,
            )
        else:
            if self._table_capability is MssqlRunStateTableCapability.PROCESS_SCOPED_V2:
                execution_date = canonical_run_execution_date(execution_date)
            rows = self.connector.get_records(
                f"SELECT TOP ({int(limit)}) * FROM {self.fq_table} "
                "WHERE dag_id = ? AND execution_date = ? ORDER BY execution_date DESC",
                (dag_id, execution_date),
                as_dict=True,
            )
        return [run_state_from_row(row) for row in rows]

    def delete_old_states(self, days_to_keep: int = 30) -> int:
        self._ensure_table_exists()
        return self.connector.execute_query(
            f"DELETE FROM {self.fq_table} WHERE execution_date < DATEADD(day, -?, SYSUTCDATETIME())",
            (days_to_keep,),
        )


class MSSQLLoadAuditStorage:
    """Store canonical dpone load lifecycle records in SQL Server."""

    def __init__(
        self,
        connector: Any,
        schema: str = "etl_state",
        table: str = "__dpone__loads",
        *,
        database: str | None = None,
        provisioning: str = "runtime",
    ) -> None:
        self.connector = connector
        self.database = database
        self.provisioning = provisioning
        self.schema = schema
        self.table = table
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return _qualified_name(self.connector, self.database, self.schema, self.table)

    def create_load_table(self) -> None:
        if self._table_created:
            return
        if self.provisioning == "external":
            require_external_table_shape(
                self.connector,
                database=self.database,
                schema=self.schema,
                table=self.table,
                contract=LOAD_AUDIT_CONTRACT,
            )
            self._table_created = True
            return
        self.connector.execute_query(*_ensure_schema_statement(self.database, self.schema))
        self.connector.execute_query(
            render_load_audit_create_sql(
                fq_table=self.fq_table,
                object_id=_object_id(self.database, self.schema, self.table),
                constraint_token=self.table,
            )
        )
        self.connector.execute_query(
            load_audit_additive_ddl(
                fq_table=self.fq_table,
                object_id=_object_id(self.database, self.schema, self.table),
            )
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
            f"""
            MERGE {self.fq_table} AS target
            USING (SELECT ? AS load_id) AS source
            ON target.load_id = source.load_id
            WHEN MATCHED THEN UPDATE SET
                status = ?, staged_at = ?, committed_at = ?, failed_at = ?,
                extracted_rows = ?, staged_rows = ?, inserted_rows = ?, updated_rows = ?, loaded_rows = ?,
                error_message = ?, artifact_uri = ?, deleted_rows = ?, reactivated_rows = ?,
                unchanged_rows = ?, soft_deleted_rows = ?, hard_deleted_rows = ?, active_rows = ?,
                total_rows = ?, commit_receipt_id = ?, commit_outcome = ?,
                __dpone__loaded_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                run_id, load_id, status, process_name, source_schema, source_table,
                target_schema, target_table, strategy, started_at, staged_at, committed_at,
                failed_at, extracted_rows, staged_rows, inserted_rows, updated_rows,
                loaded_rows, error_message, artifact_uri, deleted_rows, reactivated_rows,
                unchanged_rows, soft_deleted_rows, hard_deleted_rows, active_rows, total_rows,
                commit_receipt_id, commit_outcome, __dpone__loaded_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME()
            );
            """,
            self._merge_params(record),
        )

    @staticmethod
    def _merge_params(record: LoadAuditRecord) -> tuple[Any, ...]:
        return (
            record.load_id,
            record.status,
            record.staged_at,
            record.committed_at,
            record.failed_at,
            record.extracted_rows,
            record.staged_rows,
            record.inserted_rows,
            record.updated_rows,
            record.loaded_rows,
            record.error_message,
            record.artifact_uri,
            *load_audit_exact_metric_params(record),
            *load_audit_insert_params(record),
            *load_audit_exact_metric_params(record),
        )


def _qualified_name(connector: Any, database: str | None, schema: str, table: str) -> str:
    try:
        return str(connector.qualified_name(schema, table, database=database))
    except TypeError:
        label = f"{database}.{schema}" if database else schema
        return str(connector.qualified_name(label, table))


def _ensure_schema_statement(database: str | None, schema: str) -> tuple[str, tuple[str, ...]]:
    name = MSSQLObjectName.from_parts(database=database, schema=schema, table="__dpone_schema_probe")
    return mssql_ensure_schema_statement(name.schema_label)


def _object_id(database: str | None, schema: str, table: str) -> str:
    prefix = f"{database}." if database else ""
    return f"{prefix}{schema}.{table}"


__all__ = ["MSSQLLoadAuditStorage", "MSSQLRunStateStorage"]
