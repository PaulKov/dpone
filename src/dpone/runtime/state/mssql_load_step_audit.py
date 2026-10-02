"""Canonical SQL Server storage for route and governance step evidence."""

from __future__ import annotations

import json
from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName, quote_mssql_identifier
from dpone.runtime.runtime_throughput import enrich_step_details_with_throughput
from dpone.runtime.state.mssql_contract import (
    MssqlColumnShape,
    exact_external_table_contract,
    require_external_table_shape,
)

_STEP_SHAPES = (
    MssqlColumnShape("run_id", "nvarchar", 128, None, None, False),
    MssqlColumnShape("load_id", "nvarchar", 128, None, None, False),
    MssqlColumnShape("step_id", "nvarchar", 512, None, None, False),
    MssqlColumnShape("phase", "nvarchar", 128, None, None, False),
    MssqlColumnShape("kind", "nvarchar", 256, None, None, False),
    MssqlColumnShape("status", "nvarchar", 64, None, None, False),
    MssqlColumnShape("started_at", "datetime2", 8, None, 7, False),
    MssqlColumnShape("finished_at", "datetime2", 8, None, 7, True),
    MssqlColumnShape("error_message", "nvarchar", -1, None, None, True),
    MssqlColumnShape("details_json", "nvarchar", -1, None, None, False),
    MssqlColumnShape("__dpone__loaded_at", "datetime2", 8, None, 7, False),
)
LOAD_STEP_AUDIT_CONTRACT = exact_external_table_contract(
    columns=frozenset(shape.name for shape in _STEP_SHAPES), unique_indexes=(), shapes=_STEP_SHAPES
)


class MSSQLLoadStepAuditStorage:
    """Store route and load-governance step evidence in one SQL relation."""

    def __init__(
        self,
        connector: Any,
        schema: str = "etl_state",
        table: str = "__dpone__load_steps",
        *,
        database: str | None = None,
        provisioning: str = "runtime",
    ) -> None:
        if provisioning not in {"runtime", "external"}:
            raise ValueError("mssql_step_audit_provisioning_invalid")
        self._name = MSSQLObjectName.from_parts(database=database, schema=schema, table=table)
        self.connector = connector
        self.database = self._name.database
        self.schema = self._name.schema
        self.table = self._name.table
        self.provisioning = provisioning
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return self._name.quoted()

    def create_step_table(self) -> None:
        """Admit an external relation read-only, or provision the legacy runtime table."""

        if self._table_created:
            return
        if self.provisioning == "external":
            require_external_table_shape(
                self.connector,
                database=self.database,
                schema=self.schema,
                table=self.table,
                contract=LOAD_STEP_AUDIT_CONTRACT,
            )
            self._table_created = True
            return
        prefix = self._name.execution_scope_prefix
        self.connector.execute_query(
            f"IF NOT EXISTS (SELECT 1 FROM {prefix}sys.schemas WHERE name = ?) EXEC {prefix}sys.sp_executesql ?",
            (self.schema, f"CREATE SCHEMA {quote_mssql_identifier(self.schema)}"),
        )
        self.connector.execute_query(
            f"""
            IF OBJECT_ID(?, N'U') IS NULL
            CREATE TABLE {self.fq_table} (
                run_id nvarchar(64) NOT NULL,
                load_id nvarchar(64) NOT NULL,
                step_id nvarchar(256) NOT NULL,
                phase nvarchar(64) NOT NULL,
                kind nvarchar(128) NOT NULL,
                status nvarchar(32) NOT NULL,
                started_at datetime2 NOT NULL,
                finished_at datetime2 NULL,
                error_message nvarchar(max) NULL,
                details_json nvarchar(max) NOT NULL,
                __dpone__loaded_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
            )
            """,
            (self.fq_table,),
        )
        self.connector.execute_query(
            f"""
            IF COL_LENGTH(?, 'error_message') IS NULL
            ALTER TABLE {self.fq_table} ADD error_message nvarchar(max) NULL
            """,
            (self.fq_table,),
        )
        self.connector.execute_query(
            f"""
            IF EXISTS (
                SELECT 1
                FROM {prefix}sys.columns
                WHERE object_id = OBJECT_ID(?)
                  AND name = 'finished_at'
                  AND is_nullable = 0
            )
            ALTER TABLE {self.fq_table} ALTER COLUMN finished_at datetime2 NULL
            """,
            (self.fq_table,),
        )
        self._table_created = True

    def record_load_step(self, record: Any) -> None:
        """Route publisher entrypoint sharing the admitted governance relation."""

        self.record_step(record)

    def record_step(self, record: Any) -> None:
        """Persist either governance ``details`` or route ``details_json``."""

        self.create_step_table()
        self.connector.execute_query(
            f"""
            INSERT INTO {self.fq_table} (
                run_id, load_id, step_id, phase, kind, status, started_at,
                finished_at, error_message, details_json, __dpone__loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
            """,
            (
                record.run_id,
                record.load_id,
                record.step_id,
                record.phase,
                record.kind,
                record.status,
                record.started_at,
                record.finished_at,
                getattr(record, "error_message", None),
                _step_details_json(record),
            ),
        )


def _step_details_json(record: Any) -> str:
    details = getattr(record, "details", None)
    if details is None:
        details = getattr(record, "details_json", {})
    enriched = enrich_step_details_with_throughput(
        details,
        started_at=record.started_at,
        finished_at=record.finished_at,
        status=record.status,
    )
    return json.dumps(
        enriched,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


__all__ = ["MSSQLLoadStepAuditStorage", "LOAD_STEP_AUDIT_CONTRACT"]
