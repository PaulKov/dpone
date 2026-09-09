"""Canonical SQL Server storage for route and governance step evidence."""

from __future__ import annotations

import json
from typing import Any

from dpone.runtime.runtime_throughput import enrich_step_details_with_throughput


class MSSQLLoadStepAuditStorage:
    """Store route and load-governance step evidence in one SQL relation."""

    def __init__(
        self,
        connector: Any,
        schema: str = "etl_state",
        table: str = "__dpone__load_steps",
    ) -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return self.connector.qualified_name(self.schema, self.table)

    def create_step_table(self) -> None:
        """Create or migrate the shared route/governance audit relation."""

        if self._table_created:
            return
        self.connector.execute_query(
            f"IF SCHEMA_ID(?) IS NULL EXEC('CREATE SCHEMA {self.connector.quote_identifier(self.schema)}')",
            (self.schema,),
        )
        self.connector.execute_query(
            f"""
            IF OBJECT_ID(N'{self.schema}.{self.table}', N'U') IS NULL
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
            """
        )
        self.connector.execute_query(
            f"""
            IF COL_LENGTH(N'{self.schema}.{self.table}', 'error_message') IS NULL
            ALTER TABLE {self.fq_table} ADD error_message nvarchar(max) NULL
            """
        )
        self.connector.execute_query(
            f"""
            IF EXISTS (
                SELECT 1
                FROM sys.columns
                WHERE object_id = OBJECT_ID(N'{self.schema}.{self.table}')
                  AND name = 'finished_at'
                  AND is_nullable = 0
            )
            ALTER TABLE {self.fq_table} ALTER COLUMN finished_at datetime2 NULL
            """
        )
        self._table_created = True

    def record_step(self, record: Any) -> None:
        """Persist either governance ``details`` or route ``details_json``."""

        self.create_step_table()
        self.connector.execute_query(
            f"""
            INSERT INTO {self.fq_table} (
                run_id, load_id, step_id, phase, kind, status, started_at,
                finished_at, error_message, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


__all__ = ["MSSQLLoadStepAuditStorage"]
