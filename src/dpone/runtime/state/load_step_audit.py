"""SQL-backed load-step audit storage."""

from __future__ import annotations

import json
from typing import Any

from dpone.runtime.route_runtime_models import LoadStepAuditRecord
from dpone.runtime.runtime_throughput import enrich_step_details_with_throughput
from dpone.runtime.state.clickhouse_state_design import ClickHouseStateTableDesign
from dpone.runtime.state.mssql_load_step_audit import (
    MSSQLLoadStepAuditStorage as MSSQLGovernanceLoadStepAuditStorage,
)


class PostgresLoadStepAuditStorage:
    def __init__(self, connector: Any, schema: str = "etl_state", table: str = "__dpone__load_steps") -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return f"{_pg_identifier(self.schema)}.{_pg_identifier(self.table)}"

    def record_load_step(self, record: LoadStepAuditRecord) -> None:
        self._ensure_table()
        self.connector.execute_query(
            f"""
            INSERT INTO {self.fq_table} (
                run_id, load_id, step_id, phase, kind, status, started_at, finished_at, details_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            _params(record),
        )

    def _ensure_table(self) -> None:
        if self._table_created:
            return
        self.connector.execute_query(f"CREATE SCHEMA IF NOT EXISTS {_pg_identifier(self.schema)}")
        self.connector.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.fq_table} (
                run_id text NOT NULL,
                load_id text NOT NULL,
                step_id text NOT NULL,
                phase text NOT NULL,
                kind text NOT NULL,
                status text NOT NULL,
                started_at timestamp NOT NULL,
                finished_at timestamp NOT NULL,
                details_json jsonb NOT NULL,
                __dpone__loaded_at timestamp NOT NULL DEFAULT timezone('utc', now())
            )
            """
        )
        self._table_created = True


class MSSQLLoadStepAuditStorage:
    """Route-runtime adapter over the canonical SQL Server step store."""

    def __init__(self, connector: Any, schema: str = "etl_state", table: str = "__dpone__load_steps") -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._storage = MSSQLGovernanceLoadStepAuditStorage(connector, schema=schema, table=table)

    @property
    def fq_table(self) -> str:
        return self._storage.fq_table

    def record_load_step(self, record: LoadStepAuditRecord) -> None:
        self._storage.record_step(record)


class ClickHouseLoadStepAuditStorage:
    def __init__(
        self,
        connector: Any,
        schema: str = "etl_state",
        table: str = "__dpone__load_steps",
        *,
        cluster: Any | None = None,
        engine: str | None = None,
        table_design: ClickHouseStateTableDesign | None = None,
    ) -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        default_engine = engine if engine is not None or cluster is not None else "MergeTree"
        self._design = table_design or ClickHouseStateTableDesign.from_config(cluster=cluster, engine=default_engine)
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return f"{_ch_identifier(self.schema)}.{_ch_identifier(self.table)}"

    def record_load_step(self, record: LoadStepAuditRecord) -> None:
        self._ensure_table()
        self.connector.execute_query(
            f"""
            INSERT INTO {self.fq_table} (
                run_id, load_id, step_id, phase, kind, status, started_at, finished_at, details_json
            ) VALUES
            """,
            [_params(record)],
        )

    def _ensure_table(self) -> None:
        if self._table_created:
            return
        self._design.emit_warnings()
        self.connector.execute_query(
            f"CREATE DATABASE IF NOT EXISTS {_ch_identifier(self.schema)}{self._design.cluster_clause}"
        )
        self.connector.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.fq_table}{self._design.cluster_clause} (
                run_id String,
                load_id String,
                step_id String,
                phase String,
                kind String,
                status String,
                started_at DateTime64(6, 'UTC'),
                finished_at DateTime64(6, 'UTC'),
                details_json String,
                __dpone__loaded_at DateTime64(6, 'UTC') DEFAULT now64(6)
            )
            ENGINE = {self._design.engine_sql}
            ORDER BY (load_id, started_at, step_id)
            """
        )
        self._table_created = True


def _params(record: LoadStepAuditRecord) -> tuple[Any, ...]:
    details = enrich_step_details_with_throughput(
        record.details_json,
        started_at=record.started_at,
        finished_at=record.finished_at,
        status=record.status,
    )
    return (
        record.run_id,
        record.load_id,
        record.step_id,
        record.phase,
        record.kind,
        record.status,
        record.started_at,
        record.finished_at,
        json.dumps(details, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )


def _pg_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _ch_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


__all__ = [
    "ClickHouseLoadStepAuditStorage",
    "MSSQLLoadStepAuditStorage",
    "PostgresLoadStepAuditStorage",
]
