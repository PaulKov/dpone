"""ClickHouse-backed load and load-step audit storage."""

from __future__ import annotations

import json
from typing import Any

from dpone.governance.hooks import LoadStepAuditRecord
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.runtime_throughput import enrich_step_details_with_throughput
from dpone.runtime.state.clickhouse_state_design import ClickHouseStateTableDesign


class ClickHouseLoadAuditStorage:
    """Append-friendly canonical load state table for ClickHouse."""

    def __init__(
        self,
        connector: Any,
        schema: str = "etl_state",
        table: str = "__dpone__loads",
        *,
        cluster: Any | None = None,
        engine: str | None = None,
        table_design: ClickHouseStateTableDesign | None = None,
    ) -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._design = table_design or ClickHouseStateTableDesign.from_config(cluster=cluster, engine=engine)
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return f"{_quote(self.schema)}.{_quote(self.table)}"

    def create_load_table(self) -> None:
        if self._table_created:
            return
        self._design.emit_warnings()
        self.connector.execute_query(
            f"CREATE DATABASE IF NOT EXISTS {_quote(self.schema)}{self._design.cluster_clause}"
        )
        self.connector.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.fq_table}{self._design.cluster_clause} (
                run_id String,
                load_id String,
                status LowCardinality(String),
                process_name Nullable(String),
                source_schema String,
                source_table String,
                target_schema String,
                target_table String,
                strategy LowCardinality(String),
                started_at DateTime64(6, 'UTC'),
                staged_at Nullable(DateTime64(6, 'UTC')),
                committed_at Nullable(DateTime64(6, 'UTC')),
                failed_at Nullable(DateTime64(6, 'UTC')),
                extracted_rows Nullable(Int64),
                staged_rows Nullable(Int64),
                inserted_rows Nullable(Int64),
                updated_rows Nullable(Int64),
                loaded_rows Nullable(Int64),
                error_message Nullable(String),
                artifact_uri Nullable(String),
                __dpone__loaded_at DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC')
            )
            ENGINE = {self._design.engine_sql}
            ORDER BY load_id
            """
        )
        self._table_created = True

    def record_load_started(self, record: LoadAuditRecord) -> None:
        self._insert(record)

    def record_load_staged(self, record: LoadAuditRecord) -> None:
        self._insert(record)

    def record_load_committed(self, record: LoadAuditRecord) -> None:
        self._insert(record)

    def record_load_failed(self, record: LoadAuditRecord) -> None:
        self._insert(record)

    def _insert(self, record: LoadAuditRecord) -> None:
        self.create_load_table()
        self.connector.execute_query(
            f"""
            INSERT INTO {self.fq_table} (
                run_id, load_id, status, process_name, source_schema, source_table,
                target_schema, target_table, strategy, started_at, staged_at, committed_at,
                failed_at, extracted_rows, staged_rows, inserted_rows, updated_rows,
                loaded_rows, error_message, artifact_uri
            ) VALUES
            """,
            [_load_params(record)],
        )


class ClickHouseLoadStepAuditStorage:
    """Records hook, projection, quality and finalization step statuses."""

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
        self._design = table_design or ClickHouseStateTableDesign.from_config(cluster=cluster, engine=engine)
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return f"{_quote(self.schema)}.{_quote(self.table)}"

    def create_step_table(self) -> None:
        if self._table_created:
            return
        self._design.emit_warnings()
        self.connector.execute_query(
            f"CREATE DATABASE IF NOT EXISTS {_quote(self.schema)}{self._design.cluster_clause}"
        )
        self.connector.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.fq_table}{self._design.cluster_clause} (
                run_id String,
                load_id String,
                step_id String,
                phase LowCardinality(String),
                kind LowCardinality(String),
                status LowCardinality(String),
                started_at DateTime64(6, 'UTC'),
                finished_at Nullable(DateTime64(6, 'UTC')),
                error_message Nullable(String),
                details_json String DEFAULT '{{}}',
                __dpone__loaded_at DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC')
            )
            ENGINE = {self._design.engine_sql}
            ORDER BY (load_id, phase, step_id, started_at)
            """
        )
        self.connector.execute_query(
            f"ALTER TABLE {self.fq_table}{self._design.cluster_clause} "
            "ADD COLUMN IF NOT EXISTS details_json String DEFAULT '{}'"
        )
        self._table_created = True

    def record_step(self, record: LoadStepAuditRecord) -> None:
        self.create_step_table()
        self.connector.execute_query(
            f"""
            INSERT INTO {self.fq_table} (
                run_id, load_id, step_id, phase, kind, status, started_at,
                finished_at, error_message, details_json
            ) VALUES
            """,
            [
                (
                    record.run_id,
                    record.load_id,
                    record.step_id,
                    record.phase,
                    record.kind,
                    record.status,
                    record.started_at,
                    record.finished_at,
                    record.error_message,
                    _json(_step_details(record)),
                )
            ],
        )


def _load_params(record: LoadAuditRecord) -> tuple[Any, ...]:
    return (
        record.run_id,
        record.load_id,
        record.status,
        record.process_name,
        record.source_schema,
        record.source_table,
        record.target_schema,
        record.target_table,
        record.strategy,
        record.started_at,
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
    )


def _quote(identifier: str) -> str:
    return "`" + str(identifier).replace("`", "``") + "`"


def _json(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, default=str)


def _step_details(record: LoadStepAuditRecord) -> dict[str, Any]:
    return enrich_step_details_with_throughput(
        record.details,
        started_at=record.started_at,
        finished_at=record.finished_at,
        status=record.status,
    )


__all__ = ["ClickHouseLoadAuditStorage", "ClickHouseLoadStepAuditStorage"]
