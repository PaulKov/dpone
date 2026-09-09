"""Live CDC runtime adapters for source readers and analytical sinks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dpone.runtime.cdc.base import CDCBatch, CDCChange
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimeStream

_CDC_COLUMNS: tuple[str, ...] = (
    "dpone_cdc_stream_id",
    "dpone_cdc_route_id",
    "dpone_cdc_pipeline_name",
    "dpone_cdc_source_schema",
    "dpone_cdc_source_table",
    "dpone_cdc_operation",
    "dpone_cdc_position",
    "dpone_cdc_sequence",
    "dpone_cdc_event_hash",
    "dpone_cdc_unique_key_hash",
    "dpone_cdc_unique_key_json",
    "dpone_cdc_payload_json",
    "dpone_cdc_before_json",
    "dpone_cdc_deleted",
    "dpone_cdc_ingested_at",
)


@dataclass(frozen=True, slots=True)
class ClickHouseCdcApplyPlan:
    """Resolved ClickHouse CDC log target for one runtime stream."""

    database: str
    table: str
    apply_mode: str = "clickhouse_append_cdc_log"

    @classmethod
    def from_stream(cls, stream: CdcRuntimeStream, *, default_database: str) -> ClickHouseCdcApplyPlan:
        database, table = _split_dataset(stream.target_dataset, default_database=default_database)
        return cls(database=database, table=table)

    @property
    def qualified_table(self) -> str:
        return f"{_quote_identifier(self.database)}.{_quote_identifier(self.table)}"

    @property
    def artifact_uri(self) -> str:
        return f"clickhouse://{self.database}.{self.table}"


class ClickHouseCdcSinkApplier:
    """Append normalized CDC events to a durable ClickHouse CDC log table."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def apply(self, *, stream: CdcRuntimeStream, batch: CDCBatch) -> CdcApplyReceipt:
        plan = ClickHouseCdcApplyPlan.from_stream(stream, default_database=str(self._connector.database))
        try:
            self._ensure_table(plan)
            rows = [cdc_event_row(stream, change) for change in batch.changes]
            existing_hashes = self._existing_event_hashes(plan, rows)
            rows_to_insert = [row for row in rows if str(row["dpone_cdc_event_hash"]) not in existing_hashes]
            if rows_to_insert:
                self._insert_rows(plan, rows_to_insert)
            return _success_receipt(
                plan=plan, rows=rows_to_insert, duplicate_events_skipped=len(rows) - len(rows_to_insert)
            )
        except Exception as exc:
            return CdcApplyReceipt(
                passed=False,
                durable=False,
                rows_applied=0,
                rows_deleted=0,
                blockers=("clickhouse_cdc_apply.failed",),
                warnings=tuple(),
                metrics={
                    "sink": stream.sink,
                    "target_dataset": stream.target_dataset,
                    "apply_mode": plan.apply_mode,
                    "error": str(exc),
                },
                artifact_uri=plan.artifact_uri,
            )

    def _ensure_table(self, plan: ClickHouseCdcApplyPlan) -> None:
        self._connector.execute_query(
            f"""
CREATE TABLE IF NOT EXISTS {plan.qualified_table} (
    `dpone_cdc_stream_id` String,
    `dpone_cdc_route_id` String,
    `dpone_cdc_pipeline_name` String,
    `dpone_cdc_source_schema` String,
    `dpone_cdc_source_table` String,
    `dpone_cdc_operation` LowCardinality(String),
    `dpone_cdc_position` String,
    `dpone_cdc_sequence` String,
    `dpone_cdc_event_hash` String,
    `dpone_cdc_unique_key_hash` String,
    `dpone_cdc_unique_key_json` String,
    `dpone_cdc_payload_json` String,
    `dpone_cdc_before_json` String,
    `dpone_cdc_deleted` UInt8,
    `dpone_cdc_ingested_at` DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (`dpone_cdc_stream_id`, `dpone_cdc_unique_key_hash`, `dpone_cdc_position`, `dpone_cdc_event_hash`)
""".strip()
        )

    def _insert_rows(self, plan: ClickHouseCdcApplyPlan, rows: Sequence[Mapping[str, Any]]) -> None:
        columns_sql = ", ".join(_quote_identifier(column) for column in _CDC_COLUMNS)
        self._connector.connection.execute(f"INSERT INTO {plan.qualified_table} ({columns_sql}) VALUES", list(rows))

    def _existing_event_hashes(self, plan: ClickHouseCdcApplyPlan, rows: Sequence[Mapping[str, Any]]) -> set[str]:
        event_hashes = [str(row["dpone_cdc_event_hash"]) for row in rows]
        if not event_hashes:
            return set()
        get_records = getattr(self._connector, "get_records", None)
        if get_records is None:
            return set()
        values = ", ".join(_quote_literal(value) for value in event_hashes)
        records = get_records(
            f"""
SELECT dpone_cdc_event_hash
FROM {plan.qualified_table}
WHERE dpone_cdc_stream_id = {_quote_literal(str(rows[0]["dpone_cdc_stream_id"]))}
  AND dpone_cdc_event_hash IN ({values})
""".strip(),
            as_dict=True,
        )
        return {str(row.get("dpone_cdc_event_hash")) for row in records if row.get("dpone_cdc_event_hash") is not None}


def cdc_event_row(stream: CdcRuntimeStream, change: CDCChange) -> dict[str, Any]:
    """Render one CDC change as a ClickHouse log-table row."""

    unique_key_json = _canonical_json(_unique_key_payload(change, stream.unique_key))
    return {
        "dpone_cdc_stream_id": stream.stream_id,
        "dpone_cdc_route_id": stream.route_id,
        "dpone_cdc_pipeline_name": stream.pipeline_name,
        "dpone_cdc_source_schema": change.source_schema,
        "dpone_cdc_source_table": change.source_table,
        "dpone_cdc_operation": change.operation.value,
        "dpone_cdc_position": change.position,
        "dpone_cdc_sequence": "" if change.sequence is None else str(change.sequence),
        "dpone_cdc_event_hash": cdc_event_hash(change, unique_key=stream.unique_key),
        "dpone_cdc_unique_key_hash": _sha256(unique_key_json),
        "dpone_cdc_unique_key_json": unique_key_json,
        "dpone_cdc_payload_json": _canonical_json(change.data),
        "dpone_cdc_before_json": _canonical_json(change.before or {}),
        "dpone_cdc_deleted": 1 if change.is_delete else 0,
        "dpone_cdc_ingested_at": datetime.now(UTC),
    }


def cdc_event_hash(change: CDCChange, *, unique_key: Sequence[str]) -> str:
    """Return a stable event hash for idempotent sink-side CDC applies."""

    payload = {
        "operation": change.operation.value,
        "position": change.position,
        "source_schema": change.source_schema,
        "source_table": change.source_table,
        "transaction_id": change.transaction_id,
        "sequence": change.sequence,
        "unique_key": _unique_key_payload(change, unique_key),
        "data": dict(change.data),
        "before": dict(change.before or {}),
        "metadata": dict(change.metadata),
    }
    return _sha256(_canonical_json(payload))


def _success_receipt(
    *,
    plan: ClickHouseCdcApplyPlan,
    rows: Sequence[Mapping[str, Any]],
    duplicate_events_skipped: int,
) -> CdcApplyReceipt:
    deleted = sum(1 for row in rows if int(row.get("dpone_cdc_deleted", 0) or 0) == 1)
    return CdcApplyReceipt(
        passed=True,
        durable=True,
        rows_applied=len(rows),
        rows_deleted=deleted,
        blockers=tuple(),
        warnings=tuple(),
        metrics={
            "sink": "clickhouse",
            "target_dataset": f"{plan.database}.{plan.table}",
            "apply_mode": plan.apply_mode,
            "artifact_rows": len(rows),
            "duplicate_events_skipped": duplicate_events_skipped,
            "batch_hash": _batch_hash(rows),
        },
        artifact_uri=plan.artifact_uri,
    )


def _batch_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    return _sha256(_canonical_json([row["dpone_cdc_event_hash"] for row in rows]))


def _unique_key_payload(change: CDCChange, unique_key: Sequence[str]) -> dict[str, Any]:
    return {column: change.data.get(column) for column in unique_key}


def _split_dataset(value: str, *, default_database: str) -> tuple[str, str]:
    parts = [part for part in value.split(".") if part]
    if len(parts) == 1:
        return default_database, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"ClickHouse CDC target dataset must be table or database.table: {value!r}")


def _quote_identifier(value: str) -> str:
    if not value:
        raise ValueError("ClickHouse identifier cannot be empty")
    return "`" + value.replace("`", "``") + "`"


def _quote_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


__all__ = [
    "ClickHouseCdcApplyPlan",
    "ClickHouseCdcSinkApplier",
    "cdc_event_hash",
    "cdc_event_row",
]
