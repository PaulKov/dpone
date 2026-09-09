"""Shared mapping helpers for SQL-backed run state storage."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from dpone.runtime.state.models import RunState, RunStateStatus

_IDENTITY_CONTRACT_VERSION = "dpone.run-state.v1"


def run_state_key(dag_id: str, process_name: str, execution_date: datetime) -> bytes:
    """Build an index-width-safe identity while retaining raw collision checks."""

    execution_date = canonical_run_execution_date(execution_date)
    payload = json.dumps(
        {
            "contract": _IDENTITY_CONTRACT_VERSION,
            "dag_id": dag_id,
            "execution_date": execution_date.isoformat(timespec="microseconds"),
            "process_name": process_name,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).digest()


def canonical_run_execution_date(value: datetime) -> datetime:
    """Normalize an instant to the UTC-naive representation stored in datetime2."""

    return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo is not None else value


def run_process_name(run_state: RunState) -> str:
    """Build a stable process dimension when a legacy caller has no pipeline id."""

    return run_state.process_name or (
        f"{run_state.source_schema}.{run_state.source_table}->{run_state.target_schema}.{run_state.target_table}"
    )


def run_state_merge_params(
    run_state: RunState,
    identity: bytes,
    process_name: str,
    execution_date: datetime,
) -> tuple[Any, ...]:
    """Bind collision proof, update values, and insert values in SQL order."""

    return (
        identity,
        run_state.dag_id,
        process_name,
        execution_date,
        identity,
        run_state.dag_id,
        process_name,
        execution_date,
        run_state.state.value,
        run_state.ended_at,
        run_state.duration_min,
        run_state.error_message,
        run_state.rows_read,
        run_state.rows_written,
        run_state.rows_updated,
        run_state.rows_deleted,
        run_state.load_strategy,
        identity,
        run_state.dag_id,
        process_name,
        run_state.source_schema,
        run_state.source_table,
        run_state.target_schema,
        run_state.target_table,
        run_state.load_strategy,
        execution_date,
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


def legacy_run_state_merge_params(run_state: RunState, execution_date: datetime) -> tuple[Any, ...]:
    """Preserve the v1 `(dag_id, execution_date)` store during migration."""

    return (
        run_state.dag_id,
        execution_date,
        run_state.state.value,
        run_state.ended_at,
        run_state.duration_min,
        run_state.error_message,
        run_state.rows_read,
        run_state.rows_written,
        run_state.rows_updated,
        run_state.rows_deleted,
        run_state.load_strategy,
        run_state.dag_id,
        run_state.source_schema,
        run_state.source_table,
        run_state.target_schema,
        run_state.target_table,
        run_state.load_strategy,
        execution_date,
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


def legacy_run_state_merge_sql(fq_table: str) -> str:
    """Render the frozen v1 run-state mutation for existing installations."""

    return f"""
        MERGE {fq_table} AS target
        USING (SELECT ? AS dag_id, ? AS execution_date) AS source
        ON target.dag_id = source.dag_id AND target.execution_date = source.execution_date
        WHEN MATCHED THEN UPDATE SET
            state = ?, ended_at = ?, duration_min = ?, error_message = ?, rows_read = ?, rows_written = ?,
            rows_updated = ?, rows_deleted = ?, load_strategy = ?, __dpone__updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (
            dag_id, source_schema, source_table, target_schema, target_table, load_strategy,
            execution_date, state, started_at, ended_at, duration_min, error_message,
            rows_read, rows_written, rows_updated, rows_deleted, __dpone__loaded_at, __dpone__updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME());
    """


def run_state_from_row(row: dict[str, Any]) -> RunState:
    """Map one SQL result row into the public RunState contract."""

    return RunState(
        id=row.get("id"),
        dag_id=row["dag_id"],
        process_name=row.get("process_name"),
        source_schema=row["source_schema"],
        source_table=row["source_table"],
        target_schema=row["target_schema"],
        target_table=row["target_table"],
        load_strategy=row["load_strategy"],
        execution_date=row["execution_date"],
        state=RunStateStatus(row["state"]),
        started_at=row["started_at"],
        ended_at=row.get("ended_at"),
        duration_min=row.get("duration_min"),
        error_message=row.get("error_message"),
        rows_read=row.get("rows_read"),
        rows_written=row.get("rows_written"),
        rows_updated=row.get("rows_updated"),
        rows_deleted=row.get("rows_deleted"),
    )


__all__ = [
    "canonical_run_execution_date",
    "legacy_run_state_merge_params",
    "legacy_run_state_merge_sql",
    "run_process_name",
    "run_state_from_row",
    "run_state_key",
    "run_state_merge_params",
]
