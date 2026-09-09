"""Lightweight runtime state models.

Split from ``run_state.py`` so metadata-only and unit-test code can use the
state model classes without importing BigQuery runtime dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class RunStateStatus(Enum):
    """Execution statuses for ETL runtime runs."""

    SUCCESS = "success"
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    SKIPPED = "skipped"
    RUNNING = "running"


@dataclass
class RunState:
    """Serializable state for a single DAG execution."""

    dag_id: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    load_strategy: str
    execution_date: datetime
    state: RunStateStatus
    started_at: datetime
    ended_at: datetime | None = None
    duration_min: float | None = None
    error_message: str | None = None
    id: int | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None
    process_name: str | None = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "dag_id": self.dag_id,
            "process_name": self.process_name,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "load_strategy": self.load_strategy,
            "execution_date": self.execution_date.isoformat(),
            "state": self.state.value,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_min": self.duration_min,
            "error_message": self.error_message,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
        }

    @classmethod
    def from_dict(cls, data: dict) -> RunState:
        return cls(
            id=data.get("id"),
            dag_id=data["dag_id"],
            process_name=data.get("process_name"),
            source_schema=data["source_schema"],
            source_table=data["source_table"],
            target_schema=data["target_schema"],
            target_table=data["target_table"],
            load_strategy=data["load_strategy"],
            execution_date=datetime.fromisoformat(data["execution_date"]),
            state=RunStateStatus(data["state"]),
            started_at=datetime.fromisoformat(data["started_at"]),
            ended_at=datetime.fromisoformat(data["ended_at"]) if data.get("ended_at") else None,
            duration_min=data.get("duration_min"),
            error_message=data.get("error_message"),
            rows_read=data.get("rows_read"),
            rows_written=data.get("rows_written"),
            rows_updated=data.get("rows_updated"),
            rows_deleted=data.get("rows_deleted"),
        )
