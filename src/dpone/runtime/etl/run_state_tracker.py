"""Run state lifecycle helpers for ETLProcessor.

This module keeps persistence/update logic for runtime run-state out of the main
ETL processor orchestration flow.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from dpone.runtime.state import RunState, RunStateStatus

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class RunStateStoragePort(Protocol):
    """Minimal persistence port required by the runtime run-state tracker."""

    def save_run_state(self, state: RunState) -> None: ...

    def update_run_state(self, state: RunState) -> None: ...


@dataclass(slots=True)
class RunStateTracker:
    """Persists runtime execution state for a single processor run.

    The tracker is intentionally lightweight and stateful only for the duration
    of a single :meth:`ETLProcessor.run` call.
    """

    storage: RunStateStoragePort | None = None
    run_state: RunState | None = None
    started_at: datetime | None = None

    def start(
        self,
        load_config: LoadConfig,
        dag_id: str | None,
        execution_date: datetime | None,
        *,
        started_at: datetime | None = None,
    ) -> RunState | None:
        """Creates and persists an initial RUNNING state if storage is enabled."""

        self.started_at = started_at or datetime.now()
        if not (self.storage and dag_id and execution_date):
            self.run_state = None
            return None

        self.run_state = RunState(
            dag_id=dag_id,
            process_name=_process_name(load_config),
            source_schema=load_config.source_schema,
            source_table=load_config.source_table,
            target_schema=load_config.target_schema,
            target_table=load_config.target_table,
            load_strategy=load_config.load_strategy.value,
            execution_date=execution_date,
            state=RunStateStatus.RUNNING,
            started_at=self.started_at,
        )
        self.storage.save_run_state(self.run_state)
        return self.run_state

    def update_load_strategy(self, load_strategy: str) -> None:
        """Updates load strategy on the persisted state when runtime overrides it."""

        if self.run_state:
            self.run_state.load_strategy = load_strategy

    def mark_failed(self, result: Mapping[str, Any], exc: Exception) -> None:
        """Marks a run as failed and persists final metrics."""

        if not (self.storage and self.run_state and self.started_at):
            return
        ended_at = datetime.now()
        self.run_state.state = RunStateStatus.FAILED
        self.run_state.ended_at = ended_at
        self.run_state.duration_min = round((ended_at - self.started_at).total_seconds() / 60, 2)
        self.run_state.error_message = str(exc)
        self._apply_result_metrics(result)
        self.storage.update_run_state(self.run_state)

    def mark_success(self, result: Mapping[str, Any]) -> None:
        """Marks a run as successful and persists final metrics."""

        if not (self.storage and self.run_state and self.started_at):
            return
        ended_at = datetime.now()
        self.run_state.state = RunStateStatus.SUCCESS
        self.run_state.ended_at = ended_at
        self.run_state.duration_min = round((ended_at - self.started_at).total_seconds() / 60, 2)
        self._apply_result_metrics(result)
        self.storage.update_run_state(self.run_state)

    def mark_success_preserving_commit(self, result: Mapping[str, Any]) -> bool:
        """Keep receipt-backed success authoritative if secondary evidence fails."""

        try:
            self.mark_success(result)
            return True
        except Exception:
            outcome = result.get("commit_outcome")
            value = getattr(outcome, "value", outcome)
            if not result.get("commit_receipt_id") or value not in {
                "committed",
                "committed_after_receipt_probe",
                "replay_suppressed",
            }:
                raise
            return False

    def _apply_result_metrics(self, result: Mapping[str, Any]) -> None:
        inserted = int(result.get("inserted_rows", 0) or 0)
        updated = int(result.get("updated_rows", 0) or 0)
        if not self.run_state:
            return
        self.run_state.rows_read = int(result.get("extracted_rows", 0) or 0)
        self.run_state.rows_written = inserted + updated
        self.run_state.rows_updated = updated
        self.run_state.rows_deleted = int(result.get("soft_deleted_rows", 0) or 0)


def _process_name(load_config: LoadConfig) -> str:
    """Resolve the collision-safe pipeline dimension attached by bootstrap."""

    identity = (getattr(load_config, "options", {}) or {}).get("state_identity")
    if isinstance(identity, Mapping):
        process = str(identity.get("process") or "").strip()
        if process:
            return process
    return (
        f"{load_config.source_schema}.{load_config.source_table}"
        f"->{load_config.target_schema}.{load_config.target_table}"
    )
