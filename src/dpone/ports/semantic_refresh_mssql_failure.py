"""Failure-reconciliation state boundaries for semantic refresh MSSQL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dpone.ports.semantic_refresh_mssql_failure_records import MssqlFailureJournalReference


@dataclass(frozen=True, slots=True)
class MssqlFailureContext:
    """Durable workflow and exact journal closure selected for terminalization."""

    workflow_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    guard_set_sha256: str
    journal_set_sha256: str
    workflow_guard_resource_id: str
    guard_count: int
    owner_id: str
    status: str
    terminal_summary_sha256: str | None
    terminal_summary_json: str | None
    journals: tuple[MssqlFailureJournalReference, ...]


@dataclass(frozen=True, order=True, slots=True)
class MssqlDerivedFailureOutcome:
    """Outcome derived from durable evidence by the application service."""

    operation_id: str
    attempt_binding_sha256: str
    mssql_outcome: str
    mssql_evidence_sha256: str


@dataclass(frozen=True, slots=True)
class MssqlFailureDecision:
    """Canonical evidence-derived terminal transition persisted atomically."""

    workflow_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    terminal_summary_sha256: str
    terminal_summary_json: str
    models: tuple[MssqlDerivedFailureOutcome, ...]


class SemanticRefreshMssqlFailureContextPort(Protocol):
    """Read the durable execution and journal closure."""

    def load_failure_context(self, workflow_id: str) -> MssqlFailureContext:
        """Return the exact PREPARING or replayable failed context."""


class SemanticRefreshMssqlFailureStatePort(Protocol):
    """Persist only an evidence-derived failure decision."""

    def persist_failure(self, decision: MssqlFailureDecision) -> None:
        """Atomically terminalize journals, workflow, reservation and guards."""


__all__ = [
    "MssqlDerivedFailureOutcome",
    "MssqlFailureContext",
    "MssqlFailureDecision",
    "MssqlFailureJournalReference",
    "SemanticRefreshMssqlFailureContextPort",
    "SemanticRefreshMssqlFailureStatePort",
]
