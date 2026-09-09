"""Dependency-leaf records for MSSQL failure reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_primitives import MssqlImageKeyColumn


@dataclass(frozen=True, order=True, slots=True)
class MssqlFailureJournalReference:
    """Immutable admitted identity needed to reconcile one failed model."""

    model_unique_id: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    strategy_authority_json: str
    strategy_authority_sha256: str
    baseline_receipt_sha256: str
    baseline_kind: str
    baseline_receipt_json: str
    baseline_status: str
    image_key_columns: tuple[MssqlImageKeyColumn, ...]
    target_resource_id: str
    publication_database: str
    publication_target_table: str
    publication_scope_id: str
    target_predecessor_generation_id: str
    scope_predecessor_operation_id: str | None
    fencing_epoch: int
    replaces_failed_operation_id: str | None
    owner_id: str
    status: str
    persisted_outcome: str | None = None
    persisted_evidence_sha256: str | None = None


__all__ = ["MssqlFailureJournalReference"]
