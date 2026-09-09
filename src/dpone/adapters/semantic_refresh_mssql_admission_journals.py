"""Exact journal replay validation for MSSQL semantic-refresh admission."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from dpone.ports.semantic_refresh_mssql_primitives import mssql_image_key_columns_json

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql import MssqlAdmissionRequest


class AdmissionJournalCursor(Protocol):
    """DB-API cursor used by exact journal replay validation."""

    def execute(self, sql: str, *parameters: object) -> AdmissionJournalCursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


class MssqlAdmissionAuthorityConflict(RuntimeError):
    """Internal query-boundary conflict translated by the state adapter."""


@dataclass(frozen=True, slots=True)
class MssqlAdmissionPredecessorSnapshot:
    """Locked ClickHouse-head mirror captured before producer admission."""

    target_generation: int
    target_generation_id: str
    target_uuid: object
    target_operation_id: str
    scope_revision: int | None
    scope_operation_id: str | None
    checkpoint_sha256: str | None
    checkpoint_operation_id: str | None
    checkpoint_version: int | None


def require_existing_journals(
    cursor: AdmissionJournalCursor,
    request: MssqlAdmissionRequest,
    snapshots: Mapping[str, MssqlAdmissionPredecessorSnapshot],
    *,
    table: Callable[[str], str],
) -> None:
    """Require exact replay of every canonical PREPARING journal."""

    for journal in request.journals:
        cursor.execute(
            f"""
SELECT model_unique_id, operation_plan_sha256, attempt_binding_sha256,
       strategy_authority_json, strategy_authority_sha256,
       baseline_receipt_sha256, baseline_kind,
       baseline_receipt_json, baseline_status,
       image_key_columns_json, workflow_id,
       target_resource_id, publication_database, publication_target_table,
       publication_scope_id, target_predecessor_generation_id,
       scope_predecessor_operation_id, predecessor_target_generation,
       LOWER(CONVERT(char(36), predecessor_target_uuid)), predecessor_target_operation_id,
       predecessor_scope_revision, predecessor_checkpoint_sha256,
       predecessor_checkpoint_operation_id, predecessor_checkpoint_version,
       fencing_epoch, replaces_failed_operation_id, owner_id, journal_version, status
FROM {table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id = ?;
""".strip(),
            journal.operation_id,
        )
        snapshot = snapshots[journal.operation_id]
        expected = (
            journal.model_unique_id,
            journal.operation_plan_sha256,
            journal.attempt_binding_sha256,
            journal.strategy_authority_json,
            journal.strategy_authority_sha256,
            journal.baseline_receipt_sha256,
            journal.baseline_kind,
            journal.baseline_receipt_json,
            journal.baseline_status,
            mssql_image_key_columns_json(journal.image_key_columns),
            request.workflow_id,
            journal.target_resource_id,
            journal.publication_database,
            journal.publication_target_table,
            journal.publication_scope_id,
            journal.target_predecessor_generation_id,
            journal.scope_predecessor_operation_id,
            snapshot.target_generation,
            snapshot.target_uuid,
            snapshot.target_operation_id,
            snapshot.scope_revision,
            snapshot.checkpoint_sha256,
            snapshot.checkpoint_operation_id,
            snapshot.checkpoint_version,
            journal.fencing_epoch,
            journal.replaces_failed_operation_id,
            request.owner_id,
            1,
            "PREPARING",
        )
        if row(cursor) != expected:
            raise MssqlAdmissionAuthorityConflict("existing journal admission conflict")
    cursor.execute(
        f"SELECT COUNT_BIG(*) FROM {table('semantic_refresh_journals')} WHERE workflow_id = ?;",
        request.workflow_id,
    )
    if row(cursor) != (len(request.journals),):
        raise MssqlAdmissionAuthorityConflict("existing journal set admission conflict")


def row(cursor: AdmissionJournalCursor) -> tuple[Any, ...] | None:
    """Normalize one DB-API row."""

    value = cursor.fetchone()
    return None if value is None else tuple(value)
