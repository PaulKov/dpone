"""Exact replay and protected-authority queries for MSSQL admission."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

from dpone.adapters.semantic_refresh_mssql_admission_journals import (
    MssqlAdmissionAuthorityConflict,
    MssqlAdmissionPredecessorSnapshot,
    require_existing_journals,
)
from dpone.adapters.semantic_refresh_mssql_prerequisite_queries import (
    MssqlPrerequisiteAuthorityConflict,
    MssqlPrerequisiteAuthorityQueries,
)
from dpone.adapters.semantic_refresh_mssql_target_head_queries import (
    MssqlTargetHeadAuthorityConflict,
    MssqlTargetHeadAuthorityQueries,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql import (
        MssqlAdmissionRequest,
        MssqlJournalPreparation,
    )
    from dpone.ports.semantic_refresh_mssql_recovery_heads import (
        MssqlAdmissionTargetHeadAuthority,
    )


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...


class MssqlAdmissionAuthorityQueries:
    """Read and compare admission authority without owning transaction policy."""

    def __init__(self, control_schema: str) -> None:
        self._control_schema = control_schema
        self._prerequisites = MssqlPrerequisiteAuthorityQueries(control_schema)
        self._target_heads = MssqlTargetHeadAuthorityQueries(control_schema)

    def acknowledge_existing(self, cursor: _Cursor, request: MssqlAdmissionRequest) -> bool:
        """Return true only for an exact durable replay of the full admission."""

        cursor.execute(
            f"""
SELECT workflow_execution_id, workflow_plan_sha256, workflow_execution_binding_sha256,
       canonical_authority_sha256,
       guard_set_sha256, journal_set_sha256, workflow_guard_resource_id, guard_count,
       controller_id, owner_id, status
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
            request.workflow_id,
        )
        execution = _row(cursor)
        if execution is None:
            return False
        expected = (
            request.workflow_execution_id,
            request.workflow_plan_sha256,
            request.workflow_execution_binding_sha256,
            request.canonical_authority_sha256,
            request.guard_set_sha256,
            request.journal_set_sha256,
            request.workflow_guard.resource_id,
            len(request.resource_guards) + 1,
            request.controller_id,
            request.owner_id,
            "PREPARING",
        )
        if execution != expected:
            raise MssqlAdmissionAuthorityConflict("existing workflow admission conflict")
        snapshots = self.require_current_authorities(cursor, request)
        self._require_existing_reservation(cursor, request)
        self._require_existing_guards(cursor, request)
        require_existing_journals(cursor, request, snapshots, table=self._table)
        self._require_existing_successor(cursor, request)
        return True

    def require_current_authorities(
        self,
        cursor: _Cursor,
        request: MssqlAdmissionRequest,
    ) -> Mapping[str, MssqlAdmissionPredecessorSnapshot]:
        """Bind current baseline and target-owner rows before any admission mutation."""

        cursor.execute(
            f"""
SELECT workflow_execution_id, authority_sha256, authority_json, status
FROM {self._table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
            request.workflow_execution_binding_sha256,
        )
        if _row(cursor) != (
            request.workflow_execution_id,
            request.canonical_authority_sha256,
            request.canonical_authority_json,
            "ACTIVE",
        ):
            raise MssqlAdmissionAuthorityConflict("canonical admission authority conflict")
        try:
            self._prerequisites.require_current_for_admission(
                cursor,
                request.prerequisite_authorities,
                workflow_execution_binding_sha256=request.workflow_execution_binding_sha256,
                workflow_execution_id=request.workflow_execution_id,
                workflow_plan_sha256=request.workflow_plan_sha256,
            )
        except MssqlPrerequisiteAuthorityConflict as exc:
            raise MssqlAdmissionAuthorityConflict(str(exc)) from exc
        snapshots: dict[str, MssqlAdmissionPredecessorSnapshot] = {}
        heads = {item.target_resource_id: item for item in request.target_heads}
        for journal in request.journals:
            cursor.execute(
                f"""
SELECT model_unique_id, baseline_kind, baseline_receipt_sha256,
       baseline_receipt_json, status, is_current
FROM {self._table("semantic_refresh_baselines")} WITH (UPDLOCK, HOLDLOCK)
WHERE target_resource_id = ?;
""".strip(),
                journal.target_resource_id,
            )
            if _row(cursor) != (
                journal.model_unique_id,
                journal.baseline_kind,
                journal.baseline_receipt_sha256,
                journal.baseline_receipt_json,
                journal.baseline_status,
                True,
            ):
                raise MssqlAdmissionAuthorityConflict("current baseline authority conflict")
            snapshots[journal.operation_id] = self._predecessor_snapshot(
                cursor,
                journal,
                heads[journal.target_resource_id],
            )
        for claim in request.target_owners:
            cursor.execute(
                f"""
SELECT model_unique_id, deployment_id, owner_generation, status
FROM {self._table("semantic_refresh_target_owners")} WITH (UPDLOCK, HOLDLOCK)
WHERE target_authority_id = ?;
""".strip(),
                claim.target_authority_id,
            )
            if _row(cursor) != (
                claim.model_unique_id,
                claim.deployment_id,
                claim.owner_generation,
                "ACTIVE",
            ):
                raise MssqlAdmissionAuthorityConflict("target owner authority conflict")
        return snapshots

    def _predecessor_snapshot(
        self,
        cursor: _Cursor,
        journal: MssqlJournalPreparation,
        head: MssqlAdmissionTargetHeadAuthority,
    ) -> MssqlAdmissionPredecessorSnapshot:
        try:
            target = self._target_heads.require_current(cursor, journal, head)
        except MssqlTargetHeadAuthorityConflict as exc:
            raise MssqlAdmissionAuthorityConflict(str(exc)) from exc
        cursor.execute(
            f"""
SELECT scope_revision, operation_id
FROM {self._table("semantic_refresh_scope_heads")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ? AND scope_id = ?;
""".strip(),
            journal.publication_database,
            journal.publication_target_table,
            journal.publication_scope_id,
        )
        scope = _row(cursor)
        if journal.scope_predecessor_operation_id is None:
            if scope is not None:
                raise MssqlAdmissionAuthorityConflict("scope predecessor head authority conflict")
            checkpoint = None
        else:
            if scope is None or scope[1] != journal.scope_predecessor_operation_id:
                raise MssqlAdmissionAuthorityConflict("scope predecessor head authority conflict")
            cursor.execute(
                f"""
SELECT checkpoint_sha256, operation_id, checkpoint_version
FROM {self._table("semantic_refresh_checkpoints")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ? AND scope_id = ?;
""".strip(),
                journal.publication_database,
                journal.publication_target_table,
                journal.publication_scope_id,
            )
            checkpoint = _row(cursor)
            if checkpoint is None or checkpoint[1] != journal.scope_predecessor_operation_id:
                raise MssqlAdmissionAuthorityConflict("checkpoint predecessor authority conflict")
        return MssqlAdmissionPredecessorSnapshot(
            target_generation=target[0],
            target_generation_id=target[1],
            target_uuid=target[2],
            target_operation_id=target[3],
            scope_revision=None if scope is None else int(scope[0]),
            scope_operation_id=None if scope is None else str(scope[1]),
            checkpoint_sha256=None if checkpoint is None else str(checkpoint[0]),
            checkpoint_operation_id=None if checkpoint is None else str(checkpoint[1]),
            checkpoint_version=None if checkpoint is None else int(checkpoint[2]),
        )

    def _require_existing_reservation(self, cursor: _Cursor, request: MssqlAdmissionRequest) -> None:
        cursor.execute(
            f"""
SELECT workflow_id, workflow_execution_binding_sha256, status,
       max_prepared_models, max_sealed_extract_bytes,
       max_clickhouse_staging_bytes, max_shadow_bytes, max_peak_bytes
FROM {self._table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
WHERE reservation_id = ?;
""".strip(),
            request.reservation_id,
        )
        if _row(cursor) != (
            request.workflow_id,
            request.workflow_execution_binding_sha256,
            "PREPARING",
            request.resource_budget.max_workflow_prepared_models,
            request.resource_budget.max_workflow_sealed_extract_bytes,
            request.resource_budget.max_workflow_clickhouse_staging_bytes,
            request.resource_budget.max_workflow_shadow_bytes,
            request.resource_budget.max_workflow_peak_bytes,
        ):
            raise MssqlAdmissionAuthorityConflict("existing reservation admission conflict")

    def _require_existing_guards(self, cursor: _Cursor, request: MssqlAdmissionRequest) -> None:
        journals = {item.target_resource_id: item for item in request.journals}
        claims = (request.workflow_guard, *request.resource_guards)
        for claim in claims:
            journal = journals.get(claim.resource_id)
            cursor.execute(
                f"""
SELECT fencing_epoch, owner_id, workflow_id, operation_id,
       operation_plan_sha256, attempt_binding_sha256, strategy_authority_sha256, status
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE resource_id = ?;
""".strip(),
                claim.resource_id,
            )
            expected = (
                claim.fencing_epoch,
                request.owner_id,
                request.workflow_id,
                journal.operation_id if journal is not None else None,
                journal.operation_plan_sha256 if journal is not None else None,
                journal.attempt_binding_sha256 if journal is not None else None,
                journal.strategy_authority_sha256 if journal is not None else None,
                "HELD",
            )
            if _row(cursor) != expected:
                raise MssqlAdmissionAuthorityConflict("existing guard admission conflict")
        cursor.execute(
            f"SELECT COUNT_BIG(*) FROM {self._table('semantic_refresh_guards')} WHERE workflow_id = ?;",
            request.workflow_id,
        )
        if _row(cursor) != (len(claims),):
            raise MssqlAdmissionAuthorityConflict("existing guard set admission conflict")

    def _require_existing_successor(self, cursor: _Cursor, request: MssqlAdmissionRequest) -> None:
        claim = request.successor_claim
        if claim is None:
            return
        self._require_failed_cleanup_closure(cursor, claim.predecessor_workflow_id)
        cursor.execute(
            f"""
SELECT successor_workflow_id, replacement_plan_sha256, terminal_summary_sha256
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ? AND status = N'FAILED_PRE_COMMIT';
""".strip(),
            claim.predecessor_workflow_id,
        )
        if _row(cursor) != (
            claim.successor_workflow_id,
            claim.replacement_plan_sha256,
            claim.predecessor_workflow_summary_sha256,
        ):
            raise MssqlAdmissionAuthorityConflict("existing workflow successor conflict")

    def _require_failed_cleanup_closure(self, cursor: _Cursor, workflow_id: str) -> None:
        cursor.execute(
            f"""
SELECT COUNT_BIG(*),
       SUM(CASE WHEN ack.status = N'COMPLETE'
                 AND ack.cleanup_receipt_sha256 IS NOT NULL THEN 1 ELSE 0 END)
FROM {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_workflow_executions")} AS execution WITH (UPDLOCK, HOLDLOCK)
  ON execution.workflow_id = journal.workflow_id
LEFT JOIN {self._table("semantic_refresh_failed_cleanup_acks")} AS ack WITH (UPDLOCK, HOLDLOCK)
  ON ack.workflow_execution_binding_sha256 = execution.workflow_execution_binding_sha256
 AND ack.operation_id = journal.operation_id
WHERE journal.workflow_id = ? AND journal.status = N'FAILED_PRE_COMMIT';
""".strip(),
            workflow_id,
        )
        row = _row(cursor)
        if row is None or row[0] in {None, 0} or row[0] != row[1]:
            raise MssqlAdmissionAuthorityConflict(
                "workflow successor requires complete failed cleanup acknowledgement closure"
            )

    def _table(self, table_name: str) -> str:
        return f"[{self._control_schema}].[{table_name}]"


def _row(cursor: _Cursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


__all__ = [
    "MssqlAdmissionAuthorityConflict",
    "MssqlAdmissionAuthorityQueries",
    "MssqlAdmissionPredecessorSnapshot",
]
