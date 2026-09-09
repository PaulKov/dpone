"""Transactional insert operations for MSSQL semantic-refresh admission."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from dpone.adapters.semantic_refresh_mssql_admission_replay import MssqlAdmissionPredecessorSnapshot
from dpone.ports.semantic_refresh_mssql import MssqlAdmissionRequest, mssql_image_key_columns_json


class StatePersistenceCursor(Protocol):
    """DB-API cursor surface required by admission inserts."""

    def execute(self, sql: str, *parameters: object) -> StatePersistenceCursor: ...


class MssqlSemanticRefreshStatePersistenceMixin:
    """Persist canonical execution, reservation, and journal rows."""

    def _table(self, table_name: str) -> str:
        raise NotImplementedError

    def _insert_execution(
        self,
        cursor: StatePersistenceCursor,
        request: MssqlAdmissionRequest,
    ) -> None:
        cursor.execute(
            f"""
INSERT INTO {self._table("semantic_refresh_workflow_executions")} (
    workflow_id, workflow_execution_id, workflow_plan_sha256, workflow_execution_binding_sha256,
    canonical_authority_sha256,
    guard_set_sha256, journal_set_sha256, workflow_guard_resource_id, guard_count,
    controller_id, owner_id, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, N'PREPARING');
""".strip(),
            request.workflow_id,
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
        )

    def _insert_reservation(
        self,
        cursor: StatePersistenceCursor,
        request: MssqlAdmissionRequest,
    ) -> None:
        cursor.execute(
            f"""
INSERT INTO {self._table("semantic_refresh_reservations")} (
    reservation_id, workflow_id, workflow_execution_binding_sha256, status,
    max_prepared_models, max_sealed_extract_bytes,
    max_clickhouse_staging_bytes, max_shadow_bytes, max_peak_bytes
) VALUES (?, ?, ?, N'PREPARING', ?, ?, ?, ?, ?);
""".strip(),
            request.reservation_id,
            request.workflow_id,
            request.workflow_execution_binding_sha256,
            request.resource_budget.max_workflow_prepared_models,
            request.resource_budget.max_workflow_sealed_extract_bytes,
            request.resource_budget.max_workflow_clickhouse_staging_bytes,
            request.resource_budget.max_workflow_shadow_bytes,
            request.resource_budget.max_workflow_peak_bytes,
        )

    def _insert_journals(
        self,
        cursor: StatePersistenceCursor,
        request: MssqlAdmissionRequest,
        snapshots: Mapping[str, MssqlAdmissionPredecessorSnapshot],
    ) -> None:
        for journal in request.journals:
            snapshot = snapshots[journal.operation_id]
            cursor.execute(
                f"""
INSERT INTO {self._table("semantic_refresh_journals")} (
    model_unique_id, operation_id, operation_plan_sha256, attempt_binding_sha256,
    strategy_authority_json, strategy_authority_sha256,
    baseline_receipt_sha256, baseline_kind,
    baseline_receipt_json, baseline_status,
    image_key_columns_json, workflow_id, target_resource_id,
    publication_database, publication_target_table, publication_scope_id,
    target_predecessor_generation_id, scope_predecessor_operation_id,
    predecessor_target_generation, predecessor_target_uuid,
    predecessor_target_operation_id, predecessor_scope_revision,
    predecessor_checkpoint_sha256, predecessor_checkpoint_operation_id,
    predecessor_checkpoint_version, fencing_epoch,
    replaces_failed_operation_id,
    owner_id, journal_version, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, N'PREPARING');
""".strip(),
                journal.model_unique_id,
                journal.operation_id,
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
            )
