"""Protected authority wrapper for semantic-refresh publication state."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

from dpone.ports.semantic_refresh_clickhouse_prepared import (
    DurableClickHousePreparedPublication,
)
from dpone.runtime.semantic_refresh_clickhouse_models import semantic_refresh_fingerprint
from dpone.runtime.semantic_refresh_clickhouse_prepared_codec import (
    load_prepared_publication,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_clickhouse_authority import (
        ClickHousePublicationAuthority,
        SemanticRefreshClickHousePublicationAuthorityPort,
    )


class _PublicationState(Protocol):
    def persist_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]: ...

    def mark_committing(self, request: Mapping[str, object]) -> Mapping[str, object]: ...

    def record_target_committed(self, request: Mapping[str, object]) -> Mapping[str, object]: ...

    def record_committed_incomplete(self, request: Mapping[str, object]) -> Mapping[str, object]: ...

    def record_commit_unknown(self, request: Mapping[str, object]) -> Mapping[str, object]: ...

    def reconcile_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]: ...

    def publish_or_reconcile(self, request: Mapping[str, object]) -> Mapping[str, object]: ...

    def publish_empty_scope(self, request: Mapping[str, object]) -> Mapping[str, object]: ...


class AuthorityBoundSemanticRefreshPublicationState:
    """Authorize protected documents and target/scope coordinates before SQL CAS."""

    def __init__(
        self,
        *,
        delegate: _PublicationState,
        authority: SemanticRefreshClickHousePublicationAuthorityPort,
    ) -> None:
        self._delegate = delegate
        self._authority = authority

    def persist_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        record = self._authorize(request, terminal=False)
        _authorize_prepared_documents(request, record)
        acknowledgement = self._delegate.persist_prepared(request)
        return _restore_acknowledgement(request, acknowledgement, "PREPARED")

    def mark_committing(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self._authorize(request, terminal=False)
        acknowledgement = self._delegate.mark_committing(request)
        return _restore_acknowledgement(request, acknowledgement, "COMMITTING")

    def record_target_committed(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self._authorize(request, terminal=False)
        acknowledgement = self._delegate.record_target_committed(request)
        state = acknowledgement.get("journal_state")
        if state not in {"TARGET_COMMITTED", "COMMITTED_INCOMPLETE", "COMPLETE"}:
            raise ValueError("durable state acknowledgement does not prove target commit")
        return _restore_acknowledgement(request, acknowledgement, str(state))

    def record_committed_incomplete(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self._authorize(request, terminal=False)
        acknowledgement = self._delegate.record_committed_incomplete(request)
        return _restore_acknowledgement(request, acknowledgement, "COMMITTED_INCOMPLETE")

    def record_commit_unknown(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self._authorize(request, terminal=False)
        acknowledgement = self._delegate.record_commit_unknown(request)
        return _restore_acknowledgement(request, acknowledgement, "COMMIT_UNKNOWN")

    def reconcile_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self._authorize(request, terminal=False)
        acknowledgement = self._delegate.reconcile_prepared(request)
        return _restore_acknowledgement(request, acknowledgement, "PREPARED")

    def publish_or_reconcile(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self._authorize(request, terminal=True)
        acknowledgement = self._delegate.publish_or_reconcile(request)
        return _restore_acknowledgement(request, acknowledgement, "COMPLETE")

    def publish_empty_scope(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self._authorize(request, terminal=True)
        acknowledgement = self._delegate.publish_empty_scope(request)
        return _restore_acknowledgement(request, acknowledgement, "COMPLETE")

    def _authorize(
        self,
        request: Mapping[str, object],
        *,
        terminal: bool,
    ) -> ClickHousePublicationAuthority:
        binding = request.get("workflow_execution_binding_sha256")
        operation_id = request.get("operation_id")
        if not isinstance(binding, str) or not isinstance(operation_id, str):
            raise ValueError("publication state authority lookup identity is invalid")
        record = self._authority.load(binding, operation_id)
        expected = _authority_projection(record, terminal=terminal)
        if any(request.get(key) != value for key, value in expected.items()):
            raise ValueError("publication state target/scope authority differs")
        if terminal:
            _authorize_successors(request, record)
        return record


def _authorize_prepared_documents(
    request: Mapping[str, object],
    record: ClickHousePublicationAuthority,
) -> None:
    try:
        loaded = load_prepared_publication(
            DurableClickHousePreparedPublication(
                workflow_execution_binding_sha256=str(request["workflow_execution_binding_sha256"]),
                operation_id=str(request["operation_id"]),
                prepare_plan_sha256=str(request["prepare_plan_sha256"]),
                prepare_plan_json=str(request["prepare_plan_json"]),
                prepared_receipt_sha256=str(request["prepared_receipt_sha256"]),
                prepared_receipt_json=str(request["prepared_receipt_json"]),
                artifact_manifest_key=str(request["artifact_manifest_key"]),
                artifact_manifest_version=str(request["artifact_manifest_version"]),
                artifact_manifest_sha256=str(request["artifact_manifest_sha256"]),
            )
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("durable PREPARED documents are invalid") from exc
    plan = loaded.plan
    expected = {
        "workflow_execution_id": record.workflow_execution_id,
        "operation_id": record.operation_id,
        "operation_plan_sha256": record.operation_plan_sha256,
        "workflow_plan_sha256": record.workflow_plan_sha256,
        "workflow_execution_binding_sha256": record.workflow_execution_binding_sha256,
        "attempt_binding_sha256": record.attempt_binding_sha256,
        "fence_epoch": record.fencing_epoch,
        "target_resource_id": record.target_resource_id,
        "target_authority_id": record.target_authority_id,
        "clickhouse_cluster_authority_id": record.clickhouse_cluster_authority_id,
        "database": record.database,
        "target_table": record.target_table,
        "scope_id": record.scope_id,
        "scope_start": record.scope_start,
        "scope_end": record.scope_end,
        "scope_revision": record.scope_revision,
        "expected_target_uuid": record.expected_target_uuid,
        "expected_schema_sha256": record.expected_schema_sha256,
        "expected_physical_sha256": record.expected_physical_sha256,
        "business_columns": record.business_columns,
        "effective_key_columns": record.effective_key_columns,
        "event_time_column": record.event_time_column,
        "max_staging_rows": record.max_staging_rows,
        "max_target_scope_rows": record.max_target_scope_rows,
        "max_staging_bytes": record.max_staging_bytes,
        "max_shadow_bytes": record.max_shadow_bytes,
        "max_retained_backup_bytes": record.max_retained_backup_bytes,
        "max_total_transient_bytes": record.max_total_transient_bytes,
    }
    if any(getattr(plan, field) != value for field, value in expected.items()):
        raise ValueError("durable PREPARE plan differs from protected publication coordinates")


def _authorize_successors(
    request: Mapping[str, object],
    record: ClickHousePublicationAuthority,
) -> None:
    target_uuid = request.get("target_uuid")
    empty_scope = request.get("target_mutation_outcome") == "NOT_REQUIRED_EMPTY_SCOPE"
    target_generation = record.predecessor_target_generation + (0 if empty_scope else 1)
    target_generation_id = (
        record.target_predecessor_generation_id
        if empty_scope
        else semantic_refresh_fingerprint(
            {
                "schema": "dpone.semantic-refresh-target-generation.v1",
                "target_authority_id": record.target_authority_id,
                "operation_id": record.operation_id,
                "operation_plan_sha256": record.operation_plan_sha256,
                "target_generation": target_generation,
                "target_uuid": target_uuid,
            }
        )
    )
    checkpoint_sha256 = semantic_refresh_fingerprint(
        {
            "schema": "dpone.semantic-refresh-checkpoint.v1",
            "operation_id": record.operation_id,
            "scope_id": record.scope_id,
            "scope_revision": record.scope_revision,
            "target_generation_id": target_generation_id,
            "target_uuid": target_uuid,
            "scope_end": record.scope_end,
        }
    )
    expected = {
        "expected_target_generation": record.predecessor_target_generation,
        "target_generation": target_generation,
        "target_generation_id": target_generation_id,
        "expected_scope_revision": record.predecessor_scope_revision or 0,
        "expected_checkpoint_sha256": record.predecessor_checkpoint_sha256,
        "checkpoint_sha256": checkpoint_sha256,
        "target_mutation_outcome": ("NOT_REQUIRED_EMPTY_SCOPE" if empty_scope else "TARGET_COMMITTED"),
        "value_conversion_outcome": ("NOT_APPLICABLE_NO_DATA" if empty_scope else "CONFORMANT"),
    }
    if any(request.get(key) != value for key, value in expected.items()):
        raise ValueError("publication state successor authority differs")


def _authority_projection(
    record: ClickHousePublicationAuthority,
    *,
    terminal: bool,
) -> dict[str, object]:
    result: dict[str, object] = {
        "publication_authority_sha256": record.authority_sha256,
        "workflow_execution_id": record.workflow_execution_id,
        "operation_id": record.operation_id,
        "operation_plan_sha256": record.operation_plan_sha256,
        "workflow_execution_binding_sha256": record.workflow_execution_binding_sha256,
        "target_resource_id": record.target_resource_id,
        "target_authority_id": record.target_authority_id,
        "clickhouse_cluster_authority_id": record.clickhouse_cluster_authority_id,
        "database": record.database,
        "target_table": record.target_table,
        "scope_id": record.scope_id,
        "scope_revision": record.scope_revision,
        "target_predecessor_generation_id": record.target_predecessor_generation_id,
        "scope_predecessor_operation_id": record.scope_predecessor_operation_id,
        "predecessor_target_generation": record.predecessor_target_generation,
        "predecessor_target_uuid": record.predecessor_target_uuid,
        "predecessor_target_operation_id": record.predecessor_target_operation_id,
        "predecessor_scope_revision": record.predecessor_scope_revision,
        "predecessor_checkpoint_sha256": record.predecessor_checkpoint_sha256,
        "predecessor_checkpoint_operation_id": record.predecessor_checkpoint_operation_id,
        "predecessor_checkpoint_version": record.predecessor_checkpoint_version,
    }
    if terminal:
        result["workflow_plan_sha256"] = record.workflow_plan_sha256
    return result


def _restore_acknowledgement(
    original: Mapping[str, object],
    acknowledgement: Mapping[str, object],
    state: str,
) -> dict[str, object]:
    expected = set(original) | {"committed", "atomic", "journal_state"}
    if set(acknowledgement) != expected:
        raise ValueError("durable state acknowledgement fields are not closed")
    if any(acknowledgement.get(key) != value for key, value in original.items()):
        raise ValueError("durable state acknowledgement identity differs")
    if (
        acknowledgement.get("committed") is not True
        or acknowledgement.get("atomic") is not True
        or acknowledgement.get("journal_state") != state
    ):
        raise ValueError(f"durable state acknowledgement does not prove {state}")
    return {
        **original,
        "committed": True,
        "atomic": True,
        "journal_state": state,
    }


__all__ = ["AuthorityBoundSemanticRefreshPublicationState"]
