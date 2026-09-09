"""Durable journal and multi-head publication coordination for ClickHouse."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_publication_state import SemanticRefreshPublicationState
from dpone.runtime.semantic_refresh_clickhouse_prepared_codec import (
    prepared_publication_documents,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_clickhouse_authority import ClickHousePublicationAuthority
    from dpone.runtime.semantic_refresh_clickhouse_models import (
        ClickHouseExchangeReceipt,
        ClickHouseHeadPublicationPlan,
        ClickHousePreparedReceipt,
        ClickHousePreparePlan,
    )


class ClickHousePublicationStateCoordinator:
    """Own state requests and exact acknowledgement validation."""

    def __init__(self, state: SemanticRefreshPublicationState) -> None:
        self._state = state

    def persist_prepared(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        authority: ClickHousePublicationAuthority,
    ) -> None:
        request = _prepared_request(plan, prepared, authority)
        acknowledgement = dict(self._state.persist_prepared(request))
        _assert_acknowledgement(request, acknowledgement, "PREPARED")

    def mark_committing(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        heads: ClickHouseHeadPublicationPlan,
        authority: ClickHousePublicationAuthority,
    ) -> None:
        request = _committing_request(plan, prepared, heads, authority)
        acknowledgement = dict(self._state.mark_committing(request))
        _assert_acknowledgement(request, acknowledgement, "COMMITTING")

    def record_target_committed(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        exchange_receipt: ClickHouseExchangeReceipt,
        authority: ClickHousePublicationAuthority,
    ) -> str:
        request = _target_committed_request(plan, prepared, exchange_receipt, authority)
        acknowledgement = dict(self._state.record_target_committed(request))
        state = acknowledgement.get("journal_state")
        if state not in {"TARGET_COMMITTED", "COMMITTED_INCOMPLETE", "COMPLETE"}:
            raise ValueError("publication state acknowledgement does not prove target commit")
        _assert_acknowledgement(request, acknowledgement, str(state))
        return str(state)

    def record_committed_incomplete(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        exchange_receipt: ClickHouseExchangeReceipt,
        authority: ClickHousePublicationAuthority,
    ) -> None:
        request = _committed_incomplete_request(plan, prepared, exchange_receipt, authority)
        acknowledgement = dict(self._state.record_committed_incomplete(request))
        _assert_acknowledgement(request, acknowledgement, "COMMITTED_INCOMPLETE")

    def record_commit_unknown(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        authority: ClickHousePublicationAuthority,
    ) -> None:
        request = _commit_unknown_request(plan, prepared, authority)
        acknowledgement = dict(self._state.record_commit_unknown(request))
        _assert_acknowledgement(request, acknowledgement, "COMMIT_UNKNOWN")

    def reconcile_prepared(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        authority: ClickHousePublicationAuthority,
    ) -> None:
        request = _reconciled_prepared_request(plan, prepared, authority)
        acknowledgement = dict(self._state.reconcile_prepared(request))
        _assert_acknowledgement(request, acknowledgement, "PREPARED")

    def publish_complete(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        exchange_receipt: ClickHouseExchangeReceipt,
        heads: ClickHouseHeadPublicationPlan,
        authority: ClickHousePublicationAuthority,
        *,
        terminal_receipt_sha256: str,
        expected_journal_state: str,
    ) -> None:
        request = _complete_request(
            plan,
            prepared,
            exchange_receipt,
            heads,
            authority,
            terminal_receipt_sha256=terminal_receipt_sha256,
            expected_journal_state=expected_journal_state,
        )
        acknowledgement = dict(self._state.publish_or_reconcile(request))
        _assert_acknowledgement(request, acknowledgement, "COMPLETE")

    def publish_empty_scope(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        heads: ClickHouseHeadPublicationPlan,
        authority: ClickHousePublicationAuthority,
        *,
        empty_receipt_sha256: str,
        terminal_receipt_sha256: str,
    ) -> None:
        request = _empty_complete_request(
            plan,
            prepared,
            heads,
            authority,
            empty_receipt_sha256=empty_receipt_sha256,
            terminal_receipt_sha256=terminal_receipt_sha256,
        )
        acknowledgement = dict(self._state.publish_empty_scope(request))
        _assert_acknowledgement(request, acknowledgement, "COMPLETE")


def _prepared_request(
    plan: ClickHousePreparePlan,
    prepared: ClickHousePreparedReceipt,
    authority: ClickHousePublicationAuthority,
) -> dict[str, object]:
    return {
        **_authority_coordinates(authority),
        **prepared_publication_documents(plan, prepared),
        "operation_id": plan.operation_id,
        "operation_plan_sha256": plan.operation_plan_sha256,
        "workflow_plan_sha256": plan.workflow_plan_sha256,
        "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "fence_epoch": plan.fence_epoch,
        "prepare_receipt_sha256": prepared.receipt_sha256,
        "target_uuid": prepared.target_uuid,
        "expected_journal_state": "PREPARING",
        "next_journal_state": "PREPARED",
    }


def _committing_request(
    plan: ClickHousePreparePlan,
    prepared: ClickHousePreparedReceipt,
    heads: ClickHouseHeadPublicationPlan,
    authority: ClickHousePublicationAuthority,
) -> dict[str, object]:
    return {
        **_authority_coordinates(authority),
        "operation_id": plan.operation_id,
        "operation_plan_sha256": plan.operation_plan_sha256,
        "workflow_plan_sha256": plan.workflow_plan_sha256,
        "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "fence_epoch": plan.fence_epoch,
        "prepare_receipt_sha256": prepared.receipt_sha256,
        "target_uuid": prepared.target_uuid,
        "expected_target_uuid": authority.predecessor_target_uuid,
        "expected_target_generation": authority.predecessor_target_generation,
        "expected_scope_revision": authority.predecessor_scope_revision or 0,
        "expected_checkpoint_sha256": authority.predecessor_checkpoint_sha256,
        "expected_journal_state": "PREPARED",
        "next_journal_state": "COMMITTING",
    }


def _complete_request(
    plan: ClickHousePreparePlan,
    prepared: ClickHousePreparedReceipt,
    exchange_receipt: ClickHouseExchangeReceipt,
    heads: ClickHouseHeadPublicationPlan,
    authority: ClickHousePublicationAuthority,
    *,
    terminal_receipt_sha256: str,
    expected_journal_state: str,
) -> dict[str, object]:
    return {
        **_authority_coordinates(authority),
        "operation_id": plan.operation_id,
        "operation_plan_sha256": plan.operation_plan_sha256,
        "workflow_plan_sha256": plan.workflow_plan_sha256,
        "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "fence_epoch": plan.fence_epoch,
        "database": plan.database,
        "target_table": plan.target_table,
        "scope_id": heads.scope_id,
        "prepare_receipt_sha256": prepared.receipt_sha256,
        "clickhouse_commit_receipt_sha256": exchange_receipt.exchange_receipt_sha256,
        "terminal_receipt_sha256": terminal_receipt_sha256,
        "target_uuid": exchange_receipt.new_target_uuid,
        "expected_target_uuid": authority.predecessor_target_uuid,
        "expected_target_generation": authority.predecessor_target_generation,
        "target_generation": heads.target_generation,
        "target_generation_id": heads.target_generation_id,
        "expected_scope_revision": authority.predecessor_scope_revision or 0,
        "scope_revision": heads.scope_revision,
        "expected_checkpoint_sha256": authority.predecessor_checkpoint_sha256,
        "checkpoint_sha256": heads.checkpoint_sha256,
        "target_mutation_outcome": heads.target_mutation_outcome,
        "value_conversion_outcome": heads.value_conversion_outcome,
        "expected_journal_state": expected_journal_state,
        "terminal_journal_state": "COMPLETE",
    }


def _target_committed_request(
    plan: ClickHousePreparePlan,
    prepared: ClickHousePreparedReceipt,
    exchange_receipt: ClickHouseExchangeReceipt,
    authority: ClickHousePublicationAuthority,
) -> dict[str, object]:
    return {
        **_authority_coordinates(authority),
        "operation_id": plan.operation_id,
        "operation_plan_sha256": plan.operation_plan_sha256,
        "workflow_plan_sha256": plan.workflow_plan_sha256,
        "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "fence_epoch": plan.fence_epoch,
        "prepare_receipt_sha256": prepared.receipt_sha256,
        "clickhouse_commit_receipt_sha256": exchange_receipt.exchange_receipt_sha256,
        "expected_target_uuid": exchange_receipt.old_target_uuid,
        "target_uuid": exchange_receipt.new_target_uuid,
        "expected_journal_state": "COMMITTING",
        "next_journal_state": "TARGET_COMMITTED",
    }


def _committed_incomplete_request(
    plan: ClickHousePreparePlan,
    prepared: ClickHousePreparedReceipt,
    exchange_receipt: ClickHouseExchangeReceipt,
    authority: ClickHousePublicationAuthority,
) -> dict[str, object]:
    return {
        **_target_committed_request(plan, prepared, exchange_receipt, authority),
        "expected_journal_state": "TARGET_COMMITTED",
        "next_journal_state": "COMMITTED_INCOMPLETE",
    }


def _commit_unknown_request(
    plan: ClickHousePreparePlan,
    prepared: ClickHousePreparedReceipt,
    authority: ClickHousePublicationAuthority,
) -> dict[str, object]:
    return {
        **_authority_coordinates(authority),
        "operation_id": plan.operation_id,
        "operation_plan_sha256": plan.operation_plan_sha256,
        "workflow_plan_sha256": plan.workflow_plan_sha256,
        "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "fence_epoch": plan.fence_epoch,
        "prepare_receipt_sha256": prepared.receipt_sha256,
        "target_uuid": prepared.target_uuid,
        "expected_journal_state": "COMMITTING",
        "next_journal_state": "COMMIT_UNKNOWN",
    }


def _reconciled_prepared_request(
    plan: ClickHousePreparePlan,
    prepared: ClickHousePreparedReceipt,
    authority: ClickHousePublicationAuthority,
) -> dict[str, object]:
    return {
        **_commit_unknown_request(plan, prepared, authority),
        "expected_journal_state": "COMMIT_UNKNOWN",
        "next_journal_state": "PREPARED",
    }


def _empty_complete_request(
    plan: ClickHousePreparePlan,
    prepared: ClickHousePreparedReceipt,
    heads: ClickHouseHeadPublicationPlan,
    authority: ClickHousePublicationAuthority,
    *,
    empty_receipt_sha256: str,
    terminal_receipt_sha256: str,
) -> dict[str, object]:
    return {
        **_authority_coordinates(authority),
        "operation_id": plan.operation_id,
        "operation_plan_sha256": plan.operation_plan_sha256,
        "workflow_plan_sha256": plan.workflow_plan_sha256,
        "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "fence_epoch": plan.fence_epoch,
        "database": plan.database,
        "target_table": plan.target_table,
        "scope_id": heads.scope_id,
        "prepare_receipt_sha256": prepared.receipt_sha256,
        "clickhouse_commit_receipt_sha256": empty_receipt_sha256,
        "terminal_receipt_sha256": terminal_receipt_sha256,
        "target_uuid": prepared.target_uuid,
        "expected_target_uuid": authority.predecessor_target_uuid,
        "expected_target_generation": authority.predecessor_target_generation,
        "target_generation": heads.target_generation,
        "target_generation_id": heads.target_generation_id,
        "expected_scope_revision": authority.predecessor_scope_revision or 0,
        "scope_revision": heads.scope_revision,
        "expected_checkpoint_sha256": authority.predecessor_checkpoint_sha256,
        "checkpoint_sha256": heads.checkpoint_sha256,
        "target_mutation_outcome": heads.target_mutation_outcome,
        "value_conversion_outcome": heads.value_conversion_outcome,
        "expected_journal_state": "PREPARED",
        "terminal_journal_state": "COMPLETE",
    }


def _authority_coordinates(authority: ClickHousePublicationAuthority) -> dict[str, object]:
    return {
        "publication_authority_sha256": authority.authority_sha256,
        "workflow_execution_id": authority.workflow_execution_id,
        "target_resource_id": authority.target_resource_id,
        "target_authority_id": authority.target_authority_id,
        "clickhouse_cluster_authority_id": authority.clickhouse_cluster_authority_id,
        "database": authority.database,
        "target_table": authority.target_table,
        "scope_id": authority.scope_id,
        "scope_revision": authority.scope_revision,
        "target_predecessor_generation_id": authority.target_predecessor_generation_id,
        "scope_predecessor_operation_id": authority.scope_predecessor_operation_id,
        "predecessor_target_generation": authority.predecessor_target_generation,
        "predecessor_target_uuid": authority.predecessor_target_uuid,
        "predecessor_target_operation_id": authority.predecessor_target_operation_id,
        "predecessor_scope_revision": authority.predecessor_scope_revision,
        "predecessor_checkpoint_sha256": authority.predecessor_checkpoint_sha256,
        "predecessor_checkpoint_operation_id": authority.predecessor_checkpoint_operation_id,
        "predecessor_checkpoint_version": authority.predecessor_checkpoint_version,
    }


def _assert_acknowledgement(
    request: Mapping[str, object],
    acknowledgement: Mapping[str, object],
    expected_state: str,
) -> None:
    expected_fields = set(request) | {"committed", "atomic", "journal_state"}
    if set(acknowledgement) != expected_fields:
        raise ValueError("publication state acknowledgement fields are not closed")
    if any(acknowledgement.get(key) != value for key, value in request.items()):
        raise ValueError("publication state acknowledgement identity differs")
    if (
        acknowledgement.get("committed") is not True
        or acknowledgement.get("atomic") is not True
        or acknowledgement.get("journal_state") != expected_state
    ):
        raise ValueError(f"publication state acknowledgement does not prove {expected_state}")


__all__ = [
    "ClickHousePublicationStateCoordinator",
    "SemanticRefreshPublicationState",
]
