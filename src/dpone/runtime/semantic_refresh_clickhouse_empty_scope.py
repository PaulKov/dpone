"""No-swap empty-scope ClickHouse publication path."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

from dpone.runtime.semantic_refresh_clickhouse_conformance import (
    ClickHouseCommittedIncompleteError,
    ClickHouseConformanceError,
    ClickHouseEmptyScopeEvidence,
)
from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHouseExchangeReceipt,
    ClickHouseHeadPublicationPlan,
    ClickHousePreparedReceipt,
    ClickHousePreparePlan,
    ClickHousePublicationReceipt,
    semantic_refresh_fingerprint,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_clickhouse_authority import ClickHousePublicationAuthority
    from dpone.runtime.semantic_refresh_clickhouse_state import ClickHousePublicationStateCoordinator


class _EmptyScopeGateway(Protocol):
    def inspect_empty_scope(self, plan: Mapping[str, object]) -> Mapping[str, object]: ...


class ClickHouseEmptyScopePublisher:
    """Revalidate one no-data boundary and atomically advance non-target state."""

    def __init__(
        self,
        gateway: _EmptyScopeGateway,
        state: ClickHousePublicationStateCoordinator,
    ) -> None:
        self._gateway = gateway
        self._state = state

    def observe(self, plan: ClickHousePreparePlan) -> ClickHouseEmptyScopeEvidence:
        evidence = ClickHouseEmptyScopeEvidence.from_mapping(dict(self._gateway.inspect_empty_scope(plan.to_mapping())))
        assert_empty_scope_evidence(plan, evidence)
        return evidence

    def commit(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        heads: ClickHouseHeadPublicationPlan,
        authority: ClickHousePublicationAuthority,
    ) -> ClickHousePublicationReceipt:
        current = self.observe(plan)
        if empty_prepared_receipt(plan, current) != prepared:
            raise ClickHouseConformanceError("ClickHouse empty-scope evidence drifted before publication")
        empty_receipt_sha256 = semantic_refresh_fingerprint(
            {
                "operation_id": plan.operation_id,
                "status": "NOT_REQUIRED_EMPTY_SCOPE",
                "prepare_receipt_sha256": prepared.receipt_sha256,
                "target_uuid": prepared.target_uuid,
            }
        )
        terminal_receipt_sha256 = _terminal_receipt_sha256(plan, heads)
        exchange_receipt = ClickHouseExchangeReceipt(
            operation_id=plan.operation_id,
            status="NOT_REQUIRED_EMPTY_SCOPE",
            old_target_uuid=prepared.target_uuid,
            new_target_uuid=prepared.target_uuid,
            exchange_receipt_sha256=empty_receipt_sha256,
        )
        try:
            self._state.publish_empty_scope(
                plan,
                prepared,
                heads,
                authority,
                empty_receipt_sha256=empty_receipt_sha256,
                terminal_receipt_sha256=terminal_receipt_sha256,
            )
        except Exception as exc:
            raise ClickHouseCommittedIncompleteError(
                "ClickHouse empty scope proven but atomic publication state is unproved",
                exchange_receipt=exchange_receipt,
            ) from exc
        return ClickHousePublicationReceipt(
            operation_id=plan.operation_id,
            status="COMPLETE",
            exchange_outcome="NOT_REQUIRED_EMPTY_SCOPE",
            target_generation=heads.target_generation,
            target_generation_id=heads.target_generation_id,
            scope_revision=heads.scope_revision,
            terminal_receipt_sha256=terminal_receipt_sha256,
            target_mutation_outcome="NOT_REQUIRED_EMPTY_SCOPE",
            value_conversion_outcome="NOT_APPLICABLE_NO_DATA",
        )


def empty_prepared_receipt(
    plan: ClickHousePreparePlan,
    evidence: ClickHouseEmptyScopeEvidence,
) -> ClickHousePreparedReceipt:
    unsigned = {
        "operation_id": plan.operation_id,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "prepare_plan_sha256": plan.sha256,
        "status": "PREPARED",
        "target_uuid": evidence.target_uuid,
        "staging_uuid": None,
        "shadow_uuid": None,
        "staging_rows": 0,
        "shadow_rows": 0,
        "desired_rows": 0,
        "forward_difference_groups": 0,
        "reverse_difference_groups": 0,
        "shadow_equation": plan.shadow_equation.to_mapping(),
        "conformance_mode": "NOT_APPLICABLE_NO_DATA",
        "publication_mode": "EMPTY_SCOPE",
    }
    return ClickHousePreparedReceipt(
        operation_id=plan.operation_id,
        attempt_binding_sha256=plan.attempt_binding_sha256,
        prepare_plan_sha256=plan.sha256,
        status="PREPARED",
        target_uuid=evidence.target_uuid,
        staging_uuid=None,
        shadow_uuid=None,
        staging_rows=0,
        shadow_rows=0,
        desired_rows=0,
        forward_difference_groups=0,
        reverse_difference_groups=0,
        shadow_equation=plan.shadow_equation.to_mapping(),
        conformance_mode="NOT_APPLICABLE_NO_DATA",
        publication_mode="EMPTY_SCOPE",
        receipt_sha256=semantic_refresh_fingerprint(unsigned),
    )


def assert_empty_scope_evidence(
    plan: ClickHousePreparePlan,
    evidence: ClickHouseEmptyScopeEvidence,
) -> None:
    if not plan.is_empty_scope:
        raise ClickHouseConformanceError("ClickHouse empty-scope evidence lacks empty manifest authority")
    if evidence.target_scope_rows != 0:
        raise ClickHouseConformanceError("ClickHouse empty manifest diverges from non-empty target scope")
    if (
        evidence.target_uuid != plan.expected_target_uuid
        or evidence.schema_sha256 != plan.expected_schema_sha256
        or evidence.physical_sha256 != plan.expected_physical_sha256
        or evidence.database_engine != plan.database_engine
        or evidence.table_engine != plan.table_engine
        or evidence.shard_count != plan.shard_count
        or evidence.replica_count != plan.replica_count
    ):
        raise ClickHouseConformanceError("ClickHouse empty-scope target authority differs")
    if (
        evidence.guard_operation_id != plan.operation_id
        or evidence.guard_attempt_binding_sha256 != plan.attempt_binding_sha256
        or evidence.guard_fence_epoch != plan.fence_epoch
    ):
        raise ClickHouseConformanceError("ClickHouse empty-scope guard/fence authority differs")


def _terminal_receipt_sha256(
    plan: ClickHousePreparePlan,
    heads: ClickHouseHeadPublicationPlan,
) -> str:
    return semantic_refresh_fingerprint(
        {
            "operation_id": plan.operation_id,
            "status": "COMPLETE",
            "exchange_outcome": "NOT_REQUIRED_EMPTY_SCOPE",
            "scope_id": heads.scope_id,
            "target_generation": heads.target_generation,
            "target_generation_id": heads.target_generation_id,
            "scope_revision": heads.scope_revision,
            "checkpoint_sha256": heads.checkpoint_sha256,
            "target_mutation_outcome": heads.target_mutation_outcome,
            "value_conversion_outcome": heads.value_conversion_outcome,
            "exchange_receipt_sha256": None,
        }
    )


__all__ = [
    "ClickHouseEmptyScopePublisher",
    "assert_empty_scope_evidence",
    "empty_prepared_receipt",
]
