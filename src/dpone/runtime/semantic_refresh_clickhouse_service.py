"""PREPARE, UUID-reconciled exchange, and atomic publication service."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_clickhouse_gateway import SemanticRefreshClickHouseGateway
from dpone.runtime.semantic_refresh_clickhouse_authorization import (
    ClickHousePublicationAuthorityError,
    ClickHousePublicationAuthorization,
)
from dpone.runtime.semantic_refresh_clickhouse_conformance import (
    ClickHouseCommittedIncompleteError,
    ClickHouseConformanceError,
    ClickHouseExchangeNotCommittedError,
    ClickHousePostCommitCleanupError,
    ClickHousePrepareEvidence,
    ClickHousePrepareSqlBuilder,
    ClickHouseTargetAuthorityEvidence,
    ClickHouseUuidAmbiguityError,
    assert_prepare_evidence,
    assert_target_authority_evidence,
)
from dpone.runtime.semantic_refresh_clickhouse_empty_scope import (
    ClickHouseEmptyScopePublisher,
    empty_prepared_receipt,
)
from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHouseExchangeReceipt,
    ClickHouseHeadPublicationPlan,
    ClickHousePreparedReceipt,
    ClickHousePreparePlan,
    ClickHousePublicationReceipt,
    ClickHouseUuidMap,
    semantic_refresh_fingerprint,
)
from dpone.runtime.semantic_refresh_clickhouse_models import (
    terminal_receipt_sha256 as build_terminal_receipt_sha256,
)
from dpone.runtime.semantic_refresh_clickhouse_state import (
    ClickHousePublicationStateCoordinator,
    SemanticRefreshPublicationState,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_clickhouse_authority import (
        ClickHousePublicationAuthority,
        SemanticRefreshClickHousePublicationAuthorityPort,
    )


class ClickHousePublicationService:
    """Enforce PREPARE/COMMIT ordering without trusting client acknowledgements."""

    def __init__(
        self,
        *,
        gateway: SemanticRefreshClickHouseGateway,
        state: SemanticRefreshPublicationState,
        authority: SemanticRefreshClickHousePublicationAuthorityPort,
        sql_builder: ClickHousePrepareSqlBuilder | None = None,
    ) -> None:
        self._gateway = gateway
        self._state = ClickHousePublicationStateCoordinator(state)
        self._empty_scope = ClickHouseEmptyScopePublisher(gateway, self._state)
        self._authorization = ClickHousePublicationAuthorization(authority)
        self._sql_builder = sql_builder or ClickHousePrepareSqlBuilder()

    def prepare(self, plan: ClickHousePreparePlan) -> ClickHousePreparedReceipt:
        authority = self._authorization.prepare(plan)
        if plan.is_empty_scope:
            prepared = empty_prepared_receipt(plan, self._empty_scope.observe(plan))
            self._state.persist_prepared(plan, prepared, authority)
            return prepared
        evidence = ClickHousePrepareEvidence.from_mapping(
            dict(
                self._gateway.prepare(
                    plan.to_mapping(),
                    self._sql_builder.build(plan).to_mapping(),
                )
            )
        )
        assert_prepare_evidence(plan, evidence)
        prepared = self._prepared_receipt(plan, evidence)
        self._state.persist_prepared(plan, prepared, authority)
        return prepared

    @staticmethod
    def _prepared_receipt(
        plan: ClickHousePreparePlan,
        evidence: ClickHousePrepareEvidence,
    ) -> ClickHousePreparedReceipt:
        unsigned = {
            "operation_id": plan.operation_id,
            "attempt_binding_sha256": plan.attempt_binding_sha256,
            "prepare_plan_sha256": plan.sha256,
            "status": "PREPARED",
            "target_uuid": evidence.target_uuid,
            "staging_uuid": evidence.staging_uuid,
            "shadow_uuid": evidence.shadow_uuid,
            "staging_rows": evidence.staging_rows,
            "shadow_rows": evidence.shadow_rows,
            "desired_rows": evidence.desired_rows,
            "forward_difference_groups": evidence.forward_difference_groups,
            "reverse_difference_groups": evidence.reverse_difference_groups,
            "shadow_equation": plan.shadow_equation.to_mapping(),
            "conformance_mode": plan.conformance.mode,
            "publication_mode": "EXCHANGE",
        }
        return ClickHousePreparedReceipt(
            operation_id=plan.operation_id,
            attempt_binding_sha256=plan.attempt_binding_sha256,
            prepare_plan_sha256=plan.sha256,
            status="PREPARED",
            target_uuid=evidence.target_uuid,
            staging_uuid=evidence.staging_uuid,
            shadow_uuid=evidence.shadow_uuid,
            staging_rows=evidence.staging_rows,
            shadow_rows=evidence.shadow_rows,
            desired_rows=evidence.desired_rows,
            forward_difference_groups=evidence.forward_difference_groups,
            reverse_difference_groups=evidence.reverse_difference_groups,
            shadow_equation=plan.shadow_equation.to_mapping(),
            conformance_mode=plan.conformance.mode,
            receipt_sha256=semantic_refresh_fingerprint(unsigned),
            publication_mode="EXCHANGE",
        )

    def commit(
        self,
        plan: ClickHousePreparePlan,
        *,
        prepared: ClickHousePreparedReceipt,
        heads: ClickHouseHeadPublicationPlan,
    ) -> ClickHousePublicationReceipt:
        self._assert_prepared_identity(plan, prepared)
        authority = self._authorization.commit(plan, prepared, heads)
        if plan.is_empty_scope:
            return self._empty_scope.commit(plan, prepared, heads, authority)
        if prepared.shadow_uuid is None:
            raise ClickHouseConformanceError("ClickHouse EXCHANGE PREPARED receipt has no shadow UUID")
        original = ClickHouseUuidMap(
            target_uuid=prepared.target_uuid,
            shadow_uuid=prepared.shadow_uuid,
        )
        exchanged = ClickHouseUuidMap(
            target_uuid=prepared.shadow_uuid,
            shadow_uuid=prepared.target_uuid,
        )
        cleaned = ClickHouseUuidMap(
            target_uuid=prepared.shadow_uuid,
            shadow_uuid=None,
        )
        before = self._inspect_uuid_map(plan)
        exchange_error: Exception | None = None
        if before == original:
            self._state.reconcile_prepared(plan, prepared, authority)
            current = ClickHousePrepareEvidence.from_mapping(
                dict(
                    self._gateway.revalidate(
                        plan.to_mapping(),
                        self._sql_builder.build(plan).to_mapping(),
                    )
                )
            )
            assert_prepare_evidence(plan, current)
            if self._prepared_receipt(plan, current) != prepared:
                raise ClickHouseConformanceError("ClickHouse PREPARE evidence drifted before exchange")
            self._mark_committing(plan, prepared, heads, authority)
            try:
                self._gateway.exchange(plan.exchange_request(prepared))
            except Exception as exc:  # reconciliation, not acknowledgement, is authoritative
                exchange_error = exc
            try:
                after = self._inspect_uuid_map(plan)
            except ClickHouseUuidAmbiguityError:
                self._record_commit_unknown(plan, prepared, authority)
                raise
        elif before in (exchanged, cleaned):
            after = before
        else:
            raise ClickHouseUuidAmbiguityError("ClickHouse UUID map is not an authorized pre-exchange state")
        if after == original:
            self._state.record_commit_unknown(plan, prepared, authority)
            self._state.reconcile_prepared(plan, prepared, authority)
            message = "ClickHouse exchange is proven not committed"
            if exchange_error is not None:
                raise ClickHouseExchangeNotCommittedError(message) from exchange_error
            raise ClickHouseExchangeNotCommittedError(message)
        if after not in (exchanged, cleaned):
            self._state.record_commit_unknown(plan, prepared, authority)
            raise ClickHouseUuidAmbiguityError("ClickHouse UUID map cannot reconcile exchange outcome")
        exchange_receipt = self._exchange_receipt(plan, prepared)
        try:
            durable_state = self._state.record_target_committed(
                plan,
                prepared,
                exchange_receipt,
                authority,
            )
        except Exception as exc:
            raise ClickHouseCommittedIncompleteError(
                "ClickHouse target committed but TARGET_COMMITTED state is unproved",
                exchange_receipt=exchange_receipt,
            ) from exc
        try:
            target_authority = ClickHouseTargetAuthorityEvidence.from_mapping(
                dict(self._gateway.inspect_target_authority(plan.to_mapping()))
            )
            assert_target_authority_evidence(
                plan,
                target_authority,
                expected_target_uuid=prepared.shadow_uuid,
            )
        except Exception as exc:
            if durable_state == "TARGET_COMMITTED":
                try:
                    self._state.record_committed_incomplete(
                        plan,
                        prepared,
                        exchange_receipt,
                        authority,
                    )
                except Exception:
                    pass
            raise ClickHouseCommittedIncompleteError(
                "ClickHouse target committed but post-exchange physical authority is unproved",
                exchange_receipt=exchange_receipt,
            ) from exc
        terminal_receipt_sha256 = build_terminal_receipt_sha256(plan, exchange_receipt, heads)
        receipt = ClickHousePublicationReceipt(
            operation_id=plan.operation_id,
            status="COMPLETE",
            exchange_outcome="TARGET_COMMITTED",
            target_generation=heads.target_generation,
            target_generation_id=heads.target_generation_id,
            scope_revision=heads.scope_revision,
            terminal_receipt_sha256=terminal_receipt_sha256,
            cleanup_status="NOT_STARTED",
            retained_generation_status="RETAINED_FOR_POLICY",
        )
        try:
            self._state.publish_complete(
                plan,
                prepared,
                exchange_receipt,
                heads,
                authority,
                terminal_receipt_sha256=terminal_receipt_sha256,
                expected_journal_state=durable_state,
            )
        except Exception as exc:
            if durable_state == "TARGET_COMMITTED":
                try:
                    self._state.record_committed_incomplete(
                        plan,
                        prepared,
                        exchange_receipt,
                        authority,
                    )
                except Exception:
                    pass
            raise ClickHouseCommittedIncompleteError(
                "ClickHouse target committed but atomic publication state is unproved",
                exchange_receipt=exchange_receipt,
            ) from exc
        try:
            self._gateway.cleanup_retained(plan.cleanup_request(prepared, receipt))
        except Exception:
            return replace(receipt, cleanup_status="FAILED")
        return replace(receipt, cleanup_status="COMPLETE")

    def _record_commit_unknown(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        authority: ClickHousePublicationAuthority,
    ) -> None:
        try:
            self._state.record_commit_unknown(plan, prepared, authority)
        except Exception as exc:
            raise ClickHouseUuidAmbiguityError(
                "ClickHouse UUID map is unavailable and COMMIT_UNKNOWN persistence is unproved"
            ) from exc

    @staticmethod
    def _assert_prepared_identity(
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
    ) -> None:
        unsigned = {
            field_name: getattr(prepared, field_name)
            for field_name in prepared.__dataclass_fields__
            if field_name != "receipt_sha256"
        }
        if (
            prepared.status != "PREPARED"
            or prepared.operation_id != plan.operation_id
            or prepared.attempt_binding_sha256 != plan.attempt_binding_sha256
            or prepared.prepare_plan_sha256 != plan.sha256
            or prepared.receipt_sha256 != semantic_refresh_fingerprint(unsigned)
        ):
            raise ClickHouseConformanceError("ClickHouse PREPARED receipt identity is invalid")

    def _inspect_uuid_map(self, plan: ClickHousePreparePlan) -> ClickHouseUuidMap:
        request = {
            "workflow_execution_id": plan.workflow_execution_id,
            "operation_id": plan.operation_id,
            "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
            "attempt_binding_sha256": plan.attempt_binding_sha256,
            "fence_epoch": plan.fence_epoch,
            "clickhouse_cluster_authority_id": plan.clickhouse_cluster_authority_id,
            "database": plan.database,
            "target_table": plan.target_table,
            "shadow_table": plan.shadow_table,
        }
        try:
            return ClickHouseUuidMap.from_mapping(dict(self._gateway.inspect_uuid_map(request)))
        except Exception as exc:
            raise ClickHouseUuidAmbiguityError("ClickHouse UUID map evidence is invalid or unavailable") from exc

    @staticmethod
    def _exchange_receipt(
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
    ) -> ClickHouseExchangeReceipt:
        if prepared.shadow_uuid is None:
            raise ClickHouseConformanceError("PREPARED shadow UUID is absent")
        payload: dict[str, object] = {
            "operation_id": plan.operation_id,
            "status": "TARGET_COMMITTED",
            "old_target_uuid": prepared.target_uuid,
            "new_target_uuid": prepared.shadow_uuid,
        }
        return ClickHouseExchangeReceipt(
            operation_id=plan.operation_id,
            status="TARGET_COMMITTED",
            old_target_uuid=prepared.target_uuid,
            new_target_uuid=prepared.shadow_uuid,
            exchange_receipt_sha256=semantic_refresh_fingerprint(payload),
        )

    def _mark_committing(
        self,
        plan: ClickHousePreparePlan,
        prepared: ClickHousePreparedReceipt,
        heads: ClickHouseHeadPublicationPlan,
        authority: ClickHousePublicationAuthority,
    ) -> None:
        self._state.mark_committing(plan, prepared, heads, authority)


__all__ = [
    "ClickHouseCommittedIncompleteError",
    "ClickHouseConformanceError",
    "ClickHouseExchangeNotCommittedError",
    "ClickHousePostCommitCleanupError",
    "ClickHousePublicationService",
    "ClickHousePublicationAuthorityError",
    "ClickHouseUuidAmbiguityError",
    "SemanticRefreshClickHouseGateway",
    "SemanticRefreshPublicationState",
]
