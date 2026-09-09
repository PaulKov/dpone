"""Application-level authentication of semantic-refresh recovery plans."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dpone.contracts.dbt_semantic_refresh_plan_contracts import SemanticRefreshPlanTarget
from dpone.contracts.dbt_semantic_refresh_recovery_authority import (
    SemanticRefreshCompleteScopeReplayAuthority,
    SemanticRefreshFailedPrecommitReplacementAuthority,
    SemanticRefreshPlanRecoveryAuthority,
    SemanticRefreshPredecessorPlan,
)
from dpone.contracts.semantic_refresh_types import (
    SqlServerModelOutcome,
    replacement_action_for,
)
from dpone.ports.semantic_refresh_mssql_authority_codec import authority_from_record
from dpone.ports.semantic_refresh_mssql_authority_models import (
    MssqlCanonicalAdmissionBundle,
    mssql_model_resource_authority_from_plan_target,
)
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlFailedRecoveryReadRequest,
    MssqlPersistedModelOutcome,
    SemanticRefreshMssqlFailedRecoveryStatePort,
)

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_workflow_summary import (
        SemanticRefreshDurableModelPublication,
    )
    from dpone.ports.semantic_refresh_mssql_authority import (
        SemanticRefreshMssqlCanonicalAuthorityPort,
    )
    from dpone.ports.semantic_refresh_mssql_recovery_heads import (
        MssqlDurableRecoveryPublication,
    )
    from dpone.services.semantic_refresh_mssql_recovery_heads import (
        SemanticRefreshMssqlRecoveryHeadService,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlRecoveryAuthorityVerifier:
    """Authenticate recovery plans against protected MSSQL state."""

    recovery: SemanticRefreshMssqlRecoveryHeadService
    failed_authority: SemanticRefreshMssqlCanonicalAuthorityPort | None = None
    failed_state: SemanticRefreshMssqlFailedRecoveryStatePort | None = None

    def verify(self, authority: SemanticRefreshPlanRecoveryAuthority) -> bool:
        """Accept only an exact protected replay or failed-precommit closure."""

        try:
            if isinstance(authority, SemanticRefreshCompleteScopeReplayAuthority):
                return self._verify_replay(authority)
            if isinstance(authority, SemanticRefreshFailedPrecommitReplacementAuthority):
                return self._verify_failed_precommit(authority)
            return False
        except (LookupError, RuntimeError, TypeError, ValueError):
            return False

    def _verify_replay(self, authority: SemanticRefreshCompleteScopeReplayAuthority) -> bool:
        summary = authority.predecessor_summary
        plan = authority.predecessor_plan
        binding_sha256 = summary.workflow_execution_binding_sha256
        typed_targets = _typed_targets(plan.targets)
        if typed_targets is None:
            return False
        durable = self.recovery.load(binding_sha256)
        bundle = authority_from_record(self.recovery.authority.load(binding_sha256))
        if (
            durable.workflow_execution_id != summary.workflow_execution_id
            or durable.workflow_execution_binding_sha256 != binding_sha256
            or durable.workflow_plan_sha256 != summary.workflow_plan_sha256
            or durable.plan_bundle_sha256 != plan.plan_bundle_sha256
            or durable.canonical_authority_sha256 != bundle.authority_sha256
            or durable.terminal_summary_sha256 != summary.terminal_summary_sha256
            or durable.terminal_summary_json != _canonical_json(summary.to_dict())
            or authority.predecessor_workflow_execution_id != summary.workflow_execution_id
            or not _canonical_plan_matches(bundle, plan, typed_targets, binding_sha256)
            or not _predecessor_publications_match(bundle, durable.predecessor_publications, summary.publications)
        ):
            return False
        return tuple(
            (
                item.model_unique_id,
                item.clickhouse_target_authority_id,
                item.target_generation,
                item.target_generation_id,
                item.target_uuid,
                item.owner_operation_id,
                item.terminal_receipt_sha256,
            )
            for item in durable.target_heads
        ) == tuple(
            (
                item.model_unique_id,
                item.clickhouse_target_authority_id,
                item.target_generation,
                item.target_generation_id,
                item.target_uuid,
                item.owner_operation_id,
                item.terminal_receipt_sha256,
            )
            for item in authority.target_heads
        )

    def _verify_failed_precommit(
        self,
        authority: SemanticRefreshFailedPrecommitReplacementAuthority,
    ) -> bool:
        if self.failed_authority is None or self.failed_state is None:
            return False
        summary = authority.predecessor_summary
        plan = authority.predecessor_plan
        binding_sha256 = summary.workflow_execution_binding_sha256
        typed_targets = _typed_targets(plan.targets)
        if typed_targets is None:
            return False
        bundle = authority_from_record(self.failed_authority.load(binding_sha256))
        durable = self.failed_state.load_failed_recovery_authority(
            MssqlFailedRecoveryReadRequest(
                workflow_execution_id=bundle.workflow_execution_id,
                workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
                workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
                canonical_authority_sha256=bundle.authority_sha256,
            )
        )
        if (
            durable.workflow_execution_id != summary.workflow_id
            or durable.workflow_execution_binding_sha256 != binding_sha256
            or durable.workflow_plan_sha256 != summary.workflow_plan_sha256
            or durable.plan_bundle_sha256 != plan.plan_bundle_sha256
            or durable.canonical_authority_sha256 != bundle.authority_sha256
            or durable.terminal_summary_sha256 != summary.terminal_summary_sha256
            or durable.terminal_summary_json != _canonical_json(summary.to_dict())
            or authority.predecessor_workflow_execution_id != summary.workflow_id
            or not _canonical_plan_matches(bundle, plan, typed_targets, binding_sha256)
            or not _canonical_failed_models_match(bundle, durable.models)
        ):
            return False
        summaries = {item.operation_id: item for item in summary.models}
        actions = {item.action_id: item for item in authority.replacement_actions}
        if len(summaries) != len(durable.models) or set(actions) != {item.model_unique_id for item in durable.models}:
            return False
        for item in durable.models:
            model_summary = summaries.get(item.operation_id)
            action = actions[item.model_unique_id]
            try:
                outcome = SqlServerModelOutcome(item.mssql_outcome)
            except ValueError:
                return False
            if (
                model_summary is None
                or model_summary.attempt_binding_sha256 != item.attempt_binding_sha256
                or model_summary.mssql_outcome is not outcome
                or model_summary.mssql_evidence_sha256 != item.mssql_evidence_sha256
                or action.outcome is not outcome
                or action.action is not replacement_action_for(outcome)
            ):
                return False
        return True


def _canonical_json(value: object) -> str:
    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _typed_targets(targets: tuple[object, ...]) -> tuple[SemanticRefreshPlanTarget, ...] | None:
    values = tuple(item for item in targets if isinstance(item, SemanticRefreshPlanTarget))
    return values if len(values) == len(targets) else None


def _canonical_plan_matches(
    bundle: MssqlCanonicalAdmissionBundle,
    plan: SemanticRefreshPredecessorPlan,
    targets: tuple[SemanticRefreshPlanTarget, ...],
    workflow_execution_binding_sha256: str,
) -> bool:
    return (
        bundle.execution_binding.workflow_execution_binding_sha256 == workflow_execution_binding_sha256
        and bundle.workflow_plan == plan.workflow_plan
        and bundle.operation_plans == tuple(plan.operation_plans)
        and bundle.model_resources == tuple(mssql_model_resource_authority_from_plan_target(item) for item in targets)
    )


def _canonical_failed_models_match(
    bundle: MssqlCanonicalAdmissionBundle,
    models: tuple[MssqlPersistedModelOutcome, ...],
) -> bool:
    attempts = {item.operation_id: item for item in bundle.attempt_bindings}
    expected = tuple(
        sorted(
            (
                item.model_unique_id,
                item.operation_id,
                item.operation_plan_sha256,
                attempts[item.operation_id].attempt_binding_sha256,
            )
            for item in bundle.operation_plans
        )
    )
    actual = tuple(
        (
            item.model_unique_id,
            item.operation_id,
            item.operation_plan_sha256,
            item.attempt_binding_sha256,
        )
        for item in models
    )
    return actual == expected


def _predecessor_publications_match(
    bundle: MssqlCanonicalAdmissionBundle,
    durable: tuple[MssqlDurableRecoveryPublication, ...],
    summary: tuple[SemanticRefreshDurableModelPublication, ...],
) -> bool:
    models_by_operation = {item.operation_id: item.model_unique_id for item in bundle.operation_plans}
    durable_values = tuple(
        sorted(
            (
                item.operation_id,
                item.operation_plan_sha256,
                item.workflow_execution_binding_sha256,
                item.attempt_binding_sha256,
                item.artifact_manifest_sha256,
                item.clickhouse_terminal_receipt_sha256,
                item.terminal_receipt_sha256,
                item.target_generation,
                item.scope_revision,
            )
            for item in durable
            if models_by_operation.get(item.operation_id) == item.model_unique_id
        )
    )
    summary_values = tuple(
        (
            item.operation_id,
            item.operation_plan_sha256,
            item.workflow_execution_binding_sha256,
            item.attempt_binding_sha256,
            item.artifact_manifest_sha256,
            item.clickhouse_terminal_receipt_sha256,
            item.terminal_receipt_sha256,
            item.target_generation,
            item.scope_revision,
        )
        for item in summary
    )
    return len(durable_values) == len(durable) and durable_values == summary_values


__all__ = ["SemanticRefreshMssqlRecoveryAuthorityVerifier"]
