"""Evidence-bound SQL Server replacement planning policy."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from dpone.contracts.semantic_refresh_failure_summary import (
    FailedModelOutcome,
    SemanticRefreshFailedWorkflowSummary,
)
from dpone.contracts.semantic_refresh_plan_refs import ReplacementActionBinding
from dpone.contracts.semantic_refresh_types import (
    ReplacementAction,
    SqlServerModelOutcome,
    replacement_action_for,
)
from dpone.ports.semantic_refresh_mssql import (
    MssqlAdmissionRequest,
    MssqlWorkflowSuccessorClaim,
    mssql_journal_set_sha256,
    mssql_strategy_authority_sha256,
)
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlFailedScratchCleanupAuthority,
    MssqlPersistedModelOutcome,
    MssqlPredecessorFailureState,
    MssqlScratchCleanupRelation,
    SemanticRefreshMssqlFailedScratchCleanupStatePort,
    SemanticRefreshMssqlPredecessorStatePort,
)
from dpone.services.semantic_refresh_mssql_scratch_documents import (
    ScratchDocumentExpectation,
    authenticate_scratch_documents,
)

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_workflow_replacement import (
        SemanticRefreshWorkflowReplacementPlan,
    )


class SemanticRefreshMssqlReplacementError(RuntimeError):
    """Base safe error for replacement planning and ownership."""


class SemanticRefreshMssqlReplacementBlocked(SemanticRefreshMssqlReplacementError):
    """Raised before CAS when any producer outcome is still ambiguous."""


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlReplacementService:
    """Map outcomes and bind one successor into atomic workflow admission."""

    predecessor_state: SemanticRefreshMssqlPredecessorStatePort

    def propose(
        self,
        outcomes: Mapping[str, SqlServerModelOutcome],
    ) -> tuple[ReplacementActionBinding, ...]:
        """Return stable model actions for a canonical replacement-plan builder."""

        if not outcomes:
            raise ValueError("replacement outcomes must not be empty")
        proposal: list[ReplacementActionBinding] = []
        for action_id in sorted(outcomes):
            if not isinstance(action_id, str) or not action_id.strip():
                raise ValueError("replacement action_id must be a non-empty string")
            outcome = outcomes[action_id]
            proposal.append(
                ReplacementActionBinding(
                    action_id=action_id,
                    outcome=outcome,
                    action=replacement_action_for(outcome),
                )
            )
        return tuple(proposal)

    def bind_successor(
        self,
        *,
        admission: MssqlAdmissionRequest,
        replacement_plan: SemanticRefreshWorkflowReplacementPlan,
    ) -> MssqlAdmissionRequest:
        """Bind the recovery-authorized predecessor and successor atomically."""

        if replacement_plan.workflow_plan_sha256 != admission.workflow_plan_sha256:
            raise ValueError("replacement plan must reference the admitted workflow plan")
        predecessor = self.predecessor_state.load_predecessor(replacement_plan.predecessor_workflow_execution_id)
        if predecessor.status != "FAILED_PRE_COMMIT":
            raise ValueError("replacement predecessor is not durably failed")
        if (
            predecessor.workflow_execution_binding_sha256
            != replacement_plan.predecessor_workflow_execution_binding_sha256
        ):
            raise ValueError("replacement predecessor execution binding differs from durable state")
        durable_summary = _reconstruct_summary(predecessor)
        if durable_summary.terminal_summary_sha256 != predecessor.terminal_summary_sha256:
            raise ValueError("replacement predecessor summary does not authenticate journal outcomes")
        if durable_summary.terminal_summary_sha256 != replacement_plan.predecessor_workflow_summary_sha256:
            raise ValueError("replacement predecessor summary differs from durable state")
        action_ids = tuple(item.action_id for item in replacement_plan.replacement_actions)
        journal_ids = tuple(sorted(item.model_unique_id for item in admission.journals))
        if action_ids != journal_ids:
            raise ValueError("replacement actions must cover the exact admission journal closure")
        persisted_ids = tuple(item.model_unique_id for item in predecessor.models)
        if action_ids != persisted_ids:
            raise ValueError("replacement actions differ from predecessor journal closure")
        for action, durable in zip(replacement_plan.replacement_actions, predecessor.models, strict=True):
            successor_journal = next(item for item in admission.journals if item.model_unique_id == action.action_id)
            if successor_journal.replaces_failed_operation_id != durable.operation_id:
                raise ValueError("replacement operation lineage differs from predecessor journal")
            try:
                outcome = SqlServerModelOutcome(durable.mssql_outcome)
            except ValueError as exc:
                raise ValueError("predecessor journal outcome is unsupported") from exc
            if action.outcome is not outcome or action.action is not replacement_action_for(outcome):
                raise ValueError("replacement action differs from persisted predecessor outcome")
        blocked = tuple(
            item.action_id for item in replacement_plan.replacement_actions if item.action is ReplacementAction.BLOCK
        )
        if blocked:
            raise SemanticRefreshMssqlReplacementBlocked(
                "COMMIT_UNKNOWN blocks workflow replacement until evidence reconciliation"
            )
        admission = _bind_restore_authority(admission, replacement_plan, predecessor)
        claim = MssqlWorkflowSuccessorClaim(
            predecessor_workflow_id=replacement_plan.predecessor_workflow_execution_id,
            predecessor_workflow_summary_sha256=(replacement_plan.predecessor_workflow_summary_sha256),
            successor_workflow_id=admission.workflow_id,
            replacement_plan_sha256=replacement_plan.workflow_replacement_plan_sha256,
        )
        if admission.successor_claim is not None and admission.successor_claim != claim:
            raise ValueError("admission already binds a different workflow successor")
        return replace(admission, successor_claim=claim)


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlFailedScratchCleanupService:
    """Issue bounded cleanup authority only from protected failed state."""

    state: SemanticRefreshMssqlFailedScratchCleanupStatePort

    def load(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedScratchCleanupAuthority:
        """Return deterministic scratch names and authenticated observed UUIDs."""

        record = self.state.load_failed_scratch_cleanup(
            workflow_execution_binding_sha256,
            operation_id,
        )
        if record.mssql_outcome not in {"COMMITTED_WITH_IMAGES", "NOT_INVOKED", "ROLLED_BACK"}:
            raise SemanticRefreshMssqlReplacementBlocked("unsafe MSSQL outcome blocks failed-precommit scratch cleanup")
        request = record.request
        staging_uuid, shadow_uuid = authenticate_scratch_documents(
            expectation=ScratchDocumentExpectation(
                workflow_execution_id=request.workflow_execution_id,
                workflow_execution_binding_sha256=(request.workflow_execution_binding_sha256),
                operation_id=request.operation_id,
                operation_plan_sha256=request.operation_plan_sha256,
                attempt_binding_sha256=request.attempt_binding_sha256,
                fencing_epoch=request.fencing_epoch,
                target_authority_id=request.target_authority_id,
                database_name=request.database_name,
                target_table=request.target_table,
                target_uuid=request.target_uuid,
                staging_table=request.staging_table,
                shadow_table=request.shadow_table,
            ),
            prepare_plan_sha256=record.prepare_plan_sha256,
            prepare_plan_json=record.prepare_plan_json,
            prepared_receipt_sha256=record.prepared_receipt_sha256,
            prepared_receipt_json=record.prepared_receipt_json,
        )
        return MssqlFailedScratchCleanupAuthority(
            workflow_execution_id=request.workflow_execution_id,
            workflow_execution_binding_sha256=request.workflow_execution_binding_sha256,
            operation_id=request.operation_id,
            operation_plan_sha256=request.operation_plan_sha256,
            attempt_binding_sha256=request.attempt_binding_sha256,
            fencing_epoch=request.fencing_epoch,
            protected_target_authority_id=request.target_authority_id,
            protected_target_database=request.database_name,
            protected_target_table=request.target_table,
            protected_target_uuid=request.target_uuid,
            relations=(
                MssqlScratchCleanupRelation(
                    relation_role="SHADOW",
                    database_name=request.database_name,
                    table_name=request.shadow_table,
                    observed_uuid=shadow_uuid,
                ),
                MssqlScratchCleanupRelation(
                    relation_role="STAGING",
                    database_name=request.database_name,
                    table_name=request.staging_table,
                    observed_uuid=staging_uuid,
                ),
            ),
        )


def _bind_restore_authority(
    admission: MssqlAdmissionRequest,
    replacement_plan: SemanticRefreshWorkflowReplacementPlan,
    predecessor: MssqlPredecessorFailureState,
) -> MssqlAdmissionRequest:
    durable_by_model = {item.model_unique_id: item for item in predecessor.models}
    action_by_model = {item.action_id: item.action for item in replacement_plan.replacement_actions}
    journals = []
    for journal in admission.journals:
        if action_by_model[journal.model_unique_id] is not ReplacementAction.RESTORE_THEN_REBUILD:
            journals.append(journal)
            continue
        projection = _restore_projection(durable_by_model[journal.model_unique_id])
        try:
            strategy = json.loads(journal.strategy_authority_json)
        except json.JSONDecodeError as exc:
            raise ValueError("successor strategy authority JSON is invalid") from exc
        if not isinstance(strategy, dict) or any(field in strategy for field in projection):
            raise ValueError("successor strategy contains unprotected predecessor authority")
        strategy.update(projection)
        strategy_json = json.dumps(strategy, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        journals.append(
            replace(
                journal,
                strategy_authority_json=strategy_json,
                strategy_authority_sha256=mssql_strategy_authority_sha256(strategy_json),
            )
        )
    canonical = tuple(journals)
    return replace(
        admission,
        journals=canonical,
        expected_journal_set_sha256=mssql_journal_set_sha256(canonical),
    )


def _restore_projection(durable: MssqlPersistedModelOutcome) -> dict[str, object]:
    required = (
        durable.operation_plan_sha256,
        durable.fencing_epoch,
        durable.strategy_authority_json,
        durable.strategy_authority_sha256,
        durable.before_image_relation,
        durable.before_image_sha256,
        durable.after_image_relation,
        durable.after_image_sha256,
    )
    if any(value is None for value in required):
        raise ValueError("committed predecessor restore authority is incomplete")
    assert durable.strategy_authority_json is not None
    assert durable.strategy_authority_sha256 is not None
    if mssql_strategy_authority_sha256(durable.strategy_authority_json) != durable.strategy_authority_sha256:
        raise ValueError("predecessor strategy authority digest differs")
    try:
        strategy = json.loads(durable.strategy_authority_json)
    except json.JSONDecodeError as exc:
        raise ValueError("predecessor strategy authority JSON is invalid") from exc
    if not isinstance(strategy, dict):
        raise ValueError("predecessor strategy authority JSON is invalid")
    expected_identity = (
        durable.operation_id,
        durable.operation_plan_sha256,
        durable.attempt_binding_sha256,
        durable.fencing_epoch,
    )
    actual_identity = tuple(
        strategy.get(field)
        for field in (
            "operation_id",
            "operation_plan_sha256",
            "attempt_binding_sha256",
            "fencing_epoch",
        )
    )
    if actual_identity != expected_identity:
        raise ValueError("predecessor restore identity differs from durable strategy")
    relations = {
        "predecessor_receipt_relation": strategy.get("receipt_relation"),
        "predecessor_before_image_relation": strategy.get("before_image_relation"),
        "predecessor_after_image_relation": strategy.get("after_image_relation"),
    }
    if any(not isinstance(value, dict) for value in relations.values()):
        raise ValueError("predecessor restore relations are absent from durable strategy")
    assert durable.before_image_relation is not None
    assert durable.after_image_relation is not None
    if (
        _sql_relation(relations["predecessor_before_image_relation"]) != durable.before_image_relation
        or _sql_relation(relations["predecessor_after_image_relation"]) != durable.after_image_relation
    ):
        raise ValueError("predecessor image relations differ from durable receipt")
    return {
        **relations,
        "predecessor_operation_id": durable.operation_id,
        "predecessor_operation_plan_sha256": durable.operation_plan_sha256,
        "predecessor_attempt_binding_sha256": durable.attempt_binding_sha256,
        "predecessor_fencing_epoch": durable.fencing_epoch,
        "predecessor_before_image_sha256": durable.before_image_sha256,
        "predecessor_after_image_sha256": durable.after_image_sha256,
    }


def _sql_relation(value: object) -> str:
    if not isinstance(value, dict) or set(value) != {"database", "identifier", "schema"}:
        raise ValueError("predecessor relation authority is invalid")
    parts = tuple(value[field] for field in ("database", "schema", "identifier"))
    if any(not isinstance(item, str) or not item or "]" in item for item in parts):
        raise ValueError("predecessor relation authority is invalid")
    return ".".join(f"[{item}]" for item in parts)


def _reconstruct_summary(
    predecessor: MssqlPredecessorFailureState,
) -> SemanticRefreshFailedWorkflowSummary:
    models: list[FailedModelOutcome] = []
    for durable in predecessor.models:
        try:
            outcome = SqlServerModelOutcome(durable.mssql_outcome)
        except ValueError as exc:
            raise ValueError("predecessor journal outcome is unsupported") from exc
        if outcome is SqlServerModelOutcome.COMMIT_UNKNOWN:
            raise SemanticRefreshMssqlReplacementBlocked(
                "COMMIT_UNKNOWN blocks workflow replacement until evidence reconciliation"
            )
        models.append(
            FailedModelOutcome(
                operation_id=durable.operation_id,
                attempt_binding_sha256=durable.attempt_binding_sha256,
                mssql_outcome=outcome,
                mssql_evidence_sha256=durable.mssql_evidence_sha256,
            )
        )
    return SemanticRefreshFailedWorkflowSummary.build(
        workflow_id=predecessor.workflow_id,
        workflow_plan_sha256=predecessor.workflow_plan_sha256,
        workflow_execution_binding_sha256=predecessor.workflow_execution_binding_sha256,
        expected_operation_ids=tuple(item.operation_id for item in models),
        models=tuple(models),
    )


__all__ = [
    "SemanticRefreshMssqlFailedScratchCleanupService",
    "SemanticRefreshMssqlReplacementBlocked",
    "SemanticRefreshMssqlReplacementError",
    "SemanticRefreshMssqlReplacementService",
]
