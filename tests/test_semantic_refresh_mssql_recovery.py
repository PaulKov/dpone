from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path

import pytest

from dpone.contracts.semantic_refresh_failure_summary import (
    FailedModelOutcome,
    SemanticRefreshFailedWorkflowSummary,
)
from dpone.contracts.semantic_refresh_plan_refs import ReplacementActionBinding
from dpone.contracts.semantic_refresh_types import ReplacementAction, SqlServerModelOutcome
from dpone.contracts.semantic_refresh_workflow_replacement import (
    SemanticRefreshWorkflowReplacementPlan,
)
from dpone.ports.semantic_refresh_mssql import (
    MssqlAdmissionRequest,
    MssqlBuildReceiptEvidence,
    MssqlGuardClaim,
    MssqlImageEvidence,
    MssqlImageKeyColumn,
    MssqlJournalPreparation,
    MssqlOperationEvidence,
    MssqlPrerequisiteAuthorityClaim,
    MssqlRuntimeAssuranceClaim,
    MssqlTargetOwnerClaim,
    MssqlTransactionDisposition,
    MssqlWorkflowResourceBudget,
    mssql_guard_set_sha256,
    mssql_journal_set_sha256,
    mssql_strategy_authority_sha256,
)
from dpone.ports.semantic_refresh_mssql_failure import (
    MssqlFailureContext,
    MssqlFailureDecision,
    MssqlFailureJournalReference,
)
from dpone.ports.semantic_refresh_mssql_recovery_heads import (
    MssqlAdmissionTargetHeadAuthority,
)
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlPersistedModelOutcome,
    MssqlPredecessorFailureState,
)
from dpone.services.semantic_refresh_mssql_failure import (
    SemanticRefreshMssqlFailureBlocked,
    SemanticRefreshMssqlFailureTerminalizationService,
    SemanticRefreshMssqlOutcomeService,
)
from dpone.services.semantic_refresh_mssql_replacement import (
    SemanticRefreshMssqlReplacementBlocked,
    SemanticRefreshMssqlReplacementService,
)

OPERATION_ID = "operation-1"
PLAN_SHA = "sha256:" + "a" * 64
ATTEMPT_SHA = "sha256:" + "b" * 64
BEFORE_SHA = "sha256:" + "c" * 64
AFTER_SHA = "sha256:" + "d" * 64


def _image(role: str, digest: str) -> MssqlImageEvidence:
    return MssqlImageEvidence(
        operation_id=OPERATION_ID,
        operation_plan_sha256=PLAN_SHA,
        attempt_binding_sha256=ATTEMPT_SHA,
        fencing_epoch=7,
        image_role=role,
        image_sha256=digest,
        row_count=2,
        committed=True,
    )


def _receipt() -> MssqlBuildReceiptEvidence:
    return MssqlBuildReceiptEvidence.build_exact(
        operation_id=OPERATION_ID,
        operation_plan_sha256=PLAN_SHA,
        attempt_binding_sha256=ATTEMPT_SHA,
        fencing_epoch=7,
        before_image_sha256=BEFORE_SHA,
        after_image_sha256=AFTER_SHA,
        inserted_count=1,
        updated_count=1,
        model_unique_id="model.analytics.events",
        strategy_authority_sha256="sha256:" + "e" * 64,
        before_image_relation="[DWH].[dpone_scope_images].[before_1]",
        after_image_relation="[DWH].[dpone_scope_images].[after_1]",
    )


def _evidence(**overrides: object) -> MssqlOperationEvidence:
    values: dict[str, object] = {
        "operation_id": OPERATION_ID,
        "operation_plan_sha256": PLAN_SHA,
        "attempt_binding_sha256": ATTEMPT_SHA,
        "fencing_epoch": 7,
        "database_available": True,
        "controller_proves_not_invoked": False,
        "transaction_disposition": MssqlTransactionDisposition.UNKNOWN,
        "receipt": None,
        "before_image": None,
        "after_image": None,
    }
    values.update(overrides)
    return MssqlOperationEvidence(**values)


def _admission_request() -> MssqlAdmissionRequest:
    target = MssqlGuardClaim("mssql://warehouse/dbo/events", 6, 7)
    workflow_guard = MssqlGuardClaim("workflow://daily-events", 3, 4)
    resource_guards = (target,)
    journals = (
        MssqlJournalPreparation(
            model_unique_id="model.analytics.events",
            operation_id=OPERATION_ID,
            operation_plan_sha256=PLAN_SHA,
            attempt_binding_sha256=ATTEMPT_SHA,
            strategy_authority_json="{}",
            strategy_authority_sha256=mssql_strategy_authority_sha256("{}"),
            baseline_receipt_sha256="sha256:" + "8" * 64,
            baseline_kind="adopted_complete_relation_conformant",
            baseline_receipt_json="{}",
            baseline_status="COMPLETE",
            image_key_columns=(
                MssqlImageKeyColumn("event_id", "NATIVE"),
                MssqlImageKeyColumn("occurred_at", "NATIVE"),
            ),
            target_resource_id=target.resource_id,
            publication_database="analytics",
            publication_target_table="events",
            publication_scope_id="scope-live",
            target_predecessor_generation_id="sha256:" + "0" * 64,
            scope_predecessor_operation_id="sha256:" + "1" * 64,
            fencing_epoch=target.fencing_epoch,
            replaces_failed_operation_id="sha256:" + "4" * 64,
        ),
    )
    return MssqlAdmissionRequest(
        workflow_id="replacement-workflow",
        workflow_execution_id="replacement-workflow",
        workflow_plan_sha256="sha256:" + "6" * 64,
        workflow_execution_binding_sha256="sha256:" + "7" * 64,
        canonical_authority_sha256="sha256:" + "0" * 64,
        canonical_authority_json="{}",
        controller_id="controller-1",
        owner_id="owner-1",
        reservation_id="reservation-1",
        resource_budget=MssqlWorkflowResourceBudget(1, 10_000, 20_000, 30_000, 60_000),
        expected_guard_set_sha256=mssql_guard_set_sha256(workflow_guard, resource_guards),
        expected_journal_set_sha256=mssql_journal_set_sha256(journals),
        workflow_guard=workflow_guard,
        resource_guards=resource_guards,
        journals=journals,
        target_heads=(
            MssqlAdmissionTargetHeadAuthority(
                target_resource_id=target.resource_id,
                model_unique_id="model.analytics.events",
                clickhouse_target_authority_id="clickhouse://analytics/events",
                target_generation=1,
                target_generation_id="sha256:" + "0" * 64,
                target_uuid="00000000-0000-0000-0000-000000000001",
                owner_operation_id="sha256:" + "8" * 64,
                terminal_receipt_sha256=None,
                head_authority_receipt_sha256="sha256:" + "8" * 64,
            ),
        ),
        target_owners=(
            MssqlTargetOwnerClaim(
                target_authority_id="clickhouse://analytics/events",
                model_unique_id="model.analytics.events",
                deployment_id="sha256:" + "5" * 64,
                owner_generation=1,
            ),
        ),
        prerequisite_authorities=(
            MssqlPrerequisiteAuthorityClaim(
                release_id="sha256:" + "4" * 64,
                deployment_id="sha256:" + "5" * 64,
                model_unique_id="model.analytics.events",
                route_certification_receipt_sha256="sha256:" + "6" * 64,
                runtime_assurances=(
                    MssqlRuntimeAssuranceClaim("ddl_freeze", "sha256:" + "7" * 64, "{}"),
                    MssqlRuntimeAssuranceClaim("writer_exclusivity", "sha256:" + "8" * 64, "{}"),
                ),
            ),
        ),
    )


def _replacement_plan(
    actions: tuple[ReplacementActionBinding, ...],
    *,
    predecessor_outcome: SqlServerModelOutcome | None = None,
) -> SemanticRefreshWorkflowReplacementPlan:
    action_ids = tuple(item.action_id for item in actions)
    outcome = predecessor_outcome or actions[0].outcome
    summary_sha256 = (
        "sha256:" + "7" * 64
        if outcome is SqlServerModelOutcome.COMMIT_UNKNOWN
        else _predecessor_summary_sha256(outcome)
    )
    return SemanticRefreshWorkflowReplacementPlan.build(
        workflow_plan_sha256="sha256:" + "6" * 64,
        predecessor_workflow_execution_id="failed-workflow",
        predecessor_workflow_execution_binding_sha256="sha256:" + "9" * 64,
        predecessor_workflow_summary_sha256=summary_sha256,
        recovery_plan_digest="sha256:" + "8" * 64,
        selected_mutating_node_ids=action_ids,
        model_operation_plan_ids=action_ids,
        expected_model_outcome_ids=action_ids,
        replacement_action_ids=action_ids,
        replacement_actions=actions,
    )


@dataclass(frozen=True)
class _PredecessorState:
    state: MssqlPredecessorFailureState

    def load_predecessor(self, workflow_id: str) -> MssqlPredecessorFailureState:
        assert workflow_id == self.state.workflow_id
        return self.state


def _replacement_service(
    outcome: SqlServerModelOutcome,
    *,
    predecessor_binding: str = "sha256:" + "9" * 64,
) -> SemanticRefreshMssqlReplacementService:
    strategy_json = json.dumps(
        {
            "attempt_binding_sha256": ATTEMPT_SHA,
            "before_image_relation": {
                "database": "DWH",
                "identifier": "before_predecessor",
                "schema": "dpone_scope_images",
            },
            "fencing_epoch": 7,
            "operation_id": "sha256:" + "4" * 64,
            "operation_plan_sha256": PLAN_SHA,
            "receipt_relation": {
                "database": "DWH",
                "identifier": "semantic_refresh_receipts",
                "schema": "dpone_control",
            },
            "after_image_relation": {
                "database": "DWH",
                "identifier": "after_predecessor",
                "schema": "dpone_scope_images",
            },
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    terminal_summary_sha256 = (
        "sha256:" + "7" * 64
        if outcome is SqlServerModelOutcome.COMMIT_UNKNOWN
        else _predecessor_summary_sha256(outcome, binding=predecessor_binding)
    )
    return SemanticRefreshMssqlReplacementService(
        predecessor_state=_PredecessorState(
            MssqlPredecessorFailureState(
                workflow_id="failed-workflow",
                workflow_plan_sha256="sha256:" + "5" * 64,
                workflow_execution_binding_sha256=predecessor_binding,
                terminal_summary_sha256=terminal_summary_sha256,
                status="FAILED_PRE_COMMIT",
                models=(
                    MssqlPersistedModelOutcome(
                        model_unique_id="model.analytics.events",
                        operation_id="sha256:" + "4" * 64,
                        attempt_binding_sha256=ATTEMPT_SHA,
                        mssql_outcome=outcome.value,
                        mssql_evidence_sha256="sha256:" + "3" * 64,
                        operation_plan_sha256=PLAN_SHA,
                        fencing_epoch=7,
                        strategy_authority_json=strategy_json,
                        strategy_authority_sha256=mssql_strategy_authority_sha256(strategy_json),
                        before_image_relation="[DWH].[dpone_scope_images].[before_predecessor]",
                        before_image_sha256=BEFORE_SHA,
                        after_image_relation="[DWH].[dpone_scope_images].[after_predecessor]",
                        after_image_sha256=AFTER_SHA,
                    ),
                ),
            )
        )
    )


def _predecessor_summary_sha256(
    outcome: SqlServerModelOutcome,
    *,
    binding: str = "sha256:" + "9" * 64,
) -> str:
    return SemanticRefreshFailedWorkflowSummary.build(
        workflow_id="failed-workflow",
        workflow_plan_sha256="sha256:" + "5" * 64,
        workflow_execution_binding_sha256=binding,
        expected_operation_ids=("sha256:" + "4" * 64,),
        models=(
            FailedModelOutcome(
                operation_id="sha256:" + "4" * 64,
                attempt_binding_sha256=ATTEMPT_SHA,
                mssql_outcome=outcome,
                mssql_evidence_sha256="sha256:" + "3" * 64,
            ),
        ),
    ).terminal_summary_sha256


def test_exact_receipt_and_committed_images_prove_committed() -> None:
    outcome = SemanticRefreshMssqlOutcomeService().reconcile(
        _evidence(
            receipt=_receipt(),
            before_image=_image("BEFORE", BEFORE_SHA),
            after_image=_image("AFTER", AFTER_SHA),
        )
    )
    assert outcome is SqlServerModelOutcome.COMMITTED_WITH_IMAGES


def test_build_receipt_recomputes_exact_sql_server_nvarchar_preimage() -> None:
    receipt = MssqlBuildReceiptEvidence.build_exact(
        operation_id=OPERATION_ID,
        operation_plan_sha256=PLAN_SHA,
        attempt_binding_sha256=ATTEMPT_SHA,
        fencing_epoch=7,
        before_image_sha256=BEFORE_SHA,
        after_image_sha256=AFTER_SHA,
        inserted_count=1,
        updated_count=1,
        model_unique_id="model.analytics.events",
        strategy_authority_sha256="sha256:" + "e" * 64,
        before_image_relation="[DWH].[dpone_scope_images].[before_1]",
        after_image_relation="[DWH].[dpone_scope_images].[after_1]",
    )

    assert receipt.build_receipt_sha256 == ("sha256:c76ce7defba4551053c46349d3c0912801f17ca1c0695ab000bda41f2063032d")
    with pytest.raises(ValueError, match="digest differs"):
        replace(receipt, updated_count=2)


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (
            _evidence(
                controller_proves_not_invoked=True,
                transaction_disposition=MssqlTransactionDisposition.NOT_OPENED,
            ),
            SqlServerModelOutcome.NOT_INVOKED,
        ),
        (
            _evidence(transaction_disposition=MssqlTransactionDisposition.ROLLED_BACK),
            SqlServerModelOutcome.ROLLED_BACK,
        ),
        (_evidence(), SqlServerModelOutcome.COMMIT_UNKNOWN),
        (_evidence(database_available=False), SqlServerModelOutcome.COMMIT_UNKNOWN),
        (
            _evidence(before_image=_image("BEFORE", BEFORE_SHA)),
            SqlServerModelOutcome.COMMIT_UNKNOWN,
        ),
    ],
)
def test_outcome_reconciliation_is_fail_closed(
    evidence: MssqlOperationEvidence,
    expected: SqlServerModelOutcome,
) -> None:
    assert SemanticRefreshMssqlOutcomeService().reconcile(evidence) is expected


def test_conflicting_receipt_or_image_identity_is_commit_unknown() -> None:
    wrong_after = MssqlImageEvidence(
        operation_id=OPERATION_ID,
        operation_plan_sha256=PLAN_SHA,
        attempt_binding_sha256=ATTEMPT_SHA,
        fencing_epoch=8,
        image_role="AFTER",
        image_sha256=AFTER_SHA,
        row_count=2,
        committed=True,
    )
    evidence = _evidence(
        receipt=_receipt(),
        before_image=_image("BEFORE", BEFORE_SHA),
        after_image=wrong_after,
    )
    assert SemanticRefreshMssqlOutcomeService().reconcile(evidence) is SqlServerModelOutcome.COMMIT_UNKNOWN


def test_legacy_receipt_without_authenticated_build_digest_is_commit_unknown() -> None:
    legacy_receipt = MssqlBuildReceiptEvidence(
        operation_id=OPERATION_ID,
        operation_plan_sha256=PLAN_SHA,
        attempt_binding_sha256=ATTEMPT_SHA,
        fencing_epoch=7,
        before_image_sha256=BEFORE_SHA,
        after_image_sha256=AFTER_SHA,
        inserted_count=1,
        updated_count=1,
    )
    evidence = _evidence(
        receipt=legacy_receipt,
        before_image=_image("BEFORE", BEFORE_SHA),
        after_image=_image("AFTER", AFTER_SHA),
    )

    assert SemanticRefreshMssqlOutcomeService().reconcile(evidence) is SqlServerModelOutcome.COMMIT_UNKNOWN


def test_replacement_maps_outcomes_and_binds_one_successor_to_admission() -> None:
    service = _replacement_service(SqlServerModelOutcome.COMMITTED_WITH_IMAGES)
    proposal = service.propose(
        {
            "model.analytics.committed": SqlServerModelOutcome.COMMITTED_WITH_IMAGES,
            "model.analytics.not_invoked": SqlServerModelOutcome.NOT_INVOKED,
            "model.analytics.rolled_back": SqlServerModelOutcome.ROLLED_BACK,
        }
    )
    assert [(item.action_id, item.action) for item in proposal] == [
        ("model.analytics.committed", ReplacementAction.RESTORE_THEN_REBUILD),
        ("model.analytics.not_invoked", ReplacementAction.BUILD_FRESH),
        ("model.analytics.rolled_back", ReplacementAction.BUILD_FRESH),
    ]

    request = _admission_request()
    admission_actions = service.propose({"model.analytics.events": SqlServerModelOutcome.COMMITTED_WITH_IMAGES})
    bound = service.bind_successor(
        admission=request,
        replacement_plan=_replacement_plan(admission_actions),
    )
    assert isinstance(bound, MssqlAdmissionRequest)
    assert bound.successor_claim is not None
    assert bound.successor_claim.successor_workflow_id == request.workflow_id
    strategy = json.loads(bound.journals[0].strategy_authority_json)
    assert strategy["predecessor_operation_id"] == "sha256:" + "4" * 64
    assert strategy["predecessor_before_image_sha256"] == BEFORE_SHA
    assert strategy["predecessor_after_image_sha256"] == AFTER_SHA


def test_unknown_outcome_blocks_replacement_before_successor_cas() -> None:
    service = _replacement_service(SqlServerModelOutcome.COMMIT_UNKNOWN)
    proposal = service.propose({"model.analytics.events": SqlServerModelOutcome.COMMIT_UNKNOWN})
    assert proposal[0].action is ReplacementAction.BLOCK

    with pytest.raises(SemanticRefreshMssqlReplacementBlocked, match="COMMIT_UNKNOWN"):
        service.bind_successor(
            admission=_admission_request(),
            replacement_plan=_replacement_plan(proposal),
        )


def test_replacement_rejects_plan_action_not_proven_by_predecessor_journal() -> None:
    service = _replacement_service(SqlServerModelOutcome.ROLLED_BACK)
    forged = service.propose({"model.analytics.events": SqlServerModelOutcome.COMMITTED_WITH_IMAGES})

    with pytest.raises(ValueError, match="persisted predecessor outcome"):
        service.bind_successor(
            admission=_admission_request(),
            replacement_plan=_replacement_plan(
                forged,
                predecessor_outcome=SqlServerModelOutcome.ROLLED_BACK,
            ),
        )


def test_replacement_rejects_predecessor_identity_not_proven_by_durable_state() -> None:
    service = _replacement_service(
        SqlServerModelOutcome.ROLLED_BACK,
        predecessor_binding="sha256:" + "0" * 64,
    )
    actions = service.propose({"model.analytics.events": SqlServerModelOutcome.ROLLED_BACK})

    with pytest.raises(ValueError, match="execution binding differs"):
        service.bind_successor(
            admission=_admission_request(),
            replacement_plan=_replacement_plan(actions),
        )


def test_replacement_rejects_successor_lineage_not_bound_to_predecessor_journal() -> None:
    service = _replacement_service(SqlServerModelOutcome.ROLLED_BACK)
    actions = service.propose({"model.analytics.events": SqlServerModelOutcome.ROLLED_BACK})
    request = _admission_request()
    journals = (
        replace(
            request.journals[0],
            replaces_failed_operation_id="sha256:" + "f" * 64,
        ),
    )
    request = replace(
        request,
        journals=journals,
        expected_journal_set_sha256=mssql_journal_set_sha256(journals),
    )

    with pytest.raises(ValueError, match="operation lineage differs"):
        service.bind_successor(
            admission=request,
            replacement_plan=_replacement_plan(actions),
        )


_FAILURE_OPERATION = "sha256:" + "1" * 64
_FAILURE_PLAN = "sha256:" + "2" * 64
_FAILURE_ATTEMPT = "sha256:" + "3" * 64


@dataclass(frozen=True)
class _FailureContextReader:
    context: MssqlFailureContext

    def load_failure_context(self, workflow_id: str) -> MssqlFailureContext:
        assert workflow_id == self.context.workflow_id
        return self.context


@dataclass(frozen=True)
class _EvidenceReader:
    evidence: MssqlOperationEvidence

    def read_operation_evidence(
        self,
        *,
        operation_id: str,
        attempt_binding_sha256: str,
    ) -> MssqlOperationEvidence:
        assert (operation_id, attempt_binding_sha256) == (
            self.evidence.operation_id,
            self.evidence.attempt_binding_sha256,
        )
        return self.evidence


@dataclass
class _FailureState:
    decisions: list[MssqlFailureDecision] = field(default_factory=list)

    def persist_failure(self, decision: MssqlFailureDecision) -> None:
        self.decisions.append(decision)


def _failure_context() -> MssqlFailureContext:
    target_resource_id = "mssql://warehouse/dbo/events"
    journal = MssqlJournalPreparation(
        model_unique_id="model.analytics.events",
        operation_id=_FAILURE_OPERATION,
        operation_plan_sha256=_FAILURE_PLAN,
        attempt_binding_sha256=_FAILURE_ATTEMPT,
        strategy_authority_json="{}",
        strategy_authority_sha256=mssql_strategy_authority_sha256("{}"),
        baseline_receipt_sha256="sha256:" + "7" * 64,
        baseline_kind="adopted_complete_relation_conformant",
        baseline_receipt_json="{}",
        baseline_status="COMPLETE",
        image_key_columns=(MssqlImageKeyColumn("event_id", "NATIVE"),),
        target_resource_id=target_resource_id,
        publication_database="analytics",
        publication_target_table="events",
        publication_scope_id="scope-failure",
        target_predecessor_generation_id="sha256:" + "8" * 64,
        scope_predecessor_operation_id=None,
        fencing_epoch=9,
    )
    workflow_guard = MssqlGuardClaim("workflow://failed-workflow", 0, 1)
    resource_guards = (MssqlGuardClaim(target_resource_id, 8, 9),)
    return MssqlFailureContext(
        workflow_id="failed-workflow",
        workflow_plan_sha256="sha256:" + "4" * 64,
        workflow_execution_binding_sha256="sha256:" + "5" * 64,
        guard_set_sha256=mssql_guard_set_sha256(workflow_guard, resource_guards),
        journal_set_sha256=mssql_journal_set_sha256((journal,)),
        workflow_guard_resource_id=workflow_guard.resource_id,
        guard_count=2,
        owner_id="owner-failure",
        status="PREPARING",
        terminal_summary_sha256=None,
        terminal_summary_json=None,
        journals=(
            MssqlFailureJournalReference(
                model_unique_id="model.analytics.events",
                operation_id=_FAILURE_OPERATION,
                operation_plan_sha256=_FAILURE_PLAN,
                attempt_binding_sha256=_FAILURE_ATTEMPT,
                strategy_authority_json=journal.strategy_authority_json,
                strategy_authority_sha256=journal.strategy_authority_sha256,
                baseline_receipt_sha256=journal.baseline_receipt_sha256,
                baseline_kind=journal.baseline_kind,
                baseline_receipt_json=journal.baseline_receipt_json,
                baseline_status=journal.baseline_status,
                image_key_columns=journal.image_key_columns,
                target_resource_id=journal.target_resource_id,
                publication_database=journal.publication_database,
                publication_target_table=journal.publication_target_table,
                publication_scope_id=journal.publication_scope_id,
                target_predecessor_generation_id=journal.target_predecessor_generation_id,
                scope_predecessor_operation_id=journal.scope_predecessor_operation_id,
                fencing_epoch=9,
                replaces_failed_operation_id=None,
                owner_id="owner-failure",
                status="PREPARING",
            ),
        ),
    )


def _failure_evidence(
    disposition: MssqlTransactionDisposition,
) -> MssqlOperationEvidence:
    return MssqlOperationEvidence(
        operation_id=_FAILURE_OPERATION,
        operation_plan_sha256=_FAILURE_PLAN,
        attempt_binding_sha256=_FAILURE_ATTEMPT,
        fencing_epoch=9,
        database_available=True,
        controller_proves_not_invoked=False,
        transaction_disposition=disposition,
        receipt=None,
        before_image=None,
        after_image=None,
    )


def test_failure_terminalization_derives_outcome_and_digest_from_durable_evidence() -> None:
    state = _FailureState()
    summary = SemanticRefreshMssqlFailureTerminalizationService(
        contexts=_FailureContextReader(_failure_context()),
        evidence=_EvidenceReader(_failure_evidence(MssqlTransactionDisposition.ROLLED_BACK)),
        state=state,
    ).terminalize("failed-workflow")

    assert summary.models[0].mssql_outcome is SqlServerModelOutcome.ROLLED_BACK
    assert state.decisions[0].models[0].mssql_outcome == SqlServerModelOutcome.ROLLED_BACK.value
    assert state.decisions[0].models[0].mssql_evidence_sha256.startswith("sha256:")
    assert json.loads(state.decisions[0].terminal_summary_json) == summary.to_dict()


def test_failure_terminalization_blocks_unknown_evidence_without_mutating_state() -> None:
    state = _FailureState()
    service = SemanticRefreshMssqlFailureTerminalizationService(
        contexts=_FailureContextReader(_failure_context()),
        evidence=_EvidenceReader(_failure_evidence(MssqlTransactionDisposition.UNKNOWN)),
        state=state,
    )

    with pytest.raises(SemanticRefreshMssqlFailureBlocked, match="COMMIT_UNKNOWN"):
        service.terminalize("failed-workflow")
    assert state.decisions == []


def test_restore_compares_grouped_multisets_before_and_after_restore() -> None:
    source = Path("packages/dbt-dpone/macros/semantic_refresh_restore.sql").read_text(encoding="utf-8")
    assert source.count("COUNT_BIG(*) AS dpone_multiplicity") >= 4
    assert source.count("GROUP BY") >= 4
    assert source.index("predecessor_after_image_relation") < source.index("DELETE FROM")
    assert source.rindex("predecessor_before }}") > source.index("INSERT INTO")
