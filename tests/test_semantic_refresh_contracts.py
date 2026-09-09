"""Contract tests for semantic-refresh V2 identity documents."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

from dpone.contracts.semantic_refresh_bindings import (
    SemanticRefreshAttemptBinding,
    SemanticRefreshWorkflowExecutionBinding,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.contracts.semantic_refresh_failure_summary import (
    FailedModelOutcome,
    SemanticRefreshFailedWorkflowSummary,
)
from dpone.contracts.semantic_refresh_lifecycle_policy import (
    SemanticRefreshSqlServerLifecyclePolicy,
)
from dpone.contracts.semantic_refresh_model_proof import SemanticRefreshModelDefinitionProof
from dpone.contracts.semantic_refresh_mutation_closure import SemanticRefreshMutationClosure
from dpone.contracts.semantic_refresh_operation_plan import SemanticRefreshOperationPlan
from dpone.contracts.semantic_refresh_plan_refs import (
    OperationPlanReference,
    ReplacementActionBinding,
)
from dpone.contracts.semantic_refresh_read_dependency import (
    ReadDependencyEdge,
    ReadDependencyKind,
    SemanticRefreshReadDependencyProof,
)
from dpone.contracts.semantic_refresh_schemas import (
    render_semantic_refresh_schema,
    semantic_refresh_contract_schemas,
)
from dpone.contracts.semantic_refresh_types import (
    DATE_DOMAIN_MAX,
    DATE_DOMAIN_MIN,
    DATETIME_DOMAIN_MAX,
    DATETIME_DOMAIN_MIN,
    ClosureStatus,
    EffectiveKeyColumn,
    ReplacementAction,
    SqlServerModelOutcome,
    WorkflowMode,
)
from dpone.contracts.semantic_refresh_workflow_plan import SemanticRefreshWorkflowPlan
from dpone.contracts.semantic_refresh_workflow_replacement import SemanticRefreshWorkflowReplacementPlan

_DIGEST = "sha256:" + "a" * 64
_OTHER_DIGEST = "sha256:" + "b" * 64
_MODEL_ID = "model.analytics.events"
_SCOPE_START = "2026-08-07T00:00:00Z"
_SCOPE_END = "2026-08-08T00:00:00Z"
_WORKFLOW_EXECUTION_ID = "scheduled__2026-08-07T00:00:00+00:00"
_REPLACEMENT_EXECUTION_ID = "replacement__2026-08-07T00:00:00+00:00"
_REPLAY_EXECUTION_ID = "replay__2026-08-07T00:00:00+00:00"


def test_semantic_refresh_vocabularies_are_closed() -> None:
    assert {item.value for item in WorkflowMode} == {
        "normal",
        "failed_precommit_replacement",
        "complete_scope_replay",
    }
    assert {item.value for item in ClosureStatus} == {
        "PROVEN",
        "NONCONFORMANT",
        "UNVERIFIED",
    }
    assert {item.value for item in SqlServerModelOutcome} == {
        "NOT_INVOKED",
        "ROLLED_BACK",
        "COMMITTED_WITH_IMAGES",
        "COMMIT_UNKNOWN",
    }
    assert {item.value for item in ReplacementAction} == {
        "RESTORE_THEN_REBUILD",
        "BUILD_FRESH",
        "BLOCK",
    }


def test_normal_workflow_rejects_noninitial_scope_revision() -> None:
    with pytest.raises(ValueError, match="scope revision 1"):
        SemanticRefreshWorkflowPlan.build(
            workflow_name="daily_events",
            workflow_mode=WorkflowMode.NORMAL,
            scope_start=_SCOPE_START,
            scope_end=_SCOPE_END,
            scope_revision=2,
            mutation_closure_sha256=_DIGEST,
            selected_mutating_node_ids=(_MODEL_ID,),
            model_operation_plan_ids=(_MODEL_ID,),
            expected_model_outcome_ids=(_MODEL_ID,),
            replacement_action_ids=(),
            operation_plan_refs=(OperationPlanReference(_MODEL_ID, _DIGEST, _OTHER_DIGEST),),
        )


@pytest.mark.parametrize(
    ("source_type", "target_type", "domain_min", "domain_max", "utc_assurance"),
    [
        ("date", "Date", DATE_DOMAIN_MIN, DATE_DOMAIN_MAX, None),
        (
            "datetime2(6)",
            "DateTime64(6,'UTC')",
            DATETIME_DOMAIN_MIN,
            DATETIME_DOMAIN_MAX,
            _DIGEST,
        ),
        ("decimal(38,10)", "Decimal(38,10)", None, None, None),
    ],
)
def test_effective_key_domains_accept_only_exact_injective_pairs(
    source_type: str,
    target_type: str,
    domain_min: str | None,
    domain_max: str | None,
    utc_assurance: str | None,
) -> None:
    column = EffectiveKeyColumn(
        name="event_key",
        source_type=source_type,
        target_type=target_type,
        domain_min=domain_min,
        domain_max=domain_max,
        utc_assurance_sha256=utc_assurance,
    )

    assert EffectiveKeyColumn.from_mapping(column.to_dict()) == column


@pytest.mark.parametrize(
    ("source_type", "target_type"),
    [
        ("nvarchar(20)", "String"),
        ("float", "Float64"),
        ("datetime2(7)", "DateTime64(6,'UTC')"),
        ("decimal(39,2)", "Decimal(39,2)"),
        ("decimal(18,4)", "Decimal(18,2)"),
        ("date", "Nullable(Date)"),
    ],
)
def test_effective_key_domains_reject_non_injective_or_unsupported_pairs(
    source_type: str,
    target_type: str,
) -> None:
    with pytest.raises(ValueError):
        EffectiveKeyColumn(
            name="event_key",
            source_type=source_type,
            target_type=target_type,
        )


def test_operation_rejects_non_temporal_event_time_key_in_python_and_schema() -> None:
    operation = _proofs_and_operation()[-1]
    bigint_key = next(column for column in operation.effective_key_columns if column.name == "event_id")
    with pytest.raises(ValueError, match="event_time_column must use"):
        SemanticRefreshOperationPlan.build(
            model_unique_id=operation.model_unique_id,
            release_id=operation.release_id,
            deployment_id=operation.deployment_id,
            environment=operation.environment,
            workflow_id=operation.workflow_id,
            scope_family_id=operation.scope_family_id,
            scope_revision=operation.scope_revision,
            target_predecessor_generation_id=operation.target_predecessor_generation_id,
            owner_generation=operation.owner_generation,
            platform_policy_digest=operation.platform_policy_digest,
            resource_policy_digest=operation.resource_policy_digest,
            writer_assurance_digest=operation.writer_assurance_digest,
            operation_kind=operation.operation_kind,
            model_definition_proof_sha256=operation.model_definition_proof_sha256,
            mutation_closure_sha256=operation.mutation_closure_sha256,
            sqlserver_lifecycle_policy_sha256=operation.sqlserver_lifecycle_policy_sha256,
            read_dependency_proof_sha256=operation.read_dependency_proof_sha256,
            scope_start=operation.scope_start,
            scope_end=operation.scope_end,
            event_time_column=bigint_key.name,
            effective_key_columns=(bigint_key,),
        )

    invalid = {**operation.to_dict(), "event_time_column": bigint_key.name}
    invalid["effective_key_columns"] = [bigint_key.to_dict()]
    with pytest.raises(ValidationError):
        Draft202012Validator(semantic_refresh_contract_schemas()[operation.schema]).validate(invalid)


def _proofs_and_operation() -> tuple[object, ...]:
    model_proof = SemanticRefreshModelDefinitionProof.build(
        status=ClosureStatus.PROVEN,
        model_unique_id=_MODEL_ID,
        manifest_sha256=_DIGEST,
        raw_code_sha256=_DIGEST,
        compiled_sql_sha256=_DIGEST,
        macro_closure_sha256=_DIGEST,
        toolchain_sha256=_DIGEST,
        resolved_relation_dependency_digest=_DIGEST,
        resolved_module_dependency_digest=_DIGEST,
        catalog_observation_digest=_DIGEST,
        parser_runtime_policy_digest=_DIGEST,
        target_independence_policy_digest=_DIGEST,
    )
    mutation_closure = SemanticRefreshMutationClosure.build(
        status=ClosureStatus.PROVEN,
        selectors=(_MODEL_ID,),
        selected_node_ids=(_MODEL_ID,),
        selected_mutating_node_ids=(_MODEL_ID,),
        selected_read_only_node_ids=(),
        unclassified_mutating_node_ids=(),
    )
    lifecycle = SemanticRefreshSqlServerLifecyclePolicy.build(
        python_version="3.12.12",
        runtime_image_digest=_DIGEST,
        pyodbc_version="5.2.0",
        odbc_driver="ODBC Driver 18 for SQL Server",
        sqlserver_version="16.0",
        compatibility_level=160,
        macro_closure_sha256=_DIGEST,
        adapter_policy_digest=_DIGEST,
        project_policy_digest=_DIGEST,
        profile_policy_digest=_DIGEST,
        invocation_policy_digest=_DIGEST,
        package_artifacts_digest=_DIGEST,
        materialization_closure_digest=_DIGEST,
        dispatch_closure_digest=_DIGEST,
        driver_digest=_DIGEST,
    )
    dependency_proof = SemanticRefreshReadDependencyProof.build(
        status=ClosureStatus.PROVEN,
        model_unique_id=_MODEL_ID,
        database_name="analytics",
        compiled_sql_sha256=_DIGEST,
        catalog_snapshot_sha256=_DIGEST,
        normalized_definitions_sha256=_DIGEST,
        policy_sha256=_DIGEST,
        dependency_edges=(ReadDependencyEdge(100, 200, ReadDependencyKind.BASE_TABLE),),
        base_relation_object_ids=(200,),
        max_depth=8,
        max_nodes=100,
        max_edges=200,
        max_definition_bytes=1_000_000,
    )
    operation = SemanticRefreshOperationPlan.build(
        model_unique_id=_MODEL_ID,
        release_id=_DIGEST,
        deployment_id=_DIGEST,
        environment="production",
        workflow_id="daily_events",
        scope_family_id=_DIGEST,
        scope_revision=1,
        target_predecessor_generation_id=_DIGEST,
        owner_generation=3,
        platform_policy_digest=_DIGEST,
        resource_policy_digest=_DIGEST,
        writer_assurance_digest=_DIGEST,
        operation_kind=WorkflowMode.NORMAL,
        model_definition_proof_sha256=model_proof.model_definition_proof_sha256,
        mutation_closure_sha256=mutation_closure.mutation_closure_sha256,
        sqlserver_lifecycle_policy_sha256=lifecycle.sqlserver_lifecycle_policy_sha256,
        read_dependency_proof_sha256=dependency_proof.read_dependency_proof_sha256,
        scope_start=_SCOPE_START,
        scope_end=_SCOPE_END,
        event_time_column="event_at",
        effective_key_columns=(
            EffectiveKeyColumn(
                name="event_at",
                source_type="datetime2(6)",
                target_type="DateTime64(6,'UTC')",
                domain_min=DATETIME_DOMAIN_MIN,
                domain_max=DATETIME_DOMAIN_MAX,
                utc_assurance_sha256=_DIGEST,
            ),
            EffectiveKeyColumn(
                name="event_id",
                source_type="bigint",
                target_type="Int64",
            ),
        ),
    )
    return model_proof, mutation_closure, lifecycle, dependency_proof, operation


def _documents() -> tuple[object, ...]:
    model_proof, mutation_closure, lifecycle, dependency_proof, normal_operation = _proofs_and_operation()
    operation = _operation_for_mode(normal_operation, WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT)
    operation_ref = OperationPlanReference(
        model_unique_id=_MODEL_ID,
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
    )
    workflow = SemanticRefreshWorkflowPlan.build(
        workflow_name="daily_events",
        workflow_mode=WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT,
        scope_start=_SCOPE_START,
        scope_end=_SCOPE_END,
        scope_revision=1,
        mutation_closure_sha256=mutation_closure.mutation_closure_sha256,
        selected_mutating_node_ids=(_MODEL_ID,),
        model_operation_plan_ids=(_MODEL_ID,),
        expected_model_outcome_ids=(_MODEL_ID,),
        replacement_action_ids=(_MODEL_ID,),
        operation_plan_refs=(operation_ref,),
    )
    predecessor_execution, predecessor_summary = _predecessor_authority(normal_operation)
    replacement = SemanticRefreshWorkflowReplacementPlan.build(
        workflow_plan_sha256=workflow.workflow_plan_sha256,
        predecessor_workflow_execution_id="failed-workflow-2026-08-07",
        predecessor_workflow_execution_binding_sha256=(predecessor_execution.workflow_execution_binding_sha256),
        predecessor_workflow_summary_sha256=predecessor_summary.terminal_summary_sha256,
        recovery_plan_digest=_DIGEST,
        selected_mutating_node_ids=(_MODEL_ID,),
        model_operation_plan_ids=(_MODEL_ID,),
        expected_model_outcome_ids=(_MODEL_ID,),
        replacement_action_ids=(_MODEL_ID,),
        replacement_actions=(
            ReplacementActionBinding(
                action_id=_MODEL_ID,
                outcome=SqlServerModelOutcome.ROLLED_BACK,
                action=ReplacementAction.BUILD_FRESH,
            ),
        ),
    )
    execution = SemanticRefreshWorkflowExecutionBinding.build(
        workflow_execution_id=_REPLACEMENT_EXECUTION_ID,
        workflow_mode=WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT,
        workflow_plan_sha256=workflow.workflow_plan_sha256,
        selected_mutating_node_ids=(_MODEL_ID,),
        model_operation_plan_ids=(_MODEL_ID,),
        expected_model_outcome_ids=(_MODEL_ID,),
        replacement_action_ids=(_MODEL_ID,),
        deployment_id=_DIGEST,
        binding_set_ref="prod-bindings-v1",
        connection_registry_ref="prod-connections-v1",
        credential_runtime_ref="vault-runtime-v1",
        workflow_replacement_plan_sha256=replacement.workflow_replacement_plan_sha256,
        recovery_plan_digest=replacement.recovery_plan_digest,
    )
    attempt = SemanticRefreshAttemptBinding.build(
        workflow_execution_id=execution.workflow_execution_id,
        workflow_execution_binding_sha256=execution.workflow_execution_binding_sha256,
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        dag_run_id=execution.workflow_execution_id,
        task_id="semantic_refresh__events",
        try_number=1,
        pod_uid="0198f11c-6956-74f2-984b-4cfcb1653b87",
        fencing_epoch=7,
        owner_id="airflow-controller",
    )
    return (
        operation,
        workflow,
        replacement,
        execution,
        attempt,
        model_proof,
        mutation_closure,
        lifecycle,
        dependency_proof,
    )


def _operation_for_mode(
    normal: SemanticRefreshOperationPlan,
    mode: WorkflowMode,
) -> SemanticRefreshOperationPlan:
    if mode is WorkflowMode.NORMAL:
        return normal
    return SemanticRefreshOperationPlan.build(
        model_unique_id=normal.model_unique_id,
        release_id=normal.release_id,
        deployment_id=normal.deployment_id,
        environment=normal.environment,
        workflow_id=normal.workflow_id,
        scope_family_id=normal.scope_family_id,
        scope_revision=2 if mode is WorkflowMode.COMPLETE_SCOPE_REPLAY else normal.scope_revision,
        target_predecessor_generation_id=normal.target_predecessor_generation_id,
        owner_generation=normal.owner_generation,
        platform_policy_digest=normal.platform_policy_digest,
        resource_policy_digest=normal.resource_policy_digest,
        writer_assurance_digest=normal.writer_assurance_digest,
        operation_kind=mode,
        model_definition_proof_sha256=normal.model_definition_proof_sha256,
        mutation_closure_sha256=normal.mutation_closure_sha256,
        sqlserver_lifecycle_policy_sha256=normal.sqlserver_lifecycle_policy_sha256,
        read_dependency_proof_sha256=normal.read_dependency_proof_sha256,
        scope_start=normal.scope_start,
        scope_end=normal.scope_end,
        event_time_column=normal.event_time_column,
        effective_key_columns=normal.effective_key_columns,
        scope_predecessor_operation_id=(normal.operation_id if mode is WorkflowMode.COMPLETE_SCOPE_REPLAY else None),
        replaces_failed_operation_id=(
            normal.operation_id if mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT else None
        ),
        replacement_reason=("model_correction" if mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT else None),
        replacement_ordinal=(1 if mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT else None),
    )


def _predecessor_authority(
    operation: SemanticRefreshOperationPlan,
) -> tuple[SemanticRefreshWorkflowExecutionBinding, SemanticRefreshFailedWorkflowSummary]:
    workflow = SemanticRefreshWorkflowPlan.build(
        workflow_name="daily_events",
        workflow_mode=WorkflowMode.NORMAL,
        scope_start=operation.scope_start,
        scope_end=operation.scope_end,
        scope_revision=operation.scope_revision,
        mutation_closure_sha256=operation.mutation_closure_sha256,
        selected_mutating_node_ids=(_MODEL_ID,),
        model_operation_plan_ids=(_MODEL_ID,),
        expected_model_outcome_ids=(_MODEL_ID,),
        replacement_action_ids=(),
        operation_plan_refs=(
            OperationPlanReference(_MODEL_ID, operation.operation_id, operation.operation_plan_sha256),
        ),
    )
    execution = SemanticRefreshWorkflowExecutionBinding.build(
        workflow_execution_id="failed-workflow-2026-08-07",
        workflow_mode=WorkflowMode.NORMAL,
        workflow_plan_sha256=workflow.workflow_plan_sha256,
        selected_mutating_node_ids=(_MODEL_ID,),
        model_operation_plan_ids=(_MODEL_ID,),
        expected_model_outcome_ids=(_MODEL_ID,),
        replacement_action_ids=(),
        deployment_id=_DIGEST,
        binding_set_ref="prod-bindings-v1",
        connection_registry_ref="prod-connections-v1",
        credential_runtime_ref="vault-runtime-v1",
    )
    summary = SemanticRefreshFailedWorkflowSummary.build(
        workflow_id="failed-workflow-2026-08-07",
        workflow_plan_sha256=workflow.workflow_plan_sha256,
        workflow_execution_binding_sha256=execution.workflow_execution_binding_sha256,
        expected_operation_ids=(operation.operation_id,),
        models=(
            FailedModelOutcome(
                operation_id=operation.operation_id,
                attempt_binding_sha256=_DIGEST,
                mssql_outcome=SqlServerModelOutcome.ROLLED_BACK,
                mssql_evidence_sha256=_OTHER_DIGEST,
            ),
        ),
    )
    return execution, summary


def _mode_chain(
    mode: WorkflowMode,
) -> tuple[
    SemanticRefreshOperationPlan,
    SemanticRefreshWorkflowPlan,
    SemanticRefreshWorkflowReplacementPlan | None,
    SemanticRefreshWorkflowExecutionBinding,
]:
    normal = _proofs_and_operation()[-1]
    operation = _operation_for_mode(normal, mode)
    replacement_action_ids = (_MODEL_ID,) if mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT else ()
    workflow = SemanticRefreshWorkflowPlan.build(
        workflow_name="daily_events",
        workflow_mode=mode,
        scope_start=_SCOPE_START,
        scope_end=_SCOPE_END,
        scope_revision=operation.scope_revision,
        mutation_closure_sha256=operation.mutation_closure_sha256,
        selected_mutating_node_ids=(_MODEL_ID,),
        model_operation_plan_ids=(_MODEL_ID,),
        expected_model_outcome_ids=(_MODEL_ID,),
        replacement_action_ids=replacement_action_ids,
        operation_plan_refs=(
            OperationPlanReference(_MODEL_ID, operation.operation_id, operation.operation_plan_sha256),
        ),
    )
    predecessor_execution, predecessor_summary = _predecessor_authority(normal)
    replacement = (
        SemanticRefreshWorkflowReplacementPlan.build(
            workflow_plan_sha256=workflow.workflow_plan_sha256,
            predecessor_workflow_execution_id="failed-workflow-2026-08-07",
            predecessor_workflow_execution_binding_sha256=(predecessor_execution.workflow_execution_binding_sha256),
            predecessor_workflow_summary_sha256=predecessor_summary.terminal_summary_sha256,
            recovery_plan_digest=_DIGEST,
            selected_mutating_node_ids=(_MODEL_ID,),
            model_operation_plan_ids=(_MODEL_ID,),
            expected_model_outcome_ids=(_MODEL_ID,),
            replacement_action_ids=(_MODEL_ID,),
            replacement_actions=(
                ReplacementActionBinding(
                    action_id=_MODEL_ID,
                    outcome=SqlServerModelOutcome.ROLLED_BACK,
                    action=ReplacementAction.BUILD_FRESH,
                ),
            ),
        )
        if mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT
        else None
    )
    execution = SemanticRefreshWorkflowExecutionBinding.build(
        workflow_execution_id=_workflow_execution_id(mode),
        workflow_mode=mode,
        workflow_plan_sha256=workflow.workflow_plan_sha256,
        selected_mutating_node_ids=(_MODEL_ID,),
        model_operation_plan_ids=(_MODEL_ID,),
        expected_model_outcome_ids=(_MODEL_ID,),
        replacement_action_ids=replacement_action_ids,
        deployment_id=_DIGEST,
        binding_set_ref="prod-bindings-v1",
        connection_registry_ref="prod-connections-v1",
        credential_runtime_ref="vault-runtime-v1",
        workflow_replacement_plan_sha256=(
            replacement.workflow_replacement_plan_sha256 if replacement is not None else None
        ),
        recovery_plan_digest=replacement.recovery_plan_digest if replacement is not None else None,
    )
    return operation, workflow, replacement, execution


def _workflow_mode_vectors() -> dict[str, dict[str, object]]:
    return {mode.value: _mode_chain(mode)[1].to_dict() for mode in WorkflowMode}


def _workflow_execution_id(mode: WorkflowMode) -> str:
    return {
        WorkflowMode.NORMAL: _WORKFLOW_EXECUTION_ID,
        WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT: _REPLACEMENT_EXECUTION_ID,
        WorkflowMode.COMPLETE_SCOPE_REPLAY: _REPLAY_EXECUTION_ID,
    }[mode]


def _execution_mode_vectors() -> dict[str, dict[str, object]]:
    return {mode.value: _mode_chain(mode)[3].to_dict() for mode in WorkflowMode}


def _identity_mode_chains() -> dict[str, dict[str, object]]:
    chains: dict[str, dict[str, object]] = {}
    for mode in WorkflowMode:
        operation, workflow, replacement, execution = _mode_chain(mode)
        chain: dict[str, object] = {
            "operation_plan": operation.to_dict(),
            "workflow_plan": workflow.to_dict(),
            "execution_binding": execution.to_dict(),
        }
        if replacement is not None:
            chain["replacement_plan"] = replacement.to_dict()
            predecessor_execution, predecessor_summary = _predecessor_authority(_proofs_and_operation()[-1])
            chain["predecessor_execution_binding"] = predecessor_execution.to_dict()
            chain["predecessor_failed_summary"] = predecessor_summary.to_dict()
        chains[mode.value] = chain
    return chains


def test_all_nine_documents_reject_unknown_fields_and_recompute_digests() -> None:
    for document in _documents():
        payload = document.to_dict()
        assert type(document).from_mapping(payload) == document
        assert type(document).from_json(document.canonical_bytes()) == document

        with pytest.raises(ValueError, match="unknown"):
            type(document).from_mapping({**payload, "created_at": "runtime"})

        semantic_field = next(
            field_name for field_name in payload if field_name not in {"schema", document.digest_field}
        )
        mutated = dict(payload)
        mutated[semantic_field] = _OTHER_DIGEST if semantic_field.endswith(("sha256", "digest")) else "changed"
        with pytest.raises(ValueError, match="differs|invalid|unsupported|must|requires"):
            type(document).from_mapping(mutated)


def test_failed_workflow_summary_is_closed_and_rejects_commit_unknown() -> None:
    operation = _proofs_and_operation()[-1]
    outcome = FailedModelOutcome(
        operation_id=operation.operation_id,
        attempt_binding_sha256=_DIGEST,
        mssql_outcome=SqlServerModelOutcome.COMMITTED_WITH_IMAGES,
        mssql_evidence_sha256=_OTHER_DIGEST,
    )
    summary = SemanticRefreshFailedWorkflowSummary.build(
        workflow_id="workflow-2026-08-07",
        workflow_plan_sha256=_DIGEST,
        workflow_execution_binding_sha256=_OTHER_DIGEST,
        expected_operation_ids=(operation.operation_id,),
        models=(outcome,),
    )

    assert SemanticRefreshFailedWorkflowSummary.from_mapping(summary.to_dict()) == summary
    Draft202012Validator(semantic_refresh_contract_schemas()[summary.schema]).validate(summary.to_dict())
    with pytest.raises(ValueError, match="unresolved"):
        FailedModelOutcome(
            operation_id=operation.operation_id,
            attempt_binding_sha256=_DIGEST,
            mssql_outcome=SqlServerModelOutcome.COMMIT_UNKNOWN,
            mssql_evidence_sha256=_OTHER_DIGEST,
        )


def test_operation_identity_has_no_attempt_or_runtime_extension_surface() -> None:
    operation = _proofs_and_operation()[-1]
    for forbidden_field in (
        "dag_run_id",
        "try_number",
        "pod_uid",
        "fencing_epoch",
        "created_at",
        "row_count",
        "artifact_sha256",
        "outcome",
    ):
        with pytest.raises(ValueError, match="unknown"):
            SemanticRefreshOperationPlan.from_mapping({**operation.to_dict(), forbidden_field: "runtime"})

    original_attempt = _documents()[4]
    changed_attempt = SemanticRefreshAttemptBinding.build(
        workflow_execution_id=original_attempt.workflow_execution_id,
        workflow_execution_binding_sha256=original_attempt.workflow_execution_binding_sha256,
        operation_id=original_attempt.operation_id,
        operation_plan_sha256=original_attempt.operation_plan_sha256,
        dag_run_id=original_attempt.workflow_execution_id,
        task_id=original_attempt.task_id,
        try_number=2,
        pod_uid="different-pod",
        fencing_epoch=8,
        owner_id=original_attempt.owner_id,
    )
    assert changed_attempt.operation_id == original_attempt.operation_id


def test_execution_binding_owns_logical_dagrun_and_attempt_cannot_cross_it() -> None:
    execution = _mode_chain(WorkflowMode.NORMAL)[3]
    assert execution.workflow_execution_id == _WORKFLOW_EXECUTION_ID
    changed = SemanticRefreshWorkflowExecutionBinding.build(
        workflow_execution_id="manual__different-logical-run",
        workflow_mode=execution.workflow_mode,
        workflow_plan_sha256=execution.workflow_plan_sha256,
        selected_mutating_node_ids=execution.selected_mutating_node_ids,
        model_operation_plan_ids=execution.model_operation_plan_ids,
        expected_model_outcome_ids=execution.expected_model_outcome_ids,
        replacement_action_ids=execution.replacement_action_ids,
        deployment_id=execution.deployment_id,
        binding_set_ref=execution.binding_set_ref,
        connection_registry_ref=execution.connection_registry_ref,
        credential_runtime_ref=execution.credential_runtime_ref,
    )
    assert changed.workflow_execution_binding_sha256 != execution.workflow_execution_binding_sha256

    with pytest.raises(ValueError, match="workflow_execution_id.*dag_run_id"):
        SemanticRefreshAttemptBinding.build(
            workflow_execution_id=execution.workflow_execution_id,
            workflow_execution_binding_sha256=execution.workflow_execution_binding_sha256,
            operation_id=_DIGEST,
            operation_plan_sha256=_OTHER_DIGEST,
            dag_run_id="manual__different-logical-run",
            task_id="semantic_refresh__events",
            try_number=1,
            pod_uid="different-pod",
            fencing_epoch=1,
            owner_id="airflow-controller",
        )


def test_operation_identity_binds_every_stable_intent_and_effect_coordinate() -> None:
    operation = _proofs_and_operation()[-1]
    identity = operation.to_dict()
    identity.pop(operation.digest_field)
    identity.pop("operation_id")
    stable_fields = {
        "release_id",
        "deployment_id",
        "environment",
        "workflow_id",
        "scope_family_id",
        "scope_revision",
        "target_predecessor_generation_id",
        "owner_generation",
        "platform_policy_digest",
        "resource_policy_digest",
        "writer_assurance_digest",
        "operation_kind",
    }
    assert stable_fields <= identity.keys()
    for field_name in stable_fields:
        changed = dict(identity)
        changed[field_name] = (
            _OTHER_DIGEST
            if field_name.endswith(("_id", "_digest"))
            else 2
            if field_name in {"scope_revision", "owner_generation"}
            else "changed"
        )
        assert semantic_refresh_sha256(changed) != operation.operation_id


def test_identity_references_are_strictly_one_way() -> None:
    operation, workflow, replacement, execution, attempt, *_ = _documents()
    assert workflow.operation_plan_refs[0].operation_plan_sha256 == operation.operation_plan_sha256
    assert replacement.workflow_plan_sha256 == workflow.workflow_plan_sha256
    assert execution.workflow_plan_sha256 == workflow.workflow_plan_sha256
    assert attempt.workflow_execution_binding_sha256 == execution.workflow_execution_binding_sha256
    assert attempt.workflow_execution_id == execution.workflow_execution_id == attempt.dag_run_id
    assert "recovery_plan_digest" not in workflow.to_dict()

    for mode, chain in _identity_mode_chains().items():
        operation = SemanticRefreshOperationPlan.from_mapping(chain["operation_plan"])
        workflow = SemanticRefreshWorkflowPlan.from_mapping(chain["workflow_plan"])
        execution = SemanticRefreshWorkflowExecutionBinding.from_mapping(chain["execution_binding"])
        assert operation.operation_kind.value == mode
        assert workflow.workflow_mode.value == mode
        assert execution.workflow_mode.value == mode
        assert workflow.operation_plan_refs[0].operation_id == operation.operation_id
        assert execution.workflow_plan_sha256 == workflow.workflow_plan_sha256
        if mode == WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT.value:
            replacement = SemanticRefreshWorkflowReplacementPlan.from_mapping(chain["replacement_plan"])
            predecessor = SemanticRefreshWorkflowExecutionBinding.from_mapping(chain["predecessor_execution_binding"])
            predecessor_summary = SemanticRefreshFailedWorkflowSummary.from_mapping(chain["predecessor_failed_summary"])
            assert replacement.workflow_plan_sha256 == workflow.workflow_plan_sha256
            assert (
                replacement.predecessor_workflow_execution_binding_sha256
                == predecessor.workflow_execution_binding_sha256
            )
            assert replacement.predecessor_workflow_execution_id == predecessor.workflow_execution_id
            assert (
                predecessor_summary.workflow_execution_binding_sha256 == predecessor.workflow_execution_binding_sha256
            )
            assert replacement.predecessor_workflow_summary_sha256 == predecessor_summary.terminal_summary_sha256
            assert execution.workflow_replacement_plan_sha256 == replacement.workflow_replacement_plan_sha256


def test_workflow_modes_enforce_replacement_branch_invariants() -> None:
    workflow = _mode_chain(WorkflowMode.NORMAL)[1]
    with pytest.raises(ValueError, match="replacement"):
        SemanticRefreshWorkflowPlan.from_mapping({**workflow.to_dict(), "replacement_action_ids": [_MODEL_ID]})

    replacement = _documents()[2]
    payload = replacement.to_dict()
    payload.pop("recovery_plan_digest")
    with pytest.raises(ValueError, match="missing"):
        SemanticRefreshWorkflowReplacementPlan.from_mapping(payload)

    with pytest.raises(ValueError, match="replacement"):
        SemanticRefreshWorkflowPlan.from_mapping(
            {**workflow.to_dict(), "workflow_mode": WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT.value}
        )

    for mode, mode_plan in _workflow_mode_vectors().items():
        assert mode_plan["workflow_mode"] == mode
        assert "recovery_plan_digest" not in mode_plan


def test_replacement_action_is_derived_from_durable_mssql_outcome() -> None:
    with pytest.raises(ValueError, match="outcome"):
        ReplacementActionBinding(
            action_id=_MODEL_ID,
            outcome=SqlServerModelOutcome.COMMIT_UNKNOWN,
            action=ReplacementAction.BUILD_FRESH,
        )


def test_checked_in_schemas_match_deterministic_public_generator() -> None:
    schema_root = Path("docs/schemas/dbt")
    schemas = semantic_refresh_contract_schemas()
    assert len(schemas) == 22
    for schema_id, schema in schemas.items():
        Draft202012Validator.check_schema(schema)
        filename = f"{schema_id}.schema.json"
        checked_in = (schema_root / filename).read_bytes()
        assert checked_in == render_semantic_refresh_schema(schema_id)
        assert json.loads(checked_in) == schema
    for document in _documents():
        Draft202012Validator(schemas[document.schema]).validate(document.to_dict())
    for workflow in _workflow_mode_vectors().values():
        Draft202012Validator(schemas[workflow["schema"]]).validate(workflow)
    for execution in _execution_mode_vectors().values():
        Draft202012Validator(schemas[execution["schema"]]).validate(execution)


def test_canonical_golden_vector_is_stable() -> None:
    fixture = Path("tests/fixtures/semantic-refresh-v2/contracts/golden-v1.json")
    expected = json.loads(fixture.read_text(encoding="utf-8"))
    actual = {
        "documents": {document.schema: document.to_dict() for document in _documents()},
        "execution_mode_vectors": _execution_mode_vectors(),
        "identity_mode_chains": _identity_mode_chains(),
        "workflow_mode_vectors": _workflow_mode_vectors(),
    }
    assert actual == expected
