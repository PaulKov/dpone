"""Opt-in local SQL Server fault proof for semantic-refresh admission.

Run against the repository Docker service with::

    DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1 \
      uv run pytest tests/test_semantic_refresh_mssql_live.py -q

Standard ``DPONE_IT_MSSQL_*`` variables override the local defaults. This is
local integration evidence; it is not production-route certification.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Callable
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dpone.adapters.semantic_refresh_mssql_activation import (
    MssqlSemanticRefreshActivationStore,
    SemanticRefreshMssqlActivationError,
)
from dpone.adapters.semantic_refresh_mssql_activation_authority import (
    MssqlSemanticRefreshActivationAuthorityStore,
)
from dpone.adapters.semantic_refresh_mssql_attempt_quiescence import (
    MssqlSemanticRefreshAttemptQuiescenceObserver,
    SemanticRefreshMssqlAttemptQuiescenceError,
)
from dpone.adapters.semantic_refresh_mssql_authority import (
    MssqlSemanticRefreshCanonicalAuthorityLoader,
    SemanticRefreshMssqlAuthorityLoadError,
)
from dpone.adapters.semantic_refresh_mssql_cleanup_ack import (
    MssqlSemanticRefreshFailedPrecommitCleanupAckStore,
)
from dpone.adapters.semantic_refresh_mssql_evidence import (
    MssqlSemanticRefreshEvidenceReader,
    SemanticRefreshMssqlEvidenceReadError,
)
from dpone.adapters.semantic_refresh_mssql_failure import (
    MssqlSemanticRefreshWorkflowFailureState,
    SemanticRefreshWorkflowFailureError,
)
from dpone.adapters.semantic_refresh_mssql_prerequisites import (
    MssqlSemanticRefreshPrerequisiteAuthority,
)
from dpone.adapters.semantic_refresh_mssql_protected_authority import (
    MssqlSemanticRefreshProtectedOperationState,
)
from dpone.adapters.semantic_refresh_mssql_publication import (
    MssqlSemanticRefreshPublicationState,
    SemanticRefreshMssqlPublicationError,
)
from dpone.adapters.semantic_refresh_mssql_recovery_heads import (
    MssqlSemanticRefreshRecoveryHeadReader,
)
from dpone.adapters.semantic_refresh_mssql_replacement import (
    MssqlSemanticRefreshPredecessorStateReader,
)
from dpone.adapters.semantic_refresh_mssql_resource_authority import (
    MssqlSemanticRefreshProtectedResourceLedger,
)
from dpone.adapters.semantic_refresh_mssql_resources import (
    MssqlSemanticRefreshResourceLedger,
    SemanticRefreshResourceReservationError,
)
from dpone.adapters.semantic_refresh_mssql_run_authority import (
    MssqlSemanticRefreshWorkerRunAuthority,
)
from dpone.adapters.semantic_refresh_mssql_schema import (
    SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION,
    MssqlSemanticRefreshSchemaMigration,
)
from dpone.adapters.semantic_refresh_mssql_scratch_cleanup import (
    MssqlSemanticRefreshFailedScratchCleanupReader,
    SemanticRefreshMssqlScratchCleanupReadError,
)
from dpone.adapters.semantic_refresh_mssql_seal import (
    MssqlSemanticRefreshSealAuthorizationStore,
    SemanticRefreshMssqlSealAuthorizationError,
)
from dpone.adapters.semantic_refresh_mssql_state import (
    MssqlSemanticRefreshStateAdapter,
    SemanticRefreshMssqlStateConflict,
)
from dpone.adapters.semantic_refresh_mssql_termination import (
    MssqlSemanticRefreshTerminationAdapter,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary import (
    MssqlSemanticRefreshWorkflowSummaryState,
)
from dpone.app.semantic_refresh_recovery_authority import (
    SemanticRefreshMssqlRecoveryAuthorityVerifier,
)
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
    SemanticRefreshActivationAuthoritySet,
)
from dpone.contracts.dbt_semantic_refresh_plan_compiler import (
    SemanticRefreshPostDeploymentPlanCompiler,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshDeploymentAuthoritySubject,
)
from dpone.contracts.dbt_semantic_refresh_recovery_authority import (
    SemanticRefreshCompleteScopeReplayAuthority,
    SemanticRefreshFailedPrecommitReplacementAuthority,
)
from dpone.contracts.dbt_semantic_refresh_recovery_head import (
    SemanticRefreshRecoveryTargetHead,
)
from dpone.contracts.dbt_semantic_refresh_run_contracts import (
    SemanticRefreshRunAdmissionAuthority,
    SemanticRefreshRunAdmissionCompiler,
)
from dpone.contracts.semantic_refresh_artifact_authority_identity import (
    semantic_refresh_artifact_authority_sha256,
)
from dpone.contracts.semantic_refresh_attempt_binding import (
    SemanticRefreshAttemptBinding,
)
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError
from dpone.contracts.semantic_refresh_plan_refs import ReplacementActionBinding
from dpone.contracts.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationReceipt,
)
from dpone.contracts.semantic_refresh_types import ReplacementAction, SqlServerModelOutcome
from dpone.contracts.semantic_refresh_workflow_summary import (
    SemanticRefreshDurableModelPublication,
    SemanticRefreshDurableWorkflowSummary,
)
from dpone.ports.semantic_refresh_attempt_quiescence import (
    ClickHouseAttemptQuiescenceProof,
)
from dpone.ports.semantic_refresh_clickhouse_authority import (
    clickhouse_operation_table_names,
)
from dpone.ports.semantic_refresh_clickhouse_resources import (
    ClickHouseFailedScratchCleanupReceipt,
    ClickHouseScratchRelationAbsence,
)
from dpone.ports.semantic_refresh_mssql import (
    MssqlAdmissionRequest,
    MssqlBuildReceiptEvidence,
    MssqlGuardClaim,
    MssqlImageKeyColumn,
    MssqlJournalPreparation,
    MssqlTransactionDisposition,
    MssqlWorkflowSuccessorClaim,
    mssql_guard_set_sha256,
    mssql_journal_set_sha256,
    mssql_session_evidence_sha256,
    mssql_strategy_authority_sha256,
)
from dpone.ports.semantic_refresh_mssql_activation import (
    MssqlActivatedPackRegistration,
    MssqlRunAuthorityRegistration,
)
from dpone.ports.semantic_refresh_mssql_admission_authority import compose_admission
from dpone.ports.semantic_refresh_mssql_authority import MssqlCanonicalAuthorityRecord
from dpone.ports.semantic_refresh_mssql_authority_models import (
    MssqlCanonicalAdmissionBundle,
    mssql_model_resource_authority_from_plan_target,
)
from dpone.ports.semantic_refresh_mssql_cleanup_ack import (
    MssqlFailedPrecommitCleanupAck,
)
from dpone.ports.semantic_refresh_mssql_resources import (
    MSSQL_RESOURCE_KINDS,
    MssqlProtectedResourceAllocation,
    MssqlProtectedResourceAllocationClosure,
    mssql_resource_allocation_id,
)
from dpone.ports.semantic_refresh_mssql_worker_pack import (
    MssqlStaticProjectionIdentity,
    mssql_worker_pack_fingerprint,
)
from dpone.ports.semantic_refresh_seal_policy import (
    SemanticRefreshSealPolicyAuthority,
    SemanticRefreshSealPolicySubject,
)
from dpone.ports.semantic_refresh_termination import (
    AttemptTerminationReceipt,
    ContainerTermination,
)
from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHousePreparedReceipt,
    ClickHousePreparePlan,
    GroupedMultisetConformance,
    ShadowEquation,
    semantic_refresh_fingerprint,
)
from dpone.runtime.semantic_refresh_clickhouse_prepared_codec import (
    prepared_publication_documents,
)
from dpone.runtime.semantic_refresh_parquet_authority import (
    DponeParquetV1SealCodecAuthority,
)
from dpone.services.semantic_refresh_mssql_activation import (
    SemanticRefreshMssqlActivationService,
    SemanticRefreshMssqlWorkerRunAuthorityService,
)
from dpone.services.semantic_refresh_mssql_authority import (
    SemanticRefreshMssqlAuthorityAdmissionService,
    SemanticRefreshMssqlProtectedAuthorityService,
    semantic_refresh_mssql_authority_json,
)
from dpone.services.semantic_refresh_mssql_failure import (
    SemanticRefreshMssqlFailureTerminalizationService,
    SemanticRefreshMssqlOutcomeService,
)
from dpone.services.semantic_refresh_mssql_recovery_heads import (
    SemanticRefreshMssqlRecoveryHeadService,
)
from dpone.services.semantic_refresh_mssql_replacement import (
    SemanticRefreshMssqlFailedScratchCleanupService,
)
from dpone.services.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationIssuer,
)
from tests.test_dbt_semantic_refresh_plan_compiler import (
    _AssuranceVerifier,
    _authority,
    _compile_kwargs,
    _deployment_model,
    _plan_bundle,
    _route_receipt,
    _runtime_assurances,
)
from tests.test_semantic_refresh_mssql_authority import _activation_authority, _bundle


class _AllowLocalActivation:
    def authorize(self, **_: object) -> None:
        return None


pytestmark = [pytest.mark.integration_mssql, pytest.mark.integration_live]
_ENABLED = os.environ.get("DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE") == "1"
_SCHEMA = "dpone_sr_live"
_MACRO_SCHEMA = "dpone_sr_macro"
_FAILED_OPERATION_ID = "sha256:" + "7" * 64


def _live_activated_pack(
    *,
    plan,
    workflow_execution_id: str,
    workflow_execution_binding_sha256: str,
    workflow_plan_sha256: str,
    run_execution_bundle_sha256: str,
    activation_authority_receipt_sha256: str,
    authority_store_ref: str,
) -> MssqlActivatedPackRegistration:
    projection = MssqlStaticProjectionIdentity(
        dag_projection_sha256="sha256:" + "1" * 64,
        deployment_id=plan.release_deployment_authority.deployment_id,
        package_artifacts_sha256=plan.package_artifacts_sha256,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        pre_release_bundle_sha256=plan.pre_release_bundle_sha256,
        release_id=plan.release_deployment_authority.release_id,
        template_pack_fingerprint="sha256:" + "2" * 64,
        topology_sha256="sha256:" + "3" * 64,
        workflow_plan_sha256=workflow_plan_sha256,
    )
    return MssqlActivatedPackRegistration(
        pack_fingerprint=mssql_worker_pack_fingerprint(
            projection_identity=projection,
            run_execution_bundle_sha256=run_execution_bundle_sha256,
            activation_authority_receipt_sha256=activation_authority_receipt_sha256,
            authority_store_ref=authority_store_ref,
            run_guard_closure_sha256=plan.run_guard_closure.run_guard_closure_sha256,
        ),
        activation_authority_receipt_sha256=activation_authority_receipt_sha256,
        authority_store_ref=authority_store_ref,
        workflow_execution_id=workflow_execution_id,
        workflow_execution_binding_sha256=workflow_execution_binding_sha256,
        workflow_plan_sha256=workflow_plan_sha256,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        run_execution_bundle_sha256=run_execution_bundle_sha256,
        run_guard_closure=plan.run_guard_closure,
        projection_identity=projection,
    )


def _register_live_activated_pack(
    factory: Callable[[], Any],
    *,
    plan: Any,
    activation_receipt: SemanticRefreshActivationAuthorityReceipt,
    workflow_execution_id: str,
    workflow_execution_binding_sha256: str,
    workflow_plan_sha256: str,
    run_execution_bundle_sha256: str,
) -> None:
    MssqlSemanticRefreshActivationStore(
        factory,
        control_schema=_SCHEMA,
    ).register_activated_pack(
        _live_activated_pack(
            plan=plan,
            activation_authority_receipt_sha256=(activation_receipt.activation_authority_receipt_sha256),
            authority_store_ref=activation_receipt.authority_store_ref,
            workflow_execution_id=workflow_execution_id,
            workflow_execution_binding_sha256=(workflow_execution_binding_sha256),
            workflow_plan_sha256=workflow_plan_sha256,
            run_execution_bundle_sha256=run_execution_bundle_sha256,
        )
    )


def _connect_factory() -> Callable[[], Any]:
    pyodbc = pytest.importorskip("pyodbc")
    # A terminated worker process cannot retain an ODBC pooled session. Disable
    # process-local pooling so live continuation tests observe that production
    # boundary instead of a session cached by the pytest process.
    pyodbc.pooling = False
    connection_string = (
        f"DRIVER={{{os.environ.get('DPONE_IT_MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
        f"SERVER={os.environ.get('DPONE_IT_MSSQL_HOST', '127.0.0.1')},"
        f"{os.environ.get('DPONE_IT_MSSQL_PORT_FORWARD', '51433')};"
        f"DATABASE={os.environ.get('DPONE_IT_MSSQL_DATABASE', 'dpone_it')};"
        f"UID={os.environ.get('DPONE_IT_MSSQL_USER', 'sa')};"
        f"PWD={os.environ.get('DPONE_IT_MSSQL_PASSWORD', 'Dp0ne.Strong.Pw.2026!')};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=15;"
    )

    def connect() -> Any:
        return pyodbc.connect(connection_string, autocommit=False)

    return connect


def _claim(resource_id: str, epoch: int) -> MssqlGuardClaim:
    return MssqlGuardClaim(resource_id, epoch - 1, epoch)


def _request(*, expected_target_epoch: int = 0) -> MssqlAdmissionRequest:
    request = compose_admission(_bundle())
    if expected_target_epoch:
        target = replace(
            request.resource_guards[0],
            expected_predecessor_epoch=expected_target_epoch,
            fencing_epoch=expected_target_epoch + 1,
        )
        journals = (replace(request.journals[0], fencing_epoch=target.fencing_epoch),)
        request = replace(
            request,
            resource_guards=(target,),
            journals=journals,
            expected_guard_set_sha256=mssql_guard_set_sha256(request.workflow_guard, (target,)),
            expected_journal_set_sha256=mssql_journal_set_sha256(journals),
        )
    replacement_journals = tuple(
        replace(item, replaces_failed_operation_id="sha256:" + "6" * 64) for item in request.journals
    )
    request = replace(
        request,
        journals=replacement_journals,
        expected_journal_set_sha256=mssql_journal_set_sha256(replacement_journals),
    )
    return replace(
        request,
        successor_claim=MssqlWorkflowSuccessorClaim(
            predecessor_workflow_id="failed-workflow",
            predecessor_workflow_summary_sha256="sha256:" + "7" * 64,
            successor_workflow_id=request.workflow_id,
            replacement_plan_sha256="sha256:" + "e" * 64,
        ),
    )


def _failed_cleanup_ack(
    request: MssqlAdmissionRequest,
    bundle: MssqlCanonicalAdmissionBundle,
) -> MssqlFailedPrecommitCleanupAck:
    failed_operation_id = request.journals[0].replaces_failed_operation_id
    assert failed_operation_id is not None
    binding = "sha256:" + "8" * 64
    operation_plan = "sha256:" + "9" * 64
    attempt = "sha256:" + "a" * 64
    reservation_id = "reservation-failed-workflow"
    target_uuid = bundle.model_resources[0].clickhouse_target_uuid
    staging, shadow = clickhouse_operation_table_names(
        bundle.model_resources[0].publication_target_table,
        failed_operation_id,
    )
    scratch = ClickHouseFailedScratchCleanupReceipt(
        workflow_execution_id="failed-workflow",
        workflow_execution_binding_sha256=binding,
        operation_id=failed_operation_id,
        operation_plan_sha256=operation_plan,
        attempt_binding_sha256=attempt,
        fencing_epoch=1,
        target_uuid=target_uuid,
        relations=(
            ClickHouseScratchRelationAbsence("shadow", shadow, target_uuid),
            ClickHouseScratchRelationAbsence("staging", staging, None),
        ),
    )
    resources = MssqlProtectedResourceAllocationClosure(
        workflow_execution_binding_sha256=binding,
        operation_id=failed_operation_id,
        reservation_id=reservation_id,
        allocations=tuple(
            MssqlProtectedResourceAllocation(
                allocation_id=mssql_resource_allocation_id(
                    reservation_id,
                    failed_operation_id,
                    kind,
                ),
                reservation_id=reservation_id,
                workflow_execution_binding_sha256=binding,
                operation_id=failed_operation_id,
                resource_kind=kind,
                amount=1,
                status="RELEASED",
            )
            for kind in MSSQL_RESOURCE_KINDS
        ),
    )
    return MssqlFailedPrecommitCleanupAck.build(scratch=scratch, resources=resources)


def _request_after_released_guard(request: MssqlAdmissionRequest) -> MssqlAdmissionRequest:
    workflow_id = "daily/run-2"
    execution_binding = "sha256:" + "a" * 64
    operation_id = "sha256:" + "b" * 64
    operation_plan = "sha256:" + "c" * 64
    attempt_binding = "sha256:" + "d" * 64
    strategy = json.loads(request.journals[0].strategy_authority_json)
    strategy.update(
        operation_id=operation_id,
        operation_plan_sha256=operation_plan,
        attempt_binding_sha256=attempt_binding,
        fencing_epoch=2,
        workflow_execution_binding_sha256=execution_binding,
        workflow_execution_id=workflow_id,
        workflow_id=workflow_id,
    )
    strategy_json = _canonical_json(strategy)
    journal = replace(
        request.journals[0],
        operation_id=operation_id,
        operation_plan_sha256=operation_plan,
        attempt_binding_sha256=attempt_binding,
        strategy_authority_json=strategy_json,
        strategy_authority_sha256=mssql_strategy_authority_sha256(strategy_json),
        fencing_epoch=2,
        replaces_failed_operation_id=None,
    )
    workflow_guard = replace(
        request.workflow_guard,
        expected_predecessor_epoch=1,
        fencing_epoch=2,
    )
    resource_guard = replace(
        request.resource_guards[0],
        expected_predecessor_epoch=1,
        fencing_epoch=2,
    )
    return replace(
        request,
        workflow_id=workflow_id,
        workflow_execution_id=workflow_id,
        workflow_execution_binding_sha256=execution_binding,
        canonical_authority_sha256="sha256:" + "e" * 64,
        canonical_authority_json=_canonical_json(
            {
                "schema": "dpone.semantic-refresh-mssql-live-run-authority.v1",
                "workflow_execution_id": workflow_id,
            }
        ),
        reservation_id="reservation-daily-run-2",
        workflow_guard=workflow_guard,
        resource_guards=(resource_guard,),
        journals=(journal,),
        successor_claim=None,
        expected_guard_set_sha256=mssql_guard_set_sha256(
            workflow_guard,
            (resource_guard,),
        ),
        expected_journal_set_sha256=mssql_journal_set_sha256((journal,)),
    )


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_replacement_admission_is_atomic_and_conflict_rolls_back() -> None:
    factory = _connect_factory()
    setup = factory()
    setup.autocommit = True
    try:
        activation_receipt = _reset_schema(setup)
        adapter = MssqlSemanticRefreshStateAdapter(factory, control_schema=_SCHEMA)
        request = _request()
        bundle = _bundle()
        plan = _plan_bundle()
        _register_live_activated_pack(
            factory,
            plan=plan,
            activation_receipt=activation_receipt,
            workflow_execution_id=bundle.workflow_execution_id,
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
            run_execution_bundle_sha256="sha256:" + "3" * 64,
        )
        worker_authority = SemanticRefreshMssqlWorkerRunAuthorityService(
            MssqlSemanticRefreshWorkerRunAuthority(
                factory,
                control_schema=_SCHEMA,
            )
        )
        registered = worker_authority.load(
            workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
            workflow_execution_id=bundle.workflow_execution_id,
        )
        assert registered.admission_status == "REGISTERED"
        assert registered.attempts == ()
        first_receipt = adapter.admit(request)
        second_receipt = adapter.admit(request)
        assert second_receipt == first_receipt

        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{_SCHEMA}].[semantic_refresh_journals] "
                f"WHERE workflow_id = N'{request.workflow_id}'",
            )
            == 1
        )
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{_SCHEMA}].[semantic_refresh_reservations] "
                f"WHERE workflow_id = N'{request.workflow_id}'",
            )
            == 1
        )
        assert (
            _scalar(
                setup,
                f"SELECT successor_workflow_id FROM [{_SCHEMA}].[semantic_refresh_workflow_executions] "
                "WHERE workflow_id = N'failed-workflow'",
            )
            == request.workflow_id
        )
        _execute(
            setup,
            f"""
UPDATE [{_SCHEMA}].[semantic_refresh_journals]
SET status = N'COMPLETE' WHERE workflow_id = N'{request.workflow_id}';
UPDATE [{_SCHEMA}].[semantic_refresh_reservations]
SET status = N'COMPLETE' WHERE workflow_id = N'{request.workflow_id}';
UPDATE [{_SCHEMA}].[semantic_refresh_workflow_executions]
SET status = N'COMPLETE' WHERE workflow_id = N'{request.workflow_id}';
UPDATE [{_SCHEMA}].[semantic_refresh_guards]
SET status = N'RELEASED' WHERE workflow_id = N'{request.workflow_id}';
""".strip(),
        )
        successor = _request_after_released_guard(request)
        MssqlSemanticRefreshActivationStore(
            factory,
            control_schema=_SCHEMA,
        ).register_run_authority(
            MssqlRunAuthorityRegistration(
                record=MssqlCanonicalAuthorityRecord(
                    workflow_execution_binding_sha256=(successor.workflow_execution_binding_sha256),
                    workflow_execution_id=successor.workflow_execution_id,
                    authority_sha256=successor.canonical_authority_sha256,
                    authority_json=successor.canonical_authority_json,
                    status="ACTIVE",
                ),
                guard_epochs=tuple(
                    sorted(
                        (claim.resource_id, claim.expected_predecessor_epoch)
                        for claim in (successor.workflow_guard, *successor.resource_guards)
                    )
                ),
            )
        )
        _register_live_activated_pack(
            factory,
            plan=plan,
            activation_receipt=activation_receipt,
            workflow_execution_id=successor.workflow_execution_id,
            workflow_execution_binding_sha256=(successor.workflow_execution_binding_sha256),
            workflow_plan_sha256=successor.workflow_plan_sha256,
            run_execution_bundle_sha256="sha256:" + "4" * 64,
        )
        assert adapter.admit(successor).workflow_id == successor.workflow_id

        activation_receipt = _reset_schema(setup)
        conflict_request = _request(expected_target_epoch=9)
        _register_live_activated_pack(
            factory,
            plan=plan,
            activation_receipt=activation_receipt,
            workflow_execution_id=conflict_request.workflow_execution_id,
            workflow_execution_binding_sha256=(conflict_request.workflow_execution_binding_sha256),
            workflow_plan_sha256=conflict_request.workflow_plan_sha256,
            run_execution_bundle_sha256="sha256:" + "3" * 64,
        )
        with pytest.raises(SemanticRefreshMssqlStateConflict, match="guard admission conflict"):
            adapter.admit(conflict_request)
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{_SCHEMA}].[semantic_refresh_journals] "
                f"WHERE workflow_id = N'{request.workflow_id}'",
            )
            == 0
        )
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{_SCHEMA}].[semantic_refresh_reservations] "
                f"WHERE workflow_id = N'{request.workflow_id}'",
            )
            == 0
        )
        assert (
            _scalar(
                setup,
                f"SELECT successor_workflow_id FROM [{_SCHEMA}].[semantic_refresh_workflow_executions] "
                "WHERE workflow_id = N'failed-workflow'",
            )
            is None
        )
    finally:
        _drop_schema(setup)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_complete_workflow_head_drives_exact_replay_admission() -> None:
    factory = _connect_factory()
    setup = factory()
    setup.autocommit = True
    try:
        activation_receipt = _reset_schema(setup)
        predecessor = _bundle()
        _register_live_activated_pack(
            factory,
            plan=_plan_bundle(),
            activation_receipt=activation_receipt,
            workflow_execution_id=predecessor.workflow_execution_id,
            workflow_execution_binding_sha256=(predecessor.execution_binding.workflow_execution_binding_sha256),
            workflow_plan_sha256=predecessor.workflow_plan.workflow_plan_sha256,
            run_execution_bundle_sha256="sha256:" + "7" * 64,
        )
        admission = MssqlSemanticRefreshStateAdapter(factory, control_schema=_SCHEMA)
        admission.admit(compose_admission(predecessor))
        operation = predecessor.operation_plans[0]
        resource = predecessor.model_resources[0]
        resources = MssqlSemanticRefreshProtectedResourceLedger(
            factory,
            control_schema=_SCHEMA,
            clock=lambda: datetime(2026, 8, 8, tzinfo=UTC),
        )
        resources.reserve_operation(
            workflow_execution_binding_sha256=(predecessor.execution_binding.workflow_execution_binding_sha256),
            operation_id=operation.operation_id,
        )
        terminal_receipt = "sha256:" + "e" * 64
        target_generation_id = "sha256:" + "f" * 64
        target_uuid = "00000000-0000-0000-0000-000000000002"
        current_operation_id = "sha256:" + "8" * 64
        current_terminal_receipt = "sha256:" + "9" * 64
        current_generation_id = "sha256:" + "a" * 64
        current_target_uuid = "00000000-0000-0000-0000-000000000003"
        checkpoint_sha256 = "sha256:" + "c" * 64
        summary = SemanticRefreshDurableWorkflowSummary.build(
            workflow_execution_id=predecessor.workflow_execution_id,
            workflow_plan_sha256=predecessor.workflow_plan.workflow_plan_sha256,
            workflow_execution_binding_sha256=(predecessor.execution_binding.workflow_execution_binding_sha256),
            expected_operation_ids=(operation.operation_id,),
            publications=(
                SemanticRefreshDurableModelPublication(
                    operation_id=operation.operation_id,
                    operation_plan_sha256=operation.operation_plan_sha256,
                    workflow_execution_binding_sha256=(predecessor.execution_binding.workflow_execution_binding_sha256),
                    attempt_binding_sha256=predecessor.attempt_bindings[0].attempt_binding_sha256,
                    artifact_manifest_sha256="sha256:" + "1" * 64,
                    clickhouse_terminal_receipt_sha256="sha256:" + "2" * 64,
                    terminal_receipt_sha256=terminal_receipt,
                    target_generation=2,
                    scope_revision=operation.scope_revision,
                ),
            ),
        )
        _execute_params(
            setup,
            f"""
UPDATE [{_SCHEMA}].[semantic_refresh_journals]
SET status = N'COMPLETE', artifact_manifest_sha256 = ?,
    clickhouse_commit_receipt_sha256 = ?, terminal_receipt_sha256 = ?,
    terminal_target_generation = 2, terminal_target_generation_id = ?,
    target_uuid = ?, terminal_scope_revision = ?,
    terminal_checkpoint_sha256 = ?, terminal_checkpoint_version = 1,
    terminal_target_mutation_outcome = N'TARGET_COMMITTED'
WHERE operation_id = ?;
UPDATE [{_SCHEMA}].[semantic_refresh_target_heads]
SET target_generation = 2, target_generation_id = ?, target_uuid = ?, operation_id = ?
WHERE database_name = ? AND target_table = ?;
INSERT INTO [{_SCHEMA}].[semantic_refresh_scope_heads] (
    database_name, target_table, scope_id, scope_revision, operation_id
) VALUES (?, ?, ?, ?, ?);
INSERT INTO [{_SCHEMA}].[semantic_refresh_checkpoints] (
    database_name, target_table, scope_id, checkpoint_sha256, checkpoint_version, operation_id
) VALUES (?, ?, ?, ?, 1, ?);
""".strip(),
            "sha256:" + "1" * 64,
            "sha256:" + "2" * 64,
            terminal_receipt,
            target_generation_id,
            target_uuid,
            operation.scope_revision,
            checkpoint_sha256,
            operation.operation_id,
            target_generation_id,
            target_uuid,
            operation.operation_id,
            resource.publication_database,
            resource.publication_target_table,
            resource.publication_database,
            resource.publication_target_table,
            resource.publication_scope_id,
            operation.scope_revision,
            operation.operation_id,
            resource.publication_database,
            resource.publication_target_table,
            resource.publication_scope_id,
            checkpoint_sha256,
            operation.operation_id,
        )
        resources.release_operation(
            workflow_execution_binding_sha256=(predecessor.execution_binding.workflow_execution_binding_sha256),
            operation_id=operation.operation_id,
        )
        MssqlSemanticRefreshWorkflowSummaryState(factory, control_schema=_SCHEMA).persist(summary.to_dict())
        _execute_params(
            setup,
            f"""
SELECT * INTO #dpone_predecessor_journal
FROM [{_SCHEMA}].[semantic_refresh_journals]
WHERE operation_id = ?;
UPDATE [{_SCHEMA}].[semantic_refresh_journals]
SET operation_id = ?, workflow_id = N'interleaved-scope-b',
    publication_scope_id = N'scope-interleaved-b',
    terminal_receipt_sha256 = ?, terminal_target_generation = 3,
    terminal_target_generation_id = ?, target_uuid = ?, terminal_scope_revision = 1,
    terminal_target_mutation_outcome = N'TARGET_COMMITTED'
WHERE operation_id = ?;
INSERT INTO [{_SCHEMA}].[semantic_refresh_journals]
SELECT * FROM #dpone_predecessor_journal;
DROP TABLE #dpone_predecessor_journal;
UPDATE [{_SCHEMA}].[semantic_refresh_target_heads]
SET target_generation = 3, target_generation_id = ?, target_uuid = ?, operation_id = ?
WHERE database_name = ? AND target_table = ?;
""".strip(),
            operation.operation_id,
            current_operation_id,
            current_terminal_receipt,
            current_generation_id,
            current_target_uuid,
            operation.operation_id,
            current_generation_id,
            current_target_uuid,
            current_operation_id,
            resource.publication_database,
            resource.publication_target_table,
        )
        head_service = SemanticRefreshMssqlRecoveryHeadService(
            MssqlSemanticRefreshCanonicalAuthorityLoader(factory, control_schema=_SCHEMA),
            MssqlSemanticRefreshRecoveryHeadReader(factory, control_schema=_SCHEMA),
        )
        durable = head_service.load(predecessor.execution_binding.workflow_execution_binding_sha256)
        protected_summary = SemanticRefreshDurableWorkflowSummary.from_mapping(
            json.loads(durable.terminal_summary_json)
        )
        heads = tuple(
            SemanticRefreshRecoveryTargetHead.build(
                model_unique_id=item.model_unique_id,
                clickhouse_target_authority_id=item.clickhouse_target_authority_id,
                target_generation=item.target_generation,
                target_generation_id=item.target_generation_id,
                target_uuid=item.target_uuid,
                owner_operation_id=item.owner_operation_id,
                terminal_receipt_sha256=item.terminal_receipt_sha256,
            )
            for item in durable.target_heads
        )
        recovery = SemanticRefreshCompleteScopeReplayAuthority.build(
            predecessor_plan=_plan_bundle(),
            predecessor_summary=protected_summary,
            predecessor_workflow_execution_id=predecessor.workflow_execution_id,
            target_heads=heads,
        )
        verifier = SemanticRefreshMssqlRecoveryAuthorityVerifier(head_service)
        _execute(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_workflow_executions] SET terminal_summary_json = N'{{}}' "
            f"WHERE workflow_id = N'{predecessor.workflow_execution_id}';",
        )
        with pytest.raises(SemanticRefreshContractError, match="not protected"):
            SemanticRefreshPostDeploymentPlanCompiler(
                type("ExactLiveSubjectVerifier", (), {"verify": lambda *_: True})(),
                _AssuranceVerifier(),
                verifier,
            ).compile(**_compile_kwargs(), recovery_authority=recovery)
        _execute_params(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_workflow_executions] SET terminal_summary_json = ? "
            "WHERE workflow_id = ?;",
            _canonical_json(protected_summary.to_dict()),
            predecessor.workflow_execution_id,
        )
        with pytest.raises(SemanticRefreshContractError, match="receipt differs"):
            replace(recovery, authority_receipt_sha256="sha256:" + "0" * 64)
        _execute_params(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_journals] SET artifact_manifest_sha256 = ? WHERE operation_id = ?;",
            "sha256:" + "0" * 64,
            operation.operation_id,
        )
        with pytest.raises(SemanticRefreshContractError, match="not protected"):
            SemanticRefreshPostDeploymentPlanCompiler(
                type("ExactLiveSubjectVerifier", (), {"verify": lambda *_: True})(),
                _AssuranceVerifier(),
                verifier,
            ).compile(**_compile_kwargs(), recovery_authority=recovery)
        _execute_params(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_journals] SET artifact_manifest_sha256 = ? WHERE operation_id = ?;",
            "sha256:" + "1" * 64,
            operation.operation_id,
        )
        replay_plan = SemanticRefreshPostDeploymentPlanCompiler(
            type("ExactLiveSubjectVerifier", (), {"verify": lambda *_: True})(),
            _AssuranceVerifier(),
            verifier,
        ).compile(**_compile_kwargs(), recovery_authority=recovery)
        run = SemanticRefreshRunAdmissionCompiler(
            type("ExactLiveRunVerifier", (), {"verify": lambda *_: True})()
        ).compile(
            plan_bundle=replay_plan,
            authority=SemanticRefreshRunAdmissionAuthority(
                "daily/replay-live",
                replay_plan.plan_bundle_sha256,
                "sha256:" + "4" * 64,
            ),
        )
        replay_operation = replay_plan.operation_plans[0]
        replay_attempt = SemanticRefreshAttemptBinding.build(
            workflow_execution_id=run.workflow_execution_binding.workflow_execution_id,
            workflow_execution_binding_sha256=(run.workflow_execution_binding.workflow_execution_binding_sha256),
            operation_id=replay_operation.operation_id,
            operation_plan_sha256=replay_operation.operation_plan_sha256,
            dag_run_id=run.workflow_execution_binding.workflow_execution_id,
            task_id="dbt-build-replay",
            try_number=1,
            pod_uid="00000000-0000-0000-0000-000000000778",
            fencing_epoch=2,
            owner_id="airflow-owner-replay",
        )
        replay_target = replay_plan.targets[0]
        replay_bundle = MssqlCanonicalAdmissionBundle(
            workflow_execution_id=run.workflow_execution_binding.workflow_execution_id,
            workflow_plan=replay_plan.workflow_plan,
            execution_binding=run.workflow_execution_binding,
            operation_plans=(replay_operation,),
            attempt_bindings=(replay_attempt,),
            workflow_guard=MssqlGuardClaim(
                replay_plan.run_guard_closure.workflow_guard_resource_id,
                1,
                2,
            ),
            resource_guards=(MssqlGuardClaim(replay_target.target_resource_id, 1, 2),),
            model_resources=(mssql_model_resource_authority_from_plan_target(replay_target),),
            controller_id="airflow-controller-replay",
            owner_id=replay_attempt.owner_id,
            reservation_id="reservation-replay-live",
            resource_budget=predecessor.resource_budget,
        )
        replay_activation_receipt = _persist_live_activation_authority(replay_plan)
        _register_live_activated_pack(
            factory,
            plan=replay_plan,
            activation_receipt=replay_activation_receipt,
            workflow_execution_id=(run.workflow_execution_binding.workflow_execution_id),
            workflow_execution_binding_sha256=(run.workflow_execution_binding.workflow_execution_binding_sha256),
            workflow_plan_sha256=replay_plan.workflow_plan.workflow_plan_sha256,
            run_execution_bundle_sha256=run.run_execution_bundle_sha256,
        )
        activation = SemanticRefreshMssqlActivationService(
            MssqlSemanticRefreshActivationStore(factory, control_schema=_SCHEMA),
            type("ExactLiveDeploymentVerifier", (), {"verify": lambda *_: True})(),
            _AllowLocalActivation(),
        )
        activation.register_run(replay_bundle)
        protected_admission = SemanticRefreshMssqlAuthorityAdmissionService(
            MssqlSemanticRefreshCanonicalAuthorityLoader(factory, control_schema=_SCHEMA),
            admission,
        )
        _execute(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_target_heads] SET target_generation = 4;",
        )
        with pytest.raises(SemanticRefreshMssqlStateConflict, match="target predecessor head"):
            protected_admission.admit(replay_bundle.execution_binding.workflow_execution_binding_sha256)
        _execute(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_target_heads] SET target_generation = 3;",
        )
        _execute_params(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_journals] SET terminal_receipt_sha256 = ? WHERE operation_id = ?;",
            "sha256:" + "0" * 64,
            current_operation_id,
        )
        with pytest.raises(SemanticRefreshMssqlStateConflict, match="terminal receipt"):
            protected_admission.admit(replay_bundle.execution_binding.workflow_execution_binding_sha256)
        _execute_params(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_journals] SET terminal_receipt_sha256 = ? WHERE operation_id = ?;",
            current_terminal_receipt,
            current_operation_id,
        )
        receipt = protected_admission.admit(replay_bundle.execution_binding.workflow_execution_binding_sha256)
        assert receipt.workflow_id == "daily/replay-live"
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{_SCHEMA}].[semantic_refresh_journals] "
                "WHERE workflow_id = N'daily/replay-live' AND status = N'PREPARING';",
            )
            == 1
        )
    finally:
        _drop_schema(setup)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_canonical_authority_loader_is_exact_and_fail_closed() -> None:
    factory = _connect_factory()
    schema = "dpone_sr_v2_authority"
    setup = factory()
    setup.autocommit = True
    binding = "sha256:" + "1" * 64
    authority = "sha256:" + "2" * 64
    try:
        _drop_named_schema(setup, schema)
        MssqlSemanticRefreshSchemaMigration(factory, control_schema=schema).apply()
        _execute(
            setup,
            f"""
INSERT INTO [{schema}].[semantic_refresh_canonical_authorities] (
    workflow_execution_binding_sha256, workflow_execution_id,
    authority_sha256, authority_json, status
) VALUES (
    '{binding}', N'workflow-authority-live', '{authority}', N'{{"schema":"test"}}', N'ACTIVE'
);
""",
        )
        loader = MssqlSemanticRefreshCanonicalAuthorityLoader(factory, control_schema=schema)
        record = loader.load(binding)
        assert record.authority_sha256 == authority
        assert record.workflow_execution_id == "workflow-authority-live"
        assert record.status == "ACTIVE"
        with pytest.raises(SemanticRefreshMssqlAuthorityLoadError, match="absent"):
            loader.load("sha256:" + "3" * 64)
    finally:
        _drop_named_schema(setup, schema)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_activated_pack_is_create_once_and_replay_exact() -> None:
    factory = _connect_factory()
    schema = "dpone_sr_v2_activated_pack"
    setup = factory()
    setup.autocommit = True
    request = _live_activated_pack(
        plan=_plan_bundle(),
        activation_authority_receipt_sha256="sha256:" + "2" * 64,
        authority_store_ref="mssql://local-docker/dpone-control/activation/receipt",
        workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
        workflow_execution_binding_sha256="sha256:" + "3" * 64,
        workflow_plan_sha256="sha256:" + "4" * 64,
        run_execution_bundle_sha256="sha256:" + "6" * 64,
    )
    try:
        _drop_named_schema(setup, schema)
        MssqlSemanticRefreshSchemaMigration(factory, control_schema=schema).apply()
        store = MssqlSemanticRefreshActivationStore(factory, control_schema=schema)

        store.register_activated_pack(request)
        store.register_activated_pack(request)
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{schema}].[semantic_refresh_activated_packs] WHERE status = N'ACTIVE';",
            )
            == 1
        )

        conflict = _live_activated_pack(
            plan=_plan_bundle(),
            activation_authority_receipt_sha256=request.activation_authority_receipt_sha256,
            authority_store_ref=request.authority_store_ref,
            workflow_execution_id=request.workflow_execution_id,
            workflow_execution_binding_sha256=request.workflow_execution_binding_sha256,
            workflow_plan_sha256=request.workflow_plan_sha256,
            run_execution_bundle_sha256="sha256:" + "7" * 64,
        )
        with pytest.raises(
            SemanticRefreshMssqlActivationError,
            match="execution binding already exists",
        ):
            store.register_activated_pack(conflict)
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{schema}].[semantic_refresh_activated_packs];",
            )
            == 1
        )
    finally:
        _drop_named_schema(setup, schema)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_seal_authorization_store_rejects_missing_active_admission() -> None:
    factory = _connect_factory()
    schema = "dpone_sr_v2_seal"
    setup = factory()
    setup.autocommit = True
    try:
        _drop_named_schema(setup, schema)
        MssqlSemanticRefreshSchemaMigration(factory, control_schema=schema).apply()
        bundle = _bundle()
        receipt = _seal_receipt(bundle, serializer="a")
        store = MssqlSemanticRefreshSealAuthorizationStore(factory, control_schema=schema)

        with pytest.raises(
            SemanticRefreshMssqlSealAuthorizationError,
            match="active seal admission state",
        ):
            store.persist_exact(receipt)
    finally:
        _drop_named_schema(setup, schema)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_seal_issuer_uses_protected_state_and_recomputed_mssql_evidence() -> None:
    if os.environ.get("DPONE_IT_MSSQL_HOST", "127.0.0.1") not in {
        "127.0.0.1",
        "localhost",
    }:
        pytest.skip("cross-database issuer fixture is confined to local MSSQL")
    factory = _connect_factory()
    setup = factory()
    setup.autocommit = True
    created_database = False
    try:
        created_database = _scalar(setup, "SELECT DB_ID(N'DWH')") is None
        if created_database:
            _execute(setup, "EXEC(N'CREATE DATABASE [DWH]');")
        activation_receipt = _reset_schema(setup)
        bundle = _bundle()
        _register_live_activated_pack(
            factory,
            plan=_plan_bundle(),
            activation_receipt=activation_receipt,
            workflow_execution_id=bundle.workflow_execution_id,
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
            run_execution_bundle_sha256="sha256:" + "7" * 64,
        )
        operation = bundle.operation_plans[0]
        attempt = bundle.attempt_bindings[0]
        admission = MssqlSemanticRefreshStateAdapter(
            factory,
            control_schema=_SCHEMA,
        )
        admission.admit(compose_admission(bundle))
        build_receipt = _install_committed_image_evidence(setup, bundle)
        store = MssqlSemanticRefreshSealAuthorizationStore(
            factory,
            control_schema=_SCHEMA,
        )
        issuer = SemanticRefreshSealAuthorizationIssuer(
            protected_operation=SemanticRefreshMssqlProtectedAuthorityService(
                state=MssqlSemanticRefreshProtectedOperationState(
                    factory,
                    control_schema=_SCHEMA,
                )
            ),
            prerequisites=MssqlSemanticRefreshPrerequisiteAuthority(
                factory,
                control_schema=_SCHEMA,
            ),
            evidence=MssqlSemanticRefreshEvidenceReader(
                factory,
                control_schema=_SCHEMA,
            ),
            outcome=SemanticRefreshMssqlOutcomeService(),
            codec=DponeParquetV1SealCodecAuthority(),
            policy=_LocalSealPolicyAuthority(),
            store=store,
            now=lambda: datetime(2026, 8, 8, 12, tzinfo=UTC),
        )

        receipt = issuer.issue(
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            operation_id=operation.operation_id,
        )
        assert receipt == issuer.issue(
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            operation_id=operation.operation_id,
        )
        assert receipt.workflow_id == operation.workflow_id
        assert receipt.attempt_binding_sha256 == attempt.attempt_binding_sha256
        assert receipt.model_build_receipt_sha256 == build_receipt.build_receipt_sha256
        assert receipt.baseline_adoption_receipt_sha256 == bundle.model_resources[0].baseline_receipt_sha256
        with pytest.raises(SemanticRefreshMssqlSealAuthorizationError, match="replay differs"):
            store.persist_exact(_seal_receipt(bundle, serializer="b"))
        _execute(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_guards] "
            f"SET status = N'RELEASED' WHERE workflow_id = N'{bundle.workflow_execution_id}';",
        )
        with pytest.raises(
            SemanticRefreshMssqlSealAuthorizationError,
            match="active seal admission state differs",
        ):
            store.persist_exact(receipt)
    finally:
        _drop_live_image_tables(setup)
        _drop_schema(setup)
        if created_database:
            _execute(setup, "ALTER DATABASE [DWH] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [DWH];")
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_failed_workflow_summary_releases_authority_and_is_replay_safe() -> None:
    factory = _connect_factory()
    schema = "dpone_sr_v2_failure"
    setup = factory()
    setup.autocommit = True
    try:
        _drop_named_schema(setup, schema)
        MssqlSemanticRefreshSchemaMigration(factory, control_schema=schema).apply()
        plan = "sha256:" + "1" * 64
        execution = "sha256:" + "2" * 64
        attempt = "sha256:" + "3" * 64
        session_evidence = mssql_session_evidence_sha256(
            operation_id=_FAILED_OPERATION_ID,
            operation_plan_sha256="sha256:" + "5" * 64,
            attempt_binding_sha256=attempt,
            fencing_epoch=1,
            transaction_disposition=MssqlTransactionDisposition.ROLLED_BACK,
            controller_proves_not_invoked=False,
        )
        workflow_guard = MssqlGuardClaim("workflow://failed-live", 0, 1)
        target_guard = MssqlGuardClaim("mssql://warehouse/dbo/failed-live", 0, 1)
        failure_journal = MssqlJournalPreparation(
            model_unique_id="model.analytics.failed_live",
            operation_id=_FAILED_OPERATION_ID,
            operation_plan_sha256="sha256:" + "5" * 64,
            attempt_binding_sha256=attempt,
            strategy_authority_json="{}",
            strategy_authority_sha256=mssql_strategy_authority_sha256("{}"),
            baseline_receipt_sha256="sha256:" + "7" * 64,
            baseline_kind="adopted_complete_relation_conformant",
            baseline_receipt_json="{}",
            baseline_status="COMPLETE",
            image_key_columns=(MssqlImageKeyColumn("event_id", "NATIVE"),),
            target_resource_id=target_guard.resource_id,
            publication_database="analytics",
            publication_target_table="failed_live",
            publication_scope_id="scope-failure-live",
            target_predecessor_generation_id="sha256:" + "8" * 64,
            scope_predecessor_operation_id=None,
            fencing_epoch=target_guard.fencing_epoch,
        )
        guard_set_sha256 = mssql_guard_set_sha256(workflow_guard, (target_guard,))
        journal_set_sha256 = mssql_journal_set_sha256((failure_journal,))
        _execute(
            setup,
            f"""
INSERT INTO [{schema}].[semantic_refresh_workflow_executions] (
    workflow_id, workflow_execution_id, workflow_plan_sha256, workflow_execution_binding_sha256,
    canonical_authority_sha256,
    guard_set_sha256, journal_set_sha256, workflow_guard_resource_id, guard_count,
    owner_id, status
) VALUES (
    N'failed-workflow-live', N'failed-workflow-live', '{plan}', '{execution}', 'sha256:{"0" * 64}',
    '{guard_set_sha256}',
    '{journal_set_sha256}', N'{workflow_guard.resource_id}', 2, N'owner-failed-live', N'PREPARING'
);
INSERT INTO [{schema}].[semantic_refresh_reservations] (
    reservation_id, workflow_id, workflow_execution_binding_sha256, status,
    max_prepared_models, max_sealed_extract_bytes, max_clickhouse_staging_bytes,
    max_shadow_bytes, max_peak_bytes
) VALUES (
    N'failure-reservation', N'failed-workflow-live', '{execution}', N'PREPARING',
    1, 10000, 20000, 30000, 60000
);
INSERT INTO [{schema}].[semantic_refresh_guards] (
    resource_id, fencing_epoch, owner_id, workflow_id, status
) VALUES (
    N'{workflow_guard.resource_id}', 1, N'owner-failed-live', N'failed-workflow-live', N'HELD'
);
INSERT INTO [{schema}].[semantic_refresh_guards] (
    resource_id, fencing_epoch, owner_id, workflow_id, operation_id,
    operation_plan_sha256, attempt_binding_sha256, strategy_authority_sha256, status
    ) VALUES (
    N'{target_guard.resource_id}', 1, N'owner-failed-live', N'failed-workflow-live',
    N'{_FAILED_OPERATION_ID}', 'sha256:{"5" * 64}', '{attempt}',
    '{failure_journal.strategy_authority_sha256}', N'HELD'
);
INSERT INTO [{schema}].[semantic_refresh_journals] (
    model_unique_id, operation_id, operation_plan_sha256, attempt_binding_sha256,
    strategy_authority_json, strategy_authority_sha256,
    baseline_receipt_sha256, baseline_kind,
    baseline_receipt_json, baseline_status,
    image_key_columns_json, workflow_id, target_resource_id,
    publication_database, publication_target_table, publication_scope_id,
    target_predecessor_generation_id, scope_predecessor_operation_id,
    predecessor_target_generation, predecessor_target_uuid,
    predecessor_target_operation_id, fencing_epoch, owner_id, journal_version, status
    ) VALUES (
        N'model.analytics.failed_live', N'{_FAILED_OPERATION_ID}', 'sha256:{"5" * 64}', '{attempt}',
    N'{{}}', '{failure_journal.strategy_authority_sha256}', 'sha256:{"7" * 64}',
    N'adopted_complete_relation_conformant', N'{{}}', N'COMPLETE',
    N'[{{"name":"event_id","order_encoding":"NATIVE"}}]', N'failed-workflow-live',
    N'mssql://warehouse/dbo/failed-live', N'analytics', N'failed_live', N'scope-failure-live',
    'sha256:{"8" * 64}', NULL, 0, '00000000-0000-0000-0000-000000000010',
    N'baseline-failure', 1, N'owner-failed-live', 1, N'PREPARING'
);
INSERT INTO [{schema}].[semantic_refresh_mssql_session_outcomes] (
    operation_id, operation_plan_sha256, attempt_binding_sha256, fencing_epoch,
    transaction_disposition, controller_proves_not_invoked, evidence_sha256
) VALUES (
    N'{_FAILED_OPERATION_ID}', 'sha256:{"5" * 64}', '{attempt}', 1,
    N'ROLLED_BACK', 0, '{session_evidence}'
);
""",
        )
        resources = MssqlSemanticRefreshResourceLedger(factory, control_schema=schema)
        allocation = resources.reserve(
            allocation_id="allocation-failed-live",
            reservation_id="failure-reservation",
            resource_kind="prepared_models",
            amount=1,
        )
        assert (
            resources.reserve(
                allocation_id="allocation-failed-live",
                reservation_id="failure-reservation",
                resource_kind="prepared_models",
                amount=1,
            )
            == allocation
        )
        with pytest.raises(SemanticRefreshResourceReservationError, match="budget"):
            resources.reserve(
                allocation_id="allocation-too-large",
                reservation_id="failure-reservation",
                resource_kind="prepared_models",
                amount=1,
            )
        assert resources.release(allocation_id="allocation-failed-live")["status"] == "RELEASED"
        state = MssqlSemanticRefreshWorkflowFailureState(factory, control_schema=schema)
        service = SemanticRefreshMssqlFailureTerminalizationService(
            contexts=state,
            evidence=MssqlSemanticRefreshEvidenceReader(factory, control_schema=schema),
            state=state,
        )
        _execute(
            setup,
            f"""
INSERT INTO [{schema}].[semantic_refresh_guards] (
    resource_id, fencing_epoch, owner_id, workflow_id, status
) VALUES (N'dependency://unexpected', 1, N'owner-failed-live', N'failed-workflow-live', N'HELD');
""",
        )
        with pytest.raises(SemanticRefreshWorkflowFailureError, match="count differs"):
            service.terminalize("failed-workflow-live")
        _execute(
            setup,
            f"DELETE FROM [{schema}].[semantic_refresh_guards] WHERE resource_id = N'dependency://unexpected';",
        )
        _execute(
            setup,
            f"UPDATE [{schema}].[semantic_refresh_mssql_session_outcomes] SET evidence_sha256 = 'sha256:{'0' * 64}';",
        )
        with pytest.raises(SemanticRefreshMssqlEvidenceReadError, match="digest differs"):
            service.terminalize("failed-workflow-live")
        assert (
            _scalar(
                setup,
                f"SELECT status FROM [{schema}].[semantic_refresh_workflow_executions]",
            )
            == "PREPARING"
        )
        _execute(
            setup,
            f"UPDATE [{schema}].[semantic_refresh_mssql_session_outcomes] SET evidence_sha256 = '{session_evidence}';",
        )
        summary = service.terminalize("failed-workflow-live")
        assert summary.models[0].mssql_outcome.value == "ROLLED_BACK"
        assert service.terminalize("failed-workflow-live") == summary
        predecessor = MssqlSemanticRefreshPredecessorStateReader(
            factory,
            control_schema=schema,
        ).load_predecessor("failed-workflow-live")
        assert predecessor.workflow_execution_binding_sha256 == execution
        assert predecessor.terminal_summary_sha256 == summary.terminal_summary_sha256
        assert predecessor.models[0].mssql_outcome == "ROLLED_BACK"
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{schema}].[semantic_refresh_guards] WHERE status = N'HELD'",
            )
            == 0
        )
        assert (
            _scalar(
                setup,
                f"SELECT terminal_summary_sha256 FROM [{schema}].[semantic_refresh_workflow_executions]",
            )
            == summary.terminal_summary_sha256
        )
    finally:
        _drop_named_schema(setup, schema)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_failed_precommit_recovery_authority_is_protected_and_tamper_safe() -> None:
    factory = _connect_factory()
    setup = factory()
    setup.autocommit = True
    try:
        activation_receipt = _reset_schema(setup)
        bundle = _bundle()
        plan = _plan_bundle()
        request = compose_admission(bundle)
        _register_live_activated_pack(
            factory,
            plan=plan,
            activation_receipt=activation_receipt,
            workflow_execution_id=bundle.workflow_execution_id,
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
            run_execution_bundle_sha256="sha256:" + "7" * 64,
        )
        MssqlSemanticRefreshStateAdapter(factory, control_schema=_SCHEMA).admit(request)
        operation = bundle.operation_plans[0]
        attempt = bundle.attempt_bindings[0]
        evidence_sha256 = mssql_session_evidence_sha256(
            operation_id=operation.operation_id,
            operation_plan_sha256=operation.operation_plan_sha256,
            attempt_binding_sha256=attempt.attempt_binding_sha256,
            fencing_epoch=attempt.fencing_epoch,
            transaction_disposition=MssqlTransactionDisposition.ROLLED_BACK,
            controller_proves_not_invoked=False,
        )
        _execute_params(
            setup,
            f"""
INSERT INTO [{_SCHEMA}].[semantic_refresh_mssql_session_outcomes] (
    operation_id, operation_plan_sha256, attempt_binding_sha256, fencing_epoch,
    transaction_disposition, controller_proves_not_invoked, evidence_sha256
) VALUES (?, ?, ?, ?, N'ROLLED_BACK', 0, ?);
""".strip(),
            operation.operation_id,
            operation.operation_plan_sha256,
            attempt.attempt_binding_sha256,
            attempt.fencing_epoch,
            evidence_sha256,
        )
        scratch_documents = _prepared_documents(
            bundle,
            target_uuid=bundle.model_resources[0].clickhouse_target_uuid,
            deterministic_scratch=True,
        )
        _execute_params(
            setup,
            f"""
UPDATE [{_SCHEMA}].[semantic_refresh_journals]
SET prepare_plan_sha256 = ?, prepare_plan_json = ?,
    prepare_receipt_sha256 = ?, prepared_receipt_json = ?
WHERE operation_id = ?;
""".strip(),
            scratch_documents["prepare_plan_sha256"],
            scratch_documents["prepare_plan_json"],
            scratch_documents["prepared_receipt_sha256"],
            scratch_documents["prepared_receipt_json"],
            operation.operation_id,
        )
        failure_state = MssqlSemanticRefreshWorkflowFailureState(factory, control_schema=_SCHEMA)
        summary = SemanticRefreshMssqlFailureTerminalizationService(
            contexts=failure_state,
            evidence=MssqlSemanticRefreshEvidenceReader(factory, control_schema=_SCHEMA),
            state=failure_state,
        ).terminalize(bundle.workflow_execution_id)
        action = ReplacementActionBinding(
            action_id=operation.model_unique_id,
            outcome=SqlServerModelOutcome.ROLLED_BACK,
            action=ReplacementAction.BUILD_FRESH,
        )
        recovery = SemanticRefreshFailedPrecommitReplacementAuthority.build(
            predecessor_plan=plan,
            predecessor_summary=summary,
            predecessor_workflow_execution_id=bundle.workflow_execution_id,
            replacement_actions=(action,),
        )
        canonical = MssqlSemanticRefreshCanonicalAuthorityLoader(factory, control_schema=_SCHEMA)
        predecessor = MssqlSemanticRefreshPredecessorStateReader(factory, control_schema=_SCHEMA)
        verifier = SemanticRefreshMssqlRecoveryAuthorityVerifier(
            SemanticRefreshMssqlRecoveryHeadService(
                canonical,
                MssqlSemanticRefreshRecoveryHeadReader(factory, control_schema=_SCHEMA),
            ),
            failed_authority=canonical,
            failed_state=predecessor,
        )

        assert verifier.verify(recovery) is True
        cleanup = SemanticRefreshMssqlFailedScratchCleanupService(
            MssqlSemanticRefreshFailedScratchCleanupReader(
                factory,
                control_schema=_SCHEMA,
            )
        ).load(
            bundle.execution_binding.workflow_execution_binding_sha256,
            operation.operation_id,
        )
        assert tuple(item.relation_role for item in cleanup.relations) == ("SHADOW", "STAGING")
        assert all(item.table_name != cleanup.protected_target_table for item in cleanup.relations)
        assert tuple(item.observed_uuid for item in cleanup.relations) == (
            "00000000-0000-0000-0000-000000000004",
            "00000000-0000-0000-0000-000000000003",
        )
        assert cleanup.protected_target_uuid == bundle.model_resources[0].clickhouse_target_uuid
        _execute_params(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_journals] SET status = N'COMMIT_UNKNOWN' WHERE operation_id = ?;",
            operation.operation_id,
        )
        with pytest.raises(SemanticRefreshMssqlScratchCleanupReadError, match="journal conflict"):
            SemanticRefreshMssqlFailedScratchCleanupService(
                MssqlSemanticRefreshFailedScratchCleanupReader(
                    factory,
                    control_schema=_SCHEMA,
                )
            ).load(
                bundle.execution_binding.workflow_execution_binding_sha256,
                operation.operation_id,
            )
        _execute_params(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_journals] SET status = N'FAILED_PRE_COMMIT' WHERE operation_id = ?;",
            operation.operation_id,
        )
        _execute(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_workflow_executions] "
            "SET terminal_summary_json = N'{}' "
            f"WHERE workflow_id = N'{bundle.workflow_execution_id}';",
        )
        assert verifier.verify(recovery) is False
        _execute_params(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_workflow_executions] SET terminal_summary_json = ? "
            "WHERE workflow_id = ?;",
            _canonical_json(summary.to_dict()),
            bundle.workflow_execution_id,
        )
        with pytest.raises(SemanticRefreshContractError, match="receipt differs"):
            replace(recovery, authority_receipt_sha256="sha256:" + "0" * 64)
        _execute(
            setup,
            f"UPDATE [{_SCHEMA}].[semantic_refresh_journals] "
            f"SET mssql_evidence_sha256 = 'sha256:{'0' * 64}' "
            f"WHERE workflow_id = N'{bundle.workflow_execution_id}';",
        )
        assert verifier.verify(recovery) is False
    finally:
        _drop_schema(setup)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_schema_migrates_v20_activation_authority_key_to_current() -> None:
    factory = _connect_factory()
    schema = "dpone_sr_v21_migration"
    setup = factory()
    setup.autocommit = True
    migration = MssqlSemanticRefreshSchemaMigration(factory, control_schema=schema)
    authority_store = MssqlSemanticRefreshActivationAuthorityStore(
        factory,
        authority_store_ref=f"mssql-control://local/dpone_it/{schema}",
        control_schema=schema,
    )
    try:
        _drop_named_schema(setup, schema)
        migration.apply()
        receipt = authority_store.persist_exact(_activation_authority(persisted_at="2026-08-08T00:00:00.123456Z"))
        receipt_row_sql = f"""
SELECT deployment_id, release_id, plan_bundle_sha256, authority_store_ref,
       authority_receipt_json, activation_authority_receipt_sha256,
       CONVERT(varchar(27), persisted_at, 126), status
FROM [{schema}].[semantic_refresh_activation_authorities]
WHERE activation_authority_receipt_sha256 = '{receipt.activation_authority_receipt_sha256}';
"""
        expected_row = _one(setup, receipt_row_sql)

        def assert_receipt_is_unchanged() -> None:
            assert (
                _scalar(
                    setup,
                    f"SELECT COUNT_BIG(*) FROM [{schema}].[semantic_refresh_activation_authorities];",
                )
                == 1
            )
            assert _one(setup, receipt_row_sql) == expected_row

        _execute(
            setup,
            f"""
ALTER TABLE [{schema}].[semantic_refresh_activation_authorities]
DROP CONSTRAINT [pk_{schema}_sr_activation_authorities];
ALTER TABLE [{schema}].[semantic_refresh_activation_authorities]
ADD CONSTRAINT [legacy activation authority pk]
PRIMARY KEY (deployment_id);
UPDATE [{schema}].[semantic_refresh_schema_versions]
SET schema_version = 20
WHERE component = N'semantic_refresh_v2';
""",
        )

        migration.apply()
        assert_receipt_is_unchanged()
        migration.apply()
        assert_receipt_is_unchanged()
        assert (
            _scalar(
                setup,
                f"""
SELECT COUNT_BIG(*)
FROM sys.key_constraints AS key_constraint
JOIN sys.index_columns AS index_column
  ON index_column.object_id = key_constraint.parent_object_id
 AND index_column.index_id = key_constraint.unique_index_id
JOIN sys.columns AS column_definition
  ON column_definition.object_id = index_column.object_id
 AND column_definition.column_id = index_column.column_id
WHERE key_constraint.parent_object_id = OBJECT_ID(
    N'[{schema}].[semantic_refresh_activation_authorities]', N'U'
)
  AND key_constraint.type = N'PK'
  AND (
      (index_column.key_ordinal = 1 AND column_definition.name = N'deployment_id')
      OR (index_column.key_ordinal = 2 AND column_definition.name = N'plan_bundle_sha256')
  );
""",
            )
            == 2
        )
        assert (
            _scalar(
                setup,
                f"""
SELECT schema_version
FROM [{schema}].[semantic_refresh_schema_versions]
WHERE component = N'semantic_refresh_v2';
""",
            )
            == SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION
        )
        epoch_before = _scalar(
            setup,
            f"SELECT current_epoch FROM [{schema}].[semantic_refresh_ddl_epoch] WHERE singleton_id = 1;",
        )
        probe = f"dpone_sr_ddl_epoch_probe_{schema}"
        _execute(setup, f"CREATE TABLE [dbo].[{probe}] (id int NOT NULL);")
        assert (
            _scalar(
                setup,
                f"SELECT current_epoch FROM [{schema}].[semantic_refresh_ddl_epoch] WHERE singleton_id = 1;",
            )
            == epoch_before + 1
        )
        _execute(setup, f"DROP TABLE [dbo].[{probe}];")

        guard = factory()
        guard.autocommit = False
        guard_cursor = guard.cursor()
        blocked_probe = f"{probe}_blocked"
        try:
            guard_cursor.execute(
                """
SET NOCOUNT ON;
BEGIN TRANSACTION;
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = N'dpone:semantic-refresh:ddl-freeze',
    @LockMode = N'Shared', @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip()
            )
            assert guard_cursor.fetchone()[0] >= 0
            with pytest.raises(Exception, match="DPONE_SEMANTIC_REFRESH_DDL_FREEZE_ACTIVE"):
                _execute(setup, f"CREATE TABLE [dbo].[{blocked_probe}] (id int NOT NULL);")
            assert _scalar(setup, f"SELECT OBJECT_ID(N'[dbo].[{blocked_probe}]', N'U');") is None
        finally:
            guard.rollback()
            guard_cursor.close()
            guard.close()
    finally:
        _drop_named_schema(setup, schema)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
@pytest.mark.parametrize("empty_scope", [False, True], ids=["exchange", "empty-scope"])
def test_live_control_schema_and_atomic_multi_head_publication(empty_scope: bool) -> None:
    factory = _connect_factory()
    schema = "dpone_sr_v2_pub"
    setup = factory()
    setup.autocommit = True
    try:
        _drop_named_schema(setup, schema)
        migration = MssqlSemanticRefreshSchemaMigration(factory, control_schema=schema)
        migration.apply()
        migration.apply()
        termination = AttemptTerminationReceipt.build(
            workflow_execution_id="scheduled__2026-08-08",
            workflow_execution_binding_sha256="sha256:" + "4" * 64,
            operation_ids=("sha256:" + "7" * 64,),
            attempt_binding_sha256="sha256:" + "6" * 64,
            dag_id="competitive_pricing",
            run_id="scheduled__2026-08-08",
            task_id="dbt_build_and_tests",
            map_index=-1,
            try_number=1,
            cluster_id="dpone-semref-v2",
            namespace="dpone-semref-v2-live",
            pod_name="semantic-refresh-attempt",
            pod_uid="49851278-f049-401b-971e-f89f7b22bff6",
            pod_resource_version="950",
            terminal_phase="Succeeded",
            container_terminations=(
                ContainerTermination(
                    name="base",
                    container_id="containerd://runtime-id",
                    reason="Completed",
                    finished_at="2026-08-08T15:42:16Z",
                    exit_code=0,
                ),
            ),
            observed_at="2026-08-08T15:42:17Z",
            observer_authority="kubernetes-observer-v1",
            observer_policy_sha256="sha256:" + "3" * 64,
            observer_attestation_sha256="sha256:" + "4" * 64,
            observer_signature_sha256="sha256:" + "5" * 64,
        )
        termination_store = MssqlSemanticRefreshTerminationAdapter(
            factory,
            control_schema=schema,
        )
        termination_store.store(termination)
        termination_store.store(termination)
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{schema}].[semantic_refresh_attempt_terminations]",
            )
            == 1
        )
        bundle = _bundle()
        operation = bundle.operation_plans[0]
        attempt_binding = bundle.attempt_bindings[0]
        resource = bundle.model_resources[0]
        old_uuid = resource.clickhouse_target_uuid
        new_uuid = "00000000-0000-0000-0000-000000000002"
        old_checkpoint = "sha256:" + "8" * 64
        operation_id = operation.operation_id
        operation_plan = operation.operation_plan_sha256
        workflow_plan = bundle.workflow_plan.workflow_plan_sha256
        execution = bundle.execution_binding.workflow_execution_binding_sha256
        attempt = attempt_binding.attempt_binding_sha256
        publication_authority = bundle.authority_sha256
        publication_scope = resource.publication_scope_id
        guard_set_sha256 = mssql_guard_set_sha256(
            bundle.workflow_guard,
            bundle.resource_guards,
        )
        budget = bundle.resource_budget
        authority_json = semantic_refresh_mssql_authority_json(bundle).replace("'", "''")
        _execute(
            setup,
            f"""
INSERT INTO [{schema}].[semantic_refresh_canonical_authorities] (
    workflow_execution_binding_sha256, workflow_execution_id,
    authority_sha256, authority_json, status
) VALUES (
    '{execution}', N'{bundle.workflow_execution_id}', '{publication_authority}',
    N'{authority_json}', N'ACTIVE'
);
INSERT INTO [{schema}].[semantic_refresh_workflow_executions] (
    workflow_id, workflow_execution_id, workflow_plan_sha256,
    workflow_execution_binding_sha256, canonical_authority_sha256,
    guard_set_sha256, workflow_guard_resource_id, guard_count, owner_id, status
) VALUES (
    N'{bundle.workflow_execution_id}', N'{bundle.workflow_execution_id}', '{workflow_plan}',
    '{execution}', '{publication_authority}', '{guard_set_sha256}',
    N'{bundle.workflow_guard.resource_id}', {len(bundle.resource_guards) + 1},
    N'{bundle.owner_id}', N'PREPARING'
);
INSERT INTO [{schema}].[semantic_refresh_guards] (
    resource_id, fencing_epoch, owner_id, workflow_id, operation_id,
    operation_plan_sha256, attempt_binding_sha256, status
) VALUES (
    N'{resource.target_resource_id}', {attempt_binding.fencing_epoch},
    N'{bundle.owner_id}', N'{bundle.workflow_execution_id}', N'{operation_id}',
    '{operation_plan}', '{attempt}', N'HELD'
);
INSERT INTO [{schema}].[semantic_refresh_guards] (
    resource_id, fencing_epoch, owner_id, workflow_id, status
) VALUES (
    N'{bundle.workflow_guard.resource_id}', {bundle.workflow_guard.fencing_epoch},
    N'{bundle.owner_id}', N'{bundle.workflow_execution_id}', N'HELD'
);
INSERT INTO [{schema}].[semantic_refresh_reservations] (
    reservation_id, workflow_id, workflow_execution_binding_sha256, status,
    max_prepared_models, max_sealed_extract_bytes, max_clickhouse_staging_bytes,
    max_shadow_bytes, max_peak_bytes
) VALUES (
    N'{bundle.reservation_id}', N'{bundle.workflow_execution_id}', '{execution}', N'PREPARING',
    {budget.max_workflow_prepared_models}, {budget.max_workflow_sealed_extract_bytes},
    {budget.max_workflow_clickhouse_staging_bytes}, {budget.max_workflow_shadow_bytes},
    {budget.max_workflow_peak_bytes}
);
INSERT INTO [{schema}].[semantic_refresh_journals] (
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
    predecessor_checkpoint_version, fencing_epoch, owner_id, journal_version, status
) VALUES (
    N'{operation.model_unique_id}', N'{operation_id}', '{operation_plan}', '{attempt}', N'{{}}',
    '{mssql_strategy_authority_sha256("{}")}', '{resource.baseline_receipt_sha256}',
    N'{resource.baseline_kind}', N'{resource.baseline_receipt_json.replace("'", "''")}', N'COMPLETE',
    N'[{{"name":"event_id","order_encoding":"NATIVE"}}]', N'{bundle.workflow_execution_id}',
    N'{resource.target_resource_id}', N'{resource.publication_database}',
    N'{resource.publication_target_table}', N'{publication_scope}',
    '{operation.target_predecessor_generation_id}', NULL, 1, '{old_uuid}',
    N'{resource.baseline_receipt_sha256}', NULL, NULL, NULL, NULL,
    {attempt_binding.fencing_epoch}, N'{bundle.owner_id}', 1, N'PREPARING'
);
INSERT INTO [{schema}].[semantic_refresh_target_heads] (
    database_name, target_table, target_generation, target_generation_id, target_uuid, operation_id
) VALUES (
    N'{resource.publication_database}', N'{resource.publication_target_table}', 1,
    '{operation.target_predecessor_generation_id}', '{old_uuid}',
    N'{resource.baseline_receipt_sha256}'
);
INSERT INTO [{schema}].[semantic_refresh_checkpoints] (
    database_name, target_table, scope_id, checkpoint_sha256, checkpoint_version, operation_id
) VALUES (N'analytics', N'events', N'{publication_scope}', '{old_checkpoint}', 0, N'baseline');
""",
        )
        _execute(
            setup,
            f"DELETE FROM [{schema}].[semantic_refresh_checkpoints] WHERE scope_id = N'{publication_scope}';",
        )
        protected_projection = {
            "publication_authority_sha256": publication_authority,
            "target_resource_id": resource.target_resource_id,
            "target_authority_id": resource.target_authority_id,
            "clickhouse_cluster_authority_id": resource.clickhouse_cluster_authority_id,
            "workflow_execution_id": bundle.workflow_execution_id,
            "target_predecessor_generation_id": operation.target_predecessor_generation_id,
            "scope_predecessor_operation_id": None,
            "predecessor_target_generation": 1,
            "predecessor_target_uuid": old_uuid,
            "predecessor_target_operation_id": resource.baseline_receipt_sha256,
            "predecessor_scope_revision": None,
            "predecessor_checkpoint_sha256": None,
            "predecessor_checkpoint_operation_id": None,
            "predecessor_checkpoint_version": None,
            "database": resource.publication_database,
            "target_table": resource.publication_target_table,
            "scope_id": publication_scope,
            "scope_revision": operation.scope_revision,
        }
        assert _one(
            setup,
            f"""
SELECT target_predecessor_generation_id, scope_predecessor_operation_id,
       predecessor_target_generation, LOWER(CONVERT(varchar(36), predecessor_target_uuid)),
       predecessor_target_operation_id, predecessor_scope_revision,
       predecessor_checkpoint_sha256, predecessor_checkpoint_operation_id,
       predecessor_checkpoint_version
FROM [{schema}].[semantic_refresh_journals] WHERE operation_id = N'{operation_id}';
""".strip(),
        ) == (
            protected_projection["target_predecessor_generation_id"],
            protected_projection["scope_predecessor_operation_id"],
            protected_projection["predecessor_target_generation"],
            protected_projection["predecessor_target_uuid"],
            protected_projection["predecessor_target_operation_id"],
            protected_projection["predecessor_scope_revision"],
            protected_projection["predecessor_checkpoint_sha256"],
            protected_projection["predecessor_checkpoint_operation_id"],
            protected_projection["predecessor_checkpoint_version"],
        )
        state = MssqlSemanticRefreshPublicationState(
            factory,
            control_schema=schema,
            require_protected_authority=True,
        )
        resources = MssqlSemanticRefreshProtectedResourceLedger(
            factory,
            control_schema=schema,
            clock=lambda: datetime(2026, 8, 8, tzinfo=UTC),
        )
        resources.reserve_operation(
            workflow_execution_binding_sha256=execution,
            operation_id=operation_id,
        )
        prepared_documents = _prepared_documents(
            bundle,
            target_uuid=old_uuid,
            empty_scope=empty_scope,
        )
        prepared_receipt = str(prepared_documents["prepared_receipt_sha256"])
        prepared = {
            "operation_id": operation_id,
            "operation_plan_sha256": operation_plan,
            "workflow_plan_sha256": workflow_plan,
            "workflow_execution_binding_sha256": execution,
            "attempt_binding_sha256": attempt,
            "fence_epoch": attempt_binding.fencing_epoch,
            "prepare_receipt_sha256": prepared_receipt,
            **prepared_documents,
            "target_uuid": old_uuid,
            **protected_projection,
            "expected_journal_state": "PREPARING",
            "next_journal_state": "PREPARED",
        }
        assert state.persist_prepared(prepared)["journal_state"] == "PREPARED"
        assert state.persist_prepared(prepared)["journal_state"] == "PREPARED"
        if not empty_scope:
            committing = {
                key: value
                for key, value in prepared.items()
                if key
                not in {
                    "expected_journal_state",
                    "next_journal_state",
                    "prepare_plan_sha256",
                    "prepare_plan_json",
                    "prepared_receipt_sha256",
                    "prepared_receipt_json",
                    "artifact_manifest_key",
                    "artifact_manifest_version",
                    "artifact_manifest_sha256",
                }
            }
            committing.update(
                expected_journal_state="PREPARED",
                next_journal_state="COMMITTING",
                expected_target_uuid=old_uuid,
                expected_target_generation=1,
                expected_scope_revision=0,
                expected_checkpoint_sha256=None,
            )
            assert state.mark_committing(committing)["journal_state"] == "COMMITTING"
        request = {
            "operation_id": operation_id,
            "operation_plan_sha256": operation_plan,
            "workflow_plan_sha256": workflow_plan,
            "workflow_execution_binding_sha256": execution,
            "attempt_binding_sha256": attempt,
            "fence_epoch": attempt_binding.fencing_epoch,
            **protected_projection,
            "prepare_receipt_sha256": prepared_receipt,
            "clickhouse_commit_receipt_sha256": "sha256:" + "1" * 64,
            "terminal_receipt_sha256": "sha256:" + "2" * 64,
            "target_uuid": old_uuid if empty_scope else new_uuid,
            "expected_target_uuid": old_uuid,
            "expected_target_generation": 1,
            "target_generation": 1 if empty_scope else 2,
            "expected_scope_revision": 0,
            "scope_revision": operation.scope_revision,
            "expected_checkpoint_sha256": None,
            "checkpoint_sha256": "sha256:" + "9" * 64,
            "target_generation_id": (
                operation.target_predecessor_generation_id if empty_scope else "sha256:" + "f" * 64
            ),
            "target_mutation_outcome": ("NOT_REQUIRED_EMPTY_SCOPE" if empty_scope else "TARGET_COMMITTED"),
            "value_conversion_outcome": ("NOT_APPLICABLE_NO_DATA" if empty_scope else "CONFORMANT"),
            "expected_journal_state": "PREPARED" if empty_scope else "TARGET_COMMITTED",
            "terminal_journal_state": "COMPLETE",
        }
        if not empty_scope:
            target_committed = {
                "operation_id": operation_id,
                "operation_plan_sha256": operation_plan,
                "workflow_plan_sha256": workflow_plan,
                "workflow_execution_binding_sha256": execution,
                "attempt_binding_sha256": attempt,
                "fence_epoch": attempt_binding.fencing_epoch,
                "prepare_receipt_sha256": prepared_receipt,
                "clickhouse_commit_receipt_sha256": request["clickhouse_commit_receipt_sha256"],
                "target_uuid": new_uuid,
                "expected_target_uuid": old_uuid,
                "expected_journal_state": "COMMITTING",
                "next_journal_state": "TARGET_COMMITTED",
                **protected_projection,
            }
            assert state.record_target_committed(target_committed)["journal_state"] == "TARGET_COMMITTED"
            assert state.record_target_committed(target_committed)["journal_state"] == "TARGET_COMMITTED"
        publish = state.publish_empty_scope if empty_scope else state.publish_or_reconcile
        assert publish(request)["journal_state"] == "COMPLETE"
        assert publish(request)["journal_state"] == "COMPLETE"
        resources.release_operation(
            workflow_execution_binding_sha256=execution,
            operation_id=operation_id,
        )
        summary = _workflow_summary(
            workflow_execution_id=bundle.workflow_execution_id,
            operation_id=operation_id,
            operation_plan=operation_plan,
            workflow_plan=workflow_plan,
            execution=execution,
            attempt=attempt,
            artifact_manifest=str(prepared_documents["artifact_manifest_sha256"]),
            clickhouse_terminal_receipt=str(request["clickhouse_commit_receipt_sha256"]),
            terminal_receipt=str(request["terminal_receipt_sha256"]),
            target_generation=int(request["target_generation"]),
            scope_revision=int(request["scope_revision"]),
        )
        summary_state = MssqlSemanticRefreshWorkflowSummaryState(factory, control_schema=schema)
        assert summary_state.persist(summary)["status"] == "FULLY_COMPLETE"
        assert summary_state.persist(summary)["status"] == "FULLY_COMPLETE"
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{schema}].[semantic_refresh_guards] WHERE status = N'HELD'",
            )
            == 0
        )
        assert _scalar(
            setup,
            f"SELECT target_generation FROM [{schema}].[semantic_refresh_target_heads]",
        ) == (1 if empty_scope else 2)
        terminal_before = _one(
            setup,
            f"""
SELECT status, clickhouse_commit_receipt_sha256, terminal_receipt_sha256,
       terminal_target_generation, terminal_target_generation_id,
       terminal_scope_revision, terminal_checkpoint_sha256,
       terminal_checkpoint_version, terminal_target_mutation_outcome,
       terminal_value_conversion_outcome
FROM [{schema}].[semantic_refresh_journals]
WHERE operation_id = N'{operation_id}';
""".strip(),
        )
        with pytest.raises(SemanticRefreshMssqlPublicationError):
            publish(
                {
                    **request,
                    "clickhouse_commit_receipt_sha256": "sha256:" + "0" * 64,
                }
            )
        assert (
            _one(
                setup,
                f"""
SELECT status, clickhouse_commit_receipt_sha256, terminal_receipt_sha256,
       terminal_target_generation, terminal_target_generation_id,
       terminal_scope_revision, terminal_checkpoint_sha256,
       terminal_checkpoint_version, terminal_target_mutation_outcome,
       terminal_value_conversion_outcome
FROM [{schema}].[semantic_refresh_journals]
WHERE operation_id = N'{operation_id}';
""".strip(),
            )
            == terminal_before
        )
    finally:
        _drop_named_schema(setup, schema)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_continuation_requires_original_mssql_session_quiescence() -> None:
    factory = _connect_factory()
    schema = "dpone_sr_v2_quiescence"
    setup = factory()
    setup.autocommit = True
    original_connection: Any | None = None
    observer_connection: Any | None = None
    try:
        _drop_named_schema(setup, schema)
        MssqlSemanticRefreshSchemaMigration(factory, control_schema=schema).apply()
        bundle = _bundle()
        operation = bundle.operation_plans[0]
        original = bundle.attempt_bindings[0]
        resource = bundle.model_resources[0]
        termination = AttemptTerminationReceipt.build(
            workflow_execution_id=bundle.workflow_execution_id,
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            operation_ids=(operation.operation_id,),
            attempt_binding_sha256=original.attempt_binding_sha256,
            dag_id="semantic-refresh-live",
            run_id=original.dag_run_id,
            task_id=original.task_id,
            map_index=-1,
            try_number=original.try_number,
            cluster_id="local-docker",
            namespace="dpone-live",
            pod_name="semantic-refresh-original",
            pod_uid=original.pod_uid,
            pod_resource_version="1",
            terminal_phase="Succeeded",
            container_terminations=(
                ContainerTermination(
                    name="base",
                    container_id="containerd://semantic-refresh-live",
                    reason="Completed",
                    finished_at="2026-08-10T08:00:00.000000Z",
                    exit_code=0,
                ),
            ),
            observed_at="2026-08-10T08:00:01.000000Z",
            observer_authority="local-kubernetes-observer",
            observer_policy_sha256="sha256:" + "1" * 64,
            observer_attestation_sha256="sha256:" + "2" * 64,
            observer_signature_sha256="sha256:" + "3" * 64,
        )
        MssqlSemanticRefreshTerminationAdapter(
            factory,
            control_schema=schema,
        ).store(termination)

        class _ClickHouseQuiescence:
            def prove_quiescent(self, **values: object) -> ClickHouseAttemptQuiescenceProof:
                return ClickHouseAttemptQuiescenceProof.build(
                    workflow_execution_binding_sha256=str(values["workflow_execution_binding_sha256"]),
                    operation_id=str(values["operation_id"]),
                    original_attempt_binding_sha256=str(values["original_attempt_binding_sha256"]),
                    clickhouse_cluster_authority_id=str(values["clickhouse_cluster_authority_id"]),
                    query_id_prefix=("dpone-semref-aaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbbbbbb-"),
                    observed_at=str(values["observed_at"]),
                )

        observer = MssqlSemanticRefreshAttemptQuiescenceObserver(
            control_schema=schema,
            clickhouse=_ClickHouseQuiescence(),
        )
        original_connection = factory()
        original_connection.autocommit = False
        original_cursor = original_connection.cursor()
        original_cursor.execute(
            """
DECLARE @attempt_context varbinary(128) = HASHBYTES(
    'SHA2_256', CONVERT(varbinary(max), CONVERT(nvarchar(max), ?))
);
SET CONTEXT_INFO @attempt_context;
DECLARE @lock_result int;
EXEC @lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @lock_result;
""".strip(),
            original.attempt_binding_sha256,
            f"dpone:semantic-refresh:{resource.target_resource_id}",
        )
        assert original_cursor.fetchone()[0] >= 0

        observer_connection = factory()
        observer_connection.autocommit = False
        observer_cursor = observer_connection.cursor()
        with pytest.raises(
            SemanticRefreshMssqlAttemptQuiescenceError,
            match="not quiescent",
        ):
            observer.prove(
                observer_cursor,
                bundle=bundle,
                operation_id=operation.operation_id,
                original=original,
                guard_resource_id=resource.target_resource_id,
                clickhouse_cluster_authority_id=(resource.clickhouse_cluster_authority_id),
                observed_at="2026-08-10T08:00:02.000000Z",
            )
        observer_connection.rollback()
        original_connection.rollback()
        original_connection.close()
        original_connection = None
        closure = observer.prove(
            observer_cursor,
            bundle=bundle,
            operation_id=operation.operation_id,
            original=original,
            guard_resource_id=resource.target_resource_id,
            clickhouse_cluster_authority_id=resource.clickhouse_cluster_authority_id,
            observed_at="2026-08-10T08:00:03.000000Z",
        )
        assert closure.mssql_active_session_count == 0
        assert closure.termination == termination
        observer_connection.rollback()
    finally:
        if observer_connection is not None:
            observer_connection.close()
        if original_connection is not None:
            original_connection.close()
        _drop_named_schema(setup, schema)
        setup.close()


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_dbt_strategy_updates_then_receipts_full_scope_images() -> None:
    factory = _connect_factory()
    setup = factory()
    setup.autocommit = True
    try:
        _reset_macro_schema(setup)
        database = os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it")
        canonical_journal = compose_admission(_bundle()).journals[0]
        attempt = json.loads(canonical_journal.strategy_authority_json)
        attempt.update(
            model_unique_id="model.semantic_refresh_macro_parse.events",
            operation_id="sha256:" + "0" * 64,
            operation_plan_sha256="sha256:" + "3" * 64,
            attempt_binding_sha256="sha256:" + "4" * 64,
            workflow_execution_id="workflow-macro-live",
            owner_id="owner-macro-live",
            fencing_epoch=1,
            ddl_epoch=1,
            guard_resource_id=f"{database}.{_MACRO_SCHEMA}.events",
            guard_relation=_relation(database, "semantic_refresh_guards"),
            receipt_relation=_relation(database, "semantic_refresh_receipts"),
            before_image_relation=_relation(database, "events_before_1"),
            after_image_relation=_relation(database, "events_after_1"),
            target_relation=_relation(database, "events"),
            mssql_target_authority_id=(
                f"mssql://{attempt['mssql_connection_authority_id']}/{database}.{_MACRO_SCHEMA}.events"
            ),
            event_time_column="occurred_at",
            scope_start_utc="2026-08-08T00:00:00.000000",
            scope_end_utc="2026-08-09T00:00:00.000000",
            effective_keys=[
                {"name": "event_id", "data_type": "bigint"},
                {
                    "name": "occurred_at",
                    "data_type": "datetime2(6)",
                    "domain_min": "1900-01-01T00:00:00.000000Z",
                    "domain_max": "2299-12-31T23:59:59.999999Z",
                    "utc_assurance_sha256": "sha256:" + "8" * 64,
                },
            ],
            writable_columns=[
                {
                    "name": "event_id",
                    "nullable": False,
                    "source_type": "bigint",
                    "target_type": "Int64",
                    "writable_role": "EFFECTIVE_KEY",
                },
                {
                    "name": "occurred_at",
                    "nullable": False,
                    "source_type": "datetime2(6)",
                    "target_type": "DateTime64(6, 'UTC')",
                    "writable_role": "EFFECTIVE_KEY_EVENT_TIME",
                },
                {
                    "name": "payload",
                    "nullable": False,
                    "source_type": "varchar(32)",
                    "target_type": "String",
                    "writable_role": "MUTABLE_VALUE",
                },
            ],
            replacement_action="BUILD_FRESH",
            utc_semantics_assurance_receipt_sha256="sha256:" + "8" * 64,
        )
        local_resource_policy = dict(attempt["resource_policy"])
        local_resource_policy["min_mssql_log_free_bytes"] = 64_000_000
        local_resource_policy["resource_policy_sha256"] = (
            "sha256:"
            + hashlib.sha256(
                _canonical_json(
                    {
                        "schema": "dpone.dbt-semantic-refresh-resource-policy.v1",
                        **{
                            key: value
                            for key, value in local_resource_policy.items()
                            if key != "resource_policy_sha256"
                        },
                    }
                ).encode()
            ).hexdigest()
        )
        attempt["resource_policy"] = local_resource_policy
        authority_json = json.dumps(attempt, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        unsigned_scope_map = {
            "schema": "dpone.semantic-refresh-mssql-scope-map.v1",
            "authorities": {"model.semantic_refresh_macro_parse.events": authority_json},
            "models": {"model.semantic_refresh_macro_parse.events": attempt},
        }
        scope_map: dict[str, Any] = {
            **unsigned_scope_map,
            "scope_map_sha256": "sha256:" + hashlib.sha256(_canonical_json(unsigned_scope_map).encode()).hexdigest(),
            "signature_sha256": "sha256:" + "2" * 64,
            "verification_status": "VERIFIED",
        }
        _set_macro_guard_authority(setup, authority_json)
        project = Path("tests/fixtures/semantic-refresh-v2/mssql/dbt_macro_parse")
        limited_attempt = json.loads(json.dumps(attempt))
        limited_policy = dict(limited_attempt["resource_policy"])
        limited_policy["max_target_scope_rows"] = 1
        limited_policy["resource_policy_sha256"] = (
            "sha256:"
            + hashlib.sha256(
                _canonical_json(
                    {
                        "schema": "dpone.dbt-semantic-refresh-resource-policy.v1",
                        **{key: value for key, value in limited_policy.items() if key != "resource_policy_sha256"},
                    }
                ).encode()
            ).hexdigest()
        )
        limited_attempt["resource_policy"] = limited_policy
        limited_authority_json = json.dumps(
            limited_attempt,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        limited_unsigned = {
            "schema": "dpone.semantic-refresh-mssql-scope-map.v1",
            "authorities": {"model.semantic_refresh_macro_parse.events": limited_authority_json},
            "models": {"model.semantic_refresh_macro_parse.events": limited_attempt},
        }
        limited_scope_map = {
            **limited_unsigned,
            "scope_map_sha256": "sha256:" + hashlib.sha256(_canonical_json(limited_unsigned).encode()).hexdigest(),
            "signature_sha256": "sha256:" + "2" * 64,
            "verification_status": "VERIFIED",
        }
        _execute(
            setup,
            f"INSERT INTO [{_MACRO_SCHEMA}].[events] VALUES "
            "(2, CONVERT(datetime2(6), '2026-08-08T01:00:00.000000', 126), 'extra');",
        )
        _set_macro_guard_authority(setup, limited_authority_json)
        _run_dbt_failure(project, limited_scope_map, "DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED")
        assert _scalar(setup, f"SELECT COUNT_BIG(*) FROM [{_MACRO_SCHEMA}].[semantic_refresh_receipts]") == 0
        _execute(setup, f"DELETE FROM [{_MACRO_SCHEMA}].[events] WHERE event_id = 2;")
        _set_macro_guard_authority(setup, authority_json)
        _execute(
            setup,
            f"UPDATE [{_MACRO_SCHEMA}].[semantic_refresh_ddl_epoch] SET current_epoch = 2 WHERE singleton_id = 1;",
        )
        _run_dbt_failure(project, scope_map, "DPONE_SEMANTIC_REFRESH_DDL_EPOCH_DRIFT")
        _execute(
            setup,
            f"UPDATE [{_MACRO_SCHEMA}].[semantic_refresh_ddl_epoch] SET current_epoch = 1 WHERE singleton_id = 1;",
        )
        _run_dbt(project, scope_map)
        assert (
            _scalar(
                setup,
                f"SELECT payload FROM [{_MACRO_SCHEMA}].[events] WHERE event_id = 1",
            )
            == "payload"
        )
        assert _scalar(setup, f"SELECT COUNT_BIG(*) FROM [{_MACRO_SCHEMA}].[events_before_1]") == 1
        assert _scalar(setup, f"SELECT COUNT_BIG(*) FROM [{_MACRO_SCHEMA}].[events_after_1]") == 1
        assert _scalar(setup, f"SELECT COUNT_BIG(*) FROM [{_MACRO_SCHEMA}].[semantic_refresh_receipts]") == 1
        original_build_receipt = _scalar(
            setup,
            f"SELECT build_receipt_sha256 FROM [{_MACRO_SCHEMA}].[semantic_refresh_receipts]",
        )
        _run_dbt(project, scope_map)
        assert _scalar(setup, f"SELECT COUNT_BIG(*) FROM [{_MACRO_SCHEMA}].[semantic_refresh_receipts]") == 1
        _execute(setup, f"UPDATE [{_MACRO_SCHEMA}].[events] SET payload = 'target-drift' WHERE event_id = 1;")
        _run_dbt_failure(project, scope_map, "DPONE_DBT_MSSQL_RECEIPT_TARGET_DRIFT")
        _execute(setup, f"UPDATE [{_MACRO_SCHEMA}].[events] SET payload = 'payload' WHERE event_id = 1;")
        _execute(
            setup,
            f"UPDATE [{_MACRO_SCHEMA}].[semantic_refresh_receipts] SET build_receipt_sha256 = 'sha256:{'0' * 64}';",
        )
        _run_dbt_failure(project, scope_map, "DPONE_SEMANTIC_REFRESH_RECEIPT_IMAGE_CONFLICT")
        _execute_params(
            setup,
            f"UPDATE [{_MACRO_SCHEMA}].[semantic_refresh_receipts] SET build_receipt_sha256 = ?;",
            original_build_receipt,
        )
        _execute(
            setup,
            f"""
INSERT INTO [{_MACRO_SCHEMA}].[semantic_refresh_journals] (
    model_unique_id, operation_id, operation_plan_sha256, attempt_binding_sha256,
    strategy_authority_sha256, fencing_epoch, image_key_columns_json
) VALUES (
    N'model.semantic_refresh_macro_parse.events', 'sha256:{"0" * 64}',
    'sha256:{"3" * 64}', 'sha256:{"4" * 64}',
    '{mssql_strategy_authority_sha256(authority_json)}', 1,
    N'[{{"name":"event_id","order_encoding":"NATIVE"}},'
       + N'{{"name":"occurred_at","order_encoding":"NATIVE"}}]'
);
""",
        )
        evidence_reader = MssqlSemanticRefreshEvidenceReader(factory, control_schema=_MACRO_SCHEMA)
        image_evidence = evidence_reader.read_operation_evidence(
            operation_id="sha256:" + "0" * 64,
            attempt_binding_sha256="sha256:" + "4" * 64,
        )
        assert image_evidence.before_image is not None
        assert image_evidence.before_image.row_count == 1
        assert image_evidence.after_image is not None
        assert image_evidence.after_image.row_count == 1
        _execute(setup, f"UPDATE [{_MACRO_SCHEMA}].[events_after_1] SET payload = 'tampered';")
        with pytest.raises(SemanticRefreshMssqlEvidenceReadError, match="image digest differs"):
            evidence_reader.read_operation_evidence(
                operation_id="sha256:" + "0" * 64,
                attempt_binding_sha256="sha256:" + "4" * 64,
            )
        _execute(setup, f"UPDATE [{_MACRO_SCHEMA}].[events_after_1] SET payload = 'payload';")

        attempt = scope_map["models"]["model.semantic_refresh_macro_parse.events"]
        attempt.update(
            {
                "operation_id": "sha256:" + "7" * 64,
                "operation_plan_sha256": "sha256:" + "5" * 64,
                "attempt_binding_sha256": "sha256:" + "6" * 64,
                "fencing_epoch": 2,
                "before_image_relation": _relation(database, "events_before_2"),
                "after_image_relation": _relation(database, "events_after_2"),
                "replacement_action": "RESTORE_THEN_REBUILD",
                "predecessor_receipt_relation": _relation(database, "semantic_refresh_receipts"),
                "predecessor_operation_id": "sha256:" + "0" * 64,
                "predecessor_operation_plan_sha256": "sha256:" + "3" * 64,
                "predecessor_attempt_binding_sha256": "sha256:" + "4" * 64,
                "predecessor_fencing_epoch": 1,
                "predecessor_before_image_relation": _relation(database, "events_before_1"),
                "predecessor_before_image_sha256": _scalar(
                    setup,
                    f"SELECT before_image_sha256 FROM [{_MACRO_SCHEMA}].[semantic_refresh_receipts]",
                ),
                "predecessor_after_image_relation": _relation(database, "events_after_1"),
                "predecessor_after_image_sha256": _scalar(
                    setup,
                    f"SELECT after_image_sha256 FROM [{_MACRO_SCHEMA}].[semantic_refresh_receipts]",
                ),
            }
        )
        authority_json = _authority_json(scope_map)
        scope_map["authorities"] = {"model.semantic_refresh_macro_parse.events": authority_json}
        unsigned_scope_map = {
            "schema": scope_map["schema"],
            "authorities": scope_map["authorities"],
            "models": scope_map["models"],
        }
        scope_map["scope_map_sha256"] = (
            "sha256:" + hashlib.sha256(_canonical_json(unsigned_scope_map).encode()).hexdigest()
        )
        _execute(
            setup,
            f"""
UPDATE [{_MACRO_SCHEMA}].[semantic_refresh_guards]
SET operation_id = N'sha256:{"7" * 64}',
    operation_plan_sha256 = 'sha256:{"5" * 64}',
    attempt_binding_sha256 = 'sha256:{"6" * 64}',
    fencing_epoch = 2
WHERE resource_id = N'{database}.{_MACRO_SCHEMA}.events';
""",
        )
        _set_macro_guard_authority(setup, authority_json)
        _run_dbt(project, scope_map)
        assert _scalar(setup, f"SELECT payload FROM [{_MACRO_SCHEMA}].[events_before_2]") == "old"
        assert _scalar(setup, f"SELECT payload FROM [{_MACRO_SCHEMA}].[events_after_2]") == "payload"
        assert _scalar(setup, f"SELECT payload FROM [{_MACRO_SCHEMA}].[events]") == "payload"
        assert _scalar(setup, f"SELECT COUNT_BIG(*) FROM [{_MACRO_SCHEMA}].[semantic_refresh_receipts]") == 2
    finally:
        _drop_macro_schema(setup)
        setup.close()


def _reset_schema(connection: Any) -> SemanticRefreshActivationAuthorityReceipt:
    _drop_schema(connection)
    MssqlSemanticRefreshSchemaMigration(
        _connect_factory(),
        control_schema=_SCHEMA,
    ).apply()
    return _seed_live_admission_authority(connection, _request())


def _persist_live_activation_authority(plan_bundle: Any) -> SemanticRefreshActivationAuthorityReceipt:
    return MssqlSemanticRefreshActivationAuthorityStore(
        _connect_factory(),
        authority_store_ref=f"mssql-control://local/{_SCHEMA}",
        control_schema=_SCHEMA,
    ).persist_exact(
        SemanticRefreshActivationAuthoritySet(
            release_id=plan_bundle.release_deployment_authority.release_id,
            deployment_id=plan_bundle.release_deployment_authority.deployment_id,
            plan_bundle_sha256=plan_bundle.plan_bundle_sha256,
            baselines=tuple(item.baseline_receipt for item in plan_bundle.targets),
            route_certification=_route_receipt(),
            runtime_assurances=_runtime_assurances(),
            persisted_at="2026-08-08T00:00:00Z",
        )
    )


def _seed_live_admission_authority(
    connection: Any,
    request: MssqlAdmissionRequest,
) -> SemanticRefreshActivationAuthorityReceipt:
    """Seed exact protected deployment/run authority for the local live cell."""

    prerequisite = request.prerequisite_authorities[0]
    route = _route_receipt()
    assurances = _runtime_assurances()
    assert route.route_certification_receipt_sha256 == prerequisite.route_certification_receipt_sha256
    claims = {item.assurance_kind: item for item in prerequisite.runtime_assurances}
    for receipt in assurances:
        kind = receipt.subject.assurance_kind.value
        if kind not in claims:
            continue
        claim = claims[kind]
        assert receipt.runtime_assurance_receipt_sha256 == claim.receipt_sha256
    bundle = _bundle()
    plan_bundle = _plan_bundle()
    activation_receipt = _persist_live_activation_authority(plan_bundle)
    activation = SemanticRefreshMssqlActivationService(
        activation=MssqlSemanticRefreshActivationStore(
            _connect_factory(),
            control_schema=_SCHEMA,
        ),
        deployment_verifier=type(
            "ExactLiveDeploymentVerifier",
            (),
            {"verify": lambda *_: True},
        )(),
        activation_guard=_AllowLocalActivation(),
    )
    subject = SemanticRefreshDeploymentAuthoritySubject.build(
        _authority(),
        (_deployment_model(),),
    )
    activation.activate_deployment(subject=subject, plan_bundle=plan_bundle)
    activation.activate_deployment(subject=subject, plan_bundle=plan_bundle)
    activation.register_run(bundle)
    activation.register_run(bundle)
    _seed_failed_cleanup_closure(connection, request, bundle)
    return activation_receipt


def _seed_failed_cleanup_closure(
    connection: Any,
    request: MssqlAdmissionRequest,
    bundle: MssqlCanonicalAdmissionBundle,
) -> None:
    ack = _failed_cleanup_ack(request, bundle)
    scratch = ack.scratch
    resources = ack.resources
    resource = bundle.model_resources[0]
    _execute_params(
        connection,
        f"""
INSERT INTO [{_SCHEMA}].[semantic_refresh_workflow_executions] (
    workflow_id, workflow_execution_id, workflow_plan_sha256,
    workflow_execution_binding_sha256, canonical_authority_sha256,
    workflow_guard_resource_id, guard_count, status, terminal_summary_sha256
) VALUES (N'failed-workflow', N'failed-workflow', ?, ?, ?, N'workflow://failed-workflow',
          1, N'FAILED_PRE_COMMIT', ?);
INSERT INTO [{_SCHEMA}].[semantic_refresh_reservations] (
    reservation_id, workflow_id, workflow_execution_binding_sha256, status,
    max_prepared_models, max_sealed_extract_bytes, max_clickhouse_staging_bytes,
    max_shadow_bytes, max_peak_bytes
) VALUES (?, N'failed-workflow', ?, N'FAILED_PRE_COMMIT', 1, 1, 1, 1, 5);
INSERT INTO [{_SCHEMA}].[semantic_refresh_journals] (
    model_unique_id, operation_id, operation_plan_sha256, attempt_binding_sha256,
    strategy_authority_json, strategy_authority_sha256,
    baseline_receipt_sha256, baseline_kind, baseline_receipt_json, baseline_status,
    image_key_columns_json, workflow_id, target_resource_id,
    publication_database, publication_target_table, publication_scope_id,
    target_predecessor_generation_id, scope_predecessor_operation_id,
    predecessor_target_generation, predecessor_target_uuid,
    predecessor_target_operation_id, fencing_epoch, owner_id, journal_version,
    status, mssql_outcome, target_uuid
) VALUES (?, ?, ?, ?, N'{{}}', ?, ?, ?, ?, N'COMPLETE', N'[]', N'failed-workflow',
          ?, ?, ?, ?, ?, NULL, 1, ?, ?, 1, N'failed-owner', 1,
          N'FAILED_PRE_COMMIT', N'NOT_INVOKED', ?);
""".strip(),
        "sha256:" + "1" * 64,
        scratch.workflow_execution_binding_sha256,
        "sha256:" + "2" * 64,
        "sha256:" + "7" * 64,
        resources.reservation_id,
        scratch.workflow_execution_binding_sha256,
        bundle.operation_plans[0].model_unique_id,
        scratch.operation_id,
        scratch.operation_plan_sha256,
        scratch.attempt_binding_sha256,
        mssql_strategy_authority_sha256("{}"),
        resource.baseline_receipt_sha256,
        resource.baseline_kind,
        resource.baseline_receipt_json,
        resource.target_resource_id,
        resource.publication_database,
        resource.publication_target_table,
        resource.publication_scope_id,
        bundle.operation_plans[0].target_predecessor_generation_id,
        scratch.target_uuid,
        resource.baseline_receipt_sha256,
        scratch.target_uuid,
    )
    for allocation in resources.allocations:
        _execute_params(
            connection,
            f"""
INSERT INTO [{_SCHEMA}].[semantic_refresh_resource_allocations] (
    allocation_id, reservation_id, resource_kind, amount, status
) VALUES (?, ?, ?, ?, N'RELEASED');
""".strip(),
            allocation.allocation_id,
            allocation.reservation_id,
            allocation.resource_kind,
            allocation.amount,
        )
    _execute_params(
        connection,
        f"""
INSERT INTO [{_SCHEMA}].[semantic_refresh_failed_cleanup_acks] (
    workflow_execution_binding_sha256, operation_id, workflow_execution_id,
    operation_plan_sha256, attempt_binding_sha256, fencing_epoch, target_uuid,
    scratch_absence_evidence_sha256, scratch_receipt_json,
    resource_allocation_closure_sha256, resource_closure_json,
    cleanup_receipt_sha256, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, N'COMPLETE');
""".strip(),
        scratch.workflow_execution_binding_sha256,
        scratch.operation_id,
        scratch.workflow_execution_id,
        scratch.operation_plan_sha256,
        scratch.attempt_binding_sha256,
        scratch.fencing_epoch,
        scratch.target_uuid,
        scratch.scratch_absence_evidence_sha256,
        _canonical_json(scratch.to_mapping()),
        resources.resource_allocation_closure_sha256,
        _canonical_json(resources.to_mapping()),
        ack.cleanup_receipt_sha256,
    )
    assert (
        MssqlSemanticRefreshFailedPrecommitCleanupAckStore(
            _connect_factory(),
            control_schema=_SCHEMA,
        ).load_exact(
            workflow_execution_binding_sha256=scratch.workflow_execution_binding_sha256,
            operation_id=scratch.operation_id,
        )
        == ack
    )


def _canonical_json(value: dict[str, object]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _execute_params(connection: Any, sql: str, *parameters: object) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(sql, *parameters)
        while cursor.nextset():
            pass
    finally:
        cursor.close()


def _reset_schema_legacy_fixture(connection: Any) -> None:
    """Retained source for the pre-V2 fixture; never used by the protected cell."""

    _drop_schema(connection)
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA [{_SCHEMA}]")
        cursor.execute(
            f"""
CREATE TABLE [{_SCHEMA}].[semantic_refresh_guards] (
    resource_id nvarchar(512) NOT NULL PRIMARY KEY,
    fencing_epoch bigint NOT NULL,
    owner_id nvarchar(512) NULL,
    workflow_id nvarchar(512) NULL,
    operation_id nvarchar(512) NULL,
    operation_plan_sha256 varchar(71) NULL,
    attempt_binding_sha256 varchar(71) NULL,
    strategy_authority_sha256 varchar(71) NULL,
    status nvarchar(32) NOT NULL
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_workflow_executions] (
    workflow_id nvarchar(512) NOT NULL PRIMARY KEY,
    workflow_execution_id nvarchar(512) NULL,
    workflow_plan_sha256 varchar(71) NULL,
    workflow_execution_binding_sha256 varchar(71) NULL,
    canonical_authority_sha256 varchar(71) NULL,
    guard_set_sha256 varchar(71) NULL,
    journal_set_sha256 varchar(71) NULL,
    workflow_guard_resource_id nvarchar(512) NULL,
    guard_count bigint NULL,
    controller_id nvarchar(512) NULL,
    owner_id nvarchar(512) NULL,
    status nvarchar(32) NOT NULL,
    successor_workflow_id nvarchar(512) NULL,
    replacement_plan_sha256 varchar(71) NULL
    ,terminal_summary_sha256 varchar(71) NULL
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_canonical_authorities] (
    workflow_execution_binding_sha256 varchar(71) NOT NULL PRIMARY KEY,
    workflow_execution_id nvarchar(512) NOT NULL UNIQUE,
    authority_sha256 varchar(71) NOT NULL UNIQUE,
    authority_json nvarchar(max) NOT NULL,
    status nvarchar(32) NOT NULL
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_reservations] (
    reservation_id nvarchar(512) NOT NULL PRIMARY KEY,
    workflow_id nvarchar(512) NOT NULL,
    workflow_execution_binding_sha256 varchar(71) NOT NULL,
    status nvarchar(32) NOT NULL,
    max_prepared_models bigint NOT NULL,
    max_sealed_extract_bytes bigint NOT NULL,
    max_clickhouse_staging_bytes bigint NOT NULL,
    max_shadow_bytes bigint NOT NULL,
    max_peak_bytes bigint NOT NULL,
    reserved_prepared_models bigint NOT NULL DEFAULT 0,
    reserved_sealed_extract_bytes bigint NOT NULL DEFAULT 0,
    reserved_clickhouse_staging_bytes bigint NOT NULL DEFAULT 0,
    reserved_shadow_bytes bigint NOT NULL DEFAULT 0,
    reserved_retained_generation_bytes bigint NOT NULL DEFAULT 0,
    reserved_peak_bytes bigint NOT NULL DEFAULT 0
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_journals] (
    model_unique_id nvarchar(512) NOT NULL,
    operation_id nvarchar(512) NOT NULL PRIMARY KEY,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    strategy_authority_json nvarchar(max) NOT NULL,
    strategy_authority_sha256 varchar(71) NOT NULL,
    baseline_receipt_sha256 varchar(71) NOT NULL,
    baseline_kind nvarchar(64) NOT NULL,
    baseline_receipt_json nvarchar(max) NOT NULL,
    baseline_status nvarchar(32) NOT NULL,
    image_key_columns_json nvarchar(max) NOT NULL,
    workflow_id nvarchar(512) NOT NULL,
    target_resource_id nvarchar(512) NOT NULL,
    publication_database nvarchar(128) NOT NULL,
    publication_target_table nvarchar(128) NOT NULL,
    publication_scope_id nvarchar(512) NOT NULL,
    target_predecessor_generation_id varchar(71) NOT NULL,
    scope_predecessor_operation_id nvarchar(512) NULL,
    predecessor_target_generation bigint NOT NULL,
    predecessor_target_uuid uniqueidentifier NOT NULL,
    predecessor_target_operation_id nvarchar(512) NOT NULL,
    predecessor_scope_revision bigint NULL,
    predecessor_checkpoint_sha256 varchar(71) NULL,
    predecessor_checkpoint_operation_id nvarchar(512) NULL,
    predecessor_checkpoint_version bigint NULL,
    fencing_epoch bigint NOT NULL,
    replaces_failed_operation_id nvarchar(512) NULL,
    owner_id nvarchar(512) NOT NULL,
    status nvarchar(32) NOT NULL,
    mssql_outcome nvarchar(32) NULL,
    mssql_evidence_sha256 varchar(71) NULL
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_baselines] (
    target_resource_id nvarchar(512) NOT NULL PRIMARY KEY,
    model_unique_id nvarchar(512) NOT NULL,
    baseline_kind nvarchar(64) NOT NULL,
    baseline_receipt_sha256 varchar(71) NOT NULL,
    baseline_receipt_json nvarchar(max) NOT NULL,
    status nvarchar(32) NOT NULL,
    is_current bit NOT NULL
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_target_owners] (
    target_authority_id nvarchar(512) NOT NULL PRIMARY KEY,
    model_unique_id nvarchar(512) NOT NULL,
    deployment_id varchar(71) NOT NULL,
    owner_generation bigint NOT NULL,
    status nvarchar(32) NOT NULL
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_target_heads] (
    database_name nvarchar(128) NOT NULL,
    target_table nvarchar(128) NOT NULL,
    target_generation bigint NOT NULL,
    target_generation_id varchar(71) NOT NULL,
    target_uuid uniqueidentifier NOT NULL,
    operation_id nvarchar(512) NOT NULL,
    PRIMARY KEY (database_name, target_table)
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_scope_heads] (
    database_name nvarchar(128) NOT NULL,
    target_table nvarchar(128) NOT NULL,
    scope_id nvarchar(512) NOT NULL,
    scope_revision bigint NOT NULL,
    operation_id nvarchar(512) NOT NULL,
    PRIMARY KEY (database_name, target_table, scope_id)
);
CREATE TABLE [{_SCHEMA}].[semantic_refresh_checkpoints] (
    database_name nvarchar(128) NOT NULL,
    target_table nvarchar(128) NOT NULL,
    scope_id nvarchar(512) NOT NULL,
    checkpoint_sha256 varchar(71) NOT NULL,
    checkpoint_version bigint NOT NULL,
    operation_id nvarchar(512) NOT NULL,
    PRIMARY KEY (database_name, target_table, scope_id)
);
INSERT INTO [{_SCHEMA}].[semantic_refresh_guards] (resource_id, fencing_epoch, status)
VALUES (N'workflow://daily-events', 0, N'AVAILABLE'),
       (N'mssql://warehouse/dbo/events', 0, N'AVAILABLE');
INSERT INTO [{_SCHEMA}].[semantic_refresh_workflow_executions] (
    workflow_id, workflow_execution_id, canonical_authority_sha256, status, terminal_summary_sha256
)
VALUES (
    N'failed-workflow', N'failed-workflow', 'sha256:{"2" * 64}',
    N'FAILED_PRE_COMMIT', 'sha256:{"7" * 64}'
);
INSERT INTO [{_SCHEMA}].[semantic_refresh_canonical_authorities] (
    workflow_execution_binding_sha256, workflow_execution_id,
    authority_sha256, authority_json, status
) VALUES (
    'sha256:{"b" * 64}', N'replacement-workflow',
    'sha256:{"1" * 64}', N'{{}}', N'ACTIVE'
);
INSERT INTO [{_SCHEMA}].[semantic_refresh_target_owners]
VALUES (
    N'clickhouse://analytics/events', N'model.semantic_refresh_macro_parse.events',
    'sha256:{"9" * 64}', 1, N'ACTIVE'
);
INSERT INTO [{_SCHEMA}].[semantic_refresh_baselines]
VALUES (
    N'mssql://warehouse/dbo/events', N'model.semantic_refresh_macro_parse.events',
    N'adopted_complete_relation_conformant', 'sha256:{"8" * 64}', N'{{}}', N'COMPLETE', 1
);
INSERT INTO [{_SCHEMA}].[semantic_refresh_target_heads]
VALUES (
    N'analytics', N'events', 2, 'sha256:{"2" * 64}',
    '00000000-0000-0000-0000-000000000001', N'target-predecessor'
);
INSERT INTO [{_SCHEMA}].[semantic_refresh_scope_heads]
VALUES (N'analytics', N'events', N'scope-live', 3, 'sha256:{"3" * 64}');
INSERT INTO [{_SCHEMA}].[semantic_refresh_checkpoints]
VALUES (
    N'analytics', N'events', N'scope-live', 'sha256:{"4" * 64}',
    5, 'sha256:{"3" * 64}'
);
"""
        )
    finally:
        cursor.close()


def _relation(database: str, identifier: str) -> dict[str, str]:
    return {"database": database, "schema": _MACRO_SCHEMA, "identifier": identifier}


def _authority_json(scope_map: dict[str, Any]) -> str:
    models = scope_map["models"]
    assert isinstance(models, dict)
    return json.dumps(
        models["model.semantic_refresh_macro_parse.events"],
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _set_macro_guard_authority(connection: Any, authority_json: str) -> None:
    digest = mssql_strategy_authority_sha256(authority_json)
    _execute(
        connection,
        f"UPDATE [{_MACRO_SCHEMA}].[semantic_refresh_guards] SET strategy_authority_sha256 = '{digest}'",
    )


def _run_dbt(project: Path, scope_map: dict[str, object]) -> None:
    result = subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--no-partial-parse",
            "--vars",
            json.dumps(_semantic_refresh_dbt_vars(scope_map)),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-4000:]


def _run_dbt_failure(
    project: Path,
    scope_map: dict[str, object],
    expected_error: str,
) -> None:
    result = subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--no-partial-parse",
            "--vars",
            json.dumps(_semantic_refresh_dbt_vars(scope_map)),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert expected_error in output[-4000:]


def _semantic_refresh_dbt_vars(scope_map: dict[str, object]) -> dict[str, object]:
    models = scope_map.get("models")
    if not isinstance(models, dict) or not models:
        raise ValueError("live semantic-refresh scope map has no models")
    timeouts = {
        model["resource_policy"]["max_statement_seconds"]
        for model in models.values()
        if isinstance(model, dict) and isinstance(model.get("resource_policy"), dict)
    }
    if len(timeouts) != 1:
        raise ValueError("live semantic-refresh statement timeout closure differs")
    return {
        "dpone_semantic_refresh_scope_map": scope_map,
        "dpone_semantic_refresh_statement_timeout_seconds": timeouts.pop(),
    }


def _execute(connection: Any, sql: str) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        while cursor.nextset():
            pass
    finally:
        cursor.close()


def _reset_macro_schema(connection: Any) -> None:
    _drop_macro_schema(connection)
    database = os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it")
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA [{_MACRO_SCHEMA}]")
        cursor.execute(
            f"""
CREATE TABLE [{_MACRO_SCHEMA}].[events] (
    event_id bigint NOT NULL,
    occurred_at datetime2(6) NOT NULL,
    payload varchar(32) NOT NULL,
    CONSTRAINT [pk_dpone_sr_macro_events] PRIMARY KEY (event_id, occurred_at)
);
INSERT INTO [{_MACRO_SCHEMA}].[events] (event_id, occurred_at, payload)
VALUES (1, CONVERT(datetime2(6), '2026-08-08T00:00:00.000000', 126), 'old');
CREATE TABLE [{_MACRO_SCHEMA}].[semantic_refresh_guards] (
    resource_id nvarchar(512) NOT NULL PRIMARY KEY,
    workflow_id nvarchar(512) NOT NULL,
    owner_id nvarchar(512) NOT NULL,
    operation_id nvarchar(512) NOT NULL,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    strategy_authority_sha256 varchar(71) NULL,
    fencing_epoch bigint NOT NULL,
    status nvarchar(32) NOT NULL
);
INSERT INTO [{_MACRO_SCHEMA}].[semantic_refresh_guards]
VALUES (
    N'{database}.{_MACRO_SCHEMA}.events', N'workflow-macro-live',
    N'owner-macro-live', N'sha256:{"0" * 64}', 'sha256:{"3" * 64}',
    'sha256:{"4" * 64}', NULL, 1, N'HELD'
);
CREATE TABLE [{_MACRO_SCHEMA}].[semantic_refresh_receipts] (
    operation_id nvarchar(512) NOT NULL PRIMARY KEY,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    fencing_epoch bigint NOT NULL,
    before_image_relation nvarchar(776) NOT NULL,
    before_image_sha256 varchar(71) NOT NULL,
    after_image_relation nvarchar(776) NOT NULL,
    after_image_sha256 varchar(71) NOT NULL,
    updated_count bigint NOT NULL,
    inserted_count bigint NOT NULL,
    build_receipt_sha256 varchar(71) NOT NULL
);
CREATE TABLE [{_MACRO_SCHEMA}].[semantic_refresh_journals] (
    model_unique_id nvarchar(512) NOT NULL,
    operation_id nvarchar(512) NOT NULL PRIMARY KEY,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    strategy_authority_sha256 varchar(71) NOT NULL,
    fencing_epoch bigint NOT NULL,
    image_key_columns_json nvarchar(max) NOT NULL
);
CREATE TABLE [{_MACRO_SCHEMA}].[semantic_refresh_mssql_session_outcomes] (
    operation_id nvarchar(512) NOT NULL PRIMARY KEY,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    fencing_epoch bigint NOT NULL,
    transaction_disposition nvarchar(32) NOT NULL,
    controller_proves_not_invoked bit NOT NULL,
    evidence_sha256 varchar(71) NOT NULL
);
CREATE TABLE [{_MACRO_SCHEMA}].[semantic_refresh_ddl_epoch] (
    singleton_id tinyint NOT NULL PRIMARY KEY,
    current_epoch bigint NOT NULL
);
INSERT INTO [{_MACRO_SCHEMA}].[semantic_refresh_ddl_epoch] VALUES (1, 1);
"""
        )
    finally:
        cursor.close()


def _drop_macro_schema(connection: Any) -> None:
    cursor = connection.cursor()
    try:
        if cursor.execute(f"SELECT SCHEMA_ID(N'{_MACRO_SCHEMA}')").fetchone()[0] is None:
            return
        rows = cursor.execute(
            "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID(?) ORDER BY name DESC",
            _MACRO_SCHEMA,
        ).fetchall()
        for row in rows:
            identifier = str(row[0]).replace("]", "]]")
            cursor.execute(f"DROP TABLE [{_MACRO_SCHEMA}].[{identifier}]")
        cursor.execute(f"DROP SCHEMA [{_MACRO_SCHEMA}]")
    finally:
        cursor.close()


def _drop_schema(connection: Any) -> None:
    _drop_named_schema(connection, _SCHEMA)
    return


def _drop_schema_legacy_fixture(connection: Any) -> None:
    """Retained table list for the former partial live schema."""

    cursor = connection.cursor()
    try:
        for table in (
            "semantic_refresh_journals",
            "semantic_refresh_canonical_authorities",
            "semantic_refresh_checkpoints",
            "semantic_refresh_scope_heads",
            "semantic_refresh_target_heads",
            "semantic_refresh_baselines",
            "semantic_refresh_reservations",
            "semantic_refresh_guards",
            "semantic_refresh_target_owners",
            "semantic_refresh_workflow_executions",
        ):
            cursor.execute(f"DROP TABLE IF EXISTS [{_SCHEMA}].[{table}]")
        cursor.execute(f"IF SCHEMA_ID(N'{_SCHEMA}') IS NOT NULL DROP SCHEMA [{_SCHEMA}]")
    finally:
        cursor.close()


def _drop_named_schema(connection: Any, schema: str) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TRIGGER IF EXISTS [dpone_semantic_refresh_ddl_epoch_guard_{schema}] ON DATABASE")
        if cursor.execute("SELECT SCHEMA_ID(?)", schema).fetchone()[0] is None:
            return
        rows = cursor.execute(
            "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID(?) ORDER BY name DESC",
            schema,
        ).fetchall()
        for row in rows:
            identifier = str(row[0]).replace("]", "]]")
            cursor.execute(f"DROP TABLE [{schema}].[{identifier}]")
        cursor.execute(f"DROP SCHEMA [{schema}]")
    finally:
        cursor.close()


def _scalar(connection: Any, sql: str) -> object:
    cursor = connection.cursor()
    try:
        row = cursor.execute(sql).fetchone()
        assert row is not None
        return row[0]
    finally:
        cursor.close()


def _one(connection: Any, sql: str) -> tuple[object, ...]:
    cursor = connection.cursor()
    try:
        row = cursor.execute(sql).fetchone()
        assert row is not None
        return tuple(row)
    finally:
        cursor.close()


def _workflow_summary(
    *,
    workflow_execution_id: str,
    operation_id: str,
    operation_plan: str,
    workflow_plan: str,
    execution: str,
    attempt: str,
    artifact_manifest: str,
    clickhouse_terminal_receipt: str,
    terminal_receipt: str,
    target_generation: int,
    scope_revision: int,
) -> dict[str, object]:
    publication = SemanticRefreshDurableModelPublication(
        operation_id=operation_id,
        operation_plan_sha256=operation_plan,
        workflow_execution_binding_sha256=execution,
        attempt_binding_sha256=attempt,
        artifact_manifest_sha256=artifact_manifest,
        clickhouse_terminal_receipt_sha256=clickhouse_terminal_receipt,
        terminal_receipt_sha256=terminal_receipt,
        target_generation=target_generation,
        scope_revision=scope_revision,
    )
    return SemanticRefreshDurableWorkflowSummary.build(
        workflow_execution_id=workflow_execution_id,
        workflow_plan_sha256=workflow_plan,
        workflow_execution_binding_sha256=execution,
        expected_operation_ids=(operation_id,),
        publications=(publication,),
    ).to_dict()


def _workflow_failure_summary(
    *,
    workflow_plan: str,
    execution: str,
    attempt: str,
) -> dict[str, object]:
    unsigned: dict[str, object] = {
        "schema": "dpone.semantic-refresh-failed-workflow-summary.v1",
        "workflow_id": "failed-workflow-live",
        "workflow_plan_sha256": workflow_plan,
        "workflow_execution_binding_sha256": execution,
        "expected_operation_ids": [_FAILED_OPERATION_ID],
        "models": [
            {
                "operation_id": _FAILED_OPERATION_ID,
                "attempt_binding_sha256": attempt,
                "mssql_outcome": "COMMITTED_WITH_IMAGES",
                "mssql_evidence_sha256": "sha256:" + "4" * 64,
            }
        ],
        "status": "FAILED_PRE_COMMIT",
    }
    raw = json.dumps(unsigned, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return {**unsigned, "terminal_summary_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()}


def _seal_receipt(bundle: Any, *, serializer: str) -> SemanticRefreshSealAuthorizationReceipt:
    operation = bundle.operation_plans[0]
    attempt = bundle.attempt_bindings[0]
    resource = bundle.model_resources[0]
    return SemanticRefreshSealAuthorizationReceipt.build(
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        model_unique_id=operation.model_unique_id,
        workflow_id=operation.workflow_id,
        workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
        workflow_execution_id=bundle.workflow_execution_id,
        workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
        attempt_binding_sha256=attempt.attempt_binding_sha256,
        fencing_epoch=attempt.fencing_epoch,
        journal_version=1,
        event_time_source_type="date",
        effective_key_template_sha256=operation.effective_key_template_sha256,
        effective_key_mapping_sha256=operation.effective_key_mapping_sha256,
        ordered_writable_schema_sha256=resource.writable_schema_sha256,
        serializer_sha256="sha256:" + serializer * 64,
        parquet_schema_mapping_sha256="sha256:" + "2" * 64,
        clickhouse_input_mapping_sha256="sha256:" + "3" * 64,
        codec_mapping_certification_sha256="sha256:" + "4" * 64,
        before_image_relation_id="[DWH].[dpone_scope_images].[before]",
        before_image_sha256="sha256:" + "5" * 64,
        before_image_row_count=1,
        after_image_relation_id="[DWH].[dpone_scope_images].[after]",
        after_image_sha256="sha256:" + "6" * 64,
        after_image_row_count=1,
        model_build_receipt_sha256="sha256:" + "7" * 64,
        baseline_adoption_receipt_sha256=resource.baseline_receipt_sha256,
        route_certification_receipt_sha256=resource.route_certification_receipt_sha256,
        writer_exclusivity_assurance_receipt_sha256=(resource.writer_exclusivity_assurance_receipt_sha256),
        utc_semantics_assurance_receipt_sha256=None,
        ddl_freeze_assurance_receipt_sha256=resource.ddl_freeze_assurance_receipt_sha256,
        artifact_authority_sha256=semantic_refresh_artifact_authority_sha256(asdict(resource.artifact_authority)),
        seal_policy_sha256="sha256:" + "9" * 64,
        created_at="2026-08-08T12:00:00Z",
        issuer_authority="vault://semantic-refresh/seal-policy/v1",
        issuer_attestation_sha256="sha256:" + "a" * 64,
        issuer_signature_sha256="sha256:" + "b" * 64,
    )


class _LocalSealPolicyAuthority:
    """Test-only protected policy stand-in; Vault resolution has a separate live profile."""

    def load(
        self,
        subject: SemanticRefreshSealPolicySubject,
    ) -> SemanticRefreshSealPolicyAuthority:
        return SemanticRefreshSealPolicyAuthority.build(
            subject=subject,
            clickhouse_input_mapping_sha256="sha256:" + "1" * 64,
            codec_mapping_certification_sha256="sha256:" + "2" * 64,
            seal_policy_sha256="sha256:" + "3" * 64,
            issuer_authority="local-vault-policy-profile",
            issuer_attestation_sha256="sha256:" + "4" * 64,
            issuer_signature_sha256="sha256:" + "5" * 64,
        )


def _install_committed_image_evidence(
    connection: Any,
    bundle: Any,
) -> MssqlBuildReceiptEvidence:
    operation = bundle.operation_plans[0]
    attempt = bundle.attempt_bindings[0]
    journal = compose_admission(bundle).journals[0]
    strategy = json.loads(journal.strategy_authority_json)
    before_relation = _strategy_relation(strategy["before_image_relation"])
    after_relation = _strategy_relation(strategy["after_image_relation"])
    _execute(
        connection,
        "IF NOT EXISTS (SELECT 1 FROM [DWH].sys.schemas WHERE name = N'dpone_scope_images') "
        "EXEC [DWH].sys.sp_executesql N'CREATE SCHEMA [dpone_scope_images]';",
    )
    _execute(
        connection,
        f"""
DROP TABLE IF EXISTS {before_relation};
DROP TABLE IF EXISTS {after_relation};
CREATE TABLE {before_relation} (
    event_date date NOT NULL,
    event_id bigint NOT NULL,
    payload nvarchar(200) NULL
);
CREATE TABLE {after_relation} (
    event_date date NOT NULL,
    event_id bigint NOT NULL,
    payload nvarchar(200) NULL
);
INSERT INTO {before_relation} VALUES ('2026-08-07', 1, N'before');
INSERT INTO {after_relation} VALUES ('2026-08-07', 1, N'after');
""".strip(),
    )
    before_sha256 = _live_image_sha256(connection, before_relation)
    after_sha256 = _live_image_sha256(connection, after_relation)
    receipt = MssqlBuildReceiptEvidence.build_exact(
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        attempt_binding_sha256=attempt.attempt_binding_sha256,
        fencing_epoch=attempt.fencing_epoch,
        before_image_sha256=before_sha256,
        after_image_sha256=after_sha256,
        inserted_count=0,
        updated_count=1,
        model_unique_id=operation.model_unique_id,
        strategy_authority_sha256=journal.strategy_authority_sha256,
        before_image_relation=before_relation,
        after_image_relation=after_relation,
    )
    _execute_params(
        connection,
        f"""
INSERT INTO [{_SCHEMA}].[semantic_refresh_receipts] (
    operation_id, operation_plan_sha256, attempt_binding_sha256, fencing_epoch,
    before_image_relation, before_image_sha256, after_image_relation, after_image_sha256,
    updated_count, inserted_count, build_receipt_sha256
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""".strip(),
        receipt.operation_id,
        receipt.operation_plan_sha256,
        receipt.attempt_binding_sha256,
        receipt.fencing_epoch,
        receipt.before_image_relation,
        receipt.before_image_sha256,
        receipt.after_image_relation,
        receipt.after_image_sha256,
        receipt.updated_count,
        receipt.inserted_count,
        receipt.build_receipt_sha256,
    )
    return receipt


def _strategy_relation(value: object) -> str:
    if not isinstance(value, dict) or set(value) != {"database", "schema", "identifier"}:
        raise AssertionError("strategy relation is not closed")
    parts = tuple(str(value[field]) for field in ("database", "schema", "identifier"))
    if any(not part.replace("_", "a").isalnum() or not part[0].isalpha() for part in parts):
        raise AssertionError("strategy relation is outside the live-test identifier subset")
    return ".".join(f"[{part}]" for part in parts)


def _live_image_sha256(connection: Any, relation: str) -> str:
    return str(
        _scalar(
            connection,
            f"""
DECLARE @dpone_image_json nvarchar(max);
SELECT @dpone_image_json = (
    SELECT [event_date], [event_id], [payload]
    FROM {relation}
    ORDER BY [event_date], [event_id]
    FOR JSON PATH, INCLUDE_NULL_VALUES
);
SELECT 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES(
    'SHA2_256', COALESCE(@dpone_image_json, N'[]')
), 2));
""".strip(),
        )
    )


def _drop_live_image_tables(connection: Any) -> None:
    if _scalar(connection, "SELECT DB_ID(N'DWH')") is None:
        return
    bundle = _bundle()
    strategy = json.loads(compose_admission(bundle).journals[0].strategy_authority_json)
    before_relation = _strategy_relation(strategy["before_image_relation"])
    after_relation = _strategy_relation(strategy["after_image_relation"])
    _execute(
        connection,
        f"DROP TABLE IF EXISTS {before_relation}; DROP TABLE IF EXISTS {after_relation};",
    )


def _prepared_documents(
    bundle: Any,
    *,
    target_uuid: str,
    empty_scope: bool = False,
    deterministic_scratch: bool = False,
) -> dict[str, object]:
    operation = bundle.operation_plans[0]
    attempt = bundle.attempt_bindings[0]
    resource = bundle.model_resources[0]
    business_columns = tuple(column.name for column in resource.writable_columns)
    effective_keys = tuple(
        column.name
        for column in resource.writable_columns
        if column.writable_role in {"EFFECTIVE_KEY", "EFFECTIVE_KEY_EVENT_TIME"}
    )
    event_time_column = next(
        column.name for column in resource.writable_columns if column.writable_role == "EFFECTIVE_KEY_EVENT_TIME"
    )
    staging_table, shadow_table = (
        clickhouse_operation_table_names(
            resource.publication_target_table,
            operation.operation_id,
        )
        if deterministic_scratch
        else ("events_dpone_stage_live", "events_dpone_shadow_live")
    )
    plan = ClickHousePreparePlan(
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
        workflow_execution_id=bundle.workflow_execution_id,
        workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
        attempt_binding_sha256=attempt.attempt_binding_sha256,
        fence_epoch=attempt.fencing_epoch,
        artifact_manifest_key="semantic-refresh/live/manifest.json",
        artifact_manifest_version="v1",
        artifact_manifest_sha256="sha256:" + "b" * 64,
        target_resource_id=resource.target_resource_id,
        target_authority_id=resource.target_authority_id,
        clickhouse_cluster_authority_id=resource.clickhouse_cluster_authority_id,
        database=resource.publication_database,
        target_table=resource.publication_target_table,
        scope_id=resource.publication_scope_id,
        scope_start=operation.scope_start,
        scope_end=operation.scope_end,
        scope_revision=operation.scope_revision,
        event_time_column=event_time_column,
        staging_table=staging_table,
        shadow_table=shadow_table,
        expected_target_uuid=target_uuid,
        expected_schema_sha256=resource.writable_schema_sha256,
        expected_physical_sha256=resource.model_definition_proof_sha256,
        business_columns=business_columns,
        effective_key_columns=effective_keys,
        max_staging_rows=resource.resource_policy.max_after_image_rows,
        max_target_scope_rows=resource.resource_policy.max_target_scope_rows,
        max_staging_bytes=resource.resource_policy.max_clickhouse_staging_bytes,
        max_shadow_bytes=resource.resource_policy.max_clickhouse_shadow_bytes,
        max_retained_backup_bytes=(resource.resource_policy.max_clickhouse_retained_backup_bytes),
        max_total_transient_bytes=(resource.resource_policy.max_clickhouse_total_transient_bytes),
        shadow_equation=ShadowEquation(
            retained_target_rule="TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY",
            append_rule="APPEND_ALL_STAGING_ROWS",
        ),
        conformance=GroupedMultisetConformance(mode="BIDIRECTIONAL_GROUPED_MULTISET"),
        artifact_chunk_count=0 if empty_scope else 1,
        artifact_total_rows=0 if empty_scope else 1,
    )
    unsigned = {
        "operation_id": plan.operation_id,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "prepare_plan_sha256": plan.sha256,
        "status": "PREPARED",
        "target_uuid": target_uuid,
        "staging_uuid": None if empty_scope else "00000000-0000-0000-0000-000000000003",
        "shadow_uuid": None if empty_scope else "00000000-0000-0000-0000-000000000004",
        "staging_rows": 0 if empty_scope else 1,
        "shadow_rows": 0 if empty_scope else 1,
        "desired_rows": 0 if empty_scope else 1,
        "forward_difference_groups": 0,
        "reverse_difference_groups": 0,
        "shadow_equation": plan.shadow_equation.to_mapping(),
        "conformance_mode": "NOT_APPLICABLE_NO_DATA" if empty_scope else plan.conformance.mode,
        "publication_mode": "EMPTY_SCOPE" if empty_scope else "EXCHANGE",
    }
    receipt = ClickHousePreparedReceipt(
        **unsigned,
        receipt_sha256=semantic_refresh_fingerprint(unsigned),
    )
    return prepared_publication_documents(plan, receipt)
