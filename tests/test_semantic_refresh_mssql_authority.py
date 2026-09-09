"""Protected canonical-authority composition for MSSQL workflow admission."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, fields, replace
from pathlib import Path

import pytest

from dpone.adapters.semantic_refresh_mssql_activation import (
    MssqlSemanticRefreshActivationStore,
    SemanticRefreshMssqlActivationError,
)
from dpone.adapters.semantic_refresh_mssql_activation_authority import (
    MssqlSemanticRefreshActivationAuthorityStore,
    SemanticRefreshMssqlActivationAuthorityError,
)
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
    SemanticRefreshActivationAuthoritySet,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshDeploymentAuthoritySubject,
)
from dpone.contracts.semantic_refresh_attempt_binding import SemanticRefreshAttemptBinding
from dpone.contracts.semantic_refresh_baseline_receipt import (
    SemanticRefreshBaselineAdoptionReceipt,
)
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError, semantic_refresh_sha256
from dpone.contracts.semantic_refresh_execution_binding import (
    SemanticRefreshWorkflowExecutionBinding,
)
from dpone.contracts.semantic_refresh_failure_summary import (
    SemanticRefreshFailedWorkflowSummary,
)
from dpone.contracts.semantic_refresh_operation_plan import SemanticRefreshOperationPlan
from dpone.contracts.semantic_refresh_workflow_plan import SemanticRefreshWorkflowPlan
from dpone.contracts.semantic_refresh_workflow_replacement import (
    SemanticRefreshWorkflowReplacementPlan,
)
from dpone.ports.semantic_refresh_mssql import (
    MssqlAdmissionReceipt,
    MssqlAdmissionRequest,
    MssqlGuardClaim,
    mssql_guard_set_sha256,
    mssql_journal_set_sha256,
)
from dpone.ports.semantic_refresh_mssql_activation import (
    MssqlActivatedPackRegistration,
    MssqlDeploymentActivationRequest,
    MssqlRunAuthorityRegistration,
)
from dpone.ports.semantic_refresh_mssql_admission_authority import compose_admission
from dpone.ports.semantic_refresh_mssql_authority import (
    MssqlCanonicalAuthorityRecord,
    MssqlProtectedOperationStateRecord,
    MssqlProtectedResourcePolicy,
)
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlPersistedModelOutcome,
    MssqlPredecessorFailureState,
)
from dpone.ports.semantic_refresh_mssql_worker_admission import (
    mssql_workflow_resource_budget,
)
from dpone.ports.semantic_refresh_mssql_worker_pack import (
    MssqlStaticProjectionIdentity,
    mssql_static_projection_identity_json,
    mssql_worker_pack_fingerprint,
)
from dpone.ports.semantic_refresh_production_activation import (
    SemanticRefreshProductionActivationUnavailableError,
)
from dpone.runtime.dbt_semantic_refresh_run_authority import (
    SemanticRefreshDbtStaticProjectionIdentity,
    semantic_refresh_worker_pack_fingerprint,
)
from dpone.services.semantic_refresh_mssql_activation import (
    SemanticRefreshMssqlActivationService,
)
from dpone.services.semantic_refresh_mssql_authority import (
    MssqlCanonicalAdmissionBundle,
    SemanticRefreshMssqlAuthorityAdmissionService,
    SemanticRefreshMssqlPlanScopeMapLoader,
    SemanticRefreshMssqlProtectedAuthorityService,
    SemanticRefreshMssqlScopeMapService,
    mssql_model_resource_authority_from_plan_target,
    semantic_refresh_mssql_authority_json,
)
from dpone.services.semantic_refresh_mssql_replacement import (
    SemanticRefreshMssqlReplacementService,
)
from tests.test_dbt_semantic_refresh_plan_compiler import (
    _authority as _deployment_authority,
)
from tests.test_dbt_semantic_refresh_plan_compiler import (
    _deployment_model,
    _plan_bundle,
    _route_receipt,
    _run_bundle,
    _runtime_assurances,
)

_GOLDEN = Path("tests/fixtures/semantic-refresh-v2/contracts/golden-v1.json")


def _documents() -> tuple[
    SemanticRefreshOperationPlan,
    SemanticRefreshWorkflowPlan,
    SemanticRefreshWorkflowExecutionBinding,
    SemanticRefreshAttemptBinding,
    SemanticRefreshWorkflowReplacementPlan,
]:
    raw = json.loads(_GOLDEN.read_text(encoding="utf-8"))["documents"]
    return (
        SemanticRefreshOperationPlan.from_mapping(raw["dpone.semantic-refresh-operation-plan.v1"]),
        SemanticRefreshWorkflowPlan.from_mapping(raw["dpone.semantic-refresh-workflow-plan.v1"]),
        SemanticRefreshWorkflowExecutionBinding.from_mapping(
            raw["dpone.semantic-refresh-workflow-execution-binding.v1"]
        ),
        SemanticRefreshAttemptBinding.from_mapping(raw["dpone.semantic-refresh-attempt-binding.v1"]),
        SemanticRefreshWorkflowReplacementPlan.from_mapping(raw["dpone.semantic-refresh-workflow-replacement-plan.v1"]),
    )


def _bundle() -> MssqlCanonicalAdmissionBundle:
    plan = _plan_bundle()
    operation = plan.operation_plans[0]
    workflow = plan.workflow_plan
    execution = _run_bundle(plan).workflow_execution_binding
    attempt = SemanticRefreshAttemptBinding.build(
        workflow_execution_id=execution.workflow_execution_id,
        workflow_execution_binding_sha256=(execution.workflow_execution_binding_sha256),
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        dag_run_id=execution.workflow_execution_id,
        task_id="dbt-build",
        try_number=1,
        pod_uid="00000000-0000-0000-0000-000000000777",
        fencing_epoch=1,
        owner_id="airflow-owner",
    )
    target_plan = plan.targets[0]
    target = MssqlGuardClaim(
        target_plan.target_resource_id,
        attempt.fencing_epoch - 1,
        attempt.fencing_epoch,
    )
    return MssqlCanonicalAdmissionBundle(
        workflow_execution_id=execution.workflow_execution_id,
        workflow_plan=workflow,
        execution_binding=execution,
        operation_plans=(operation,),
        attempt_bindings=(attempt,),
        workflow_guard=MssqlGuardClaim(
            plan.run_guard_closure.workflow_guard_resource_id,
            0,
            1,
        ),
        resource_guards=(target,),
        model_resources=(mssql_model_resource_authority_from_plan_target(target_plan),),
        controller_id="airflow-controller",
        owner_id=attempt.owner_id,
        reservation_id="reservation-2026-08-07",
        resource_budget=mssql_workflow_resource_budget(plan),
        replacement_plan=None,
    )


def _resource_policy() -> MssqlProtectedResourcePolicy:
    values = {
        field_name: 1_000_000
        for field_name in MssqlProtectedResourcePolicy.__dataclass_fields__
        if field_name != "resource_policy_sha256"
    }
    digest = semantic_refresh_sha256({"schema": "dpone.dbt-semantic-refresh-resource-policy.v1", **values})
    return MssqlProtectedResourcePolicy(**values, resource_policy_sha256=digest)


def _model_baseline(operation: SemanticRefreshOperationPlan) -> SemanticRefreshBaselineAdoptionReceipt:
    baseline = _deployment_model().baseline_receipt
    excluded = {
        "baseline_adoption_receipt_sha256",
        "historical_target_multiset_equivalence",
        "schema",
        "source_to_staging_canonical_value_equivalence",
        "status",
        "historical_clickhouse_internal_multiset_conformance",
        "historical_cross_engine_payload_value_equivalence",
    }
    values = {item.name: getattr(baseline, item.name) for item in fields(baseline) if item.name not in excluded}
    values.update(
        model_unique_id=operation.model_unique_id,
        release_id=operation.release_id,
        deployment_id=operation.deployment_id,
        mssql_relation_id="warehouse.dbo.events",
        mssql_connection_authority_id="mssql-prod",
        mssql_target_authority_id="mssql://mssql-prod/warehouse/dbo.events",
        clickhouse_relation_id="analytics.events",
        clickhouse_cluster_authority_id="clickhouse-prod",
        clickhouse_target_authority_id="clickhouse://clickhouse-prod/analytics/events",
        clickhouse_target_uuid="00000000-0000-0000-0000-000000000001",
    )
    return SemanticRefreshBaselineAdoptionReceipt.build(**values)


@dataclass
class _ProtectedAuthority:
    record: MssqlCanonicalAuthorityRecord
    requested: list[str] = field(default_factory=list)

    def load(self, workflow_execution_binding_sha256: str) -> MssqlCanonicalAuthorityRecord:
        self.requested.append(workflow_execution_binding_sha256)
        return self.record


@dataclass(frozen=True)
class _ProtectedOperationState:
    record: MssqlProtectedOperationStateRecord

    def load_operation_state(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedOperationStateRecord:
        assert workflow_execution_binding_sha256 == self.record.authority.workflow_execution_binding_sha256
        return self.record


@dataclass(frozen=True)
class _PredecessorState:
    state: MssqlPredecessorFailureState

    def load_predecessor(self, workflow_id: str) -> MssqlPredecessorFailureState:
        assert workflow_id == self.state.workflow_id
        return self.state


class _ProtectedPlanState:
    def __init__(self, *states: MssqlProtectedOperationStateRecord) -> None:
        self.states = states

    def load_plan_states(
        self,
        *,
        workflow_execution_binding_sha256: str,
    ) -> tuple[MssqlProtectedOperationStateRecord, ...]:
        assert all(
            item.authority.workflow_execution_binding_sha256 == workflow_execution_binding_sha256
            for item in self.states
        )
        return self.states


@dataclass
class _RecordingAdmission:
    requests: list[MssqlAdmissionRequest] = field(default_factory=list)

    def admit(self, request: MssqlAdmissionRequest) -> MssqlAdmissionReceipt:
        self.requests.append(request)
        return MssqlAdmissionReceipt(
            workflow_id=request.workflow_id,
            reservation_id=request.reservation_id,
            guards=(request.workflow_guard, *request.resource_guards),
            preparing_operation_ids=tuple(item.operation_id for item in request.journals),
        )


@dataclass
class _RecordingActivation:
    deployments: list[MssqlDeploymentActivationRequest] = field(default_factory=list)
    runs: list[MssqlRunAuthorityRegistration] = field(default_factory=list)

    def activate_deployment(self, request: MssqlDeploymentActivationRequest) -> None:
        self.deployments.append(request)

    def register_run_authority(self, request: MssqlRunAuthorityRegistration) -> None:
        self.runs.append(request)


class _ExactDeploymentVerifier:
    def verify(self, subject: SemanticRefreshDeploymentAuthoritySubject) -> bool:
        return subject == SemanticRefreshDeploymentAuthoritySubject.build(
            _deployment_authority(),
            (_deployment_model(),),
        )


class _AllowLocalActivation:
    def authorize(self, **_: object) -> None:
        return None


class _RejectProductionActivation:
    def authorize(self, **_: object) -> None:
        raise RuntimeError("production activation unavailable")


class _AuthorityStoreCursor:
    def __init__(
        self,
        *,
        conflict: bool = False,
        activation_row: tuple[object, ...] | None = None,
    ) -> None:
        self.conflict = conflict
        self.activation_row = activation_row
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self._row: tuple[object, ...] | None = None

    def execute(self, sql: str, *parameters: object) -> _AuthorityStoreCursor:
        self.executions.append((sql, tuple(parameters)))
        if "sp_getapplock" in sql:
            self._row = (0,)
        elif self.conflict and "SELECT model_unique_id" in sql:
            self._row = ("wrong-model", "wrong-kind", "bad", "{}", "COMPLETE", True)
        elif "SELECT deployment_id, release_id, plan_bundle_sha256" in sql:
            self._row = self.activation_row
        else:
            self._row = None
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        row = self._row
        self._row = None
        return row

    def close(self) -> None:
        return None


class _AuthorityStoreConnection:
    def __init__(self, cursor: _AuthorityStoreCursor) -> None:
        self.autocommit = True
        self.cursor_instance = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _AuthorityStoreCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


class _StatefulAuthorityStoreCursor(_AuthorityStoreCursor):
    _KEY_LENGTHS = {
        "semantic_refresh_activation_authorities": 2,
        "semantic_refresh_baselines": 1,
        "semantic_refresh_route_authorities": 1,
        "semantic_refresh_runtime_assurance_authorities": 3,
    }

    def __init__(self, rows: dict[str, dict[tuple[object, ...], tuple[object, ...]]]) -> None:
        super().__init__()
        self.rows = rows

    def execute(self, sql: str, *parameters: object) -> _StatefulAuthorityStoreCursor:
        super().execute(sql, *parameters)
        table = next((name for name in self._KEY_LENGTHS if f"[{name}]" in sql), None)
        if table is None:
            return self
        if "SELECT TOP (1) release_id, authority_store_ref, status" in sql:
            deployment_id, release_id, authority_store_ref = parameters
            self._row = next(
                (
                    (stored[0], stored[1], stored[-1])
                    for key, stored in self.rows.get(table, {}).items()
                    if key[0] == deployment_id
                    and (stored[0], stored[1], stored[-1]) != (release_id, authority_store_ref, "ACTIVE")
                ),
                None,
            )
            return self
        key_length = self._KEY_LENGTHS[table]
        key = tuple(parameters[:key_length])
        if sql.startswith("SELECT"):
            stored = self.rows.get(table, {}).get(key)
            if (
                stored is not None
                and table == "semantic_refresh_activation_authorities"
                and ("SELECT deployment_id, release_id, plan_bundle_sha256" in sql)
            ):
                self._row = (key[0], stored[0], key[1], *stored[1:])
            else:
                self._row = stored
        elif sql.startswith("INSERT INTO"):
            self.rows.setdefault(table, {})[key] = tuple(parameters[key_length:])
            self._row = None
        return self


class _StatefulAuthorityStoreFactory:
    def __init__(self) -> None:
        self.rows: dict[str, dict[tuple[object, ...], tuple[object, ...]]] = {}
        self.connections: list[_AuthorityStoreConnection] = []

    def __call__(self) -> _AuthorityStoreConnection:
        connection = _AuthorityStoreConnection(_StatefulAuthorityStoreCursor(self.rows))
        self.connections.append(connection)
        return connection


class _ActivatedPackCursor:
    def __init__(
        self,
        *,
        primary_row: tuple[object, ...] | None = None,
        binding_row: tuple[object, ...] | None = None,
    ) -> None:
        self.primary_row = primary_row
        self.binding_row = binding_row
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self._row: tuple[object, ...] | None = None
        self.rowcount = 0

    def execute(self, sql: str, *parameters: object) -> _ActivatedPackCursor:
        self.executions.append((sql, tuple(parameters)))
        if "sp_getapplock" in sql:
            self._row = (0,)
        elif "WHERE pack_fingerprint = ?" in sql:
            self._row = self.primary_row
        elif "WHERE workflow_execution_binding_sha256 = ?" in sql:
            self._row = self.binding_row
        elif "semantic_refresh_guards" in sql and sql.startswith("SELECT fencing_epoch"):
            self._row = (0, None, None, None, "AVAILABLE")
        else:
            self._row = None
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row

    def close(self) -> None:
        return None


class _ActivatedPackConnection:
    def __init__(self, cursor: _ActivatedPackCursor) -> None:
        self.autocommit = True
        self.cursor_instance = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _ActivatedPackCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


def _activated_pack() -> MssqlActivatedPackRegistration:
    closure = _plan_bundle().run_guard_closure
    projection = _static_projection_identity(
        plan_bundle_sha256="sha256:" + "5" * 64,
        workflow_plan_sha256="sha256:" + "4" * 64,
    )
    fingerprint = mssql_worker_pack_fingerprint(
        projection_identity=projection,
        run_execution_bundle_sha256="sha256:" + "6" * 64,
        activation_authority_receipt_sha256="sha256:" + "2" * 64,
        authority_store_ref="mssql-control://prod/DWH/dpone_control",
        run_guard_closure_sha256=closure.run_guard_closure_sha256,
    )
    return MssqlActivatedPackRegistration(
        pack_fingerprint=fingerprint,
        activation_authority_receipt_sha256="sha256:" + "2" * 64,
        authority_store_ref="mssql-control://prod/DWH/dpone_control",
        workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
        workflow_execution_binding_sha256="sha256:" + "3" * 64,
        workflow_plan_sha256="sha256:" + "4" * 64,
        plan_bundle_sha256="sha256:" + "5" * 64,
        run_execution_bundle_sha256="sha256:" + "6" * 64,
        run_guard_closure=closure,
        projection_identity=projection,
    )


def _static_projection_identity(
    *,
    plan_bundle_sha256: str,
    workflow_plan_sha256: str,
) -> MssqlStaticProjectionIdentity:
    return MssqlStaticProjectionIdentity(
        dag_projection_sha256="sha256:" + "7" * 64,
        deployment_id="sha256:" + "8" * 64,
        package_artifacts_sha256="sha256:" + "9" * 64,
        plan_bundle_sha256=plan_bundle_sha256,
        pre_release_bundle_sha256="sha256:" + "a" * 64,
        release_id="sha256:" + "b" * 64,
        template_pack_fingerprint="sha256:" + "c" * 64,
        topology_sha256="sha256:" + "d" * 64,
        workflow_plan_sha256=workflow_plan_sha256,
    )


def test_protected_authority_derives_guard_and_journal_closure() -> None:
    bundle = _bundle()
    authority = _ProtectedAuthority(_record(bundle))
    admission = _RecordingAdmission()

    receipt = SemanticRefreshMssqlAuthorityAdmissionService(
        authority=authority,
        admission=admission,
    ).admit(bundle.execution_binding.workflow_execution_binding_sha256)

    assert authority.requested == [bundle.execution_binding.workflow_execution_binding_sha256]
    request = admission.requests[0]
    assert request.expected_guard_set_sha256 == mssql_guard_set_sha256(
        bundle.workflow_guard,
        bundle.resource_guards,
    )
    assert request.expected_journal_set_sha256 == mssql_journal_set_sha256(request.journals)
    assert request.journals[0].attempt_binding_sha256 == bundle.attempt_bindings[0].attempt_binding_sha256
    assert request.journals[0].baseline_receipt_sha256 == bundle.model_resources[0].baseline_receipt_sha256
    assert request.journals[0].baseline_status == "COMPLETE"
    assert receipt.preparing_operation_ids == (bundle.operation_plans[0].operation_id,)


def test_canonical_admission_bundle_uses_plan_guard_closure() -> None:
    bundle = _bundle()
    closure = _plan_bundle().run_guard_closure

    assert bundle.workflow_guard.resource_id == closure.workflow_guard_resource_id
    assert tuple(item.resource_id for item in bundle.resource_guards) == closure.resource_guard_ids
    assert bundle.resource_budget == mssql_workflow_resource_budget(_plan_bundle())


def test_activation_service_derives_create_only_deployment_and_run_state() -> None:
    activation = _RecordingActivation()
    service = SemanticRefreshMssqlActivationService(
        activation,
        _ExactDeploymentVerifier(),
        _AllowLocalActivation(),
    )
    subject = SemanticRefreshDeploymentAuthoritySubject.build(
        _deployment_authority(),
        (_deployment_model(),),
    )
    service.activate_deployment(subject=subject, plan_bundle=_plan_bundle())
    bundle = _bundle()
    service.register_run(bundle)

    deployment = activation.deployments[0]
    assert deployment.deployment_subject_sha256 == subject.subject_sha256
    assert deployment.baselines[0].baseline_receipt_sha256 == (
        _deployment_model().baseline_receipt.baseline_adoption_receipt_sha256
    )
    assert deployment.target_heads[0].target_generation == _deployment_model().baseline_receipt.clickhouse_generation
    assert deployment.target_heads[0].baseline_operation_id == (
        _deployment_model().baseline_receipt.baseline_adoption_receipt_sha256
    )
    run = activation.runs[0]
    assert run.record.authority_sha256 == _bundle().authority_sha256
    assert run.guard_epochs == tuple(
        sorted(
            (
                (bundle.workflow_guard.resource_id, bundle.workflow_guard.expected_predecessor_epoch),
                (
                    bundle.resource_guards[0].resource_id,
                    bundle.resource_guards[0].expected_predecessor_epoch,
                ),
            )
        )
    )


def test_activation_service_requires_guard_before_physical_mutation() -> None:
    activation = _RecordingActivation()
    service = SemanticRefreshMssqlActivationService(
        activation,
        _ExactDeploymentVerifier(),
        _RejectProductionActivation(),
    )

    with pytest.raises(RuntimeError, match="production activation unavailable"):
        service.activate_deployment(
            subject=SemanticRefreshDeploymentAuthoritySubject.build(
                _deployment_authority(),
                (_deployment_model(),),
            ),
            plan_bundle=_plan_bundle(),
        )

    assert activation.deployments == []


def test_activation_service_two_argument_api_defaults_to_preview_block() -> None:
    activation = _RecordingActivation()
    service = SemanticRefreshMssqlActivationService(
        activation,
        _ExactDeploymentVerifier(),
    )

    assert service.activation is activation
    assert isinstance(service.deployment_verifier, _ExactDeploymentVerifier)

    with pytest.raises(
        SemanticRefreshProductionActivationUnavailableError,
        match="DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE",
    ):
        service.activate_deployment(
            subject=SemanticRefreshDeploymentAuthoritySubject.build(
                _deployment_authority(),
                (_deployment_model(),),
            ),
            plan_bundle=_plan_bundle(),
        )

    assert activation.deployments == []


def test_activation_service_blocks_run_registration_before_mutation() -> None:
    activation = _RecordingActivation()
    service = SemanticRefreshMssqlActivationService(
        activation,
        _ExactDeploymentVerifier(),
    )

    with pytest.raises(
        SemanticRefreshProductionActivationUnavailableError,
        match="DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE",
    ):
        service.register_run(_bundle())

    assert activation.runs == []


def test_activated_pack_registration_is_create_once_and_replay_exact() -> None:
    request = _activated_pack()
    created = _ActivatedPackConnection(_ActivatedPackCursor())

    MssqlSemanticRefreshActivationStore(lambda: created).register_activated_pack(request)

    inserts = [sql for sql, _ in created.cursor_instance.executions if sql.startswith("INSERT INTO")]
    assert len(inserts) == 1
    assert "semantic_refresh_activated_packs" in inserts[0]
    assert created.commits == 1
    expected = (
        request.activation_authority_receipt_sha256,
        request.authority_store_ref,
        request.workflow_execution_id,
        request.workflow_execution_binding_sha256,
        request.workflow_plan_sha256,
        request.plan_bundle_sha256,
        request.run_execution_bundle_sha256,
        request.run_guard_closure.run_guard_closure_sha256,
        request.run_guard_closure.workflow_guard_resource_id,
        json.dumps(
            request.run_guard_closure.resource_guard_ids,
            ensure_ascii=True,
            separators=(",", ":"),
        ),
        mssql_static_projection_identity_json(request.projection_identity),
        "ACTIVE",
    )
    replay = _ActivatedPackConnection(_ActivatedPackCursor(primary_row=expected))

    MssqlSemanticRefreshActivationStore(lambda: replay).register_activated_pack(request)

    assert not any(sql.startswith("INSERT INTO") for sql, _ in replay.cursor_instance.executions)
    assert replay.commits == 1


def test_activated_pack_registration_rejects_binding_reuse() -> None:
    connection = _ActivatedPackConnection(
        _ActivatedPackCursor(binding_row=("sha256:" + "f" * 64,)),
    )

    with pytest.raises(SemanticRefreshMssqlActivationError, match="execution binding"):
        MssqlSemanticRefreshActivationStore(lambda: connection).register_activated_pack(
            _activated_pack(),
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_mssql_worker_pack_fingerprint_matches_dbt_runtime_formula() -> None:
    request = _activated_pack()
    identity = SemanticRefreshDbtStaticProjectionIdentity.from_mapping(request.projection_identity.to_mapping())

    assert request.pack_fingerprint == semantic_refresh_worker_pack_fingerprint(
        projection_identity=identity,
        run_execution_bundle_sha256=request.run_execution_bundle_sha256,
        activation_authority_receipt_sha256=request.activation_authority_receipt_sha256,
        authority_store_ref=request.authority_store_ref,
        run_guard_closure_sha256=request.run_guard_closure.run_guard_closure_sha256,
    )


def test_mssql_activation_authority_store_persists_full_typed_receipt_closure() -> None:
    plan = _plan_bundle()
    authority = SemanticRefreshActivationAuthoritySet(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        baselines=tuple(item.baseline_receipt for item in plan.targets),
        route_certification=_route_receipt(),
        runtime_assurances=_runtime_assurances(),
        persisted_at="2026-08-08T00:00:00Z",
    )
    connection = _AuthorityStoreConnection(_AuthorityStoreCursor())
    store = MssqlSemanticRefreshActivationAuthorityStore(
        lambda: connection,
        authority_store_ref="mssql-control://prod/DWH/dpone_control",
    )

    receipt = store.persist_exact(authority)

    assert receipt.baseline_receipts == authority.baseline_receipts
    assert receipt.runtime_assurance_receipts == authority.runtime_assurance_receipts
    inserts = [sql for sql, _ in connection.cursor_instance.executions if sql.startswith("INSERT INTO")]
    assert any("semantic_refresh_activation_authorities" in sql for sql in inserts)
    assert any("semantic_refresh_route_authorities" in sql for sql in inserts)
    assert any("semantic_refresh_runtime_assurance_authorities" in sql for sql in inserts)
    receipt_reads = [
        (sql, parameters)
        for sql, parameters in connection.cursor_instance.executions
        if "FROM [dpone_control].[semantic_refresh_activation_authorities]" in sql and "SELECT TOP (1)" not in sql
    ]
    assert len(receipt_reads) == 1
    assert "deployment_id COLLATE Latin1_General_100_BIN2" in receipt_reads[0][0]
    assert "plan_bundle_sha256 COLLATE Latin1_General_100_BIN2" in receipt_reads[0][0]
    assert receipt_reads[0][1] == (authority.deployment_id, authority.plan_bundle_sha256)
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_mssql_activation_authority_store_loads_exact_typed_receipt() -> None:
    plan = _plan_bundle()
    receipt = SemanticRefreshActivationAuthorityReceipt.build(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        authority_store_ref="mssql-control://prod/DWH/dpone_control",
        baseline_receipts=tuple(sorted((item.model_unique_id, item.baseline_receipt_sha256) for item in plan.targets)),
        route_certification_receipt_sha256=_route_receipt().route_certification_receipt_sha256,
        runtime_assurance_receipts=tuple(
            sorted(
                (
                    item.subject.model_unique_id,
                    item.subject.assurance_kind.value,
                    item.runtime_assurance_receipt_sha256,
                )
                for item in _runtime_assurances()
            )
        ),
        persisted_at="2026-08-08T00:00:00Z",
    )
    row = (
        receipt.deployment_id,
        receipt.release_id,
        receipt.plan_bundle_sha256,
        receipt.authority_store_ref,
        json.dumps(receipt.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        receipt.activation_authority_receipt_sha256,
        receipt.persisted_at,
        "ACTIVE",
    )
    connection = _AuthorityStoreConnection(_AuthorityStoreCursor(activation_row=row))

    loaded = MssqlSemanticRefreshActivationAuthorityStore(
        lambda: connection,
        authority_store_ref=receipt.authority_store_ref,
    ).load_exact(
        release_id=receipt.release_id,
        deployment_id=receipt.deployment_id,
        plan_bundle_sha256=receipt.plan_bundle_sha256,
    )

    assert loaded == receipt
    receipt_reads = [
        (sql, parameters)
        for sql, parameters in connection.cursor_instance.executions
        if "FROM [dpone_control].[semantic_refresh_activation_authorities]" in sql
    ]
    assert len(receipt_reads) == 1
    assert "deployment_id COLLATE Latin1_General_100_BIN2" in receipt_reads[0][0]
    assert "plan_bundle_sha256 COLLATE Latin1_General_100_BIN2" in receipt_reads[0][0]
    assert receipt_reads[0][1] == (receipt.deployment_id, receipt.plan_bundle_sha256)
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_mssql_activation_authority_store_accepts_datetime2_6_persisted_at() -> None:
    authority = _activation_authority(persisted_at="2026-08-08T00:00:00.123456Z")
    connection = _AuthorityStoreConnection(_AuthorityStoreCursor())

    receipt = MssqlSemanticRefreshActivationAuthorityStore(
        lambda: connection,
        authority_store_ref="mssql-control://prod/DWH/dpone_control",
    ).persist_exact(authority)

    assert receipt.persisted_at == authority.persisted_at
    assert connection.commits == 1


@pytest.mark.parametrize(
    "persisted_at",
    (
        "2026-08-08T00:00:00.1234567Z",
        "2026-08-08T00:00:00.9999999Z",
        "2026-08-08T00:00:00,123456Z",
        "2026-08-08T00:00:00,1234567Z",
    ),
)
def test_activation_authority_rejects_persisted_at_beyond_datetime2_6(
    persisted_at: str,
) -> None:
    with pytest.raises(
        SemanticRefreshContractError,
        match=r"datetime2\(6\)",
    ):
        _activation_authority(persisted_at=persisted_at)


def test_mssql_activation_authority_store_rejects_overprecision_receipt_on_load() -> None:
    authority = _activation_authority(persisted_at="2026-08-08T00:00:00.123456Z")
    receipt = SemanticRefreshActivationAuthorityReceipt.build(
        release_id=authority.release_id,
        deployment_id=authority.deployment_id,
        plan_bundle_sha256=authority.plan_bundle_sha256,
        authority_store_ref="mssql-control://prod/DWH/dpone_control",
        baseline_receipts=authority.baseline_receipts,
        route_certification_receipt_sha256=(authority.route_certification.route_certification_receipt_sha256),
        runtime_assurance_receipts=authority.runtime_assurance_receipts,
        persisted_at=authority.persisted_at,
    )
    receipt_document = receipt.to_dict()
    receipt_document["persisted_at"] = "2026-08-08T00:00:00.1234564Z"
    row = (
        receipt.deployment_id,
        receipt.release_id,
        receipt.plan_bundle_sha256,
        receipt.authority_store_ref,
        json.dumps(receipt_document, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        receipt.activation_authority_receipt_sha256,
        "2026-08-08T00:00:00.123456Z",
        "ACTIVE",
    )
    connection = _AuthorityStoreConnection(_AuthorityStoreCursor(activation_row=row))

    with pytest.raises(
        SemanticRefreshMssqlActivationAuthorityError,
        match="differs from protected storage",
    ):
        MssqlSemanticRefreshActivationAuthorityStore(
            lambda: connection,
            authority_store_ref=receipt.authority_store_ref,
        ).load_exact(
            release_id=receipt.release_id,
            deployment_id=receipt.deployment_id,
            plan_bundle_sha256=receipt.plan_bundle_sha256,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_mssql_activation_authority_store_keeps_predecessor_and_successor_plans() -> None:
    plan = _plan_bundle()
    predecessor = SemanticRefreshActivationAuthoritySet(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        baselines=tuple(item.baseline_receipt for item in plan.targets),
        route_certification=_route_receipt(),
        runtime_assurances=_runtime_assurances(),
        persisted_at="2026-08-08T00:00:00Z",
    )
    successor = replace(
        predecessor,
        plan_bundle_sha256="sha256:" + "f" * 64,
        persisted_at="2026-08-08T00:00:01Z",
    )
    factory = _StatefulAuthorityStoreFactory()
    store = MssqlSemanticRefreshActivationAuthorityStore(
        factory,
        authority_store_ref="mssql-control://prod/DWH/dpone_control",
    )

    predecessor_receipt = store.persist_exact(predecessor)
    successor_receipt = store.persist_exact(successor)

    assert predecessor_receipt.deployment_id == successor_receipt.deployment_id
    assert predecessor_receipt.plan_bundle_sha256 != successor_receipt.plan_bundle_sha256
    assert predecessor_receipt.activation_authority_receipt_sha256 != (
        successor_receipt.activation_authority_receipt_sha256
    )
    assert (
        store.load_exact(
            release_id=predecessor.release_id,
            deployment_id=predecessor.deployment_id,
            plan_bundle_sha256=predecessor.plan_bundle_sha256,
        )
        == predecessor_receipt
    )
    assert (
        store.load_exact(
            release_id=successor.release_id,
            deployment_id=successor.deployment_id,
            plan_bundle_sha256=successor.plan_bundle_sha256,
        )
        == successor_receipt
    )
    assert len(factory.rows["semantic_refresh_activation_authorities"]) == 2
    assert len(factory.rows["semantic_refresh_route_authorities"]) == 1


def test_mssql_activation_authority_store_rejects_cross_release_deployment_reuse() -> None:
    plan = _plan_bundle()
    authority = SemanticRefreshActivationAuthoritySet(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        baselines=tuple(item.baseline_receipt for item in plan.targets),
        route_certification=_route_receipt(),
        runtime_assurances=_runtime_assurances(),
        persisted_at="2026-08-08T00:00:00Z",
    )
    factory = _StatefulAuthorityStoreFactory()
    factory.rows["semantic_refresh_activation_authorities"] = {
        (authority.deployment_id, "sha256:" + "e" * 64): (
            "sha256:" + "d" * 64,
            "mssql-control://prod/DWH/dpone_control",
            "{}",
            "sha256:" + "c" * 64,
            "2026-08-07T00:00:00Z",
            "ACTIVE",
        )
    }

    with pytest.raises(SemanticRefreshMssqlActivationAuthorityError, match="deployment identity"):
        MssqlSemanticRefreshActivationAuthorityStore(
            factory,
            authority_store_ref="mssql-control://prod/DWH/dpone_control",
        ).persist_exact(authority)

    assert factory.connections[-1].commits == 0
    assert factory.connections[-1].rollbacks == 1


def _activation_authority(*, persisted_at: str) -> SemanticRefreshActivationAuthoritySet:
    plan = _plan_bundle()
    return SemanticRefreshActivationAuthoritySet(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        baselines=tuple(item.baseline_receipt for item in plan.targets),
        route_certification=_route_receipt(),
        runtime_assurances=_runtime_assurances(),
        persisted_at=persisted_at,
    )


def test_mssql_activation_authority_store_rejects_conflicting_baseline_replay() -> None:
    plan = _plan_bundle()
    authority = SemanticRefreshActivationAuthoritySet(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        baselines=tuple(item.baseline_receipt for item in plan.targets),
        route_certification=_route_receipt(),
        runtime_assurances=_runtime_assurances(),
        persisted_at="2026-08-08T00:00:00Z",
    )
    connection = _AuthorityStoreConnection(_AuthorityStoreCursor(conflict=True))

    with pytest.raises(SemanticRefreshMssqlActivationAuthorityError, match="replay differs"):
        MssqlSemanticRefreshActivationAuthorityStore(
            lambda: connection,
            authority_store_ref="mssql-control://prod/DWH/dpone_control",
        ).persist_exact(authority)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_activation_service_rejects_unverified_deployment_subject() -> None:
    service = SemanticRefreshMssqlActivationService(
        _RecordingActivation(),
        _ExactDeploymentVerifier(),
        _AllowLocalActivation(),
    )
    subject = SemanticRefreshDeploymentAuthoritySubject.build(
        _deployment_authority(),
        (_deployment_model(),),
    )

    with pytest.raises(ValueError, match="subject digest differs"):
        service.activate_deployment(
            subject=replace(subject, subject_sha256="sha256:" + "0" * 64),
            plan_bundle=_plan_bundle(),
        )


def test_caller_cannot_substitute_an_execution_binding_after_authority_lookup() -> None:
    bundle = _bundle()
    service = SemanticRefreshMssqlAuthorityAdmissionService(
        authority=_ProtectedAuthority(_record(bundle)),
        admission=_RecordingAdmission(),
    )

    with pytest.raises(ValueError, match="different execution binding"):
        service.admit("sha256:" + "f" * 64)


def test_bundle_rejects_incomplete_target_resource_authority() -> None:
    bundle = _bundle()

    with pytest.raises(ValueError, match="model_resources"):
        MssqlCanonicalAdmissionBundle(
            workflow_execution_id=bundle.workflow_execution_id,
            workflow_plan=bundle.workflow_plan,
            execution_binding=bundle.execution_binding,
            operation_plans=bundle.operation_plans,
            attempt_bindings=bundle.attempt_bindings,
            workflow_guard=bundle.workflow_guard,
            resource_guards=bundle.resource_guards,
            model_resources=(),
            controller_id=bundle.controller_id,
            owner_id=bundle.owner_id,
            reservation_id=bundle.reservation_id,
            resource_budget=bundle.resource_budget,
            replacement_plan=bundle.replacement_plan,
        )


def test_bundle_rejects_execution_closure_not_bound_to_workflow() -> None:
    bundle = _bundle()
    execution = bundle.execution_binding
    mismatched = SemanticRefreshWorkflowExecutionBinding.build(
        workflow_execution_id=execution.workflow_execution_id,
        workflow_mode=execution.workflow_mode,
        workflow_plan_sha256=execution.workflow_plan_sha256,
        selected_mutating_node_ids=("model.analytics.other",),
        model_operation_plan_ids=("model.analytics.other",),
        expected_model_outcome_ids=("model.analytics.other",),
        replacement_action_ids=execution.replacement_action_ids,
        deployment_id=execution.deployment_id,
        binding_set_ref=execution.binding_set_ref,
        connection_registry_ref=execution.connection_registry_ref,
        credential_runtime_ref=execution.credential_runtime_ref,
        workflow_replacement_plan_sha256=execution.workflow_replacement_plan_sha256,
        recovery_plan_digest=execution.recovery_plan_digest,
    )

    with pytest.raises(ValueError, match="model closure"):
        MssqlCanonicalAdmissionBundle(
            **{**_bundle_arguments(bundle), "execution_binding": mismatched},
        )


def test_bundle_rejects_execution_deployment_not_bound_to_operation() -> None:
    bundle = _bundle()
    execution = bundle.execution_binding
    mismatched = SemanticRefreshWorkflowExecutionBinding.build(
        workflow_execution_id=execution.workflow_execution_id,
        workflow_mode=execution.workflow_mode,
        workflow_plan_sha256=execution.workflow_plan_sha256,
        selected_mutating_node_ids=execution.selected_mutating_node_ids,
        model_operation_plan_ids=execution.model_operation_plan_ids,
        expected_model_outcome_ids=execution.expected_model_outcome_ids,
        replacement_action_ids=execution.replacement_action_ids,
        deployment_id="sha256:" + "f" * 64,
        binding_set_ref=execution.binding_set_ref,
        connection_registry_ref=execution.connection_registry_ref,
        credential_runtime_ref=execution.credential_runtime_ref,
        workflow_replacement_plan_sha256=execution.workflow_replacement_plan_sha256,
        recovery_plan_digest=execution.recovery_plan_digest,
    )

    with pytest.raises(ValueError, match="operation deployment"):
        MssqlCanonicalAdmissionBundle(
            **{**_bundle_arguments(bundle), "execution_binding": mismatched},
        )


def test_bundle_rejects_attempt_owner_not_bound_to_writer() -> None:
    bundle = _bundle()
    attempt = bundle.attempt_bindings[0]
    mismatched = SemanticRefreshAttemptBinding.build(
        workflow_execution_id=attempt.workflow_execution_id,
        workflow_execution_binding_sha256=attempt.workflow_execution_binding_sha256,
        operation_id=attempt.operation_id,
        operation_plan_sha256=attempt.operation_plan_sha256,
        dag_run_id=attempt.dag_run_id,
        task_id=attempt.task_id,
        try_number=attempt.try_number,
        pod_uid=attempt.pod_uid,
        fencing_epoch=attempt.fencing_epoch,
        owner_id="another-writer",
    )

    with pytest.raises(ValueError, match="attempt owner"):
        MssqlCanonicalAdmissionBundle(
            **{**_bundle_arguments(bundle), "attempt_bindings": (mismatched,)},
        )


def test_protected_authority_rejects_tampered_or_inactive_document() -> None:
    bundle = _bundle()
    record = _record(bundle)
    tampered = MssqlCanonicalAuthorityRecord(
        workflow_execution_binding_sha256=record.workflow_execution_binding_sha256,
        workflow_execution_id=record.workflow_execution_id,
        authority_sha256=record.authority_sha256,
        authority_json=record.authority_json.replace("airflow-controller", "controller-fake"),
        status=record.status,
    )
    admission = _RecordingAdmission()

    with pytest.raises(ValueError, match="document digest"):
        SemanticRefreshMssqlAuthorityAdmissionService(
            authority=_ProtectedAuthority(tampered),
            admission=admission,
        ).admit(bundle.execution_binding.workflow_execution_binding_sha256)
    assert admission.requests == []

    inactive = MssqlCanonicalAuthorityRecord(
        workflow_execution_binding_sha256=record.workflow_execution_binding_sha256,
        workflow_execution_id=record.workflow_execution_id,
        authority_sha256=record.authority_sha256,
        authority_json=record.authority_json,
        status="REVOKED",
    )
    with pytest.raises(ValueError, match="not ACTIVE"):
        SemanticRefreshMssqlAuthorityAdmissionService(
            authority=_ProtectedAuthority(inactive),
            admission=admission,
        ).admit(bundle.execution_binding.workflow_execution_binding_sha256)

    wrong_execution = MssqlCanonicalAuthorityRecord(
        workflow_execution_binding_sha256=record.workflow_execution_binding_sha256,
        workflow_execution_id="another-workflow-execution",
        authority_sha256=record.authority_sha256,
        authority_json=record.authority_json,
        status=record.status,
    )
    with pytest.raises(ValueError, match="execution identity differs"):
        SemanticRefreshMssqlAuthorityAdmissionService(
            authority=_ProtectedAuthority(wrong_execution),
            admission=admission,
        ).admit(bundle.execution_binding.workflow_execution_binding_sha256)


def test_operation_authority_exposes_only_authenticated_target_and_scope() -> None:
    bundle = _bundle()
    operation = bundle.operation_plans[0]

    protected = SemanticRefreshMssqlProtectedAuthorityService(
        state=_ProtectedOperationState(_operation_state(bundle))
    ).load_operation(
        workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
        operation_id=operation.operation_id,
    )

    assert protected.operation_plan_sha256 == operation.operation_plan_sha256
    assert protected.workflow_id == operation.workflow_id
    resource = bundle.model_resources[0]
    assert protected.baseline_receipt_sha256 == resource.baseline_receipt_sha256
    assert protected.target_authority_id == resource.target_authority_id
    assert protected.publication_database == resource.publication_database
    assert protected.publication_target_table == resource.publication_target_table
    assert protected.publication_scope_id == f"{operation.scope_start}/{operation.scope_end}"


def test_scope_map_is_derived_only_from_authenticated_admitted_strategy() -> None:
    bundle = _bundle()
    operation = bundle.operation_plans[0]
    service = SemanticRefreshMssqlScopeMapService(
        SemanticRefreshMssqlProtectedAuthorityService(state=_ProtectedOperationState(_operation_state(bundle)))
    )

    scope_map = service.build(
        workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
        operation_id=operation.operation_id,
    )

    assert scope_map["verification_status"] == "VERIFIED"
    assert scope_map["signature_sha256"].startswith("sha256:")
    assert scope_map["models"] == {
        operation.model_unique_id: json.loads(compose_admission(bundle).journals[0].strategy_authority_json)
    }


def test_plan_scope_map_loader_closes_the_exact_canonical_model_set() -> None:
    bundle = _bundle()
    operation = bundle.operation_plans[0]
    loader = SemanticRefreshMssqlPlanScopeMapLoader(
        state=_ProtectedPlanState(_operation_state(bundle)),
    )

    scope_map = loader.load(
        workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
        model_unique_ids=(operation.model_unique_id,),
    )

    assert tuple(scope_map["models"]) == (operation.model_unique_id,)
    assert tuple(scope_map["authorities"]) == (operation.model_unique_id,)
    assert scope_map["verification_status"] == "VERIFIED"
    with pytest.raises(ValueError, match="model closure differs"):
        loader.load(
            workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
            model_unique_ids=("model.analytics.unauthorized",),
        )


def test_operation_authority_rejects_an_unprotected_operation_id() -> None:
    bundle = _bundle()
    with pytest.raises(ValueError, match="absent from protected"):
        SemanticRefreshMssqlProtectedAuthorityService(
            state=_ProtectedOperationState(_operation_state(bundle))
        ).load_operation(
            workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
            operation_id="sha256:" + "f" * 64,
        )


def _record(bundle: MssqlCanonicalAdmissionBundle) -> MssqlCanonicalAuthorityRecord:
    return MssqlCanonicalAuthorityRecord(
        workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
        workflow_execution_id=bundle.workflow_execution_id,
        authority_sha256=bundle.authority_sha256,
        authority_json=semantic_refresh_mssql_authority_json(bundle),
        status="ACTIVE",
    )


def _operation_state(bundle: MssqlCanonicalAdmissionBundle) -> MssqlProtectedOperationStateRecord:
    operation = bundle.operation_plans[0]
    attempt = bundle.attempt_bindings[0]
    resource = bundle.model_resources[0]
    journal = compose_admission(bundle).journals[0]
    return MssqlProtectedOperationStateRecord(
        authority=_record(bundle),
        workflow_execution_id=bundle.workflow_execution_id,
        workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        attempt_binding_sha256=attempt.attempt_binding_sha256,
        fencing_epoch=attempt.fencing_epoch,
        owner_id=bundle.owner_id,
        guard_resource_id=resource.target_resource_id,
        guard_status="HELD",
        journal_status="PREPARING",
        strategy_authority_json=journal.strategy_authority_json,
        strategy_authority_sha256=journal.strategy_authority_sha256,
        target_predecessor_generation_id=operation.target_predecessor_generation_id,
        scope_predecessor_operation_id=operation.scope_predecessor_operation_id,
        predecessor_target_generation=8,
        predecessor_target_uuid=resource.clickhouse_target_uuid,
        predecessor_target_operation_id=resource.baseline_receipt_sha256,
        predecessor_scope_revision=None,
        predecessor_checkpoint_sha256=None,
        predecessor_checkpoint_operation_id=None,
        predecessor_checkpoint_version=None,
        before_image_relation=None,
        before_image_sha256=None,
        after_image_relation=None,
        after_image_sha256=None,
    )


def _replacement_service(
    bundle: MssqlCanonicalAdmissionBundle,
) -> SemanticRefreshMssqlReplacementService:
    replacement = bundle.replacement_plan
    assert replacement is not None
    operation = bundle.operation_plans[0]
    action = replacement.replacement_actions[0]
    assert operation.replaces_failed_operation_id is not None
    raw = json.loads(_GOLDEN.read_text(encoding="utf-8"))
    summary = SemanticRefreshFailedWorkflowSummary.from_mapping(
        raw["identity_mode_chains"]["failed_precommit_replacement"]["predecessor_failed_summary"]
    )
    predecessor_model = summary.models[0]
    return SemanticRefreshMssqlReplacementService(
        predecessor_state=_PredecessorState(
            MssqlPredecessorFailureState(
                workflow_id=replacement.predecessor_workflow_execution_id,
                workflow_plan_sha256=summary.workflow_plan_sha256,
                workflow_execution_binding_sha256=(replacement.predecessor_workflow_execution_binding_sha256),
                terminal_summary_sha256=summary.terminal_summary_sha256,
                status="FAILED_PRE_COMMIT",
                models=(
                    MssqlPersistedModelOutcome(
                        model_unique_id=operation.model_unique_id,
                        operation_id=predecessor_model.operation_id,
                        attempt_binding_sha256=predecessor_model.attempt_binding_sha256,
                        mssql_outcome=action.outcome.value,
                        mssql_evidence_sha256=predecessor_model.mssql_evidence_sha256,
                    ),
                ),
            )
        )
    )


def _bundle_arguments(bundle: MssqlCanonicalAdmissionBundle) -> dict[str, object]:
    return {
        field_name: getattr(bundle, field_name)
        for field_name in bundle.__dataclass_fields__
        if field_name != "authority_sha256"
    }
