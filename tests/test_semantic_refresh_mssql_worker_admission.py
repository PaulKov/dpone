"""Atomic worker-run admission authority tests."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from dpone.adapters.semantic_refresh_mssql_worker_admission import (
    MssqlSemanticRefreshAtomicWorkerAdmission,
    SemanticRefreshMssqlAtomicWorkerAdmissionError,
)
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
)
from dpone.ports.semantic_refresh_mssql_activation import MssqlActivatedPackRegistration
from dpone.ports.semantic_refresh_mssql_authority_codec import authority_json
from dpone.ports.semantic_refresh_mssql_worker_admission import (
    MssqlGuardEpochSnapshot,
    MssqlTrustedAttemptCoordinate,
    MssqlWorkerAdmissionCoordinates,
    compose_worker_admission_bundle,
    mssql_workflow_resource_budget,
    validate_worker_admission_bundle,
)
from dpone.ports.semantic_refresh_mssql_worker_pack import (
    MssqlStaticProjectionIdentity,
    mssql_worker_pack_fingerprint,
)
from tests.test_dbt_semantic_refresh_plan_compiler import _plan_bundle, _run_bundle


def _coordinates(plan=None) -> MssqlWorkerAdmissionCoordinates:
    plan = _plan_bundle() if plan is None else plan
    return MssqlWorkerAdmissionCoordinates(
        attempts=tuple(
            MssqlTrustedAttemptCoordinate(
                operation_id=operation.operation_id,
                task_id="dbt-build",
                try_number=1,
                pod_uid="00000000-0000-0000-0000-000000000777",
            )
            for operation in plan.operation_plans
        ),
    )


def _epochs(plan=None) -> tuple[MssqlGuardEpochSnapshot, ...]:
    plan = _plan_bundle() if plan is None else plan
    closure = plan.run_guard_closure
    return tuple(
        MssqlGuardEpochSnapshot(resource_id, index)
        for index, resource_id in enumerate(
            sorted((closure.workflow_guard_resource_id, *closure.resource_guard_ids)),
        )
    )


def _pack(plan, run) -> MssqlActivatedPackRegistration:
    binding = run.workflow_execution_binding
    receipt = _activation_receipt(plan)
    projection = MssqlStaticProjectionIdentity(
        dag_projection_sha256="sha256:" + "1" * 64,
        deployment_id=plan.release_deployment_authority.deployment_id,
        package_artifacts_sha256=plan.package_artifacts_sha256,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        pre_release_bundle_sha256=plan.pre_release_bundle_sha256,
        release_id=plan.release_deployment_authority.release_id,
        template_pack_fingerprint="sha256:" + "3" * 64,
        topology_sha256="sha256:" + "4" * 64,
        workflow_plan_sha256=plan.workflow_plan.workflow_plan_sha256,
    )
    fingerprint = mssql_worker_pack_fingerprint(
        projection_identity=projection,
        run_execution_bundle_sha256=run.run_execution_bundle_sha256,
        activation_authority_receipt_sha256=receipt.activation_authority_receipt_sha256,
        authority_store_ref="mssql-control://test/activation",
        run_guard_closure_sha256=plan.run_guard_closure.run_guard_closure_sha256,
    )
    return MssqlActivatedPackRegistration(
        pack_fingerprint=fingerprint,
        activation_authority_receipt_sha256=receipt.activation_authority_receipt_sha256,
        authority_store_ref="mssql-control://test/activation",
        workflow_execution_id=binding.workflow_execution_id,
        workflow_execution_binding_sha256=binding.workflow_execution_binding_sha256,
        workflow_plan_sha256=plan.workflow_plan.workflow_plan_sha256,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        run_execution_bundle_sha256=run.run_execution_bundle_sha256,
        run_guard_closure=plan.run_guard_closure,
        projection_identity=projection,
    )


class _AtomicCursor:
    def __init__(self, plan, *, existing_bundle=None) -> None:
        self.plan = plan
        self.existing_bundle = existing_bundle
        self.activation_receipt = _activation_receipt(plan)
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self._row = None
        self._rows: tuple[tuple[object, ...], ...] = ()

    def execute(self, sql: str, *parameters: object):
        self.executions.append((sql, tuple(parameters)))
        self._row = None
        self._rows = ()
        if "sp_getapplock" in sql:
            self._row = (0,)
        elif "semantic_refresh_activated_packs" in sql and "WHERE pack_fingerprint" in sql:
            self._row = None
        elif "semantic_refresh_activated_packs" in sql and "WHERE workflow_execution_binding" in sql:
            self._row = None
        elif "semantic_refresh_activation_authorities" in sql:
            self._row = (
                self.plan.release_deployment_authority.deployment_id,
                self.plan.release_deployment_authority.release_id,
                self.plan.plan_bundle_sha256,
                "mssql-control://test/activation",
                json.dumps(
                    self.activation_receipt.to_dict(),
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                self.activation_receipt.activation_authority_receipt_sha256,
                self.activation_receipt.persisted_at,
                "ACTIVE",
            )
        elif "semantic_refresh_guards" in sql and sql.startswith("SELECT fencing_epoch"):
            resource_id = str(parameters[0])
            if resource_id == self.plan.run_guard_closure.workflow_guard_resource_id:
                self._row = None
            else:
                self._row = (4, None, None, None, "AVAILABLE")
        elif "semantic_refresh_canonical_authorities" in sql and sql.startswith("SELECT"):
            if self.existing_bundle is not None:
                self._rows = (
                    (
                        self.existing_bundle.execution_binding.workflow_execution_binding_sha256,
                        self.existing_bundle.workflow_execution_id,
                        self.existing_bundle.authority_sha256,
                        authority_json(self.existing_bundle),
                        "ACTIVE",
                    ),
                )
        elif "OUTPUT inserted.authority_sha256" in sql:
            self._row = (parameters[2],)
        return self

    def fetchone(self):
        row = self._row
        self._row = None
        return row

    def fetchall(self):
        return self._rows

    def close(self) -> None:
        return None


class _AtomicConnection:
    def __init__(self, cursor, *, commit_error: Exception | None = None) -> None:
        self.autocommit = True
        self._cursor = cursor
        self._commit_error = commit_error
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0
        self.timeout = 0
        cursor.connection = self

    def cursor(self):
        self._cursor.timeout_at_creation = self.timeout
        return self._cursor

    def commit(self) -> None:
        self.commits += 1
        if self._commit_error is not None:
            raise self._commit_error

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closes += 1


class _AtomicState:
    def __init__(self, epochs) -> None:
        self.epochs = epochs
        self.requests = []

    def lock_admissible_guard_epoch(self, _cursor, resource_id):
        return self.epochs[resource_id]

    def admit_in_transaction(self, _cursor, request):
        from dpone.ports.semantic_refresh_mssql import MssqlAdmissionReceipt

        self.requests.append(request)
        return MssqlAdmissionReceipt(
            workflow_id=request.workflow_id,
            reservation_id=request.reservation_id,
            guards=(request.workflow_guard, *request.resource_guards),
            preparing_operation_ids=tuple(item.operation_id for item in request.journals),
        )


class _AttemptAuthority:
    def __init__(self, coordinates) -> None:
        self.coordinates = coordinates

    def load_attempts(self, *, plan_bundle_sha256, workflow_execution_id, operation_ids):
        assert plan_bundle_sha256.startswith("sha256:")
        assert workflow_execution_id
        assert operation_ids == tuple(item.operation_id for item in self.coordinates.attempts)
        return self.coordinates


class _ContinuationSpy:
    def __init__(self) -> None:
        self.calls = []

    def reconcile(self, cursor, *, bundle, coordinates):
        self.calls.append((cursor, bundle, coordinates))
        return ()


def _activation_receipt(plan) -> SemanticRefreshActivationAuthorityReceipt:
    assurances: list[tuple[str, str, str]] = []
    for target in plan.targets:
        assurances.extend(
            (
                (target.model_unique_id, "ddl_freeze", target.ddl_freeze_assurance_receipt_sha256),
                (
                    target.model_unique_id,
                    "writer_exclusivity",
                    target.writer_exclusivity_assurance_receipt_sha256,
                ),
            )
        )
        if target.utc_semantics_assurance_receipt_sha256 is not None:
            assurances.append(
                (
                    target.model_unique_id,
                    "utc_semantics",
                    target.utc_semantics_assurance_receipt_sha256,
                )
            )
    routes = {item.route_certification_receipt_sha256 for item in plan.targets}
    assert len(routes) == 1
    return SemanticRefreshActivationAuthorityReceipt.build(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        authority_store_ref="mssql-control://test/activation",
        baseline_receipts=tuple(sorted((item.model_unique_id, item.baseline_receipt_sha256) for item in plan.targets)),
        route_certification_receipt_sha256=next(iter(routes)),
        runtime_assurance_receipts=tuple(sorted(assurances)),
        persisted_at="2026-08-08T00:00:00Z",
    )


def test_worker_bundle_allocates_fences_only_from_locked_guard_epochs() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    epochs = _epochs(plan)

    bundle = compose_worker_admission_bundle(
        plan_bundle=plan,
        run_execution=run,
        coordinates=_coordinates(plan),
        guard_epochs=epochs,
    )

    predecessor_by_resource = {item.resource_id: item.predecessor_epoch for item in epochs}
    assert bundle.workflow_guard.resource_id == plan.run_guard_closure.workflow_guard_resource_id
    assert (
        bundle.workflow_guard.expected_predecessor_epoch == predecessor_by_resource[bundle.workflow_guard.resource_id]
    )
    assert bundle.workflow_guard.fencing_epoch == bundle.workflow_guard.expected_predecessor_epoch + 1
    assert tuple(item.resource_id for item in bundle.resource_guards) == plan.run_guard_closure.resource_guard_ids
    assert bundle.attempt_bindings[0].fencing_epoch == bundle.resource_guards[0].fencing_epoch
    assert bundle.controller_id.startswith("semantic-refresh-controller:")
    assert bundle.owner_id.startswith("semantic-refresh-owner:")
    assert bundle.reservation_id.startswith("sha256:")
    assert bundle.resource_budget == mssql_workflow_resource_budget(plan)
    validate_worker_admission_bundle(
        bundle,
        plan_bundle=plan,
        run_execution=run,
        coordinates=_coordinates(plan),
    )


def test_worker_bundle_rejects_missing_or_caller_substituted_guard_closure() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    epochs = _epochs(plan)

    with pytest.raises(ValueError, match="locked guard epochs"):
        compose_worker_admission_bundle(
            plan_bundle=plan,
            run_execution=run,
            coordinates=_coordinates(plan),
            guard_epochs=epochs[:-1],
        )
    with pytest.raises(ValueError, match="locked guard epochs"):
        compose_worker_admission_bundle(
            plan_bundle=plan,
            run_execution=run,
            coordinates=_coordinates(plan),
            guard_epochs=(replace(epochs[0], resource_id="caller://guard"), *epochs[1:]),
        )


def test_worker_bundle_replay_rejects_changed_airflow_attempt() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    coordinates = _coordinates(plan)
    bundle = compose_worker_admission_bundle(
        plan_bundle=plan,
        run_execution=run,
        coordinates=coordinates,
        guard_epochs=_epochs(plan),
    )
    changed = replace(
        coordinates,
        attempts=(replace(coordinates.attempts[0], try_number=2),),
    )

    with pytest.raises(ValueError, match="attempt authority differs"):
        validate_worker_admission_bundle(
            bundle,
            plan_bundle=plan,
            run_execution=run,
            coordinates=changed,
        )


def test_worker_bundle_rejects_run_from_another_plan() -> None:
    plan = _plan_bundle()
    run = replace(_run_bundle(plan), plan_bundle_sha256="sha256:" + "f" * 64)

    with pytest.raises(ValueError, match="run execution differs"):
        compose_worker_admission_bundle(
            plan_bundle=plan,
            run_execution=run,
            coordinates=_coordinates(plan),
            guard_epochs=_epochs(plan),
        )


def test_atomic_worker_admission_creates_pack_authority_and_state_in_one_commit() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    cursor = _AtomicCursor(plan)
    connection = _AtomicConnection(cursor)
    adapter = MssqlSemanticRefreshAtomicWorkerAdmission(
        lambda: connection,
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
        attempt_authority=_AttemptAuthority(_coordinates(plan)),
    )
    epochs = {item.resource_id: item.predecessor_epoch for item in _epochs(plan)}
    state = _AtomicState(epochs)
    adapter._state = state

    receipt = adapter.admit_run(
        activated_pack=_pack(plan, run),
        plan_bundle=plan,
        run_execution=run,
    )

    assert receipt.workflow_id == run.workflow_execution_binding.workflow_execution_id
    assert connection.commits == 1
    assert connection.rollbacks == 0
    inserts = [sql for sql, _ in cursor.executions if sql.startswith("INSERT INTO")]
    assert any("semantic_refresh_activated_packs" in sql for sql in inserts)
    assert any("semantic_refresh_canonical_authorities" in sql for sql in inserts)
    assert len(state.requests) == 1


def test_atomic_worker_admission_reports_commit_acknowledgement_unknown_without_rollback() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    cursor = _AtomicCursor(plan)
    connection = _AtomicConnection(cursor, commit_error=TimeoutError("commit acknowledgement lost"))
    adapter = MssqlSemanticRefreshAtomicWorkerAdmission(
        lambda: connection,
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
        attempt_authority=_AttemptAuthority(_coordinates(plan)),
    )
    epochs = {item.resource_id: item.predecessor_epoch for item in _epochs(plan)}
    adapter._state = _AtomicState(epochs)

    with pytest.raises(
        SemanticRefreshMssqlAtomicWorkerAdmissionError,
        match="DPONE_SEMANTIC_REFRESH_WORKER_ADMISSION_COMMIT_UNKNOWN",
    ):
        adapter.admit_run(
            activated_pack=_pack(plan, run),
            plan_bundle=plan,
            run_execution=run,
        )

    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closes == 1


def test_atomic_worker_admission_sets_timeout_before_cursor_creation() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    cursor = _AtomicCursor(plan)
    connection = _AtomicConnection(cursor)
    adapter = MssqlSemanticRefreshAtomicWorkerAdmission(
        lambda: connection,
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
        attempt_authority=_AttemptAuthority(_coordinates(plan)),
    )
    adapter._state = _AtomicState({item.resource_id: item.predecessor_epoch for item in _epochs(plan)})

    adapter.admit_run(
        activated_pack=_pack(plan, run),
        plan_bundle=plan,
        run_execution=run,
    )

    assert cursor.timeout_at_creation == min(item.resource_policy.max_statement_seconds for item in plan.targets)


def test_atomic_worker_admission_normalizes_domain_error_after_commit_starts() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    cursor = _AtomicCursor(plan)
    connection = _AtomicConnection(
        cursor,
        commit_error=SemanticRefreshMssqlAtomicWorkerAdmissionError("driver wrapper failure"),
    )
    adapter = MssqlSemanticRefreshAtomicWorkerAdmission(
        lambda: connection,
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
        attempt_authority=_AttemptAuthority(_coordinates(plan)),
    )
    adapter._state = _AtomicState({item.resource_id: item.predecessor_epoch for item in _epochs(plan)})

    with pytest.raises(
        SemanticRefreshMssqlAtomicWorkerAdmissionError,
        match="DPONE_SEMANTIC_REFRESH_WORKER_ADMISSION_COMMIT_UNKNOWN",
    ):
        adapter.admit_run(
            activated_pack=_pack(plan, run),
            plan_bundle=plan,
            run_execution=run,
        )

    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closes == 1


def test_atomic_worker_admission_reconciles_a_later_try_before_state_replay() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    original_coordinates = _coordinates(plan)
    bundle = compose_worker_admission_bundle(
        plan_bundle=plan,
        run_execution=run,
        coordinates=original_coordinates,
        guard_epochs=_epochs(plan),
    )
    later_coordinates = replace(
        original_coordinates,
        attempts=(
            replace(
                original_coordinates.attempts[0],
                try_number=2,
                pod_uid="00000000-0000-0000-0000-000000000888",
            ),
        ),
    )
    cursor = _AtomicCursor(plan, existing_bundle=bundle)
    connection = _AtomicConnection(cursor)
    adapter = MssqlSemanticRefreshAtomicWorkerAdmission(
        lambda: connection,
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
        attempt_authority=_AttemptAuthority(later_coordinates),
    )
    state = _AtomicState({})
    continuation = _ContinuationSpy()
    adapter._state = state
    adapter._continuations = continuation

    adapter.admit_run(
        activated_pack=_pack(plan, run),
        plan_bundle=plan,
        run_execution=run,
    )

    assert connection.commits == 1
    assert len(continuation.calls) == 1
    assert continuation.calls[0][2] == later_coordinates.attempts
    assert len(state.requests) == 1


def test_atomic_worker_admission_rejects_pack_from_another_run_before_writes() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    cursor = _AtomicCursor(plan)
    connection = _AtomicConnection(cursor)
    adapter = MssqlSemanticRefreshAtomicWorkerAdmission(
        lambda: connection,
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
        attempt_authority=_AttemptAuthority(_coordinates(plan)),
    )
    original = _pack(plan, run)
    changed_run = "sha256:" + "f" * 64
    pack = replace(
        original,
        pack_fingerprint=mssql_worker_pack_fingerprint(
            projection_identity=original.projection_identity,
            run_execution_bundle_sha256=changed_run,
            activation_authority_receipt_sha256=original.activation_authority_receipt_sha256,
            authority_store_ref=original.authority_store_ref,
            run_guard_closure_sha256=original.run_guard_closure.run_guard_closure_sha256,
        ),
        run_execution_bundle_sha256=changed_run,
    )

    with pytest.raises(SemanticRefreshMssqlAtomicWorkerAdmissionError, match="activated pack"):
        adapter.admit_run(
            activated_pack=pack,
            plan_bundle=plan,
            run_execution=run,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any(sql.startswith("INSERT INTO") for sql, _ in cursor.executions)


def test_atomic_worker_admission_rejects_cross_wired_static_deployment_identity() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    cursor = _AtomicCursor(plan)
    connection = _AtomicConnection(cursor)
    adapter = MssqlSemanticRefreshAtomicWorkerAdmission(
        lambda: connection,
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
        attempt_authority=_AttemptAuthority(_coordinates(plan)),
    )
    original = _pack(plan, run)
    changed_projection = replace(
        original.projection_identity,
        deployment_id="sha256:" + "f" * 64,
    )
    changed = replace(
        original,
        pack_fingerprint=mssql_worker_pack_fingerprint(
            projection_identity=changed_projection,
            run_execution_bundle_sha256=original.run_execution_bundle_sha256,
            activation_authority_receipt_sha256=original.activation_authority_receipt_sha256,
            authority_store_ref=original.authority_store_ref,
            run_guard_closure_sha256=original.run_guard_closure.run_guard_closure_sha256,
        ),
        projection_identity=changed_projection,
    )

    with pytest.raises(SemanticRefreshMssqlAtomicWorkerAdmissionError, match="activated pack"):
        adapter.admit_run(
            activated_pack=changed,
            plan_bundle=plan,
            run_execution=run,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any(sql.startswith("INSERT INTO") for sql, _ in cursor.executions)
