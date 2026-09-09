"""Concrete worker-time composition for semantic-refresh DagRuns."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.semantic_refresh_mssql_run_authority import (
    MssqlSemanticRefreshWorkerRunAuthority,
)
from dpone.app.semantic_refresh_airflow_composition import (
    build_semantic_refresh_airflow_callables,
)
from dpone.app.semantic_refresh_worker_composition import (
    SemanticRefreshAirflowWorkerRuntime,
    SemanticRefreshMssqlDbtRunAdmission,
    build_semantic_refresh_airflow_worker_runtime,
)
from dpone.contracts.dbt_semantic_refresh_run_contracts import (
    SemanticRefreshRunAdmissionCompiler,
)
from dpone.ports.semantic_refresh_mssql_activation import (
    MssqlStaticProjectionIdentity,
)
from dpone.ports.semantic_refresh_production_activation import (
    SemanticRefreshProductionActivationUnavailableError,
)
from dpone.runtime.dbt_semantic_refresh_run_authority import (
    SemanticRefreshDbtStaticProjectionIdentity,
)
from tests.test_dbt_semantic_refresh_plan_compiler import (
    _activation_receipt,
    _plan_bundle,
)
from tests.test_semantic_refresh_airflow_composition import _runtime as _publication_runtime


class _AllowLocalActivation:
    def authorize(self, **_: object) -> None:
        return None


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _projection(plan) -> SemanticRefreshDbtStaticProjectionIdentity:
    return SemanticRefreshDbtStaticProjectionIdentity(
        dag_projection_sha256=_digest("1"),
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        workflow_plan_sha256=plan.workflow_plan.workflow_plan_sha256,
        topology_sha256=_digest("2"),
        pre_release_bundle_sha256=plan.pre_release_bundle_sha256,
        package_artifacts_sha256=plan.package_artifacts_sha256,
        template_pack_fingerprint=_digest("3"),
    )


class _DeploymentAuthorities:
    def __init__(self, plan) -> None:
        self.receipt = _activation_receipt(plan)
        self.calls: list[dict[str, str]] = []

    def load_exact(self, **coordinates: str):
        self.calls.append(coordinates)
        return self.receipt


class _AtomicAdmission:
    def __init__(self) -> None:
        self.registration = None

    def admit_run(self, *, activated_pack, plan_bundle, run_execution):
        assert activated_pack.plan_bundle_sha256 == plan_bundle.plan_bundle_sha256
        assert activated_pack.run_execution_bundle_sha256 == run_execution.run_execution_bundle_sha256
        self.registration = activated_pack
        return SimpleNamespace(admission_receipt_sha256=_digest("4"))


def test_production_guard_blocks_worker_before_loading_or_mutating() -> None:
    plan = _plan_bundle()
    authorities = _DeploymentAuthorities(plan)
    atomic = _AtomicAdmission()
    admission = SemanticRefreshMssqlDbtRunAdmission(
        deployment_authorities=authorities,  # type: ignore[arg-type]
        compiler=SemanticRefreshRunAdmissionCompiler(type("Verifier", (), {"verify": lambda *_: True})()),
        admission=atomic,  # type: ignore[arg-type]
        run_authority=_ActiveRunAuthority(
            atomic,
            tuple(item.operation_id for item in plan.operation_plans),
        ),
    )

    with pytest.raises(
        SemanticRefreshProductionActivationUnavailableError,
        match="DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE",
    ):
        admission.admit(
            plan_bundle=plan.to_dict(),
            workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
            projection_identity=_projection(plan),
        )

    assert authorities.calls == []
    assert atomic.registration is None


class _ActiveRunAuthority:
    def __init__(
        self,
        admission: _AtomicAdmission,
        operation_ids: tuple[str, ...],
        *,
        journal_status: str = "PREPARING",
    ) -> None:
        self._admission = admission
        self._operation_ids = operation_ids
        self._journal_status = journal_status

    def locate(self, workflow_plan_sha256: str, workflow_execution_id: str):
        registration = self._admission.registration
        assert registration is not None
        assert registration.workflow_plan_sha256 == workflow_plan_sha256
        assert registration.workflow_execution_id == workflow_execution_id
        return SimpleNamespace(
            record=SimpleNamespace(authority_sha256=_digest("5")),
            admission_status="ADMITTED",
            pack_fingerprint=registration.pack_fingerprint,
            activation_authority_receipt_sha256=(registration.activation_authority_receipt_sha256),
            authority_store_ref=registration.authority_store_ref,
            plan_bundle_sha256=registration.plan_bundle_sha256,
            run_execution_bundle_sha256=registration.run_execution_bundle_sha256,
            projection_identity=registration.projection_identity,
            attempts=tuple(
                SimpleNamespace(operation_id=value, journal_status=self._journal_status)
                for value in self._operation_ids
            ),
        )


class _BindingAuthority:
    def __init__(self, projection: SemanticRefreshDbtStaticProjectionIdentity) -> None:
        self._projection = projection

    def locate_binding(self, workflow_plan_sha256: str, workflow_execution_id: str):
        assert workflow_plan_sha256 == self._projection.workflow_plan_sha256
        return SimpleNamespace(
            projection_identity=MssqlStaticProjectionIdentity(**self._projection.to_mapping()),
            record=SimpleNamespace(
                workflow_execution_id=workflow_execution_id,
                workflow_execution_binding_sha256=_digest("6"),
            ),
        )


class _Noop:
    def load(self, **_: object) -> dict[str, object]:
        return {}

    def recheck(self, **_: object) -> object:
        return object()


def test_actual_dagrun_is_compiled_and_admitted_as_one_protected_run() -> None:
    plan = _plan_bundle()
    projection = _projection(plan)
    atomic = _AtomicAdmission()
    operations = tuple(item.operation_id for item in plan.operation_plans)
    admission = SemanticRefreshMssqlDbtRunAdmission(
        deployment_authorities=_DeploymentAuthorities(plan),  # type: ignore[arg-type]
        compiler=SemanticRefreshRunAdmissionCompiler(type("Verifier", (), {"verify": lambda *_: True})()),
        admission=atomic,  # type: ignore[arg-type]
        run_authority=_ActiveRunAuthority(atomic, operations),
        activation_guard=_AllowLocalActivation(),
    )

    admitted = admission.admit(
        plan_bundle=plan.to_dict(),
        workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
        projection_identity=projection,
    )

    assert admitted.workflow_execution_id == "scheduled__2026-08-09T00:00:00+00:00"
    assert admitted.plan_bundle_sha256 == plan.plan_bundle_sha256
    assert admitted.model_unique_ids == tuple(sorted(item.model_unique_id for item in plan.operation_plans))
    assert atomic.registration is not None
    assert atomic.registration.projection_identity.to_mapping() == projection.to_mapping()


@pytest.mark.parametrize(
    "journal_status",
    [
        "PREPARED",
        "COMMITTING",
        "COMMIT_UNKNOWN",
        "TARGET_COMMITTED",
        "COMMITTED_INCOMPLETE",
        "COMPLETE",
    ],
)
def test_dbt_gate_never_reenters_after_preparing(journal_status: str) -> None:
    plan = _plan_bundle()
    atomic = _AtomicAdmission()
    admission = SemanticRefreshMssqlDbtRunAdmission(
        deployment_authorities=_DeploymentAuthorities(plan),  # type: ignore[arg-type]
        compiler=SemanticRefreshRunAdmissionCompiler(type("Verifier", (), {"verify": lambda *_: True})()),
        admission=atomic,  # type: ignore[arg-type]
        run_authority=_ActiveRunAuthority(
            atomic,
            tuple(item.operation_id for item in plan.operation_plans),
            journal_status=journal_status,
        ),
        activation_guard=_AllowLocalActivation(),
    )

    with pytest.raises(ValueError, match="differs from worker authority"):
        admission.admit(
            plan_bundle=plan.to_dict(),
            workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
            projection_identity=_projection(plan),
        )


def test_later_tasks_resolve_immutable_binding_after_mutable_attempt_state() -> None:
    plan = _plan_bundle()
    projection = _projection(plan)
    runtime = SemanticRefreshAirflowWorkerRuntime(
        package_source_root=Path("/platform/dbt-dpone"),
        run_admission=object(),  # type: ignore[arg-type]
        scope_map_loader=_Noop(),
        immutable_proof_rechecker=_Noop(),  # type: ignore[arg-type]
        binding_authority=_BindingAuthority(projection),
    )

    binding = runtime.resolve_execution_binding(
        **projection.to_mapping(),
        workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
    )

    assert binding == _digest("6")

    with pytest.raises(ValueError, match="differs from indexed sidecar"):
        runtime.resolve_execution_binding(
            **{
                **projection.to_mapping(),
                "dag_projection_sha256": _digest("f"),
            },
            workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
        )


def test_production_worker_builder_is_parse_inert_and_wires_one_capability(
    tmp_path: Path,
) -> None:
    calls = 0

    def _connection():
        nonlocal calls
        calls += 1
        raise AssertionError("parse-time composition must not open MSSQL")

    worker = build_semantic_refresh_airflow_worker_runtime(
        mssql_connection_factory=_connection,
        authority_store_ref="mssql://dpone-control/activation-authorities",
        run_admission_verifier=type("Verifier", (), {"verify": lambda *_: True})(),
        immutable_proof_rechecker=_Noop(),  # type: ignore[arg-type]
        run_authority=MssqlSemanticRefreshWorkerRunAuthority(_connection),
        package_source_root=tmp_path / "package",
        pod_identity_root=tmp_path / "podinfo",
        pod_uid_relative_path="uid",
        clock=lambda: pytest.fail("parse-time composition must not read the clock"),
        clickhouse_quiescence=_Noop(),  # type: ignore[arg-type]
    )
    callables = build_semantic_refresh_airflow_callables(
        runtime=_publication_runtime(),
        worker=worker,
    )

    assert calls == 0
    assert getattr(callables.dbt_build_test, "__self__", None) is worker
    assert getattr(callables.resolve_execution_binding, "__self__", None) is worker
