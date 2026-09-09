"""Cohesive worker-time composition for one authenticated semantic-refresh DagRun."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.adapters.semantic_refresh_airflow_attempt_authority import (
    AirflowKubernetesWorkerAttemptAuthority,
)
from dpone.adapters.semantic_refresh_mssql_activation_authority import (
    MssqlSemanticRefreshActivationAuthorityStore,
)
from dpone.adapters.semantic_refresh_mssql_protected_authority import (
    MssqlSemanticRefreshProtectedOperationState,
)
from dpone.adapters.semantic_refresh_mssql_replacement import (
    MssqlSemanticRefreshPredecessorStateReader,
)
from dpone.adapters.semantic_refresh_mssql_run_binding import (
    MssqlSemanticRefreshWorkerRunBindingAuthority,
)
from dpone.adapters.semantic_refresh_mssql_worker_admission import (
    MssqlSemanticRefreshAtomicWorkerAdmission,
)
from dpone.contracts.dbt_semantic_refresh_plan_codec import (
    semantic_refresh_plan_bundle_from_mapping,
)
from dpone.contracts.dbt_semantic_refresh_run_contracts import (
    SemanticRefreshRunAdmissionAuthority,
    SemanticRefreshRunAdmissionCompiler,
    SemanticRefreshRunAdmissionVerifierPort,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.manifest.confined_files import read_confined_file
from dpone.ports.semantic_refresh_attempt_quiescence import (
    SemanticRefreshClickHouseAttemptQuiescencePort,
)
from dpone.ports.semantic_refresh_mssql_activation import (
    MssqlActivatedPackRegistration,
    MssqlStaticProjectionIdentity,
    SemanticRefreshMssqlWorkerRunBindingAuthorityPort,
)
from dpone.ports.semantic_refresh_production_activation import (
    SemanticRefreshProductionActivationGuardPort,
    UnavailableSemanticRefreshProductionActivationGuard,
)
from dpone.runtime.dbt_execution_bootstrap import execute_semantic_refresh_dbt_pack
from dpone.runtime.dbt_semantic_refresh_run_authority import (
    SemanticRefreshDbtAdmittedRun,
    SemanticRefreshDbtImmutableProofRecheckPort,
    SemanticRefreshDbtRunAdmissionPort,
    SemanticRefreshDbtStaticProjectionIdentity,
    semantic_refresh_worker_pack_fingerprint,
    validate_static_projection_identity,
)
from dpone.services.dbt_semantic_refresh_activation import (
    load_exact_deployment_authority_receipt,
)
from dpone.services.semantic_refresh_mssql_authority import (
    SemanticRefreshMssqlPlanScopeMapLoader,
)
from dpone.services.semantic_refresh_mssql_replacement import (
    SemanticRefreshMssqlReplacementService,
)
from dpone.services.semantic_refresh_mssql_worker_validation import (
    validate_semantic_refresh_admitted_run,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_run_authority import SemanticRefreshMssqlWorkerRunAuthorityPort
    from dpone.readiness.dbt_semantic_refresh_runtime_proof import (
        SemanticRefreshImmutableProofRechecker,
    )
    from dpone.runtime.dbt_semantic_refresh_execution import (
        SemanticRefreshDbtScopeMapLoaderPort,
    )

_RUN_AUTHORITY_SCHEMA = "dpone.semantic-refresh-vault-run-authority.v1"
_DBT_TASK_FIELDS = {
    "dbt_execution_pack",
    "plan_bundle",
    "profile_sha256",
    "project_config_overlay",
    "projection_identity",
    "topology_sha256",
    "workflow_execution_id",
}
_RUN_COORDINATE_FIELDS = {
    "dag_projection_sha256",
    "deployment_id",
    "package_artifacts_sha256",
    "plan_bundle_sha256",
    "pre_release_bundle_sha256",
    "release_id",
    "template_pack_fingerprint",
    "topology_sha256",
    "workflow_execution_id",
    "workflow_plan_sha256",
}


@dataclass(frozen=True, slots=True)
class ConfinedPodUidReader:
    """Read one Downward-API UID through the repository's no-follow boundary."""

    root: Path
    relative_path: str

    def __post_init__(self) -> None:
        if not isinstance(self.root, Path) or not self.root.is_absolute():
            raise ValueError("pod identity root must be an absolute Path")
        if not isinstance(self.relative_path, str) or not self.relative_path.strip():
            raise ValueError("pod UID relative path must be non-empty")

    def __call__(self) -> bytes:
        return read_confined_file(
            self.root,
            self.relative_path,
            max_bytes=128,
        )


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlDbtRunAdmission(SemanticRefreshDbtRunAdmissionPort):
    """Compile and atomically admit one actual DagRun from protected inputs."""

    deployment_authorities: MssqlSemanticRefreshActivationAuthorityStore
    compiler: SemanticRefreshRunAdmissionCompiler
    admission: MssqlSemanticRefreshAtomicWorkerAdmission
    run_authority: SemanticRefreshMssqlWorkerRunAuthorityPort
    activation_guard: SemanticRefreshProductionActivationGuardPort = field(
        default_factory=UnavailableSemanticRefreshProductionActivationGuard,
        compare=False,
        repr=False,
    )

    def admit(
        self,
        *,
        plan_bundle: Mapping[str, object],
        workflow_execution_id: str,
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
    ) -> SemanticRefreshDbtAdmittedRun:
        """Persist/replay one exact binding, attempts, fences, guards and journals."""

        plan = semantic_refresh_plan_bundle_from_mapping(plan_bundle)
        self.activation_guard.authorize(
            subject=plan.release_deployment_authority,
            plan_bundle=plan,
        )
        validate_static_projection_identity(
            projection_identity,
            plan_bundle=plan_bundle,
            topology_sha256=projection_identity.topology_sha256,
        )
        run_authority = SemanticRefreshRunAdmissionAuthority(
            workflow_execution_id=workflow_execution_id,
            plan_bundle_sha256=plan.plan_bundle_sha256,
            authority_receipt_sha256=_run_authority_receipt_sha256(
                plan.plan_bundle_sha256,
                workflow_execution_id,
            ),
        )
        run_execution = self.compiler.compile(
            plan_bundle=plan,
            authority=run_authority,
        )
        deployment_receipt = load_exact_deployment_authority_receipt(
            self.deployment_authorities,
            plan_bundle=plan,
        )
        mssql_projection = MssqlStaticProjectionIdentity(**projection_identity.to_mapping())
        pack_fingerprint = semantic_refresh_worker_pack_fingerprint(
            projection_identity=projection_identity,
            run_execution_bundle_sha256=run_execution.run_execution_bundle_sha256,
            activation_authority_receipt_sha256=(deployment_receipt.activation_authority_receipt_sha256),
            authority_store_ref=deployment_receipt.authority_store_ref,
            run_guard_closure_sha256=plan.run_guard_closure.run_guard_closure_sha256,
        )
        registration = MssqlActivatedPackRegistration(
            pack_fingerprint=pack_fingerprint,
            activation_authority_receipt_sha256=(deployment_receipt.activation_authority_receipt_sha256),
            authority_store_ref=deployment_receipt.authority_store_ref,
            workflow_execution_id=workflow_execution_id,
            workflow_execution_binding_sha256=(
                run_execution.workflow_execution_binding.workflow_execution_binding_sha256
            ),
            workflow_plan_sha256=plan.workflow_plan.workflow_plan_sha256,
            plan_bundle_sha256=plan.plan_bundle_sha256,
            run_execution_bundle_sha256=run_execution.run_execution_bundle_sha256,
            run_guard_closure=plan.run_guard_closure,
            projection_identity=mssql_projection,
        )
        self.admission.admit_run(
            activated_pack=registration,
            plan_bundle=plan,
            run_execution=run_execution,
        )
        protected = self.run_authority.locate(
            plan.workflow_plan.workflow_plan_sha256,
            workflow_execution_id,
        )
        validate_semantic_refresh_admitted_run(
            protected,
            registration=registration,
            operation_ids=tuple(item.operation_id for item in plan.operation_plans),
        )
        return SemanticRefreshDbtAdmittedRun(
            workflow_execution_id=workflow_execution_id,
            workflow_execution_binding_sha256=(
                run_execution.workflow_execution_binding.workflow_execution_binding_sha256
            ),
            plan_bundle_sha256=plan.plan_bundle_sha256,
            pre_release_bundle_sha256=plan.pre_release_bundle_sha256,
            package_artifacts_sha256=plan.package_artifacts_sha256,
            model_unique_ids=tuple(sorted(item.model_unique_id for item in plan.operation_plans)),
            protected_state_receipt_sha256=protected.record.authority_sha256,
        )


@dataclass(frozen=True, slots=True)
class SemanticRefreshAirflowWorkerRuntime:
    """One correlated production capability for dbt admission and binding lookup."""

    package_source_root: Path
    run_admission: SemanticRefreshMssqlDbtRunAdmission
    scope_map_loader: SemanticRefreshDbtScopeMapLoaderPort
    immutable_proof_rechecker: SemanticRefreshDbtImmutableProofRecheckPort
    binding_authority: SemanticRefreshMssqlWorkerRunBindingAuthorityPort

    def dbt_build_test(self, **kwargs: object) -> object:
        """Execute the V2 dbt gate after actual-DagRun atomic admission."""

        if set(kwargs) != _DBT_TASK_FIELDS:
            raise ValueError("semantic-refresh dbt worker fields are not closed")
        return execute_semantic_refresh_dbt_pack(
            dbt_execution_pack=_mapping(kwargs["dbt_execution_pack"], "dbt_execution_pack"),
            project_config_overlay=_mapping(
                kwargs["project_config_overlay"],
                "project_config_overlay",
            ),
            profile_sha256=_text(kwargs["profile_sha256"], "profile_sha256"),
            topology_sha256=_text(kwargs["topology_sha256"], "topology_sha256"),
            plan_bundle=_mapping(kwargs["plan_bundle"], "plan_bundle"),
            projection_identity=_mapping(
                kwargs["projection_identity"],
                "projection_identity",
            ),
            workflow_execution_id=_text(
                kwargs["workflow_execution_id"],
                "workflow_execution_id",
            ),
            package_source_root=self.package_source_root,
            run_admission=self.run_admission,
            scope_map_loader=self.scope_map_loader,
            immutable_proof_rechecker=self.immutable_proof_rechecker,
        )

    def resolve_execution_binding(self, **kwargs: object) -> str:
        """Resolve the immutable pack/binding for any later task state."""

        if set(kwargs) != _RUN_COORDINATE_FIELDS:
            raise ValueError("semantic-refresh run locator fields are not closed")
        workflow_execution_id = _text(
            kwargs["workflow_execution_id"],
            "workflow_execution_id",
        )
        identity = SemanticRefreshDbtStaticProjectionIdentity.from_mapping(
            {key: value for key, value in kwargs.items() if key != "workflow_execution_id"}
        )
        protected = self.binding_authority.locate_binding(
            identity.workflow_plan_sha256,
            workflow_execution_id,
        )
        if (
            protected.projection_identity.to_mapping() != identity.to_mapping()
            or protected.record.workflow_execution_id != workflow_execution_id
        ):
            raise ValueError("semantic-refresh durable binding differs from indexed sidecar")
        return protected.record.workflow_execution_binding_sha256


def build_semantic_refresh_airflow_worker_runtime(
    *,
    mssql_connection_factory: Callable[[], Any],
    authority_store_ref: str,
    run_admission_verifier: SemanticRefreshRunAdmissionVerifierPort,
    immutable_proof_rechecker: SemanticRefreshImmutableProofRechecker,
    run_authority: SemanticRefreshMssqlWorkerRunAuthorityPort,
    package_source_root: Path,
    pod_identity_root: Path,
    pod_uid_relative_path: str,
    clock: Callable[[], datetime],
    clickhouse_quiescence: SemanticRefreshClickHouseAttemptQuiescencePort,
    control_schema: str = "dpone_control",
) -> SemanticRefreshAirflowWorkerRuntime:
    """Build the sole production worker capability without parse-time I/O."""

    binding_locator = MssqlSemanticRefreshWorkerRunBindingAuthority(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    resources = MssqlSemanticRefreshProtectedOperationState(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    admission = SemanticRefreshMssqlDbtRunAdmission(
        deployment_authorities=MssqlSemanticRefreshActivationAuthorityStore(
            mssql_connection_factory,
            authority_store_ref=authority_store_ref,
            control_schema=control_schema,
        ),
        compiler=SemanticRefreshRunAdmissionCompiler(run_admission_verifier),
        admission=MssqlSemanticRefreshAtomicWorkerAdmission(
            mssql_connection_factory,
            control_schema=control_schema,
            clock=clock,
            attempt_authority=AirflowKubernetesWorkerAttemptAuthority(
                pod_uid_reader=ConfinedPodUidReader(
                    pod_identity_root,
                    pod_uid_relative_path,
                ),
            ),
            replacement=SemanticRefreshMssqlReplacementService(
                MssqlSemanticRefreshPredecessorStateReader(
                    mssql_connection_factory,
                    control_schema=control_schema,
                )
            ),
            clickhouse_quiescence=clickhouse_quiescence,
        ),
        run_authority=run_authority,
        activation_guard=UnavailableSemanticRefreshProductionActivationGuard(),
    )
    return SemanticRefreshAirflowWorkerRuntime(
        package_source_root=package_source_root,
        run_admission=admission,
        scope_map_loader=SemanticRefreshMssqlPlanScopeMapLoader(resources),
        immutable_proof_rechecker=immutable_proof_rechecker,
        binding_authority=binding_locator,
    )


def _run_authority_receipt_sha256(
    plan_bundle_sha256: str,
    workflow_execution_id: str,
) -> str:
    return semantic_refresh_sha256(
        {
            "plan_bundle_sha256": plan_bundle_sha256,
            "schema": _RUN_AUTHORITY_SCHEMA,
            "workflow_execution_id": workflow_execution_id,
        }
    )


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"semantic-refresh {field_name} must be a mapping")
    return value


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"semantic-refresh {field_name} must be non-empty text")
    return value


__all__ = [
    "ConfinedPodUidReader",
    "SemanticRefreshAirflowWorkerRuntime",
    "SemanticRefreshMssqlDbtRunAdmission",
    "build_semantic_refresh_airflow_worker_runtime",
]
