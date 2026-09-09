"""Authenticated deployment and logical-run activation for MSSQL control state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshDeploymentAuthoritySubject,
    SemanticRefreshPlanBundle,
    SemanticRefreshReleaseDeploymentVerifierPort,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.ports.semantic_refresh_mssql_activation import (
    MssqlBaselineActivation,
    MssqlCanonicalAuthorityRecord,
    MssqlDeploymentActivationRequest,
    MssqlRunAuthorityRegistration,
    MssqlTargetHeadActivation,
    MssqlTargetOwnerActivation,
    SemanticRefreshMssqlActivationPort,
)
from dpone.ports.semantic_refresh_production_activation import (
    SemanticRefreshProductionActivationGuardPort,
    UnavailableSemanticRefreshProductionActivationGuard,
)
from dpone.services.semantic_refresh_mssql_authority import (
    MssqlCanonicalAdmissionBundle,
    semantic_refresh_mssql_authority_json,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_run_authority import (
        MssqlWorkerRunAuthority,
        SemanticRefreshMssqlWorkerRunAuthorityPort,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlActivationService:
    """Install only authority derived from verified canonical deployment/run documents."""

    activation: SemanticRefreshMssqlActivationPort
    deployment_verifier: SemanticRefreshReleaseDeploymentVerifierPort
    activation_guard: SemanticRefreshProductionActivationGuardPort = field(
        default_factory=UnavailableSemanticRefreshProductionActivationGuard,
        compare=False,
        repr=False,
    )

    def activate_deployment(
        self,
        *,
        subject: SemanticRefreshDeploymentAuthoritySubject,
        plan_bundle: SemanticRefreshPlanBundle,
    ) -> None:
        """Create initial target state and guards from one verified deployment."""

        self.activation_guard.authorize(subject=subject, plan_bundle=plan_bundle)
        _validate_deployment(subject, plan_bundle, self.deployment_verifier)
        deployment_by_model = {item.model_unique_id: item for item in subject.models}
        targets = tuple(sorted(plan_bundle.targets, key=lambda item: item.model_unique_id))
        baselines = tuple(
            MssqlBaselineActivation(
                target_resource_id=target.target_resource_id,
                model_unique_id=target.model_unique_id,
                baseline_kind=deployment_by_model[target.model_unique_id].baseline_receipt.baseline_kind.value,
                baseline_receipt_sha256=target.baseline_receipt_sha256,
                baseline_receipt_json=_canonical_json(
                    deployment_by_model[target.model_unique_id].baseline_receipt.to_dict()
                ),
            )
            for target in targets
        )
        owners = tuple(
            MssqlTargetOwnerActivation(
                target_authority_id=target.clickhouse_target_authority_id,
                model_unique_id=target.model_unique_id,
                deployment_id=plan_bundle.release_deployment_authority.deployment_id,
                owner_generation=deployment_by_model[target.model_unique_id].owner_generation,
            )
            for target in targets
        )
        heads = tuple(
            MssqlTargetHeadActivation(
                database_name=target.publication_database,
                target_table=target.publication_target_table,
                target_generation=deployment_by_model[target.model_unique_id].baseline_receipt.clickhouse_generation,
                target_generation_id=deployment_by_model[target.model_unique_id].target_predecessor_generation_id,
                target_uuid=deployment_by_model[target.model_unique_id].baseline_receipt.clickhouse_target_uuid,
                baseline_operation_id=target.baseline_receipt_sha256,
            )
            for target in targets
        )
        self.activation.activate_deployment(
            MssqlDeploymentActivationRequest(
                deployment_id=plan_bundle.release_deployment_authority.deployment_id,
                deployment_subject_sha256=subject.subject_sha256,
                baselines=baselines,
                target_owners=owners,
                target_heads=heads,
                target_guard_resource_ids=tuple(sorted(item.target_resource_id for item in targets)),
            )
        )

    def register_run(self, bundle: MssqlCanonicalAdmissionBundle) -> None:
        """Persist canonical run authority and initialize only absent guard rows."""

        if not isinstance(bundle, MssqlCanonicalAdmissionBundle):
            raise TypeError("bundle must be a canonical MSSQL admission bundle")
        self.activation_guard.authorize(
            subject=bundle.execution_binding,
            plan_bundle=bundle,
        )
        record = MssqlCanonicalAuthorityRecord(
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            workflow_execution_id=bundle.workflow_execution_id,
            authority_sha256=bundle.authority_sha256,
            authority_json=semantic_refresh_mssql_authority_json(bundle),
            status="ACTIVE",
        )
        claims = (bundle.workflow_guard, *bundle.resource_guards)
        self.activation.register_run_authority(
            MssqlRunAuthorityRegistration(
                record=record,
                guard_epochs=tuple(
                    sorted(
                        ((item.resource_id, item.expected_predecessor_epoch) for item in claims),
                        key=lambda item: item[0],
                    )
                ),
            )
        )


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlWorkerRunAuthorityService:
    """Resolve worker inputs by protected workflow plan and actual DagRun ID."""

    authority: SemanticRefreshMssqlWorkerRunAuthorityPort

    def load(
        self,
        *,
        workflow_plan_sha256: str,
        workflow_execution_id: str,
    ) -> MssqlWorkerRunAuthority:
        """Return registered run authority and only admitted attempt/fence inputs."""

        return self.authority.locate(workflow_plan_sha256, workflow_execution_id)


def _validate_deployment(
    subject: SemanticRefreshDeploymentAuthoritySubject,
    plan: SemanticRefreshPlanBundle,
    verifier: SemanticRefreshReleaseDeploymentVerifierPort,
) -> None:
    if not isinstance(subject, SemanticRefreshDeploymentAuthoritySubject):
        raise TypeError("subject must be a canonical deployment authority subject")
    if not isinstance(plan, SemanticRefreshPlanBundle):
        raise TypeError("plan_bundle must be a canonical semantic-refresh plan bundle")
    subject_payload = subject.to_dict()
    subject_digest = subject_payload.pop("subject_sha256")
    if subject_digest != semantic_refresh_sha256(subject_payload):
        raise ValueError("deployment authority subject digest differs")
    plan_payload = plan.to_dict()
    plan_digest = plan_payload.pop("plan_bundle_sha256")
    if plan_digest != semantic_refresh_sha256(plan_payload):
        raise ValueError("semantic-refresh plan bundle digest differs")
    if verifier.verify(subject) is not True:
        raise ValueError("deployment authority subject is not protected and exact")
    if subject.authority != plan.release_deployment_authority:
        raise ValueError("deployment authority differs from canonical plan bundle")
    deployments = {item.model_unique_id: item for item in subject.models}
    targets = {item.model_unique_id: item for item in plan.targets}
    operations = {item.model_unique_id: item for item in plan.operation_plans}
    if len(deployments) != len(subject.models) or set(deployments) != set(targets) or set(targets) != set(operations):
        raise ValueError("deployment, target and operation model closures differ")
    for model_id, target in targets.items():
        deployment = deployments[model_id]
        operation = operations[model_id]
        receipt = deployment.baseline_receipt
        if (
            target.target_resource_id != deployment.target_resource_id
            or target.mssql_connection_authority_id != deployment.mssql_connection_authority_id
            or target.mssql_target_authority_id != deployment.mssql_target_authority_id
            or target.mssql_control_database != deployment.mssql_control_database
            or target.mssql_control_schema != deployment.mssql_control_schema
            or target.mssql_image_schema != deployment.mssql_image_schema
            or target.scope_image_namespace_policy_sha256 != deployment.scope_image_namespace_policy_sha256
            or target.clickhouse_cluster_authority_id != deployment.clickhouse_cluster_authority_id
            or target.clickhouse_target_authority_id != deployment.clickhouse_target_authority_id
            or target.publication_database != deployment.publication_database
            or target.publication_target_table != deployment.publication_target_table
            or target.baseline_receipt != receipt
            or target.baseline_receipt_sha256 != receipt.baseline_adoption_receipt_sha256
            or target.artifact_authority != deployment.artifact_authority
            or operation.target_predecessor_generation_id != target.target_predecessor_generation_id
            or operation.owner_generation != deployment.owner_generation
        ):
            raise ValueError("plan target differs from verified deployment/baseline authority")


def _canonical_json(value: dict[str, object]) -> str:
    import json

    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


__all__ = [
    "SemanticRefreshMssqlActivationService",
    "SemanticRefreshMssqlWorkerRunAuthorityService",
]
