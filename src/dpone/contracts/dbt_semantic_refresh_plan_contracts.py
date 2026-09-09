"""Typed two-phase inputs and outputs for dbt semantic-refresh plans."""

from __future__ import annotations

from dataclasses import dataclass, field

from dpone.contracts.dbt_semantic_refresh_certification import (
    SemanticRefreshCertificationDecision,
    SemanticRefreshCertificationRequest,
)
from dpone.contracts.dbt_semantic_refresh_deployment_contracts import (
    SemanticRefreshDeploymentAuthoritySubject,
    SemanticRefreshDeploymentModelAuthority,
    SemanticRefreshPlanTarget,
    SemanticRefreshReleaseDeploymentAuthority,
    SemanticRefreshReleaseDeploymentVerifierPort,
)
from dpone.contracts.dbt_semantic_refresh_lifecycle import (
    SemanticRefreshLifecycleReport,
)
from dpone.contracts.dbt_semantic_refresh_plan_bundle import (
    PLAN_BUNDLE_SCHEMA,
    SemanticRefreshPlanBundle,
    plan_payload,
)
from dpone.contracts.dbt_semantic_refresh_plan_policy import (
    RESOURCE_POLICY_SCHEMA,
    SemanticRefreshArtifactAuthority,
    SemanticRefreshResourcePolicy,
    SemanticRefreshWritableColumn,
    SemanticRefreshWritableRole,
    validate_writable_columns,
)
from dpone.contracts.dbt_semantic_refresh_run_guard import SemanticRefreshRunGuardClosure
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    canonical_string_set,
    require_digest,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_effective_key_identity import (
    EffectiveKeyTemplateColumn,
    semantic_refresh_effective_key_template_sha256,
)
from dpone.contracts.semantic_refresh_lifecycle_policy import SemanticRefreshSqlServerLifecyclePolicy
from dpone.contracts.semantic_refresh_model_proof import SemanticRefreshModelDefinitionProof
from dpone.contracts.semantic_refresh_mutation_closure import SemanticRefreshMutationClosure
from dpone.contracts.semantic_refresh_read_dependency import SemanticRefreshReadDependencyProof

PRE_RELEASE_PROOF_BUNDLE_SCHEMA = "dpone.dbt-semantic-refresh-pre-release-proof-bundle.v1"


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshPreReleaseModelInput:
    """Release-independent, proven inputs for one exact selected model."""

    model_unique_id: str
    scope_family_id: str
    event_time_column: str
    effective_key_templates: tuple[EffectiveKeyTemplateColumn, ...]
    writable_columns: tuple[SemanticRefreshWritableColumn, ...]
    model_definition_proof: SemanticRefreshModelDefinitionProof
    read_dependency_proof: SemanticRefreshReadDependencyProof
    effective_key_template_sha256: str = field(init=False, compare=True)
    writable_schema_sha256: str = field(init=False, compare=True)

    def __post_init__(self) -> None:
        require_text(self.model_unique_id, "model_unique_id")
        require_digest(self.scope_family_id, "scope_family_id")
        require_text(self.event_time_column, "event_time_column")
        if not self.effective_key_templates:
            raise SemanticRefreshContractError("effective_key_templates must be non-empty")
        validate_writable_columns(
            self.writable_columns,
            effective_keys=self.effective_key_templates,
            event_time_column=self.event_time_column,
        )
        if self.model_definition_proof.status.value != "PROVEN":
            raise SemanticRefreshContractError("model definition proof must be PROVEN")
        if self.read_dependency_proof.status.value != "PROVEN":
            raise SemanticRefreshContractError("read dependency proof must be PROVEN")
        if self.model_definition_proof.model_unique_id != self.model_unique_id:
            raise SemanticRefreshContractError("model definition proof identity differs from model_unique_id")
        if self.read_dependency_proof.model_unique_id != self.model_unique_id:
            raise SemanticRefreshContractError("read dependency proof identity differs from model_unique_id")
        object.__setattr__(
            self,
            "effective_key_template_sha256",
            semantic_refresh_effective_key_template_sha256(self.effective_key_templates),
        )
        object.__setattr__(
            self,
            "writable_schema_sha256",
            semantic_refresh_sha256(
                {
                    "schema": "dpone.dbt-semantic-refresh-writable-schema.v1",
                    "writable_columns": [item.to_dict() for item in self.writable_columns],
                }
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "effective_key_template_sha256": self.effective_key_template_sha256,
            "effective_key_templates": [item.to_dict() for item in self.effective_key_templates],
            "event_time_column": self.event_time_column,
            "model_definition_proof": self.model_definition_proof.to_dict(),
            "model_unique_id": self.model_unique_id,
            "read_dependency_proof": self.read_dependency_proof.to_dict(),
            "scope_family_id": self.scope_family_id,
            "writable_columns": [item.to_dict() for item in self.writable_columns],
            "writable_schema_sha256": self.writable_schema_sha256,
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshPreReleaseProofBundle:
    """Immutable proof closure that intentionally has no final plan identity."""

    workflow_name: str
    environment: str
    manifest_sha256: str
    profile_sha256: str
    toolchain_sha256: str
    certification_coordinate_sha256: str
    route_certification_receipt_sha256: str
    platform_policy_digest: str
    resource_policy_digest: str
    resource_policy: SemanticRefreshResourcePolicy
    mutation_closure: SemanticRefreshMutationClosure
    lifecycle_policy: SemanticRefreshSqlServerLifecyclePolicy
    lifecycle_report: SemanticRefreshLifecycleReport
    models: tuple[SemanticRefreshPreReleaseModelInput, ...]
    pre_release_bundle_sha256: str
    schema: str = PRE_RELEASE_PROOF_BUNDLE_SCHEMA

    @classmethod
    def build(
        cls,
        *,
        workflow_name: str,
        environment: str,
        manifest_sha256: str,
        profile_sha256: str,
        toolchain_sha256: str,
        certification_request: SemanticRefreshCertificationRequest,
        certification_decision: SemanticRefreshCertificationDecision,
        platform_policy_digest: str,
        resource_policy: SemanticRefreshResourcePolicy,
        mutation_closure: SemanticRefreshMutationClosure,
        lifecycle_policy: SemanticRefreshSqlServerLifecyclePolicy,
        lifecycle_report: SemanticRefreshLifecycleReport,
        models: tuple[SemanticRefreshPreReleaseModelInput, ...],
    ) -> SemanticRefreshPreReleaseProofBundle:
        digests = {
            name: require_digest(value, name)
            for name, value in (
                ("manifest_sha256", manifest_sha256),
                ("profile_sha256", profile_sha256),
                ("toolchain_sha256", toolchain_sha256),
                ("platform_policy_digest", platform_policy_digest),
            )
        }
        canonical_models = _canonical_models(models)
        if not isinstance(resource_policy, SemanticRefreshResourcePolicy):
            raise SemanticRefreshContractError("resource_policy must be a typed bounded policy")
        digests["resource_policy_digest"] = resource_policy.resource_policy_sha256
        _validate_proofs(digests["manifest_sha256"], mutation_closure, canonical_models)
        _validate_certification(
            certification_request,
            certification_decision,
            manifest_sha256=digests["manifest_sha256"],
            profile_sha256=digests["profile_sha256"],
            toolchain_sha256=digests["toolchain_sha256"],
        )
        _validate_lifecycle_report(
            lifecycle_policy,
            lifecycle_report,
            certification_request.certification_coordinate_sha256,
        )
        route_receipt = certification_decision.receipt_sha256
        if route_receipt is None:
            raise SemanticRefreshContractError("certified route decision omitted its protected receipt")
        values: dict[str, object] = {
            "workflow_name": require_text(workflow_name, "workflow_name"),
            "environment": require_text(environment, "environment"),
            "certification_coordinate_sha256": certification_request.certification_coordinate_sha256,
            "route_certification_receipt_sha256": route_receipt,
            **digests,
        }
        payload = pre_release_payload(
            values,
            resource_policy,
            mutation_closure,
            lifecycle_policy,
            lifecycle_report,
            canonical_models,
        )
        return cls(
            workflow_name=workflow_name,
            environment=environment,
            manifest_sha256=manifest_sha256,
            profile_sha256=profile_sha256,
            toolchain_sha256=toolchain_sha256,
            certification_coordinate_sha256=certification_request.certification_coordinate_sha256,
            route_certification_receipt_sha256=route_receipt,
            platform_policy_digest=platform_policy_digest,
            resource_policy_digest=digests["resource_policy_digest"],
            resource_policy=resource_policy,
            mutation_closure=mutation_closure,
            lifecycle_policy=lifecycle_policy,
            lifecycle_report=lifecycle_report,
            models=canonical_models,
            pre_release_bundle_sha256=semantic_refresh_sha256(payload),
        )

    def to_dict(self) -> dict[str, object]:
        values = {
            field: getattr(self, field)
            for field in (
                "workflow_name",
                "environment",
                "manifest_sha256",
                "profile_sha256",
                "toolchain_sha256",
                "certification_coordinate_sha256",
                "route_certification_receipt_sha256",
                "platform_policy_digest",
                "resource_policy_digest",
            )
        }
        return {
            **pre_release_payload(
                values,
                self.resource_policy,
                self.mutation_closure,
                self.lifecycle_policy,
                self.lifecycle_report,
                self.models,
            ),
            "pre_release_bundle_sha256": self.pre_release_bundle_sha256,
        }


def pre_release_payload(
    values: dict[str, object],
    resource_policy: SemanticRefreshResourcePolicy,
    mutation: SemanticRefreshMutationClosure,
    lifecycle: SemanticRefreshSqlServerLifecyclePolicy,
    lifecycle_report: SemanticRefreshLifecycleReport,
    models: tuple[SemanticRefreshPreReleaseModelInput, ...],
) -> dict[str, object]:
    return {
        **values,
        "lifecycle_policy": lifecycle.to_dict(),
        "lifecycle_report": lifecycle_report.to_dict(),
        "models": [item.to_dict() for item in models],
        "mutation_closure": mutation.to_dict(),
        "resource_policy": resource_policy.to_dict(),
        "schema": PRE_RELEASE_PROOF_BUNDLE_SCHEMA,
    }


def _validate_lifecycle_report(
    policy: SemanticRefreshSqlServerLifecyclePolicy,
    report: SemanticRefreshLifecycleReport,
    certification_coordinate_sha256: str,
) -> None:
    if not isinstance(report, SemanticRefreshLifecycleReport):
        raise SemanticRefreshContractError("lifecycle report must be typed immutable evidence")
    if report.status != "PROVEN" or report.issues:
        raise SemanticRefreshContractError("lifecycle report must be PROVEN")
    if report.lifecycle_policy_sha256 != policy.sqlserver_lifecycle_policy_sha256:
        raise SemanticRefreshContractError("lifecycle report differs from canonical lifecycle policy")
    if report.certification_coordinate_sha256 != certification_coordinate_sha256:
        raise SemanticRefreshContractError("lifecycle report differs from certification coordinate")


def _validate_proofs(
    manifest_sha256: str,
    mutation: SemanticRefreshMutationClosure,
    models: tuple[SemanticRefreshPreReleaseModelInput, ...],
) -> None:
    if mutation.status.value != "PROVEN":
        raise SemanticRefreshContractError("mutation closure must be PROVEN")
    if tuple(item.model_unique_id for item in models) != mutation.selected_mutating_node_ids:
        raise SemanticRefreshContractError("pre-release models differ from the exact mutation closure")
    if any(item.model_definition_proof.manifest_sha256 != manifest_sha256 for item in models):
        raise SemanticRefreshContractError("model proof manifest identity differs from pre-release authority")


def _validate_certification(
    request: SemanticRefreshCertificationRequest,
    decision: SemanticRefreshCertificationDecision,
    *,
    manifest_sha256: str,
    profile_sha256: str,
    toolchain_sha256: str,
) -> None:
    if (
        decision.request_sha256 != request.request_sha256
        or not decision.valid_at(request.verification_time)
        or request.manifest_sha256 != manifest_sha256
        or request.profile_sha256 != profile_sha256
        or request.toolchain_sha256 != toolchain_sha256
    ):
        raise SemanticRefreshContractError("route certification is not current for the exact proof subject")


def _canonical_models(
    models: tuple[SemanticRefreshPreReleaseModelInput, ...],
) -> tuple[SemanticRefreshPreReleaseModelInput, ...]:
    if not models or any(not isinstance(item, SemanticRefreshPreReleaseModelInput) for item in models):
        raise SemanticRefreshContractError("pre-release models must be a non-empty typed tuple")
    result = tuple(sorted(models))
    canonical_string_set(tuple(item.model_unique_id for item in result), "model_unique_ids")
    return result


__all__ = [
    "PLAN_BUNDLE_SCHEMA",
    "PRE_RELEASE_PROOF_BUNDLE_SCHEMA",
    "RESOURCE_POLICY_SCHEMA",
    "SemanticRefreshArtifactAuthority",
    "SemanticRefreshDeploymentAuthoritySubject",
    "SemanticRefreshDeploymentModelAuthority",
    "SemanticRefreshPlanBundle",
    "SemanticRefreshPlanTarget",
    "SemanticRefreshPreReleaseModelInput",
    "SemanticRefreshPreReleaseProofBundle",
    "SemanticRefreshResourcePolicy",
    "SemanticRefreshRunGuardClosure",
    "SemanticRefreshReleaseDeploymentAuthority",
    "SemanticRefreshReleaseDeploymentVerifierPort",
    "SemanticRefreshWritableColumn",
    "SemanticRefreshWritableRole",
    "plan_payload",
]
