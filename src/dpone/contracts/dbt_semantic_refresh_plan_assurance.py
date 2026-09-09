"""Protected deployment and assurance validation for semantic-refresh plans."""

from __future__ import annotations

from datetime import datetime

from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshProtectedAssuranceVerifierPort,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshDeploymentModelAuthority,
    SemanticRefreshPreReleaseModelInput,
    SemanticRefreshPreReleaseProofBundle,
    SemanticRefreshReleaseDeploymentAuthority,
)
from dpone.contracts.semantic_refresh_baseline_receipt import (
    semantic_refresh_target_predecessor_generation_id,
)
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError
from dpone.contracts.semantic_refresh_route_certification import (
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_runtime_assurance import (
    RuntimeAssuranceKind,
    RuntimeAssuranceSubjectType,
    SemanticRefreshRuntimeAssuranceReceipt,
    SemanticRefreshRuntimeAssuranceSubject,
)
from dpone.contracts.semantic_refresh_types import EffectiveKeyColumn

RuntimeAssuranceIndex = dict[
    tuple[str, RuntimeAssuranceKind],
    SemanticRefreshRuntimeAssuranceReceipt,
]


def validate_route_certification(
    pre_release: SemanticRefreshPreReleaseProofBundle,
    receipt: SemanticRefreshRouteLiveCertificationReceipt,
    verification_time: datetime,
    verifier: SemanticRefreshProtectedAssuranceVerifierPort,
) -> None:
    """Require the exact current protected route coordinate used pre-release."""

    if (
        not isinstance(receipt, SemanticRefreshRouteLiveCertificationReceipt)
        or receipt.route_certification_receipt_sha256 != pre_release.route_certification_receipt_sha256
        or receipt.coordinates.environment != pre_release.environment
        or receipt.coordinates.toolchain_sha256 != pre_release.toolchain_sha256
        or receipt.coordinates.runtime_image_digest != pre_release.lifecycle_policy.runtime_image_digest
        or receipt.coordinates.load_strategy != "semantic_refresh_v2"
        or verifier.verify_route(
            receipt,
            expected_coordinate_sha256=pre_release.certification_coordinate_sha256,
        )
        is not True
        or not receipt.authorizes(
            verification_time,
            trusted_receipt_sha256=receipt.route_certification_receipt_sha256,
        )
    ):
        raise SemanticRefreshContractError("route certification is not protected, current, and exact")


def deployment_model_index(
    values: tuple[SemanticRefreshDeploymentModelAuthority, ...],
    models: tuple[SemanticRefreshPreReleaseModelInput, ...],
) -> dict[str, SemanticRefreshDeploymentModelAuthority]:
    """Close protected deployment inputs over the pre-release model set."""

    if not values or any(not isinstance(item, SemanticRefreshDeploymentModelAuthority) for item in values):
        raise SemanticRefreshContractError("deployment models must be a non-empty typed tuple")
    result = {item.model_unique_id: item for item in values}
    if len(result) != len(values) or set(result) != {item.model_unique_id for item in models}:
        raise SemanticRefreshContractError("deployment models differ from the pre-release model closure")
    return result


def validate_baseline_authority(
    authority: SemanticRefreshReleaseDeploymentAuthority,
    deployment: SemanticRefreshDeploymentModelAuthority,
) -> None:
    """Bind the complete baseline receipt to both physical engines."""

    receipt = deployment.baseline_receipt
    expected_clickhouse_relation = f"{deployment.publication_database}.{deployment.publication_target_table}"
    if (
        receipt.model_unique_id != deployment.model_unique_id
        or receipt.release_id != authority.release_id
        or receipt.deployment_id != authority.deployment_id
        or receipt.mssql_relation_id != deployment.target_resource_id
        or receipt.mssql_connection_authority_id != deployment.mssql_connection_authority_id
        or receipt.mssql_target_authority_id != deployment.mssql_target_authority_id
        or receipt.clickhouse_relation_id != expected_clickhouse_relation
        or receipt.clickhouse_cluster_authority_id != deployment.clickhouse_cluster_authority_id
        or receipt.clickhouse_target_authority_id != deployment.clickhouse_target_authority_id
        or deployment.target_predecessor_generation_id != semantic_refresh_target_predecessor_generation_id(receipt)
    ):
        raise SemanticRefreshContractError(
            "baseline receipt differs from the exact model/release/deployment/target authority"
        )


def runtime_assurance_index(
    values: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...],
    models: tuple[SemanticRefreshPreReleaseModelInput, ...],
) -> RuntimeAssuranceIndex:
    """Index a canonical typed assurance closure without losing model identity."""

    if any(not isinstance(item, SemanticRefreshRuntimeAssuranceReceipt) for item in values):
        raise SemanticRefreshContractError("runtime assurances must be a typed tuple")
    result = {(item.subject.model_unique_id, item.subject.assurance_kind): item for item in values}
    if len(result) != len(values) or set(item[0] for item in result) != {model.model_unique_id for model in models}:
        raise SemanticRefreshContractError("runtime assurance model closure is invalid")
    return result


def validate_model_assurances(
    *,
    pre_release: SemanticRefreshPreReleaseProofBundle,
    authority: SemanticRefreshReleaseDeploymentAuthority,
    model: SemanticRefreshPreReleaseModelInput,
    deployment: SemanticRefreshDeploymentModelAuthority,
    assurances: RuntimeAssuranceIndex,
    verification_time: datetime,
    verifier: SemanticRefreshProtectedAssuranceVerifierPort,
) -> tuple[EffectiveKeyColumn, ...]:
    """Authenticate every required axis and materialize the final key mapping."""

    expected = _expected_assurance_subjects(pre_release, authority, model, deployment)
    observed = {
        kind: receipt
        for (observed_model, kind), receipt in assurances.items()
        if observed_model == model.model_unique_id
    }
    if set(observed) != set(expected):
        raise SemanticRefreshContractError("runtime assurance closure differs from the exact model key template")
    for kind, subject in expected.items():
        receipt = observed[kind]
        if verifier.verify_runtime(receipt) is not True or not receipt.authorizes(
            verification_time,
            trusted_digest=receipt.runtime_assurance_receipt_sha256,
            expected_subject=subject,
        ):
            raise SemanticRefreshContractError("runtime assurance is not protected, current, and exact")
    utc = observed.get(RuntimeAssuranceKind.UTC_SEMANTICS)
    utc_digest = utc.runtime_assurance_receipt_sha256 if utc is not None else None
    return tuple(
        EffectiveKeyColumn(
            item.name,
            item.source_type,
            item.target_type,
            item.domain_min,
            item.domain_max,
            utc_digest if item.utc_assurance_required else None,
        )
        for item in model.effective_key_templates
    )


def _expected_assurance_subjects(
    pre_release: SemanticRefreshPreReleaseProofBundle,
    authority: SemanticRefreshReleaseDeploymentAuthority,
    model: SemanticRefreshPreReleaseModelInput,
    deployment: SemanticRefreshDeploymentModelAuthority,
) -> dict[RuntimeAssuranceKind, SemanticRefreshRuntimeAssuranceSubject]:
    common = {
        "release_id": authority.release_id,
        "deployment_id": authority.deployment_id,
        "environment": pre_release.environment,
        "database": deployment.target_resource_id.partition(".")[0],
        "model_unique_id": model.model_unique_id,
        "mssql_target_authority_id": deployment.mssql_target_authority_id,
        "model_definition_proof_sha256": (model.model_definition_proof.model_definition_proof_sha256),
        "effective_key_template_sha256": model.effective_key_template_sha256,
        "writable_schema_sha256": model.writable_schema_sha256,
        "sqlserver_lifecycle_policy_sha256": (pre_release.lifecycle_policy.sqlserver_lifecycle_policy_sha256),
        "route_certification_receipt_sha256": (pre_release.route_certification_receipt_sha256),
        "mssql_control_database": deployment.mssql_control_database,
        "mssql_control_schema": deployment.mssql_control_schema,
        "mssql_image_schema": deployment.mssql_image_schema,
        "scope_image_namespace_policy_sha256": (deployment.scope_image_namespace_policy_sha256),
    }
    result = {
        kind: SemanticRefreshRuntimeAssuranceSubject(
            assurance_kind=kind,
            subject_type=RuntimeAssuranceSubjectType.TARGET,
            **common,
        )
        for kind in (
            RuntimeAssuranceKind.WRITER_EXCLUSIVITY,
            RuntimeAssuranceKind.DDL_FREEZE,
        )
    }
    if any(item.utc_assurance_required for item in model.effective_key_templates):
        result[RuntimeAssuranceKind.UTC_SEMANTICS] = SemanticRefreshRuntimeAssuranceSubject(
            assurance_kind=RuntimeAssuranceKind.UTC_SEMANTICS,
            subject_type=RuntimeAssuranceSubjectType.COLUMN,
            column_name=model.event_time_column,
            **common,
        )
    return result


__all__ = [
    "RuntimeAssuranceIndex",
    "deployment_model_index",
    "runtime_assurance_index",
    "validate_baseline_authority",
    "validate_model_assurances",
    "validate_route_certification",
]
