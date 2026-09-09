"""Closed decoder for deployment-indexed semantic-refresh plan bundles."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from dpone.contracts.dbt_semantic_refresh_deployment_contracts import (
    SemanticRefreshPlanTarget,
    SemanticRefreshReleaseDeploymentAuthority,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    PLAN_BUNDLE_SCHEMA,
    SemanticRefreshPlanBundle,
)
from dpone.contracts.dbt_semantic_refresh_plan_policy import (
    RESOURCE_POLICY_SCHEMA,
    SemanticRefreshArtifactAuthority,
    SemanticRefreshResourcePolicy,
    SemanticRefreshWritableColumn,
    SemanticRefreshWritableRole,
)
from dpone.contracts.dbt_semantic_refresh_run_guard import SemanticRefreshRunGuardClosure
from dpone.contracts.semantic_refresh_baseline_receipt import SemanticRefreshBaselineAdoptionReceipt
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_closed_mapping,
    require_digest,
    require_positive_int,
    require_text,
)
from dpone.contracts.semantic_refresh_operation_plan import SemanticRefreshOperationPlan
from dpone.contracts.semantic_refresh_workflow_plan import SemanticRefreshWorkflowPlan
from dpone.contracts.semantic_refresh_workflow_replacement import SemanticRefreshWorkflowReplacementPlan

_PLAN_REQUIRED = frozenset(
    {
        "operation_plans",
        "package_artifacts_sha256",
        "plan_bundle_sha256",
        "pre_release_bundle_sha256",
        "release_deployment_authority",
        "run_guard_closure",
        "schema",
        "targets",
        "workflow_plan",
    }
)
_PLAN_OPTIONAL = frozenset({"workflow_replacement_plan"})
_DEPLOYMENT_AUTHORITY_FIELDS = frozenset(
    {
        "authority_receipt_sha256",
        "binding_set_ref",
        "connection_registry_ref",
        "credential_runtime_ref",
        "deployment_id",
        "release_id",
    }
)
_TARGET_REQUIRED = frozenset(
    {
        "artifact_authority",
        "baseline_receipt",
        "baseline_receipt_sha256",
        "baseline_status",
        "clickhouse_cluster_authority_id",
        "clickhouse_target_authority_id",
        "clickhouse_target_uuid",
        "ddl_freeze_assurance_receipt_sha256",
        "ddl_epoch",
        "effective_key_mapping_sha256",
        "effective_key_template_sha256",
        "model_definition_proof_sha256",
        "model_unique_id",
        "mssql_connection_authority_id",
        "mssql_control_database",
        "mssql_control_schema",
        "mssql_image_schema",
        "mssql_target_authority_id",
        "publication_database",
        "publication_scope_id",
        "publication_target_table",
        "resource_policy",
        "resource_policy_sha256",
        "route_certification_receipt_sha256",
        "scope_image_namespace_policy_sha256",
        "strategy_template_sha256",
        "target_head_authority_receipt_sha256",
        "target_head_terminal_receipt_sha256",
        "target_predecessor_generation",
        "target_predecessor_generation_id",
        "target_predecessor_operation_id",
        "target_resource_id",
        "writable_columns",
        "writable_schema_sha256",
        "writer_exclusivity_assurance_receipt_sha256",
    }
)
_TARGET_OPTIONAL = frozenset({"utc_semantics_assurance_receipt_sha256"})
_WRITABLE_FIELDS = frozenset({"name", "nullable", "source_type", "target_type", "writable_role"})
_RESOURCE_FIELDS = frozenset(
    {
        "max_after_image_bytes",
        "max_after_image_rows",
        "max_before_image_bytes",
        "max_before_image_rows",
        "max_clickhouse_retained_backup_bytes",
        "max_clickhouse_shadow_bytes",
        "max_clickhouse_staging_bytes",
        "max_clickhouse_total_transient_bytes",
        "max_dbt_temp_bytes",
        "max_dbt_temp_rows",
        "max_mssql_scope_lock_seconds",
        "max_mssql_version_store_bytes",
        "max_scope_image_total_bytes",
        "max_source_scope_rows",
        "max_statement_seconds",
        "max_target_scope_rows",
        "max_transaction_log_bytes",
        "min_mssql_log_free_bytes",
    }
)
_ARTIFACT_FIELDS = frozenset(
    {
        "artifact_prefix",
        "bucket_or_container_authority_id",
        "capability_evidence_sha256",
        "endpoint_authority_id",
        "encryption_policy_sha256",
        "kms_key_authority_id",
        "max_artifact_bytes",
        "provider",
        "provider_profile",
        "retention_days",
        "retention_issued_at",
        "retention_policy_id",
        "retention_policy_sha256",
        "retention_until",
        "writer_scope",
    }
)


def semantic_refresh_plan_bundle_from_mapping(value: object) -> SemanticRefreshPlanBundle:
    """Decode a full closed plan and recompute every nested canonical identity."""

    raw = require_closed_mapping(
        value,
        "semantic_refresh_plan_bundle",
        required=_PLAN_REQUIRED,
        optional=_PLAN_OPTIONAL,
    )
    if raw.get("schema") != PLAN_BUNDLE_SCHEMA:
        raise SemanticRefreshContractError("semantic_refresh_plan_bundle schema is unsupported")
    operations = tuple(
        SemanticRefreshOperationPlan.from_mapping(item)
        for item in _array(raw.get("operation_plans"), "operation_plans")
    )
    targets = tuple(_plan_target(item) for item in _array(raw.get("targets"), "targets"))
    replacement_raw = raw.get("workflow_replacement_plan")
    replacement = (
        SemanticRefreshWorkflowReplacementPlan.from_mapping(replacement_raw) if replacement_raw is not None else None
    )
    return SemanticRefreshPlanBundle(
        pre_release_bundle_sha256=require_digest(raw.get("pre_release_bundle_sha256"), "pre_release_bundle_sha256"),
        package_artifacts_sha256=require_digest(raw.get("package_artifacts_sha256"), "package_artifacts_sha256"),
        release_deployment_authority=_release_deployment_authority(raw.get("release_deployment_authority")),
        operation_plans=operations,
        workflow_plan=SemanticRefreshWorkflowPlan.from_mapping(raw.get("workflow_plan")),
        targets=targets,
        run_guard_closure=SemanticRefreshRunGuardClosure.from_mapping(raw.get("run_guard_closure")),
        plan_bundle_sha256=require_digest(raw.get("plan_bundle_sha256"), "plan_bundle_sha256"),
        workflow_replacement_plan=replacement,
    )


def _release_deployment_authority(value: object) -> SemanticRefreshReleaseDeploymentAuthority:
    raw = require_closed_mapping(
        value,
        "release_deployment_authority",
        required=_DEPLOYMENT_AUTHORITY_FIELDS,
    )
    return SemanticRefreshReleaseDeploymentAuthority(
        release_id=require_digest(raw.get("release_id"), "release_id"),
        deployment_id=require_digest(raw.get("deployment_id"), "deployment_id"),
        binding_set_ref=require_text(raw.get("binding_set_ref"), "binding_set_ref"),
        connection_registry_ref=require_text(raw.get("connection_registry_ref"), "connection_registry_ref"),
        credential_runtime_ref=require_text(raw.get("credential_runtime_ref"), "credential_runtime_ref"),
        authority_receipt_sha256=require_digest(raw.get("authority_receipt_sha256"), "authority_receipt_sha256"),
    )


def _plan_target(value: object) -> SemanticRefreshPlanTarget:
    raw = require_closed_mapping(value, "plan_target", required=_TARGET_REQUIRED, optional=_TARGET_OPTIONAL)
    terminal = raw.get("target_head_terminal_receipt_sha256")
    utc_receipt = raw.get("utc_semantics_assurance_receipt_sha256")
    return SemanticRefreshPlanTarget(
        model_unique_id=_text(raw, "model_unique_id"),
        target_resource_id=_text(raw, "target_resource_id"),
        mssql_connection_authority_id=_text(raw, "mssql_connection_authority_id"),
        mssql_target_authority_id=_text(raw, "mssql_target_authority_id"),
        mssql_control_database=_text(raw, "mssql_control_database"),
        mssql_control_schema=_text(raw, "mssql_control_schema"),
        mssql_image_schema=_text(raw, "mssql_image_schema"),
        scope_image_namespace_policy_sha256=_digest(raw, "scope_image_namespace_policy_sha256"),
        clickhouse_cluster_authority_id=_text(raw, "clickhouse_cluster_authority_id"),
        clickhouse_target_authority_id=_text(raw, "clickhouse_target_authority_id"),
        clickhouse_target_uuid=_text(raw, "clickhouse_target_uuid"),
        target_predecessor_generation=require_positive_int(
            raw.get("target_predecessor_generation"), "target_predecessor_generation"
        ),
        target_predecessor_generation_id=_digest(raw, "target_predecessor_generation_id"),
        target_predecessor_operation_id=_digest(raw, "target_predecessor_operation_id"),
        target_head_terminal_receipt_sha256=(
            require_digest(terminal, "target_head_terminal_receipt_sha256") if terminal is not None else None
        ),
        target_head_authority_receipt_sha256=_digest(raw, "target_head_authority_receipt_sha256"),
        publication_database=_text(raw, "publication_database"),
        publication_target_table=_text(raw, "publication_target_table"),
        publication_scope_id=_text(raw, "publication_scope_id"),
        baseline_receipt=SemanticRefreshBaselineAdoptionReceipt.from_mapping(raw.get("baseline_receipt")),
        baseline_receipt_sha256=_digest(raw, "baseline_receipt_sha256"),
        baseline_status=_text(raw, "baseline_status"),
        strategy_template_sha256=_digest(raw, "strategy_template_sha256"),
        model_definition_proof_sha256=_digest(raw, "model_definition_proof_sha256"),
        effective_key_template_sha256=_digest(raw, "effective_key_template_sha256"),
        effective_key_mapping_sha256=_digest(raw, "effective_key_mapping_sha256"),
        writable_columns=tuple(
            _writable_column(item) for item in _array(raw.get("writable_columns"), "writable_columns")
        ),
        writable_schema_sha256=_digest(raw, "writable_schema_sha256"),
        resource_policy=_resource_policy(raw.get("resource_policy")),
        resource_policy_sha256=_digest(raw, "resource_policy_sha256"),
        route_certification_receipt_sha256=_digest(raw, "route_certification_receipt_sha256"),
        writer_exclusivity_assurance_receipt_sha256=_digest(raw, "writer_exclusivity_assurance_receipt_sha256"),
        ddl_freeze_assurance_receipt_sha256=_digest(raw, "ddl_freeze_assurance_receipt_sha256"),
        ddl_epoch=require_positive_int(raw.get("ddl_epoch"), "ddl_epoch"),
        artifact_authority=_artifact_authority(raw.get("artifact_authority")),
        utc_semantics_assurance_receipt_sha256=(
            require_digest(utc_receipt, "utc_semantics_assurance_receipt_sha256") if utc_receipt is not None else None
        ),
    )


def _writable_column(value: object) -> SemanticRefreshWritableColumn:
    raw = require_closed_mapping(value, "writable_column", required=_WRITABLE_FIELDS)
    nullable = raw.get("nullable")
    if not isinstance(nullable, bool):
        raise SemanticRefreshContractError("writable column nullable must be boolean")
    try:
        role = SemanticRefreshWritableRole(raw.get("writable_role"))
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshContractError("writable column role is unsupported") from exc
    return SemanticRefreshWritableColumn(
        _text(raw, "name"),
        _text(raw, "source_type"),
        _text(raw, "target_type"),
        nullable,
        role,
    )


def _resource_policy(value: object) -> SemanticRefreshResourcePolicy:
    raw = require_closed_mapping(
        value,
        "resource_policy",
        required=_RESOURCE_FIELDS | {"resource_policy_sha256", "schema"},
    )
    if raw.get("schema") != RESOURCE_POLICY_SCHEMA:
        raise SemanticRefreshContractError("resource_policy schema is unsupported")
    policy = SemanticRefreshResourcePolicy(
        **{field: require_positive_int(raw.get(field), field) for field in _RESOURCE_FIELDS}
    )
    if policy.resource_policy_sha256 != require_digest(raw.get("resource_policy_sha256"), "resource_policy_sha256"):
        raise SemanticRefreshContractError("resource policy digest differs from canonical content")
    return policy


def _artifact_authority(value: object) -> SemanticRefreshArtifactAuthority:
    raw = require_closed_mapping(value, "artifact_authority", required=_ARTIFACT_FIELDS)
    return SemanticRefreshArtifactAuthority(
        provider=_text(raw, "provider"),
        provider_profile=_text(raw, "provider_profile"),
        endpoint_authority_id=_text(raw, "endpoint_authority_id"),
        bucket_or_container_authority_id=_text(raw, "bucket_or_container_authority_id"),
        kms_key_authority_id=_text(raw, "kms_key_authority_id"),
        capability_evidence_sha256=_digest(raw, "capability_evidence_sha256"),
        writer_scope=_text(raw, "writer_scope"),
        artifact_prefix=_text(raw, "artifact_prefix"),
        encryption_policy_sha256=_digest(raw, "encryption_policy_sha256"),
        retention_policy_id=_text(raw, "retention_policy_id"),
        retention_policy_sha256=_digest(raw, "retention_policy_sha256"),
        retention_days=require_positive_int(raw.get("retention_days"), "retention_days"),
        retention_issued_at=_text(raw, "retention_issued_at"),
        retention_until=_text(raw, "retention_until"),
        max_artifact_bytes=require_positive_int(raw.get("max_artifact_bytes"), "max_artifact_bytes"),
    )


def _array(value: object, field: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes) or not value:
        raise SemanticRefreshContractError(f"{field} must be a non-empty array")
    return value


def _text(raw: Mapping[str, object], field: str) -> str:
    return require_text(raw.get(field), field)


def _digest(raw: Mapping[str, object], field: str) -> str:
    return require_digest(raw.get(field), field)


__all__ = ["semantic_refresh_plan_bundle_from_mapping"]
