"""Closed codec for one protected MSSQL model-resource authority."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.ports.semantic_refresh_mssql_authority_codec_values import (
    closed_mapping,
    mapping,
    mapping_bool,
    mapping_int,
    mapping_sequence,
    mapping_text,
    optional_mapping_text,
)
from dpone.ports.semantic_refresh_mssql_authority_models import MssqlModelResourceAuthority
from dpone.ports.semantic_refresh_mssql_authority_records import (
    MssqlProtectedArtifactAuthority,
    MssqlProtectedResourcePolicy,
    MssqlProtectedWritableColumn,
)


def model_resource_from_mapping(value: object) -> MssqlModelResourceAuthority:
    """Parse one closed protected model-resource authority."""

    fields = {
        "baseline_receipt_sha256",
        "baseline_kind",
        "baseline_receipt_json",
        "baseline_status",
        "baseline_clickhouse_generation",
        "baseline_clickhouse_target_uuid",
        "baseline_target_predecessor_generation_id",
        "artifact_authority",
        "clickhouse_cluster_authority_id",
        "clickhouse_target_uuid",
        "ddl_freeze_assurance_receipt_sha256",
        "ddl_epoch",
        "effective_key_mapping_sha256",
        "effective_key_template_sha256",
        "model_definition_proof_sha256",
        "model_unique_id",
        "mssql_control_database",
        "mssql_control_schema",
        "mssql_image_schema",
        "mssql_connection_authority_id",
        "mssql_target_authority_id",
        "publication_database",
        "publication_scope_id",
        "publication_target_table",
        "resource_policy",
        "route_certification_receipt_sha256",
        "scope_image_namespace_policy_sha256",
        "strategy_template_sha256",
        "target_authority_id",
        "target_head_authority_receipt_sha256",
        "target_head_terminal_receipt_sha256",
        "target_predecessor_generation",
        "target_predecessor_generation_id",
        "target_predecessor_operation_id",
        "target_resource_id",
        "utc_semantics_assurance_receipt_sha256",
        "writable_columns",
        "writable_schema_sha256",
        "writer_exclusivity_assurance_receipt_sha256",
    }
    raw = closed_mapping(value, "model_resource", fields)
    artifact = mapping(raw, "artifact_authority")
    return MssqlModelResourceAuthority(
        model_unique_id=mapping_text(raw, "model_unique_id"),
        target_resource_id=mapping_text(raw, "target_resource_id"),
        target_authority_id=mapping_text(raw, "target_authority_id"),
        publication_database=mapping_text(raw, "publication_database"),
        publication_target_table=mapping_text(raw, "publication_target_table"),
        publication_scope_id=mapping_text(raw, "publication_scope_id"),
        mssql_connection_authority_id=mapping_text(raw, "mssql_connection_authority_id"),
        mssql_target_authority_id=mapping_text(raw, "mssql_target_authority_id"),
        clickhouse_cluster_authority_id=mapping_text(raw, "clickhouse_cluster_authority_id"),
        mssql_control_database=mapping_text(raw, "mssql_control_database"),
        mssql_control_schema=mapping_text(raw, "mssql_control_schema"),
        mssql_image_schema=mapping_text(raw, "mssql_image_schema"),
        scope_image_namespace_policy_sha256=mapping_text(raw, "scope_image_namespace_policy_sha256"),
        strategy_template_sha256=mapping_text(raw, "strategy_template_sha256"),
        baseline_receipt_sha256=mapping_text(raw, "baseline_receipt_sha256"),
        baseline_kind=mapping_text(raw, "baseline_kind"),
        baseline_receipt_json=mapping_text(raw, "baseline_receipt_json"),
        baseline_status=mapping_text(raw, "baseline_status"),
        baseline_clickhouse_generation=mapping_int(raw, "baseline_clickhouse_generation"),
        baseline_clickhouse_target_uuid=mapping_text(raw, "baseline_clickhouse_target_uuid"),
        baseline_target_predecessor_generation_id=mapping_text(raw, "baseline_target_predecessor_generation_id"),
        clickhouse_target_uuid=mapping_text(raw, "clickhouse_target_uuid"),
        target_predecessor_generation=mapping_int(raw, "target_predecessor_generation"),
        target_predecessor_generation_id=mapping_text(raw, "target_predecessor_generation_id"),
        target_predecessor_operation_id=mapping_text(raw, "target_predecessor_operation_id"),
        target_head_authority_receipt_sha256=mapping_text(raw, "target_head_authority_receipt_sha256"),
        target_head_terminal_receipt_sha256=optional_mapping_text(raw, "target_head_terminal_receipt_sha256"),
        model_definition_proof_sha256=mapping_text(raw, "model_definition_proof_sha256"),
        effective_key_template_sha256=mapping_text(raw, "effective_key_template_sha256"),
        effective_key_mapping_sha256=mapping_text(raw, "effective_key_mapping_sha256"),
        writable_columns=tuple(
            MssqlProtectedWritableColumn(
                name=mapping_text(item, "name"),
                source_type=mapping_text(item, "source_type"),
                target_type=mapping_text(item, "target_type"),
                nullable=mapping_bool(item, "nullable"),
                writable_role=mapping_text(item, "writable_role"),
            )
            for item in mapping_sequence(raw, "writable_columns")
        ),
        writable_schema_sha256=mapping_text(raw, "writable_schema_sha256"),
        resource_policy=resource_policy_from_mapping(raw),
        route_certification_receipt_sha256=mapping_text(raw, "route_certification_receipt_sha256"),
        writer_exclusivity_assurance_receipt_sha256=mapping_text(raw, "writer_exclusivity_assurance_receipt_sha256"),
        ddl_freeze_assurance_receipt_sha256=mapping_text(raw, "ddl_freeze_assurance_receipt_sha256"),
        ddl_epoch=mapping_int(raw, "ddl_epoch"),
        artifact_authority=MssqlProtectedArtifactAuthority(
            provider=mapping_text(artifact, "provider"),
            provider_profile=mapping_text(artifact, "provider_profile"),
            endpoint_authority_id=mapping_text(artifact, "endpoint_authority_id"),
            bucket_or_container_authority_id=mapping_text(artifact, "bucket_or_container_authority_id"),
            kms_key_authority_id=mapping_text(artifact, "kms_key_authority_id"),
            capability_evidence_sha256=mapping_text(artifact, "capability_evidence_sha256"),
            writer_scope=mapping_text(artifact, "writer_scope"),
            artifact_prefix=mapping_text(artifact, "artifact_prefix"),
            encryption_policy_sha256=mapping_text(artifact, "encryption_policy_sha256"),
            retention_policy_id=mapping_text(artifact, "retention_policy_id"),
            retention_policy_sha256=mapping_text(artifact, "retention_policy_sha256"),
            retention_days=mapping_int(artifact, "retention_days"),
            retention_issued_at=mapping_text(artifact, "retention_issued_at"),
            retention_until=mapping_text(artifact, "retention_until"),
            max_artifact_bytes=mapping_int(artifact, "max_artifact_bytes"),
        ),
        utc_semantics_assurance_receipt_sha256=optional_mapping_text(raw, "utc_semantics_assurance_receipt_sha256"),
    )


def resource_policy_from_mapping(value: Mapping[str, object]) -> MssqlProtectedResourcePolicy:
    """Parse the nested protected resource policy."""

    policy = mapping(value, "resource_policy")
    return MssqlProtectedResourcePolicy(
        max_source_scope_rows=mapping_int(policy, "max_source_scope_rows"),
        max_target_scope_rows=mapping_int(policy, "max_target_scope_rows"),
        max_before_image_rows=mapping_int(policy, "max_before_image_rows"),
        max_before_image_bytes=mapping_int(policy, "max_before_image_bytes"),
        max_after_image_rows=mapping_int(policy, "max_after_image_rows"),
        max_after_image_bytes=mapping_int(policy, "max_after_image_bytes"),
        max_transaction_log_bytes=mapping_int(policy, "max_transaction_log_bytes"),
        min_mssql_log_free_bytes=mapping_int(policy, "min_mssql_log_free_bytes"),
        max_mssql_scope_lock_seconds=mapping_int(policy, "max_mssql_scope_lock_seconds"),
        max_mssql_version_store_bytes=mapping_int(policy, "max_mssql_version_store_bytes"),
        max_scope_image_total_bytes=mapping_int(policy, "max_scope_image_total_bytes"),
        max_dbt_temp_rows=mapping_int(policy, "max_dbt_temp_rows"),
        max_dbt_temp_bytes=mapping_int(policy, "max_dbt_temp_bytes"),
        max_statement_seconds=mapping_int(policy, "max_statement_seconds"),
        max_clickhouse_staging_bytes=mapping_int(policy, "max_clickhouse_staging_bytes"),
        max_clickhouse_shadow_bytes=mapping_int(policy, "max_clickhouse_shadow_bytes"),
        max_clickhouse_retained_backup_bytes=mapping_int(policy, "max_clickhouse_retained_backup_bytes"),
        max_clickhouse_total_transient_bytes=mapping_int(policy, "max_clickhouse_total_transient_bytes"),
        resource_policy_sha256=mapping_text(policy, "resource_policy_sha256"),
    )
