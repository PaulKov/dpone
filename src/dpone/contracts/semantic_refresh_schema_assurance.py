"""Deterministic schemas for runtime assurance and transfer authorization."""

from __future__ import annotations

from typing import Any

from dpone.contracts.semantic_refresh_runtime_assurance import (
    RUNTIME_ASSURANCE_RECEIPT_SCHEMA,
    RuntimeAssuranceKind,
    RuntimeAssuranceStatus,
    RuntimeAssuranceSubjectType,
)
from dpone.contracts.semantic_refresh_schema_common import (
    DIGEST_SCHEMA,
    POSITIVE_INTEGER_SCHEMA,
    TEXT_SCHEMA,
    closed_schema,
    schema_discriminator,
)
from dpone.contracts.semantic_refresh_seal_authorization import (
    SEAL_AUTHORIZATION_RECEIPT_SCHEMA,
    SealEventTimeSourceType,
)

_UTC_TIMESTAMP: dict[str, object] = {"format": "date-time", "pattern": "Z$", "type": "string"}
_NONNEGATIVE_INTEGER: dict[str, object] = {"minimum": 0, "type": "integer"}


def assurance_contract_schemas() -> dict[str, dict[str, Any]]:
    """Return fresh runtime-assurance and seal-authorization schemas."""

    return {
        RUNTIME_ASSURANCE_RECEIPT_SCHEMA: _runtime_assurance_schema(),
        SEAL_AUTHORIZATION_RECEIPT_SCHEMA: _seal_authorization_schema(),
    }


def _runtime_subject_schema() -> dict[str, object]:
    properties: dict[str, object] = {
        "assurance_kind": {"enum": [item.value for item in RuntimeAssuranceKind], "type": "string"},
        "column_name": TEXT_SCHEMA,
        "database": TEXT_SCHEMA,
        "deployment_id": DIGEST_SCHEMA,
        "effective_key_template_sha256": DIGEST_SCHEMA,
        "environment": TEXT_SCHEMA,
        "model_definition_proof_sha256": DIGEST_SCHEMA,
        "model_unique_id": TEXT_SCHEMA,
        "mssql_control_database": TEXT_SCHEMA,
        "mssql_control_schema": TEXT_SCHEMA,
        "mssql_image_schema": TEXT_SCHEMA,
        "mssql_target_authority_id": TEXT_SCHEMA,
        "release_id": DIGEST_SCHEMA,
        "route_certification_receipt_sha256": DIGEST_SCHEMA,
        "scope_image_namespace_policy_sha256": DIGEST_SCHEMA,
        "sqlserver_lifecycle_policy_sha256": DIGEST_SCHEMA,
        "subject_type": {"enum": [item.value for item in RuntimeAssuranceSubjectType], "type": "string"},
        "writable_schema_sha256": DIGEST_SCHEMA,
    }
    required = sorted(set(properties) - {"column_name"})
    return {
        "additionalProperties": False,
        "oneOf": [
            {
                "properties": {
                    "assurance_kind": {"const": RuntimeAssuranceKind.UTC_SEMANTICS.value},
                    "subject_type": {"const": RuntimeAssuranceSubjectType.COLUMN.value},
                },
                "required": ["column_name"],
            },
            {
                "not": {"required": ["column_name"]},
                "properties": {
                    "assurance_kind": {
                        "enum": [
                            RuntimeAssuranceKind.WRITER_EXCLUSIVITY.value,
                            RuntimeAssuranceKind.DDL_FREEZE.value,
                        ]
                    },
                    "subject_type": {"const": RuntimeAssuranceSubjectType.TARGET.value},
                },
            },
        ],
        "properties": properties,
        "required": required,
        "type": "object",
    }


def _runtime_assurance_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "acl_policy_version": TEXT_SCHEMA,
        "approver_attestation_sha256": DIGEST_SCHEMA,
        "approver_authority": TEXT_SCHEMA,
        "approver_signature_sha256": DIGEST_SCHEMA,
        "effective_from": _UTC_TIMESTAMP,
        "engine_acl_proof_sha256": DIGEST_SCHEMA,
        "engine_acl_proof_status": {"const": "PASS"},
        "evidence_sha256": DIGEST_SCHEMA,
        "ddl_epoch": POSITIVE_INTEGER_SCHEMA,
        "expires_at": _UTC_TIMESTAMP,
        "external_job_inventory_sha256": DIGEST_SCHEMA,
        "external_job_inventory_status": {"const": "PASS"},
        "organizational_control_sha256": DIGEST_SCHEMA,
        "organizational_control_status": {"const": "CERTIFIED"},
        "platform_allowlist_sha256": DIGEST_SCHEMA,
        "platform_allowlist_status": {"const": "PASS"},
        "producer_version": TEXT_SCHEMA,
        "revocation_reason": TEXT_SCHEMA,
        "revoked_at": _UTC_TIMESTAMP,
        "revoked_receipt_sha256": DIGEST_SCHEMA,
        "runtime_assurance_policy_sha256": DIGEST_SCHEMA,
        "runtime_assurance_receipt_sha256": DIGEST_SCHEMA,
        "schema": schema_discriminator(RUNTIME_ASSURANCE_RECEIPT_SCHEMA),
        "status": {"enum": [item.value for item in RuntimeAssuranceStatus], "type": "string"},
        "subject": _runtime_subject_schema(),
        "transformation_version": TEXT_SCHEMA,
    }
    active = ["effective_from", "expires_at", "evidence_sha256"]
    writer = [
        "engine_acl_proof_sha256",
        "engine_acl_proof_status",
        "platform_allowlist_sha256",
        "platform_allowlist_status",
        "external_job_inventory_sha256",
        "external_job_inventory_status",
        "organizational_control_sha256",
        "organizational_control_status",
    ]
    revoked = ["revoked_receipt_sha256", "revoked_at", "revocation_reason"]
    required = [
        "acl_policy_version",
        "approver_attestation_sha256",
        "approver_authority",
        "approver_signature_sha256",
        "producer_version",
        "runtime_assurance_policy_sha256",
        "runtime_assurance_receipt_sha256",
        "schema",
        "status",
        "subject",
        "transformation_version",
    ]
    return closed_schema(
        RUNTIME_ASSURANCE_RECEIPT_SCHEMA,
        properties,
        required,
        comment=(
            "Authorization additionally requires an external trusted digest whose resolver has verified signature "
            "and the current revocation index. Absence or expiry fails closed."
        ),
        one_of=[
            {
                "not": {"anyOf": [{"required": [field]} for field in [*revoked, "ddl_epoch"]]},
                "properties": {
                    "status": {"const": RuntimeAssuranceStatus.CERTIFIED.value},
                    "subject": {
                        "properties": {"assurance_kind": {"const": RuntimeAssuranceKind.WRITER_EXCLUSIVITY.value}}
                    },
                },
                "required": [*active, *writer],
            },
            {
                "not": {"anyOf": [{"required": [field]} for field in [*writer, *revoked]]},
                "properties": {
                    "status": {"const": RuntimeAssuranceStatus.CERTIFIED.value},
                    "subject": {"properties": {"assurance_kind": {"const": RuntimeAssuranceKind.DDL_FREEZE.value}}},
                },
                "required": [*active, "ddl_epoch"],
            },
            {
                "not": {"anyOf": [{"required": [field]} for field in [*writer, *revoked, "ddl_epoch"]]},
                "properties": {
                    "status": {"const": RuntimeAssuranceStatus.CERTIFIED.value},
                    "subject": {"properties": {"assurance_kind": {"const": RuntimeAssuranceKind.UTC_SEMANTICS.value}}},
                },
                "required": active,
            },
            {
                "not": {"anyOf": [{"required": [field]} for field in [*active, *writer, "ddl_epoch"]]},
                "properties": {"status": {"const": RuntimeAssuranceStatus.REVOKED.value}},
                "required": revoked,
            },
        ],
    )


def _seal_authorization_schema() -> dict[str, Any]:
    digest_fields = {
        "after_image_sha256",
        "artifact_authority_sha256",
        "attempt_binding_sha256",
        "baseline_adoption_receipt_sha256",
        "before_image_sha256",
        "clickhouse_input_mapping_sha256",
        "codec_mapping_certification_sha256",
        "ddl_freeze_assurance_receipt_sha256",
        "effective_key_mapping_sha256",
        "effective_key_template_sha256",
        "issuer_attestation_sha256",
        "issuer_signature_sha256",
        "model_build_receipt_sha256",
        "operation_id",
        "operation_plan_sha256",
        "ordered_writable_schema_sha256",
        "parquet_schema_mapping_sha256",
        "route_certification_receipt_sha256",
        "seal_authorization_receipt_sha256",
        "seal_policy_sha256",
        "serializer_sha256",
        "utc_semantics_assurance_receipt_sha256",
        "workflow_execution_binding_sha256",
        "workflow_plan_sha256",
        "writer_exclusivity_assurance_receipt_sha256",
    }
    properties: dict[str, object] = {field: DIGEST_SCHEMA for field in digest_fields}
    properties.update(
        {
            "after_image_relation_id": TEXT_SCHEMA,
            "after_image_row_count": _NONNEGATIVE_INTEGER,
            "before_image_relation_id": TEXT_SCHEMA,
            "before_image_row_count": _NONNEGATIVE_INTEGER,
            "created_at": _UTC_TIMESTAMP,
            "event_time_source_type": {
                "enum": [item.value for item in SealEventTimeSourceType],
                "type": "string",
            },
            "fencing_epoch": POSITIVE_INTEGER_SCHEMA,
            "issuer_authority": TEXT_SCHEMA,
            "journal_state": {"const": "PREPARING"},
            "journal_version": POSITIVE_INTEGER_SCHEMA,
            "model_unique_id": TEXT_SCHEMA,
            "mssql_outcome": {"const": "COMMITTED_WITH_IMAGES"},
            "schema": schema_discriminator(SEAL_AUTHORIZATION_RECEIPT_SCHEMA),
            "workflow_execution_id": TEXT_SCHEMA,
            "workflow_id": TEXT_SCHEMA,
        }
    )
    required = sorted(set(properties) - {"utc_semantics_assurance_receipt_sha256"})
    return closed_schema(
        SEAL_AUTHORIZATION_RECEIPT_SCHEMA,
        properties,
        required,
        comment="Only a reconciled PREPARING journal with COMMITTED_WITH_IMAGES may authorize sealing.",
        one_of=[
            {
                "not": {"required": ["utc_semantics_assurance_receipt_sha256"]},
                "properties": {"event_time_source_type": {"const": SealEventTimeSourceType.DATE.value}},
            },
            {
                "properties": {"event_time_source_type": {"const": SealEventTimeSourceType.DATETIME2.value}},
                "required": ["utc_semantics_assurance_receipt_sha256"],
            },
        ],
    )


__all__ = ["assurance_contract_schemas"]
