"""Deterministic schemas for semantic-refresh baseline, route and artifacts."""

from __future__ import annotations

from typing import Any

from dpone.contracts.semantic_refresh_artifact_manifest import SEALED_ARTIFACT_MANIFEST_SCHEMA
from dpone.contracts.semantic_refresh_baseline_receipt import (
    BASELINE_ADOPTION_RECEIPT_SCHEMA,
    BaselineAssuranceKind,
)
from dpone.contracts.semantic_refresh_route_certification import (
    ROUTE_LIVE_CERTIFICATION_RECEIPT_SCHEMA,
    LiveCertificationStatus,
)
from dpone.contracts.semantic_refresh_schema_common import (
    DIGEST_SCHEMA,
    POSITIVE_INTEGER_SCHEMA,
    TEXT_SCHEMA,
    closed_schema,
    schema_discriminator,
)

_UTC_TIMESTAMP: dict[str, object] = {"format": "date-time", "pattern": "Z$", "type": "string"}
_NONNEGATIVE_INTEGER: dict[str, object] = {"minimum": 0, "type": "integer"}
_UUID: dict[str, object] = {
    "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    "type": "string",
}


def evidence_contract_schemas() -> dict[str, dict[str, Any]]:
    """Return fresh baseline, live-certification and sealed-manifest schemas."""

    return {
        BASELINE_ADOPTION_RECEIPT_SCHEMA: _baseline_schema(),
        ROUTE_LIVE_CERTIFICATION_RECEIPT_SCHEMA: _route_certification_schema(),
        SEALED_ARTIFACT_MANIFEST_SCHEMA: _artifact_manifest_schema(),
    }


def _baseline_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "adopted_at": _UTC_TIMESTAMP,
        "baseline_kind": {"enum": [item.value for item in BaselineAssuranceKind], "type": "string"},
        "baseline_adoption_receipt_sha256": DIGEST_SCHEMA,
        "certified_codec_mapping_sha256": DIGEST_SCHEMA,
        "clickhouse_assurance_sha256": DIGEST_SCHEMA,
        "clickhouse_coverage_sha256": DIGEST_SCHEMA,
        "clickhouse_generation": POSITIVE_INTEGER_SCHEMA,
        "clickhouse_cluster_authority_id": TEXT_SCHEMA,
        "clickhouse_key_sha256": DIGEST_SCHEMA,
        "clickhouse_physical_sha256": DIGEST_SCHEMA,
        "clickhouse_relation_id": TEXT_SCHEMA,
        "clickhouse_schema_sha256": DIGEST_SCHEMA,
        "clickhouse_target_uuid": _UUID,
        "clickhouse_target_authority_id": TEXT_SCHEMA,
        "coverage_end": _UTC_TIMESTAMP,
        "coverage_start": _UTC_TIMESTAMP,
        "ddl_assurance_sha256": DIGEST_SCHEMA,
        "deployment_id": DIGEST_SCHEMA,
        "historical_clickhouse_internal_multiset_conformance": {"const": "PROVEN"},
        "historical_clickhouse_internal_multiset_evidence_sha256": DIGEST_SCHEMA,
        "historical_cross_engine_payload_value_equivalence": {"const": "NOT_CLAIMED"},
        "model_unique_id": TEXT_SCHEMA,
        "mssql_assurance_sha256": DIGEST_SCHEMA,
        "mssql_connection_authority_id": TEXT_SCHEMA,
        "mssql_coverage_sha256": DIGEST_SCHEMA,
        "mssql_generation": POSITIVE_INTEGER_SCHEMA,
        "mssql_key_sha256": DIGEST_SCHEMA,
        "mssql_physical_sha256": DIGEST_SCHEMA,
        "mssql_relation_id": TEXT_SCHEMA,
        "mssql_schema_sha256": DIGEST_SCHEMA,
        "mssql_target_authority_id": TEXT_SCHEMA,
        "release_id": DIGEST_SCHEMA,
        "schema": schema_discriminator(BASELINE_ADOPTION_RECEIPT_SCHEMA),
        "source_assurance_sha256": DIGEST_SCHEMA,
        "source_coverage_sha256": DIGEST_SCHEMA,
        "source_generation": POSITIVE_INTEGER_SCHEMA,
        "source_key_sha256": DIGEST_SCHEMA,
        "source_physical_sha256": DIGEST_SCHEMA,
        "source_relation_id": TEXT_SCHEMA,
        "source_schema_sha256": DIGEST_SCHEMA,
        "source_snapshot_sha256": DIGEST_SCHEMA,
        "status": {"const": "COMPLETE"},
        "utc_assurance_sha256": DIGEST_SCHEMA,
        "writer_assurance_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        BASELINE_ADOPTION_RECEIPT_SCHEMA,
        properties,
        sorted(properties),
        comment=(
            "Both baseline paths bind protected engine authorities and complete source/MSSQL/ClickHouse state. "
            "Only ClickHouse-internal multiset conformance is PROVEN; historical cross-engine payload value "
            "equivalence remains NOT_CLAIMED and is never inferred from counts."
        ),
    )


def _route_coordinates_schema() -> dict[str, object]:
    properties: dict[str, object] = {
        "capability_policy_sha256": DIGEST_SCHEMA,
        "environment": TEXT_SCHEMA,
        "load_strategy": TEXT_SCHEMA,
        "route_policy_sha256": DIGEST_SCHEMA,
        "runtime_image_digest": DIGEST_SCHEMA,
        "sink_capability_sha256": DIGEST_SCHEMA,
        "sink_connector": TEXT_SCHEMA,
        "sink_connector_version": TEXT_SCHEMA,
        "source_capability_sha256": DIGEST_SCHEMA,
        "source_connector": TEXT_SCHEMA,
        "source_connector_version": TEXT_SCHEMA,
        "toolchain_sha256": DIGEST_SCHEMA,
    }
    return {
        "additionalProperties": False,
        "properties": properties,
        "required": sorted(properties),
        "type": "object",
    }


def _route_certification_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "certification_policy_sha256": DIGEST_SCHEMA,
        "coordinates": _route_coordinates_schema(),
        "effective_from": _UTC_TIMESTAMP,
        "expires_at": _UTC_TIMESTAMP,
        "failure_matrix_sha256": DIGEST_SCHEMA,
        "issuer_attestation_sha256": DIGEST_SCHEMA,
        "issuer_authority": TEXT_SCHEMA,
        "issuer_signature_sha256": DIGEST_SCHEMA,
        "live_environment_sha256": DIGEST_SCHEMA,
        "live_evidence_sha256": DIGEST_SCHEMA,
        "revocation_reason": TEXT_SCHEMA,
        "revoked_at": _UTC_TIMESTAMP,
        "revoked_receipt_sha256": DIGEST_SCHEMA,
        "route_certification_receipt_sha256": DIGEST_SCHEMA,
        "schema": schema_discriminator(ROUTE_LIVE_CERTIFICATION_RECEIPT_SCHEMA),
        "status": {"enum": [item.value for item in LiveCertificationStatus], "type": "string"},
        "tested_at": _UTC_TIMESTAMP,
    }
    timing = ["tested_at", "effective_from", "expires_at"]
    live = ["live_environment_sha256", "live_evidence_sha256", "failure_matrix_sha256"]
    revoked = ["revoked_receipt_sha256", "revoked_at", "revocation_reason"]
    return closed_schema(
        ROUTE_LIVE_CERTIFICATION_RECEIPT_SCHEMA,
        properties,
        [
            "certification_policy_sha256",
            "coordinates",
            "issuer_attestation_sha256",
            "issuer_authority",
            "issuer_signature_sha256",
            "route_certification_receipt_sha256",
            "schema",
            "status",
        ],
        comment="Only an unexpired, unrevoked exact PASS authorizes a route; local/unavailable execution remains UNVERIFIED.",
        one_of=[
            {
                "not": {"anyOf": [{"required": [field]} for field in revoked]},
                "properties": {"status": {"const": status}},
                "required": [*timing, *live],
            }
            for status in (LiveCertificationStatus.PASS.value, LiveCertificationStatus.FAIL.value)
        ]
        + [
            {
                "not": {"anyOf": [{"required": [field]} for field in [*live, *revoked]]},
                "properties": {"status": {"const": LiveCertificationStatus.UNVERIFIED.value}},
                "required": timing,
            },
            {
                "not": {"anyOf": [{"required": [field]} for field in [*timing, *live]]},
                "properties": {"status": {"const": LiveCertificationStatus.UNVERIFIED.value}},
                "required": revoked,
            },
        ],
    )


def _artifact_manifest_schema() -> dict[str, Any]:
    chunk_properties: dict[str, object] = {
        "byte_count": POSITIVE_INTEGER_SCHEMA,
        "chunk_sha256": DIGEST_SCHEMA,
        "object_key": TEXT_SCHEMA,
        "ordinal": POSITIVE_INTEGER_SCHEMA,
        "provider_version": TEXT_SCHEMA,
        "row_count": _NONNEGATIVE_INTEGER,
    }
    chunk = {
        "additionalProperties": False,
        "properties": chunk_properties,
        "required": sorted(chunk_properties),
        "type": "object",
    }
    properties: dict[str, object] = {
        "artifact_format": {"const": "dpone_parquet_v1"},
        "artifact_manifest_sha256": DIGEST_SCHEMA,
        "artifact_prefix": TEXT_SCHEMA,
        "attempt_binding_sha256": DIGEST_SCHEMA,
        "chunk_count": _NONNEGATIVE_INTEGER,
        "chunks": {"items": chunk, "type": "array"},
        "effective_key_mapping_sha256": DIGEST_SCHEMA,
        "effective_key_template_sha256": DIGEST_SCHEMA,
        "encryption_policy_sha256": DIGEST_SCHEMA,
        "seal_authorization_receipt_sha256": DIGEST_SCHEMA,
        "model_build_receipt_sha256": DIGEST_SCHEMA,
        "model_unique_id": TEXT_SCHEMA,
        "operation_id": DIGEST_SCHEMA,
        "operation_plan_sha256": DIGEST_SCHEMA,
        "provider": TEXT_SCHEMA,
        "publication_order": {"const": "MANIFEST_LAST"},
        "retention_policy_sha256": DIGEST_SCHEMA,
        "route_certification_receipt_sha256": DIGEST_SCHEMA,
        "schema": schema_discriminator(SEALED_ARTIFACT_MANIFEST_SCHEMA),
        "serializer_sha256": DIGEST_SCHEMA,
        "parquet_schema_mapping_sha256": DIGEST_SCHEMA,
        "clickhouse_input_mapping_sha256": DIGEST_SCHEMA,
        "codec_mapping_certification_sha256": DIGEST_SCHEMA,
        "ordered_writable_schema_sha256": DIGEST_SCHEMA,
        "total_bytes": _NONNEGATIVE_INTEGER,
        "total_rows": _NONNEGATIVE_INTEGER,
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        SEALED_ARTIFACT_MANIFEST_SCHEMA,
        properties,
        sorted(properties),
        comment="Python validation requires contiguous ordinals, unique version-pinned keys, and exact total counts.",
    )


__all__ = ["evidence_contract_schemas"]
