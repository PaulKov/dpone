"""Closed V1 field vocabulary for semantic-refresh sealed manifests."""

SEALED_ARTIFACT_MANIFEST_SCHEMA = "dpone.semantic-refresh-sealed-artifact-manifest.v1"
ARTIFACT_MANIFEST_DIGEST_FIELD = "artifact_manifest_sha256"
ARTIFACT_FORMAT = "dpone_parquet_v1"
PUBLICATION_ORDER = "MANIFEST_LAST"
SEAL_PROJECTION_FIELDS = (
    "operation_id",
    "operation_plan_sha256",
    "model_unique_id",
    "workflow_execution_binding_sha256",
    "attempt_binding_sha256",
    "model_build_receipt_sha256",
    "serializer_sha256",
    "parquet_schema_mapping_sha256",
    "clickhouse_input_mapping_sha256",
    "codec_mapping_certification_sha256",
    "effective_key_template_sha256",
    "effective_key_mapping_sha256",
    "ordered_writable_schema_sha256",
    "route_certification_receipt_sha256",
)
CHUNK_FIELDS = frozenset({"ordinal", "object_key", "provider_version", "chunk_sha256", "byte_count", "row_count"})
MANIFEST_FIELDS = frozenset(
    {
        "schema",
        ARTIFACT_MANIFEST_DIGEST_FIELD,
        "artifact_format",
        "publication_order",
        "operation_id",
        "operation_plan_sha256",
        "model_unique_id",
        "workflow_execution_binding_sha256",
        "attempt_binding_sha256",
        "seal_authorization_receipt_sha256",
        "model_build_receipt_sha256",
        "serializer_sha256",
        "parquet_schema_mapping_sha256",
        "clickhouse_input_mapping_sha256",
        "codec_mapping_certification_sha256",
        "effective_key_template_sha256",
        "effective_key_mapping_sha256",
        "ordered_writable_schema_sha256",
        "route_certification_receipt_sha256",
        "artifact_prefix",
        "provider",
        "encryption_policy_sha256",
        "retention_policy_sha256",
        "chunk_count",
        "total_bytes",
        "total_rows",
        "chunks",
    }
)

__all__ = [
    "ARTIFACT_FORMAT",
    "ARTIFACT_MANIFEST_DIGEST_FIELD",
    "CHUNK_FIELDS",
    "MANIFEST_FIELDS",
    "PUBLICATION_ORDER",
    "SEALED_ARTIFACT_MANIFEST_SCHEMA",
    "SEAL_PROJECTION_FIELDS",
]
