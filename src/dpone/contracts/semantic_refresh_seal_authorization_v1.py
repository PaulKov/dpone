"""Closed V1 vocabulary for semantic-refresh seal authorization."""

from enum import Enum

SEAL_AUTHORIZATION_RECEIPT_SCHEMA = "dpone.semantic-refresh-seal-authorization-receipt.v1"
SEAL_AUTHORIZATION_DIGEST_FIELD = "seal_authorization_receipt_sha256"
SEAL_JOURNAL_STATE = "PREPARING"
SEAL_AUTHORIZATION_REQUIRED_FIELDS = frozenset(
    {
        "schema",
        SEAL_AUTHORIZATION_DIGEST_FIELD,
        "operation_id",
        "operation_plan_sha256",
        "model_unique_id",
        "workflow_id",
        "workflow_plan_sha256",
        "workflow_execution_id",
        "workflow_execution_binding_sha256",
        "attempt_binding_sha256",
        "fencing_epoch",
        "journal_version",
        "journal_state",
        "mssql_outcome",
        "event_time_source_type",
        "effective_key_template_sha256",
        "effective_key_mapping_sha256",
        "ordered_writable_schema_sha256",
        "serializer_sha256",
        "parquet_schema_mapping_sha256",
        "clickhouse_input_mapping_sha256",
        "codec_mapping_certification_sha256",
        "before_image_relation_id",
        "before_image_sha256",
        "before_image_row_count",
        "after_image_relation_id",
        "after_image_sha256",
        "after_image_row_count",
        "model_build_receipt_sha256",
        "baseline_adoption_receipt_sha256",
        "route_certification_receipt_sha256",
        "writer_exclusivity_assurance_receipt_sha256",
        "ddl_freeze_assurance_receipt_sha256",
        "artifact_authority_sha256",
        "seal_policy_sha256",
        "created_at",
        "issuer_authority",
        "issuer_attestation_sha256",
        "issuer_signature_sha256",
    }
)
SEAL_AUTHORIZATION_OPTIONAL_FIELDS = frozenset({"utc_semantics_assurance_receipt_sha256"})


class SealEventTimeSourceType(str, Enum):  # noqa: UP042
    """Event-time physical source types supported by the certified cell."""

    DATE = "date"
    DATETIME2 = "datetime2(6)"


__all__ = [
    "SEAL_AUTHORIZATION_DIGEST_FIELD",
    "SEAL_AUTHORIZATION_OPTIONAL_FIELDS",
    "SEAL_AUTHORIZATION_RECEIPT_SCHEMA",
    "SEAL_AUTHORIZATION_REQUIRED_FIELDS",
    "SEAL_JOURNAL_STATE",
    "SealEventTimeSourceType",
]
