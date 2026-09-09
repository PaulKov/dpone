"""MSSQL-backed transfer authorization for semantic-refresh artifact sealing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_evidence_common import require_nonnegative_int, require_utc_timestamp
from dpone.contracts.semantic_refresh_seal_authorization_v1 import (
    SEAL_AUTHORIZATION_DIGEST_FIELD,
    SEAL_AUTHORIZATION_OPTIONAL_FIELDS,
    SEAL_AUTHORIZATION_RECEIPT_SCHEMA,
    SEAL_AUTHORIZATION_REQUIRED_FIELDS,
    SEAL_JOURNAL_STATE,
    SealEventTimeSourceType,
)
from dpone.contracts.semantic_refresh_types import SqlServerModelOutcome


@dataclass(frozen=True, slots=True)
class SemanticRefreshSealAuthorizationReceipt(SemanticRefreshDocumentCodec):
    """Immutable authorization emitted only after committed-image reconciliation."""

    operation_id: str
    operation_plan_sha256: str
    model_unique_id: str
    workflow_id: str
    workflow_plan_sha256: str
    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    journal_version: int
    event_time_source_type: SealEventTimeSourceType
    effective_key_template_sha256: str
    effective_key_mapping_sha256: str
    ordered_writable_schema_sha256: str
    serializer_sha256: str
    parquet_schema_mapping_sha256: str
    clickhouse_input_mapping_sha256: str
    codec_mapping_certification_sha256: str
    before_image_relation_id: str
    before_image_sha256: str
    before_image_row_count: int
    after_image_relation_id: str
    after_image_sha256: str
    after_image_row_count: int
    model_build_receipt_sha256: str
    baseline_adoption_receipt_sha256: str
    route_certification_receipt_sha256: str
    writer_exclusivity_assurance_receipt_sha256: str
    ddl_freeze_assurance_receipt_sha256: str
    artifact_authority_sha256: str
    seal_policy_sha256: str
    created_at: str
    issuer_authority: str
    issuer_attestation_sha256: str
    issuer_signature_sha256: str
    seal_authorization_receipt_sha256: str
    utc_semantics_assurance_receipt_sha256: str | None = None
    journal_state: str = SEAL_JOURNAL_STATE
    mssql_outcome: SqlServerModelOutcome = SqlServerModelOutcome.COMMITTED_WITH_IMAGES
    schema: str = SEAL_AUTHORIZATION_RECEIPT_SCHEMA

    schema_id: ClassVar[str] = SEAL_AUTHORIZATION_RECEIPT_SCHEMA
    digest_field: ClassVar[str] = SEAL_AUTHORIZATION_DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if self.journal_state != SEAL_JOURNAL_STATE:
            raise SemanticRefreshContractError("seal authorization journal_state must be PREPARING")
        if self.mssql_outcome is not SqlServerModelOutcome.COMMITTED_WITH_IMAGES:
            raise SemanticRefreshContractError("seal authorization mssql_outcome must be COMMITTED_WITH_IMAGES")
        if not isinstance(self.event_time_source_type, SealEventTimeSourceType):
            raise SemanticRefreshContractError("event_time_source_type is unsupported")
        if self.event_time_source_type is SealEventTimeSourceType.DATETIME2:
            require_digest(
                self.utc_semantics_assurance_receipt_sha256,
                "utc_semantics_assurance_receipt_sha256",
            )
        elif self.utc_semantics_assurance_receipt_sha256 is not None:
            raise SemanticRefreshContractError("date event time forbids UTC semantics assurance receipt")
        for field in (
            "model_unique_id",
            "workflow_id",
            "workflow_execution_id",
            "before_image_relation_id",
            "after_image_relation_id",
            "issuer_authority",
        ):
            require_text(getattr(self, field), field)
        for field in (
            "operation_id",
            "operation_plan_sha256",
            "effective_key_template_sha256",
            "effective_key_mapping_sha256",
            "ordered_writable_schema_sha256",
            "serializer_sha256",
            "parquet_schema_mapping_sha256",
            "clickhouse_input_mapping_sha256",
            "codec_mapping_certification_sha256",
            "workflow_plan_sha256",
            "workflow_execution_binding_sha256",
            "attempt_binding_sha256",
            "before_image_sha256",
            "after_image_sha256",
            "model_build_receipt_sha256",
            "baseline_adoption_receipt_sha256",
            "route_certification_receipt_sha256",
            "writer_exclusivity_assurance_receipt_sha256",
            "ddl_freeze_assurance_receipt_sha256",
            "artifact_authority_sha256",
            "seal_policy_sha256",
            "issuer_attestation_sha256",
            "issuer_signature_sha256",
        ):
            require_digest(getattr(self, field), field)
        require_positive_int(self.fencing_epoch, "fencing_epoch")
        require_positive_int(self.journal_version, "journal_version")
        require_nonnegative_int(self.before_image_row_count, "before_image_row_count")
        require_nonnegative_int(self.after_image_row_count, "after_image_row_count")
        require_utc_timestamp(self.created_at, "created_at")
        validate_digest(self._unsigned(), self.seal_authorization_receipt_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        operation_id: str,
        operation_plan_sha256: str,
        model_unique_id: str,
        workflow_id: str,
        workflow_plan_sha256: str,
        workflow_execution_id: str,
        workflow_execution_binding_sha256: str,
        attempt_binding_sha256: str,
        fencing_epoch: int,
        journal_version: int,
        event_time_source_type: str,
        effective_key_template_sha256: str,
        effective_key_mapping_sha256: str,
        ordered_writable_schema_sha256: str,
        serializer_sha256: str,
        parquet_schema_mapping_sha256: str,
        clickhouse_input_mapping_sha256: str,
        codec_mapping_certification_sha256: str,
        before_image_relation_id: str,
        before_image_sha256: str,
        before_image_row_count: int,
        after_image_relation_id: str,
        after_image_sha256: str,
        after_image_row_count: int,
        model_build_receipt_sha256: str,
        baseline_adoption_receipt_sha256: str,
        route_certification_receipt_sha256: str,
        writer_exclusivity_assurance_receipt_sha256: str,
        utc_semantics_assurance_receipt_sha256: str | None,
        ddl_freeze_assurance_receipt_sha256: str,
        artifact_authority_sha256: str,
        seal_policy_sha256: str,
        created_at: str,
        issuer_authority: str,
        issuer_attestation_sha256: str,
        issuer_signature_sha256: str,
    ) -> SemanticRefreshSealAuthorizationReceipt:
        """Build transfer authority for one reconciled COMMITTED_WITH_IMAGES attempt."""

        unsigned: dict[str, object] = {
            "after_image_relation_id": after_image_relation_id,
            "after_image_row_count": after_image_row_count,
            "after_image_sha256": after_image_sha256,
            "artifact_authority_sha256": artifact_authority_sha256,
            "attempt_binding_sha256": attempt_binding_sha256,
            "baseline_adoption_receipt_sha256": baseline_adoption_receipt_sha256,
            "before_image_relation_id": before_image_relation_id,
            "before_image_row_count": before_image_row_count,
            "before_image_sha256": before_image_sha256,
            "created_at": created_at,
            "clickhouse_input_mapping_sha256": clickhouse_input_mapping_sha256,
            "codec_mapping_certification_sha256": codec_mapping_certification_sha256,
            "ddl_freeze_assurance_receipt_sha256": ddl_freeze_assurance_receipt_sha256,
            "effective_key_mapping_sha256": effective_key_mapping_sha256,
            "effective_key_template_sha256": effective_key_template_sha256,
            "event_time_source_type": event_time_source_type,
            "fencing_epoch": fencing_epoch,
            "issuer_attestation_sha256": issuer_attestation_sha256,
            "issuer_authority": issuer_authority,
            "issuer_signature_sha256": issuer_signature_sha256,
            "journal_state": SEAL_JOURNAL_STATE,
            "journal_version": journal_version,
            "model_unique_id": model_unique_id,
            "model_build_receipt_sha256": model_build_receipt_sha256,
            "mssql_outcome": SqlServerModelOutcome.COMMITTED_WITH_IMAGES.value,
            "operation_id": operation_id,
            "operation_plan_sha256": operation_plan_sha256,
            "ordered_writable_schema_sha256": ordered_writable_schema_sha256,
            "parquet_schema_mapping_sha256": parquet_schema_mapping_sha256,
            "route_certification_receipt_sha256": route_certification_receipt_sha256,
            "schema": cls.schema_id,
            "seal_policy_sha256": seal_policy_sha256,
            "serializer_sha256": serializer_sha256,
            "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
            "workflow_execution_id": workflow_execution_id,
            "workflow_id": workflow_id,
            "workflow_plan_sha256": workflow_plan_sha256,
            "writer_exclusivity_assurance_receipt_sha256": writer_exclusivity_assurance_receipt_sha256,
        }
        if utc_semantics_assurance_receipt_sha256 is not None:
            unsigned["utc_semantics_assurance_receipt_sha256"] = utc_semantics_assurance_receipt_sha256
        return cls.from_mapping({**unsigned, cls.digest_field: semantic_refresh_sha256(unsigned)})

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshSealAuthorizationReceipt:
        """Parse one closed transfer authorization and recompute its digest."""

        raw = require_closed_mapping(
            value,
            "seal_authorization_receipt",
            required=SEAL_AUTHORIZATION_REQUIRED_FIELDS,
            optional=SEAL_AUTHORIZATION_OPTIONAL_FIELDS,
        )
        if "utc_semantics_assurance_receipt_sha256" in raw and raw["utc_semantics_assurance_receipt_sha256"] is None:
            raise SemanticRefreshContractError("optional UTC semantics assurance receipt cannot be null")
        return cls(
            operation_id=require_digest(raw.get("operation_id"), "operation_id"),
            operation_plan_sha256=require_digest(raw.get("operation_plan_sha256"), "operation_plan_sha256"),
            model_unique_id=require_text(raw.get("model_unique_id"), "model_unique_id"),
            workflow_id=require_text(raw.get("workflow_id"), "workflow_id"),
            workflow_plan_sha256=require_digest(raw.get("workflow_plan_sha256"), "workflow_plan_sha256"),
            workflow_execution_id=require_text(raw.get("workflow_execution_id"), "workflow_execution_id"),
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"), "workflow_execution_binding_sha256"
            ),
            attempt_binding_sha256=require_digest(raw.get("attempt_binding_sha256"), "attempt_binding_sha256"),
            fencing_epoch=require_positive_int(raw.get("fencing_epoch"), "fencing_epoch"),
            journal_version=require_positive_int(raw.get("journal_version"), "journal_version"),
            event_time_source_type=require_enum(
                raw.get("event_time_source_type"), "event_time_source_type", SealEventTimeSourceType
            ),
            effective_key_template_sha256=require_digest(
                raw.get("effective_key_template_sha256"), "effective_key_template_sha256"
            ),
            effective_key_mapping_sha256=require_digest(
                raw.get("effective_key_mapping_sha256"), "effective_key_mapping_sha256"
            ),
            ordered_writable_schema_sha256=require_digest(
                raw.get("ordered_writable_schema_sha256"), "ordered_writable_schema_sha256"
            ),
            serializer_sha256=require_digest(raw.get("serializer_sha256"), "serializer_sha256"),
            parquet_schema_mapping_sha256=require_digest(
                raw.get("parquet_schema_mapping_sha256"), "parquet_schema_mapping_sha256"
            ),
            clickhouse_input_mapping_sha256=require_digest(
                raw.get("clickhouse_input_mapping_sha256"), "clickhouse_input_mapping_sha256"
            ),
            codec_mapping_certification_sha256=require_digest(
                raw.get("codec_mapping_certification_sha256"), "codec_mapping_certification_sha256"
            ),
            before_image_relation_id=require_text(raw.get("before_image_relation_id"), "before_image_relation_id"),
            before_image_sha256=require_digest(raw.get("before_image_sha256"), "before_image_sha256"),
            before_image_row_count=require_nonnegative_int(raw.get("before_image_row_count"), "before_image_row_count"),
            after_image_relation_id=require_text(raw.get("after_image_relation_id"), "after_image_relation_id"),
            after_image_sha256=require_digest(raw.get("after_image_sha256"), "after_image_sha256"),
            after_image_row_count=require_nonnegative_int(raw.get("after_image_row_count"), "after_image_row_count"),
            model_build_receipt_sha256=require_digest(
                raw.get("model_build_receipt_sha256"), "model_build_receipt_sha256"
            ),
            baseline_adoption_receipt_sha256=require_digest(
                raw.get("baseline_adoption_receipt_sha256"), "baseline_adoption_receipt_sha256"
            ),
            route_certification_receipt_sha256=require_digest(
                raw.get("route_certification_receipt_sha256"), "route_certification_receipt_sha256"
            ),
            writer_exclusivity_assurance_receipt_sha256=require_digest(
                raw.get("writer_exclusivity_assurance_receipt_sha256"),
                "writer_exclusivity_assurance_receipt_sha256",
            ),
            ddl_freeze_assurance_receipt_sha256=require_digest(
                raw.get("ddl_freeze_assurance_receipt_sha256"), "ddl_freeze_assurance_receipt_sha256"
            ),
            artifact_authority_sha256=require_digest(raw.get("artifact_authority_sha256"), "artifact_authority_sha256"),
            seal_policy_sha256=require_digest(raw.get("seal_policy_sha256"), "seal_policy_sha256"),
            created_at=require_utc_timestamp(raw.get("created_at"), "created_at"),
            issuer_authority=require_text(raw.get("issuer_authority"), "issuer_authority"),
            issuer_attestation_sha256=require_digest(raw.get("issuer_attestation_sha256"), "issuer_attestation_sha256"),
            issuer_signature_sha256=require_digest(raw.get("issuer_signature_sha256"), "issuer_signature_sha256"),
            seal_authorization_receipt_sha256=require_digest(
                raw.get(SEAL_AUTHORIZATION_DIGEST_FIELD), SEAL_AUTHORIZATION_DIGEST_FIELD
            ),
            utc_semantics_assurance_receipt_sha256=(
                require_digest(
                    raw["utc_semantics_assurance_receipt_sha256"],
                    "utc_semantics_assurance_receipt_sha256",
                )
                if "utc_semantics_assurance_receipt_sha256" in raw
                else None
            ),
            journal_state=require_text(raw.get("journal_state"), "journal_state"),
            mssql_outcome=require_enum(raw.get("mssql_outcome"), "mssql_outcome", SqlServerModelOutcome),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        result: dict[str, object] = {
            "after_image_relation_id": self.after_image_relation_id,
            "after_image_row_count": self.after_image_row_count,
            "after_image_sha256": self.after_image_sha256,
            "artifact_authority_sha256": self.artifact_authority_sha256,
            "attempt_binding_sha256": self.attempt_binding_sha256,
            "baseline_adoption_receipt_sha256": self.baseline_adoption_receipt_sha256,
            "before_image_relation_id": self.before_image_relation_id,
            "before_image_row_count": self.before_image_row_count,
            "before_image_sha256": self.before_image_sha256,
            "clickhouse_input_mapping_sha256": self.clickhouse_input_mapping_sha256,
            "codec_mapping_certification_sha256": self.codec_mapping_certification_sha256,
            "created_at": self.created_at,
            "ddl_freeze_assurance_receipt_sha256": self.ddl_freeze_assurance_receipt_sha256,
            "effective_key_mapping_sha256": self.effective_key_mapping_sha256,
            "effective_key_template_sha256": self.effective_key_template_sha256,
            "event_time_source_type": self.event_time_source_type.value,
            "fencing_epoch": self.fencing_epoch,
            "issuer_attestation_sha256": self.issuer_attestation_sha256,
            "issuer_authority": self.issuer_authority,
            "issuer_signature_sha256": self.issuer_signature_sha256,
            "journal_state": self.journal_state,
            "journal_version": self.journal_version,
            "model_build_receipt_sha256": self.model_build_receipt_sha256,
            "model_unique_id": self.model_unique_id,
            "mssql_outcome": self.mssql_outcome.value,
            "operation_id": self.operation_id,
            "operation_plan_sha256": self.operation_plan_sha256,
            "ordered_writable_schema_sha256": self.ordered_writable_schema_sha256,
            "parquet_schema_mapping_sha256": self.parquet_schema_mapping_sha256,
            "route_certification_receipt_sha256": self.route_certification_receipt_sha256,
            "schema": self.schema,
            "seal_policy_sha256": self.seal_policy_sha256,
            "serializer_sha256": self.serializer_sha256,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
            "workflow_execution_id": self.workflow_execution_id,
            "workflow_id": self.workflow_id,
            "workflow_plan_sha256": self.workflow_plan_sha256,
            "writer_exclusivity_assurance_receipt_sha256": self.writer_exclusivity_assurance_receipt_sha256,
        }
        if self.utc_semantics_assurance_receipt_sha256 is not None:
            result["utc_semantics_assurance_receipt_sha256"] = self.utc_semantics_assurance_receipt_sha256
        return result

    def to_dict(self) -> dict[str, object]:
        """Return the exact transfer authority including its canonical digest."""

        return {**self._unsigned(), self.digest_field: self.seal_authorization_receipt_sha256}
