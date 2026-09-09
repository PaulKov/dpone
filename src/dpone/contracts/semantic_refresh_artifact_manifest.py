"""Manifest-last sealed artifact authority for semantic refresh V2."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_artifact_manifest_v1 import (
    ARTIFACT_FORMAT,
    ARTIFACT_MANIFEST_DIGEST_FIELD,
    CHUNK_FIELDS,
    MANIFEST_FIELDS,
    PUBLICATION_ORDER,
    SEAL_PROJECTION_FIELDS,
    SEALED_ARTIFACT_MANIFEST_SCHEMA,
)
from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_evidence_common import require_nonnegative_int
from dpone.contracts.semantic_refresh_seal_authorization import SemanticRefreshSealAuthorizationReceipt


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshArtifactChunk:
    """One immutable version-pinned artifact chunk."""

    ordinal: int
    object_key: str
    provider_version: str
    chunk_sha256: str
    byte_count: int
    row_count: int

    def __post_init__(self) -> None:
        require_positive_int(self.ordinal, "chunk.ordinal")
        for field in ("object_key", "provider_version"):
            require_text(getattr(self, field), f"chunk.{field}")
        require_digest(self.chunk_sha256, "chunk.chunk_sha256")
        require_positive_int(self.byte_count, "chunk.byte_count")
        require_nonnegative_int(self.row_count, "chunk.row_count")
        if self.object_key.startswith("/") or ".." in self.object_key.split("/"):
            raise SemanticRefreshContractError("chunk.object_key must be relative and confined")

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshArtifactChunk:
        """Parse one closed chunk descriptor."""

        raw = require_closed_mapping(value, "chunk", required=CHUNK_FIELDS)
        return cls(
            ordinal=require_positive_int(raw.get("ordinal"), "chunk.ordinal"),
            object_key=require_text(raw.get("object_key"), "chunk.object_key"),
            provider_version=require_text(raw.get("provider_version"), "chunk.provider_version"),
            chunk_sha256=require_digest(raw.get("chunk_sha256"), "chunk.chunk_sha256"),
            byte_count=require_positive_int(raw.get("byte_count"), "chunk.byte_count"),
            row_count=require_nonnegative_int(raw.get("row_count"), "chunk.row_count"),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the canonical chunk descriptor."""

        return {
            "byte_count": self.byte_count,
            "chunk_sha256": self.chunk_sha256,
            "object_key": self.object_key,
            "ordinal": self.ordinal,
            "provider_version": self.provider_version,
            "row_count": self.row_count,
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshSealedArtifactManifest(SemanticRefreshDocumentCodec):
    """Sealed create-only chunk closure published only after all chunks exist."""

    operation_id: str
    operation_plan_sha256: str
    model_unique_id: str
    workflow_execution_binding_sha256: str
    attempt_binding_sha256: str
    seal_authorization_receipt_sha256: str
    model_build_receipt_sha256: str
    serializer_sha256: str
    parquet_schema_mapping_sha256: str
    clickhouse_input_mapping_sha256: str
    codec_mapping_certification_sha256: str
    effective_key_template_sha256: str
    effective_key_mapping_sha256: str
    ordered_writable_schema_sha256: str
    route_certification_receipt_sha256: str
    artifact_prefix: str
    provider: str
    encryption_policy_sha256: str
    retention_policy_sha256: str
    chunks: tuple[SemanticRefreshArtifactChunk, ...]
    chunk_count: int
    total_bytes: int
    total_rows: int
    artifact_manifest_sha256: str
    artifact_format: str = ARTIFACT_FORMAT
    publication_order: str = PUBLICATION_ORDER
    schema: str = SEALED_ARTIFACT_MANIFEST_SCHEMA

    schema_id: ClassVar[str] = SEALED_ARTIFACT_MANIFEST_SCHEMA
    digest_field: ClassVar[str] = ARTIFACT_MANIFEST_DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if self.artifact_format != ARTIFACT_FORMAT or self.publication_order != PUBLICATION_ORDER:
            raise SemanticRefreshContractError("sealed artifact format/publication order is unsupported")
        for field in (
            "operation_id",
            "operation_plan_sha256",
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
            "encryption_policy_sha256",
            "retention_policy_sha256",
        ):
            require_digest(getattr(self, field), field)
        require_text(self.model_unique_id, "model_unique_id")
        require_text(self.artifact_prefix, "artifact_prefix")
        require_text(self.provider, "provider")
        if self.artifact_prefix.startswith("/") or ".." in self.artifact_prefix.split("/"):
            raise SemanticRefreshContractError("artifact_prefix must be relative and confined")
        _validate_chunks(self.chunks)
        if self.chunk_count != len(self.chunks):
            raise SemanticRefreshContractError("chunk_count differs from chunk closure")
        if self.total_bytes != sum(item.byte_count for item in self.chunks):
            raise SemanticRefreshContractError("total_bytes differs from chunk closure")
        if self.total_rows != sum(item.row_count for item in self.chunks):
            raise SemanticRefreshContractError("total_rows differs from chunk closure")
        validate_digest(self._unsigned(), self.artifact_manifest_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        seal_authorization: SemanticRefreshSealAuthorizationReceipt,
        artifact_prefix: str,
        provider: str,
        encryption_policy_sha256: str,
        retention_policy_sha256: str,
        chunks: tuple[SemanticRefreshArtifactChunk, ...],
    ) -> SemanticRefreshSealedArtifactManifest:
        """Build a canonical manifest after validating exact contiguous chunks."""

        if not isinstance(seal_authorization, SemanticRefreshSealAuthorizationReceipt):
            raise SemanticRefreshContractError("seal_authorization must be a canonical typed receipt")
        canonical = _validate_chunks(chunks)
        unsigned = _unsigned_mapping(
            operation_id=seal_authorization.operation_id,
            operation_plan_sha256=seal_authorization.operation_plan_sha256,
            model_unique_id=seal_authorization.model_unique_id,
            workflow_execution_binding_sha256=seal_authorization.workflow_execution_binding_sha256,
            attempt_binding_sha256=seal_authorization.attempt_binding_sha256,
            seal_authorization_receipt_sha256=seal_authorization.seal_authorization_receipt_sha256,
            model_build_receipt_sha256=seal_authorization.model_build_receipt_sha256,
            serializer_sha256=seal_authorization.serializer_sha256,
            parquet_schema_mapping_sha256=seal_authorization.parquet_schema_mapping_sha256,
            clickhouse_input_mapping_sha256=seal_authorization.clickhouse_input_mapping_sha256,
            codec_mapping_certification_sha256=seal_authorization.codec_mapping_certification_sha256,
            effective_key_template_sha256=seal_authorization.effective_key_template_sha256,
            effective_key_mapping_sha256=seal_authorization.effective_key_mapping_sha256,
            ordered_writable_schema_sha256=seal_authorization.ordered_writable_schema_sha256,
            route_certification_receipt_sha256=seal_authorization.route_certification_receipt_sha256,
            artifact_prefix=artifact_prefix,
            provider=provider,
            encryption_policy_sha256=encryption_policy_sha256,
            retention_policy_sha256=retention_policy_sha256,
            chunks=canonical,
            chunk_count=len(canonical),
            total_bytes=sum(item.byte_count for item in canonical),
            total_rows=sum(item.row_count for item in canonical),
        )
        return cls(
            seal_authorization.operation_id,
            seal_authorization.operation_plan_sha256,
            seal_authorization.model_unique_id,
            seal_authorization.workflow_execution_binding_sha256,
            seal_authorization.attempt_binding_sha256,
            seal_authorization.seal_authorization_receipt_sha256,
            seal_authorization.model_build_receipt_sha256,
            seal_authorization.serializer_sha256,
            seal_authorization.parquet_schema_mapping_sha256,
            seal_authorization.clickhouse_input_mapping_sha256,
            seal_authorization.codec_mapping_certification_sha256,
            seal_authorization.effective_key_template_sha256,
            seal_authorization.effective_key_mapping_sha256,
            seal_authorization.ordered_writable_schema_sha256,
            seal_authorization.route_certification_receipt_sha256,
            artifact_prefix,
            provider,
            encryption_policy_sha256,
            retention_policy_sha256,
            canonical,
            len(canonical),
            sum(item.byte_count for item in canonical),
            sum(item.row_count for item in canonical),
            semantic_refresh_sha256(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshSealedArtifactManifest:
        """Parse a closed manifest and verify totals and digest."""

        raw = require_closed_mapping(value, "sealed_artifact_manifest", required=MANIFEST_FIELDS)
        raw_chunks = raw.get("chunks")
        if not isinstance(raw_chunks, Sequence) or isinstance(raw_chunks, str | bytes):
            raise SemanticRefreshContractError("chunks must be an array")
        return cls(
            operation_id=require_digest(raw.get("operation_id"), "operation_id"),
            operation_plan_sha256=require_digest(raw.get("operation_plan_sha256"), "operation_plan_sha256"),
            model_unique_id=require_text(raw.get("model_unique_id"), "model_unique_id"),
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"), "workflow_execution_binding_sha256"
            ),
            attempt_binding_sha256=require_digest(raw.get("attempt_binding_sha256"), "attempt_binding_sha256"),
            seal_authorization_receipt_sha256=require_digest(
                raw.get("seal_authorization_receipt_sha256"), "seal_authorization_receipt_sha256"
            ),
            model_build_receipt_sha256=require_digest(
                raw.get("model_build_receipt_sha256"), "model_build_receipt_sha256"
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
            effective_key_template_sha256=require_digest(
                raw.get("effective_key_template_sha256"), "effective_key_template_sha256"
            ),
            effective_key_mapping_sha256=require_digest(
                raw.get("effective_key_mapping_sha256"), "effective_key_mapping_sha256"
            ),
            ordered_writable_schema_sha256=require_digest(
                raw.get("ordered_writable_schema_sha256"), "ordered_writable_schema_sha256"
            ),
            route_certification_receipt_sha256=require_digest(
                raw.get("route_certification_receipt_sha256"), "route_certification_receipt_sha256"
            ),
            artifact_prefix=require_text(raw.get("artifact_prefix"), "artifact_prefix"),
            provider=require_text(raw.get("provider"), "provider"),
            encryption_policy_sha256=require_digest(raw.get("encryption_policy_sha256"), "encryption_policy_sha256"),
            retention_policy_sha256=require_digest(raw.get("retention_policy_sha256"), "retention_policy_sha256"),
            chunks=tuple(SemanticRefreshArtifactChunk.from_mapping(item) for item in raw_chunks),
            chunk_count=require_nonnegative_int(raw.get("chunk_count"), "chunk_count"),
            total_bytes=require_nonnegative_int(raw.get("total_bytes"), "total_bytes"),
            total_rows=require_nonnegative_int(raw.get("total_rows"), "total_rows"),
            artifact_manifest_sha256=require_digest(
                raw.get(ARTIFACT_MANIFEST_DIGEST_FIELD), ARTIFACT_MANIFEST_DIGEST_FIELD
            ),
            artifact_format=require_text(raw.get("artifact_format"), "artifact_format"),
            publication_order=require_text(raw.get("publication_order"), "publication_order"),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            operation_id=self.operation_id,
            operation_plan_sha256=self.operation_plan_sha256,
            model_unique_id=self.model_unique_id,
            workflow_execution_binding_sha256=self.workflow_execution_binding_sha256,
            attempt_binding_sha256=self.attempt_binding_sha256,
            seal_authorization_receipt_sha256=self.seal_authorization_receipt_sha256,
            model_build_receipt_sha256=self.model_build_receipt_sha256,
            serializer_sha256=self.serializer_sha256,
            parquet_schema_mapping_sha256=self.parquet_schema_mapping_sha256,
            clickhouse_input_mapping_sha256=self.clickhouse_input_mapping_sha256,
            codec_mapping_certification_sha256=self.codec_mapping_certification_sha256,
            effective_key_template_sha256=self.effective_key_template_sha256,
            effective_key_mapping_sha256=self.effective_key_mapping_sha256,
            ordered_writable_schema_sha256=self.ordered_writable_schema_sha256,
            route_certification_receipt_sha256=self.route_certification_receipt_sha256,
            artifact_prefix=self.artifact_prefix,
            provider=self.provider,
            encryption_policy_sha256=self.encryption_policy_sha256,
            retention_policy_sha256=self.retention_policy_sha256,
            chunks=self.chunks,
            chunk_count=self.chunk_count,
            total_bytes=self.total_bytes,
            total_rows=self.total_rows,
        )

    def matches_seal_authorization(self, receipt: SemanticRefreshSealAuthorizationReceipt) -> bool:
        """Require an authenticated receipt to equal every direct anti-swap projection."""

        if not isinstance(receipt, SemanticRefreshSealAuthorizationReceipt):
            raise SemanticRefreshContractError("receipt must be a canonical seal authorization")
        return bool(
            self.seal_authorization_receipt_sha256 == receipt.seal_authorization_receipt_sha256
            and all(getattr(self, field) == getattr(receipt, field) for field in SEAL_PROJECTION_FIELDS)
        )

    def to_dict(self) -> dict[str, object]:
        """Return the sealed manifest with its digest."""

        return {**self._unsigned(), self.digest_field: self.artifact_manifest_sha256}


def _validate_chunks(values: tuple[SemanticRefreshArtifactChunk, ...]) -> tuple[SemanticRefreshArtifactChunk, ...]:
    if not isinstance(values, tuple) or any(not isinstance(item, SemanticRefreshArtifactChunk) for item in values):
        raise SemanticRefreshContractError("chunks must be a tuple")
    if tuple(item.ordinal for item in values) != tuple(range(1, len(values) + 1)):
        raise SemanticRefreshContractError("chunk ordinals must be contiguous from one")
    keys = tuple(item.object_key for item in values)
    versions = tuple((item.object_key, item.provider_version) for item in values)
    if len(keys) != len(set(keys)) or len(versions) != len(set(versions)):
        raise SemanticRefreshContractError("chunk identities must be unique")
    return values


def _unsigned_mapping(**values: object) -> dict[str, object]:
    chunks = values.pop("chunks")
    assert isinstance(chunks, tuple)
    return {
        "artifact_format": ARTIFACT_FORMAT,
        **values,
        "chunks": [item.to_dict() for item in chunks],
        "publication_order": PUBLICATION_ORDER,
        "schema": SEALED_ARTIFACT_MANIFEST_SCHEMA,
    }


__all__ = [
    "SEALED_ARTIFACT_MANIFEST_SCHEMA",
    "SemanticRefreshArtifactChunk",
    "SemanticRefreshSealedArtifactManifest",
]
