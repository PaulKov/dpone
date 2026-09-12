"""Immutable effect receipt models and their intrinsic validation/encoding."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, fields
from datetime import datetime
from uuid import UUID

from dpone.contracts.mssql_r1_v3_authority import MssqlGenerationAuthoritySetV2
from dpone.contracts.mssql_r1_v3_identity import (
    EFFECT_CONTRACT_VERSION,
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    parse_canonical_utc_text,
    require_count,
    require_digest,
    require_positive,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_mutation import validate_mutation_plan_generation_coordinates
from dpone.contracts.mssql_r1_v3_quality import (
    MssqlBatchQualityEvidenceV3,
    MssqlXminQualityEvidenceV3,
    decode_quality_evidence,
)
from dpone.contracts.mssql_r1_v3_staging import R1SealedStageManifestV1
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

_BATCH_BODY_DOMAIN = b"dpone-r1-batch-effect-receipt-body-v3\0"

_XMIN_BODY_DOMAIN = b"dpone-r1-xmin-effect-receipt-body-v3\0"


@dataclass(frozen=True, slots=True)
class MssqlBatchEffectReceiptBodyV3:
    source_snapshot_digest: bytes
    source_schema_digest: bytes
    manifest_bytes: bytes
    manifest_digest: bytes
    payload_row_count: int
    target_row_count_before: int
    target_row_count_after: int
    quality_bytes: bytes
    quality_digest: bytes

    def __post_init__(self) -> None:
        _validate_body(self, SourceMode.BATCH_FULL_REFRESH)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_BATCH_BODY_DOMAIN, tuple(getattr(self, item.name) for item in fields(self)))

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlBatchEffectReceiptBodyV3:
        return cls(*decode_canonical_bytes(payload, _BATCH_BODY_DOMAIN, field_count=9))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlXminEffectReceiptBodyV3:
    source_snapshot_digest: bytes
    source_schema_digest: bytes
    delta_manifest_bytes: bytes
    delta_manifest_digest: bytes
    complete_keys_manifest_bytes: bytes
    complete_keys_manifest_digest: bytes
    affected_count: int
    inserted_count: int
    updated_count: int
    deleted_count: int
    no_effect_count: int
    previous_checkpoint_payload: bytes | None
    previous_checkpoint_value: int | None
    candidate_checkpoint_payload: bytes
    candidate_checkpoint_value: int
    quality_bytes: bytes
    quality_digest: bytes

    def __post_init__(self) -> None:
        _validate_body(self, SourceMode.XMIN_CURRENT_STATE)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_XMIN_BODY_DOMAIN, tuple(getattr(self, item.name) for item in fields(self)))

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlXminEffectReceiptBodyV3:
        return cls(*decode_canonical_bytes(payload, _XMIN_BODY_DOMAIN, field_count=17))  # type: ignore[arg-type]


EffectReceiptBodyV3 = MssqlBatchEffectReceiptBodyV3 | MssqlXminEffectReceiptBodyV3


def _validate_body(body: EffectReceiptBodyV3, mode: SourceMode) -> None:
    for item in fields(body):
        value = getattr(body, item.name)
        if item.name.endswith("_digest"):
            require_digest(value, item.name)
        elif item.name.endswith("_count"):
            require_count(value, item.name)
    quality = decode_quality_evidence(body.quality_bytes)
    if quality.digest != body.quality_digest or quality.source_mode is not mode:
        raise MssqlR1V3ContractError("receipt body quality bytes/digest/kind mismatch")
    if isinstance(body, MssqlBatchEffectReceiptBodyV3):
        _validate_batch(body, quality)
    else:
        _validate_xmin(body, quality)


def _validate_batch(body: MssqlBatchEffectReceiptBodyV3, quality: object) -> None:
    manifest = R1SealedStageManifestV1.from_canonical_bytes(body.manifest_bytes)
    if manifest.manifest_digest != body.manifest_digest or manifest.observed_row_count != body.payload_row_count:
        raise MssqlR1V3ContractError("Batch receipt body manifest proof is inconsistent")
    if not isinstance(quality, MssqlBatchQualityEvidenceV3) or quality.staged_rows != body.payload_row_count:
        raise MssqlR1V3ContractError("Batch receipt quality differs from manifest count")
    require_count(body.target_row_count_before, "target_row_count_before")
    require_count(body.target_row_count_after, "target_row_count_after")
    if body.target_row_count_after != quality.candidate_target_rows:
        raise MssqlR1V3ContractError("Batch receipt target count differs from quality")


def _validate_xmin(body: MssqlXminEffectReceiptBodyV3, quality: object) -> None:
    delta = R1SealedStageManifestV1.from_canonical_bytes(body.delta_manifest_bytes)
    complete = R1SealedStageManifestV1.from_canonical_bytes(body.complete_keys_manifest_bytes)
    if (delta.manifest_digest, complete.manifest_digest) != (
        body.delta_manifest_digest,
        body.complete_keys_manifest_digest,
    ):
        raise MssqlR1V3ContractError("XMin receipt body manifest proof is inconsistent")
    if not isinstance(body.candidate_checkpoint_payload, bytes) or not body.candidate_checkpoint_payload:
        raise MssqlR1V3ContractError("XMin receipt body requires a candidate checkpoint")
    if body.previous_checkpoint_payload is not None and (
        not isinstance(body.previous_checkpoint_payload, bytes) or not body.previous_checkpoint_payload
    ):
        raise MssqlR1V3ContractError("XMin receipt body predecessor checkpoint is invalid")
    if (body.previous_checkpoint_payload is None) != (body.previous_checkpoint_value is None):
        raise MssqlR1V3ContractError("XMin receipt body predecessor checkpoint is partial")
    if body.previous_checkpoint_value is not None:
        require_count(body.previous_checkpoint_value, "previous_checkpoint_value")
    require_count(body.candidate_checkpoint_value, "candidate_checkpoint_value")
    observed = (
        body.affected_count,
        body.inserted_count,
        body.updated_count,
        body.deleted_count,
        body.no_effect_count,
        complete.observed_row_count,
        delta.observed_row_count,
    )
    if not isinstance(quality, MssqlXminQualityEvidenceV3) or observed != (
        quality.affected_key_count,
        quality.inserted_count,
        quality.updated_count,
        quality.deleted_count,
        quality.delta_no_effect_count,
        quality.complete_key_count,
        quality.expected_present_keys,
    ):
        raise MssqlR1V3ContractError("XMin receipt metrics differ from typed quality/manifests")


_HEADER_DOMAIN = b"dpone-r1-effect-receipt-header-v3\0"

_RECEIPT_DOMAIN = b"dpone-r1-effect-receipt-v3\0"

_EMPTY_AUTHORITY_SET_DIGEST = MssqlGenerationAuthoritySetV2().digest


@dataclass(frozen=True, slots=True)
class MssqlR1EffectReceiptHeaderV3:
    receipt_id: UUID
    receipt_kind: SourceMode
    contract_version: str
    contract_digest: bytes
    operation_key: bytes
    effect_key: bytes
    physical_coordinate_digest: bytes
    registered_physical_authority_digest: bytes
    target_binding_uuid: UUID
    target_object_uuid: UUID
    recovery_identity_digest: bytes
    writer_mode: SourceMode
    expected_writer_generation: int | None
    candidate_writer_generation: int
    expected_head_revision: int | None
    candidate_head_revision: int
    operation_epoch: int
    operation_projection_revision: int
    predecessor_receipt_id: UUID | None
    predecessor_receipt_digest: bytes | None
    route_identity_sha256: bytes
    source_authority_sha256: bytes
    registration_id: UUID
    registration_payload_digest: bytes
    verification_policy_digest: bytes
    registration_admitted_at: datetime
    sealed_request_digest: bytes
    artifact_set_digest: bytes
    mutation_plan_digest: bytes
    generation_transition_digest: bytes
    generation_authority_issuance_id: UUID | None
    generation_authority_issuance_payload_digest: bytes | None
    generation_authority_verification_receipt_digest: bytes | None
    authority_set_digest: bytes
    body_digest: bytes
    revoked_override_id: UUID | None
    revoked_override_digest: bytes | None
    committed_at: datetime

    def __post_init__(self) -> None:
        for name in ("receipt_id", "target_binding_uuid", "target_object_uuid", "registration_id"):
            require_uuid(getattr(self, name), name)
        if self.contract_version != EFFECT_CONTRACT_VERSION or self.receipt_kind is not self.writer_mode:
            raise MssqlR1V3ContractError("V3 receipt kind/contract discriminator is invalid")
        for item in fields(self):
            if item.name.endswith("_digest") or item.name.endswith("_sha256"):
                value = getattr(self, item.name)
                if value is not None:
                    require_digest(value, item.name)
        require_positive(self.candidate_writer_generation, "candidate_writer_generation")
        require_positive(self.candidate_head_revision, "candidate_head_revision")
        require_positive(self.operation_epoch, "operation_epoch")
        require_positive(self.operation_projection_revision, "operation_projection_revision")
        if (self.expected_writer_generation is None) != (self.expected_head_revision is None):
            raise MssqlR1V3ContractError("receipt predecessor generation is partial")
        validate_mutation_plan_generation_coordinates(
            self.expected_writer_generation,
            self.candidate_writer_generation,
            self.expected_head_revision,
            self.candidate_head_revision,
            allow_same_generation=self.receipt_kind is SourceMode.XMIN_CURRENT_STATE,
        )
        if (self.predecessor_receipt_id is None) != (self.predecessor_receipt_digest is None):
            raise MssqlR1V3ContractError("receipt predecessor proof is partial")
        if (self.expected_writer_generation is None) != (self.predecessor_receipt_id is None):
            raise MssqlR1V3ContractError("receipt predecessor proof differs from generation predecessor")
        issuance = (
            self.generation_authority_issuance_id,
            self.generation_authority_issuance_payload_digest,
            self.generation_authority_verification_receipt_digest,
        )
        if any(value is None for value in issuance) and any(value is not None for value in issuance):
            raise MssqlR1V3ContractError("receipt generation-authority issuance identity is partial")
        if (self.authority_set_digest == _EMPTY_AUTHORITY_SET_DIGEST) != all(value is None for value in issuance):
            raise MssqlR1V3ContractError("receipt issuance discriminator differs from canonical authority set")
        if self.generation_authority_issuance_id is not None:
            require_uuid(self.generation_authority_issuance_id, "generation_authority_issuance_id")
        if self.revoked_override_id is not None or self.revoked_override_digest is not None:
            raise MssqlR1V3ContractError("revoked-registration override is not an active R1 capability")
        canonical_utc_text(self.registration_admitted_at, "registration_admitted_at")
        canonical_utc_text(self.committed_at, "committed_at")
        if self.receipt_id != _receipt_uuid(self.identity_bytes):
            raise MssqlR1V3ContractError("receipt ID differs from immutable V3 header")

    @property
    def identity_bytes(self) -> bytes:
        return canonical_bytes(
            _HEADER_DOMAIN, tuple(getattr(self, item.name) for item in fields(self) if item.name != "receipt_id")
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_HEADER_DOMAIN, tuple(getattr(self, item.name) for item in fields(self)))

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1EffectReceiptHeaderV3:
        values = list(decode_canonical_bytes(payload, _HEADER_DOMAIN, field_count=38))
        values[1] = expect_enum(SourceMode, values[1], "receipt_kind")
        values[11] = expect_enum(SourceMode, values[11], "writer_mode")
        values[25] = parse_canonical_utc_text(values[25], "registration_admitted_at")
        values[37] = parse_canonical_utc_text(values[37], "committed_at")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1EffectReceiptV3:
    header: MssqlR1EffectReceiptHeaderV3
    body: EffectReceiptBodyV3

    def __post_init__(self) -> None:
        expected = (
            MssqlBatchEffectReceiptBodyV3
            if self.header.receipt_kind is SourceMode.BATCH_FULL_REFRESH
            else MssqlXminEffectReceiptBodyV3
        )
        if not isinstance(self.body, expected):
            raise MssqlR1V3ContractError("receipt header/body kind mismatch")
        if self.header.body_digest != self.body.digest:
            raise MssqlR1V3ContractError("receipt header does not bind the exact typed body")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_RECEIPT_DOMAIN, (self.header.canonical_bytes, self.body.canonical_bytes))

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1EffectReceiptV3:
        """Strictly reconstruct a V3 effect receipt and its typed body."""
        header_bytes, body_bytes = decode_canonical_bytes(payload, _RECEIPT_DOMAIN, field_count=2)
        header = MssqlR1EffectReceiptHeaderV3.from_canonical_bytes(expect_bytes(header_bytes, "receipt_header"))
        body: MssqlBatchEffectReceiptBodyV3 | MssqlXminEffectReceiptBodyV3
        if header.receipt_kind is SourceMode.BATCH_FULL_REFRESH:
            body = MssqlBatchEffectReceiptBodyV3.from_canonical_bytes(expect_bytes(body_bytes, "receipt_body"))
        else:
            body = MssqlXminEffectReceiptBodyV3.from_canonical_bytes(expect_bytes(body_bytes, "receipt_body"))
        return MssqlR1EffectReceiptV3(header, body)


def _receipt_uuid(identity_bytes: bytes) -> UUID:
    return UUID(bytes=hashlib.sha256(identity_bytes).digest()[:16])


__all__ = [
    "EffectReceiptBodyV3",
    "MssqlBatchEffectReceiptBodyV3",
    "MssqlR1EffectReceiptHeaderV3",
    "MssqlR1EffectReceiptV3",
    "MssqlXminEffectReceiptBodyV3",
    "MssqlR1ReceiptObservationV3",
]


@dataclass(frozen=True, slots=True)
class MssqlR1ReceiptObservationV3:
    """Values computed by the target provider inside the active transaction."""

    committed_at: datetime
    batch_target_row_count_before: int | None

    def __post_init__(self) -> None:
        canonical_utc_text(self.committed_at, "committed_at")
        if self.batch_target_row_count_before is not None:
            require_count(self.batch_target_row_count_before, "batch_target_row_count_before")
