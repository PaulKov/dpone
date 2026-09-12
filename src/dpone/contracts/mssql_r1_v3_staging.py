"""Typed OPEN, scan-evidence and SEALED staging contracts for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_enum,
    expect_text,
    expect_tuple,
    require_count,
    require_digest,
    require_identifier,
    require_positive,
    require_sql_int,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_stage_evidence import (
    R1BoundedStageChunkV1,
    R1StageArtifactKindV1,
    R1StageBusinessColumnV1,
    R1StageCellStateV1,
    R1TypedStageCellV1,
    R1TypedStageRowV1,
    R1TypedStageScanEvidenceV1,
    artifact_digest_for_rows,
    canonical_artifact_digest,
)

OPEN_STAGE_PLAN_CODEC = "dpone-r1-open-stage-plan-v1"
SEALED_STAGE_MANIFEST_CODEC = "dpone-r1-sealed-stage-manifest-v1"
_OPEN_PLAN_DOMAIN = b"dpone-r1-open-stage-plan-v1\0"
_SEALED_MANIFEST_DOMAIN = b"dpone-r1-sealed-stage-manifest-v1\0"
_SEALED_STAGE_SET_DOMAIN = b"dpone-r1-sealed-stage-set-v1\0"
_MAX_UINT32 = 2**32 - 1


@dataclass(frozen=True, slots=True)
class R1OpenStagePlanV1:
    artifact_id: UUID
    artifact_kind: R1StageArtifactKindV1
    target_binding_uuid: UUID
    effect_key: bytes
    owner_epoch: int
    server_lease_seconds: int
    exact_stage_ddl_digest: bytes
    schema_digest: bytes
    catalog_contract_digest: bytes
    permission_contract_digest: bytes
    type_policy_digest: bytes
    ordered_business_columns: tuple[R1StageBusinessColumnV1, ...]
    canonical_key_payload_column: str
    canonical_row_payload_column: str | None
    canonical_row_hash_column: str | None
    maximum_key_bytes: int
    maximum_row_bytes: int
    codec_version: str = OPEN_STAGE_PLAN_CODEC

    def __post_init__(self) -> None:
        require_uuid(self.artifact_id, "artifact_id")
        require_uuid(self.target_binding_uuid, "target_binding_uuid")
        require_digest(self.effect_key, "effect_key")
        require_positive(self.owner_epoch, "owner_epoch")
        if isinstance(self.server_lease_seconds, bool) or not 1 <= self.server_lease_seconds <= 3_600:
            raise MssqlR1V3ContractError("server_lease_seconds must be in 1..3600")
        for name in (
            "exact_stage_ddl_digest",
            "schema_digest",
            "catalog_contract_digest",
            "permission_contract_digest",
            "type_policy_digest",
        ):
            require_digest(getattr(self, name), name)
        _validate_columns(self.ordered_business_columns, self.artifact_kind)
        require_identifier(self.canonical_key_payload_column, "canonical_key_payload_column")
        _validate_key_bound(self.maximum_key_bytes)
        require_count(self.maximum_row_bytes, "maximum_row_bytes")
        _validate_kind_shape(self)
        if self.codec_version != OPEN_STAGE_PLAN_CODEC:
            raise MssqlR1V3ContractError("OPEN stage plan codec is unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _OPEN_PLAN_DOMAIN,
            (
                self.codec_version,
                self.artifact_id,
                self.artifact_kind,
                self.target_binding_uuid,
                self.effect_key,
                self.owner_epoch,
                self.server_lease_seconds,
                self.exact_stage_ddl_digest,
                self.schema_digest,
                self.catalog_contract_digest,
                self.permission_contract_digest,
                self.type_policy_digest,
                tuple(item.canonical_values for item in self.ordered_business_columns),
                self.canonical_key_payload_column,
                self.canonical_row_payload_column,
                self.canonical_row_hash_column,
                self.maximum_key_bytes,
                self.maximum_row_bytes,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> R1OpenStagePlanV1:
        values = list(decode_canonical_bytes(payload, _OPEN_PLAN_DOMAIN, field_count=18))
        values[2] = expect_enum(R1StageArtifactKindV1, values[2], "artifact_kind")
        values[12] = tuple(
            R1StageBusinessColumnV1.from_values(item) for item in expect_tuple(values[12], "ordered_business_columns")
        )
        return cls(*values[1:], codec_version=expect_text(values[0], "codec_version"))  # type: ignore[arg-type,misc]


@dataclass(frozen=True, slots=True)
class R1SealedStageManifestV1:
    artifact_id: UUID
    artifact_kind: R1StageArtifactKindV1
    target_binding_uuid: UUID
    effect_key: bytes
    schema_digest: bytes
    catalog_contract_digest: bytes
    permission_contract_digest: bytes
    type_policy_digest: bytes
    object_uuid: UUID
    object_id: int
    physical_token: UUID
    target_local_schema: str
    target_local_object: str
    ordered_business_columns: tuple[R1StageBusinessColumnV1, ...]
    canonical_key_payload_column: str
    canonical_row_payload_column: str | None
    canonical_row_hash_column: str | None
    maximum_key_bytes: int
    maximum_row_bytes: int
    observed_row_count: int
    observed_payload_bytes: int
    artifact_digest: bytes
    typed_scan_evidence: R1TypedStageScanEvidenceV1
    row_hash_rule: str | None
    open_stage_plan_digest: bytes
    exact_stage_ddl_digest: bytes
    codec_version: str = SEALED_STAGE_MANIFEST_CODEC

    def __post_init__(self) -> None:
        for name in ("artifact_id", "target_binding_uuid", "object_uuid", "physical_token"):
            require_uuid(getattr(self, name), name)
        for name in (
            "effect_key",
            "schema_digest",
            "catalog_contract_digest",
            "permission_contract_digest",
            "type_policy_digest",
            "artifact_digest",
            "open_stage_plan_digest",
            "exact_stage_ddl_digest",
        ):
            require_digest(getattr(self, name), name)
        require_sql_int(self.object_id, "object_id")
        require_identifier(self.target_local_schema, "target_local_schema")
        require_identifier(self.target_local_object, "target_local_object")
        _validate_columns(self.ordered_business_columns, self.artifact_kind)
        require_identifier(self.canonical_key_payload_column, "canonical_key_payload_column")
        _validate_key_bound(self.maximum_key_bytes)
        require_count(self.maximum_row_bytes, "maximum_row_bytes")
        require_count(self.observed_row_count, "observed_row_count")
        require_count(self.observed_payload_bytes, "observed_payload_bytes")
        _validate_kind_shape(self)
        complete = self.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
        if self.row_hash_rule != (None if complete else "sha256_canonical_row_v1"):
            raise MssqlR1V3ContractError("row_hash_rule conflicts with artifact kind")
        if not isinstance(self.typed_scan_evidence, R1TypedStageScanEvidenceV1) or (
            self.typed_scan_evidence.artifact_kind,
            self.typed_scan_evidence.artifact_digest,
            self.typed_scan_evidence.observed_row_count,
            self.typed_scan_evidence.observed_payload_bytes,
        ) != (self.artifact_kind, self.artifact_digest, self.observed_row_count, self.observed_payload_bytes):
            raise MssqlR1V3ContractError("typed stage scan evidence differs from sealed manifest")
        if self.codec_version != SEALED_STAGE_MANIFEST_CODEC:
            raise MssqlR1V3ContractError("sealed stage manifest codec is unsupported")

    def matches_open_plan(self, plan: R1OpenStagePlanV1) -> bool:
        return isinstance(plan, R1OpenStagePlanV1) and self._open_values == (
            plan.artifact_id,
            plan.artifact_kind,
            plan.target_binding_uuid,
            plan.effect_key,
            plan.schema_digest,
            plan.catalog_contract_digest,
            plan.permission_contract_digest,
            plan.type_policy_digest,
            plan.ordered_business_columns,
            plan.canonical_key_payload_column,
            plan.canonical_row_payload_column,
            plan.canonical_row_hash_column,
            plan.maximum_key_bytes,
            plan.maximum_row_bytes,
            plan.digest,
            plan.exact_stage_ddl_digest,
        )

    @property
    def _open_values(self) -> tuple[object, ...]:
        return (
            self.artifact_id,
            self.artifact_kind,
            self.target_binding_uuid,
            self.effect_key,
            self.schema_digest,
            self.catalog_contract_digest,
            self.permission_contract_digest,
            self.type_policy_digest,
            self.ordered_business_columns,
            self.canonical_key_payload_column,
            self.canonical_row_payload_column,
            self.canonical_row_hash_column,
            self.maximum_key_bytes,
            self.maximum_row_bytes,
            self.open_stage_plan_digest,
            self.exact_stage_ddl_digest,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SEALED_MANIFEST_DOMAIN,
            (
                self.codec_version,
                self.artifact_id,
                self.artifact_kind,
                self.target_binding_uuid,
                self.effect_key,
                self.schema_digest,
                self.catalog_contract_digest,
                self.permission_contract_digest,
                self.type_policy_digest,
                self.object_uuid,
                self.object_id,
                self.physical_token,
                self.target_local_schema,
                self.target_local_object,
                tuple(column.canonical_values for column in self.ordered_business_columns),
                self.canonical_key_payload_column,
                self.canonical_row_payload_column,
                self.canonical_row_hash_column,
                self.maximum_key_bytes,
                self.maximum_row_bytes,
                self.observed_row_count,
                self.observed_payload_bytes,
                self.artifact_digest,
                self.typed_scan_evidence.canonical_bytes,
                self.row_hash_rule,
                self.open_stage_plan_digest,
                self.exact_stage_ddl_digest,
            ),
        )

    @property
    def manifest_digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> R1SealedStageManifestV1:
        values = list(decode_canonical_bytes(payload, _SEALED_MANIFEST_DOMAIN, field_count=27))
        values[2] = expect_enum(R1StageArtifactKindV1, values[2], "artifact_kind")
        values[14] = tuple(
            R1StageBusinessColumnV1.from_values(item) for item in expect_tuple(values[14], "ordered_business_columns")
        )
        values[23] = R1TypedStageScanEvidenceV1.from_canonical_bytes(values[23])  # type: ignore[arg-type]
        return cls(*values[1:], codec_version=expect_text(values[0], "codec_version"))  # type: ignore[arg-type,misc]


def sealed_stage_set_digest(manifests: tuple[R1SealedStageManifestV1, ...]) -> bytes:
    if not isinstance(manifests, tuple) or not manifests:
        raise MssqlR1V3ContractError("sealed stage set must be a non-empty tuple")
    if not all(isinstance(item, R1SealedStageManifestV1) for item in manifests):
        raise MssqlR1V3ContractError("sealed stage set contains an invalid manifest")
    if tuple(item.artifact_kind for item in manifests) not in {
        (R1StageArtifactKindV1.BATCH_PAYLOAD,),
        (R1StageArtifactKindV1.XMIN_DELTA, R1StageArtifactKindV1.XMIN_COMPLETE_KEYS),
    }:
        raise MssqlR1V3ContractError("sealed stage set has an invalid kind/order")
    if len({item.artifact_id for item in manifests}) != len(manifests):
        raise MssqlR1V3ContractError("sealed stage set contains a duplicate artifact")
    if len({(item.target_binding_uuid, item.effect_key) for item in manifests}) != 1:
        raise MssqlR1V3ContractError("sealed stage set must bind one target effect")
    return hashlib.sha256(
        canonical_bytes(_SEALED_STAGE_SET_DOMAIN, tuple(item.canonical_bytes for item in manifests))
    ).digest()


def _validate_columns(columns: tuple[R1StageBusinessColumnV1, ...], kind: R1StageArtifactKindV1) -> None:
    if (
        not isinstance(columns, tuple)
        or not columns
        or not all(isinstance(item, R1StageBusinessColumnV1) for item in columns)
    ):
        raise MssqlR1V3ContractError("ordered business columns must be a non-empty typed tuple")
    source = tuple(item.source_ordinal for item in columns)
    target = tuple(item.target_ordinal for item in columns)
    if source != tuple(range(1, len(columns) + 1)) or target != tuple(range(1, len(columns) + 1)):
        raise MssqlR1V3ContractError("business column mappings require contiguous canonical ordering")
    if len({item.stage_column for item in columns}) != len(columns) or len(
        {item.target_column for item in columns}
    ) != len(columns):
        raise MssqlR1V3ContractError("business column mappings must be unique")
    keys = tuple(item for item in columns if item.is_business_key)
    if len(keys) != 1:
        raise MssqlR1V3ContractError("stage mapping requires exactly one supported business key")
    if kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS and columns != keys:
        raise MssqlR1V3ContractError("complete-keys stage contains only the business-key mapping")


def _validate_kind_shape(value: R1OpenStagePlanV1 | R1SealedStageManifestV1) -> None:
    complete = value.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
    if complete:
        if (
            value.canonical_row_payload_column is not None
            or value.canonical_row_hash_column is not None
            or value.maximum_row_bytes != 0
        ):
            raise MssqlR1V3ContractError("complete-keys stage forbids row payload and hash fields")
        return
    if (
        value.canonical_row_payload_column is None
        or value.canonical_row_hash_column is None
        or value.maximum_row_bytes < 1
    ):
        raise MssqlR1V3ContractError("row stage requires canonical row payload and hash fields")
    require_identifier(value.canonical_row_payload_column, "canonical_row_payload_column")
    require_identifier(value.canonical_row_hash_column, "canonical_row_hash_column")


def _validate_key_bound(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= _MAX_UINT32:
        raise MssqlR1V3ContractError("maximum_key_bytes must fit a positive uint32")


__all__ = [
    "MssqlR1V3ContractError",
    "OPEN_STAGE_PLAN_CODEC",
    "R1BoundedStageChunkV1",
    "R1OpenStagePlanV1",
    "R1SealedStageManifestV1",
    "R1StageArtifactKindV1",
    "R1StageBusinessColumnV1",
    "R1StageCellStateV1",
    "R1TypedStageCellV1",
    "R1TypedStageRowV1",
    "R1TypedStageScanEvidenceV1",
    "SEALED_STAGE_MANIFEST_CODEC",
    "artifact_digest_for_rows",
    "canonical_artifact_digest",
    "canonical_bytes",
    "require_identifier",
    "sealed_stage_set_digest",
]
