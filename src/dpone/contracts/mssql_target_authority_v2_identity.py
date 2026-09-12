"""Stable effect and physical-target identities for MSSQL authority V2."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from uuid import UUID, uuid5

from dpone._compat import StrEnum
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

CONTRACT_VERSION = "mssql_effect_receipt_v2"
MAX_SQL_BIGINT = 2**63 - 1
_RECEIPT_NAMESPACE = UUID("dd363283-73d6-56ab-a99b-e8b46e80e5b6")


class MssqlReceiptContractError(ValueError):
    """An authority record cannot provide the exact R1 proof."""


class WriterMode(StrEnum):
    BATCH_FULL_REFRESH = "batch_full_refresh"
    XMIN_CURRENT_STATE = "xmin_current_state"


class ReceiptKind(StrEnum):
    BATCH = "batch"
    XMIN = "xmin"


def require_digest(value: bytes, field: str) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise MssqlReceiptContractError(f"{field} must be exactly 32 bytes")
    return value


def require_positive(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_SQL_BIGINT:
        raise MssqlReceiptContractError(f"{field} must fit a positive SQL bigint")
    return value


def require_count(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SQL_BIGINT:
        raise MssqlReceiptContractError(f"{field} must fit a non-negative SQL bigint")
    return value


def require_uuid(value: object, field: str) -> UUID:
    if not isinstance(value, UUID):
        raise MssqlReceiptContractError(f"{field} must be a UUID")
    return value


def canonical_hash(domain: bytes, values: tuple[object, ...]) -> bytes:
    payload = bytearray(domain + b"\0")
    for value in values:
        encoded = _encode(value)
        payload.extend(len(encoded).to_bytes(4, "big"))
        payload.extend(encoded)
    return hashlib.sha256(payload).digest()


def _encode(value: object) -> bytes:
    if value is None:
        return b"n"
    if isinstance(value, Enum):
        return _encode(value.value)
    if isinstance(value, UUID):
        return b"u" + value.bytes
    if isinstance(value, bytes):
        return b"b" + len(value).to_bytes(8, "big") + value
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return b"s" + len(raw).to_bytes(8, "big") + raw
    if isinstance(value, int) and not isinstance(value, bool):
        return b"i" + value.to_bytes(8, "big", signed=True)
    if is_dataclass(value) and not isinstance(value, type):
        encoded = tuple(
            _encode(getattr(value, item.name)) for item in fields(value) if item.metadata.get("canonical", True)
        )
        return b"d" + b"".join(len(item).to_bytes(4, "big") + item for item in encoded)
    raise MssqlReceiptContractError(f"unsupported canonical authority value: {type(value).__name__}")


def build_operation_key(
    *,
    route_identity_sha256: bytes,
    invocation_identity: str,
    source_mode: SourceMode,
    target_binding_uuid: UUID,
    contract_version: str = CONTRACT_VERSION,
) -> bytes:
    """Build the pre-source identity; runtime counters are intentionally absent."""

    require_digest(route_identity_sha256, "route_identity_sha256")
    if not invocation_identity or invocation_identity != invocation_identity.strip():
        raise MssqlReceiptContractError("invocation identity must be canonical non-empty text")
    if not isinstance(source_mode, SourceMode):
        raise MssqlReceiptContractError("source mode is unsupported")
    return canonical_hash(
        b"dpone-r1-operation-key-v1",
        (contract_version, route_identity_sha256, invocation_identity, source_mode, target_binding_uuid),
    )


def build_effect_key(
    *,
    operation_key: bytes,
    source_mode: SourceMode,
    target_binding_uuid: UUID,
    contract_version: str = CONTRACT_VERSION,
) -> bytes:
    """Bind one stable operation to its target and source semantics."""

    return canonical_hash(
        b"dpone-r1-effect-key-v1",
        (contract_version, target_binding_uuid, source_mode, require_digest(operation_key, "operation_key")),
    )


def deterministic_receipt_id(effect_key: bytes) -> UUID:
    return uuid5(_RECEIPT_NAMESPACE, require_digest(effect_key, "effect_key").hex())


@dataclass(frozen=True, slots=True)
class MssqlBusinessKeyV2:
    """Closed one-column key proof required by the certified target profile."""

    column_name: str
    source_type: str
    target_type: str
    maximum_ordinary_indexes: int

    def __post_init__(self) -> None:
        if not self.column_name or self.column_name != self.column_name.strip():
            raise MssqlReceiptContractError("business key column must be canonical text")
        allowed = {"int2": "smallint", "int4": "int", "int8": "bigint", "uuid": "uniqueidentifier"}
        if allowed.get(self.source_type) != self.target_type:
            raise MssqlReceiptContractError("business key type is outside the closed R1 profile")
        require_positive(self.maximum_ordinary_indexes, "maximum_ordinary_indexes")


@dataclass(frozen=True, slots=True)
class MssqlTargetIdentityV2:
    """Registered physical target and recovery identity re-proved per effect."""

    target_binding_uuid: UUID
    target_object_uuid: UUID
    server_instance_identity_sha256: bytes
    database_guid: UUID
    database_family_guid: UUID
    recovery_fork_guid: UUID
    recovery_domain_uuid: UUID
    recovery_domain_epoch: int
    database_name_digest: bytes
    schema_name_digest: bytes
    object_name_digest: bytes
    object_id: int
    physical_generation_uuid: UUID
    catalog_contract_digest: bytes
    target_contract_revision: int
    business_key: MssqlBusinessKeyV2 | None = field(default=None, compare=False, metadata={"canonical": False})

    def __post_init__(self) -> None:
        for item in fields(self):
            value = getattr(self, item.name)
            if item.name.endswith("uuid") or item.name.endswith("guid"):
                require_uuid(value, item.name)
            elif item.name.endswith("digest") or item.name.endswith("sha256"):
                require_digest(value, item.name)
        require_positive(self.recovery_domain_epoch, "recovery_domain_epoch")
        require_positive(self.object_id, "object_id")
        require_positive(self.target_contract_revision, "target_contract_revision")


@dataclass(frozen=True, slots=True)
class MssqlTargetHeadV2:
    target_binding_uuid: UUID
    writer_mode: WriterMode
    writer_generation: int
    head_revision: int
    last_receipt_id: UUID
    recovery_domain_epoch: int
    recovery_domain_uuid: UUID | None = None
    checkpoint: MssqlCheckpointPointerV2 | None = None

    def __post_init__(self) -> None:
        require_uuid(self.target_binding_uuid, "target_binding_uuid")
        require_positive(self.writer_generation, "writer_generation")
        require_positive(self.head_revision, "head_revision")
        require_uuid(self.last_receipt_id, "last_receipt_id")
        require_positive(self.recovery_domain_epoch, "recovery_domain_epoch")
        if self.recovery_domain_uuid is not None:
            require_uuid(self.recovery_domain_uuid, "recovery_domain_uuid")


@dataclass(frozen=True, slots=True)
class MssqlCheckpointPointerV2:
    """Complete nullable projection of one append-only XMin checkpoint."""

    state_key_digest: bytes
    writer_generation: int
    revision: int

    def __post_init__(self) -> None:
        require_digest(self.state_key_digest, "state_key_digest")
        require_positive(self.writer_generation, "writer_generation")
        require_positive(self.revision, "revision")


@dataclass(frozen=True, slots=True)
class MssqlArtifactAuthorityV2:
    """Exact sealed staging-object identity and logical byte authority."""

    artifact_id: UUID
    artifact_kind: str
    object_uuid: UUID
    object_id: int
    physical_token: UUID
    catalog_digest: bytes
    row_count: int
    payload_bytes: int
    ordered_logical_digest: bytes
    permission_contract_digest: bytes

    def __post_init__(self) -> None:
        for name in ("artifact_id", "object_uuid", "physical_token"):
            require_uuid(getattr(self, name), name)
        if self.artifact_kind not in {"batch_payload", "xmin_delta", "xmin_complete_keys"}:
            raise MssqlReceiptContractError("staging artifact kind is unsupported")
        require_positive(self.object_id, "object_id")
        require_count(self.row_count, "row_count")
        require_count(self.payload_bytes, "payload_bytes")
        for name in ("catalog_digest", "ordered_logical_digest", "permission_contract_digest"):
            require_digest(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class MssqlSealedIntentV2:
    effect_key: bytes
    intent_digest: bytes
    artifact_set_digest: bytes
    row_count: int
    payload_bytes: int
    artifacts: tuple[MssqlArtifactAuthorityV2, ...] = ()
    source_snapshot_digest: bytes | None = None
    source_schema_digest: bytes | None = None
    artifact_manifest_digest: bytes | None = None
    mutation_plan_digest: bytes | None = None
    source_snapshot_authority: bytes | None = None
    artifact_set_manifest: bytes | None = None

    def __post_init__(self) -> None:
        for name in ("effect_key", "intent_digest", "artifact_set_digest"):
            require_digest(getattr(self, name), name)
        require_count(self.row_count, "row_count")
        require_count(self.payload_bytes, "payload_bytes")
        if not isinstance(self.artifacts, tuple):
            raise MssqlReceiptContractError("sealed artifacts must be an immutable tuple")
        if len({item.artifact_kind for item in self.artifacts}) != len(self.artifacts):
            raise MssqlReceiptContractError("sealed artifact kinds must be unique")
        for name in (
            "source_snapshot_digest",
            "source_schema_digest",
            "artifact_manifest_digest",
            "mutation_plan_digest",
        ):
            value = getattr(self, name)
            if value is not None:
                require_digest(value, name)
        for name in ("source_snapshot_authority", "artifact_set_manifest"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, bytes):
                raise MssqlReceiptContractError(f"{name} must be immutable bytes")


@dataclass(frozen=True, slots=True)
class MssqlArtifactProofV2:
    artifact_set_digest: bytes
    row_count: int
    payload_bytes: int

    def __post_init__(self) -> None:
        require_digest(self.artifact_set_digest, "artifact_set_digest")
        require_count(self.row_count, "row_count")
        require_count(self.payload_bytes, "payload_bytes")


@dataclass(frozen=True, slots=True)
class MssqlMutationResultV2:
    before_row_count: int
    after_row_count: int
    payload_bytes: int

    def __post_init__(self) -> None:
        require_count(self.before_row_count, "before_row_count")
        require_count(self.after_row_count, "after_row_count")
        require_count(self.payload_bytes, "payload_bytes")


@dataclass(frozen=True, slots=True)
class MssqlQualityEvidenceV2:
    payload: bytes

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(b"dpone-r1-quality-evidence-v1\0" + self.payload).digest()


@dataclass(frozen=True, slots=True)
class MssqlXminCheckpointTransitionV2:
    state_key_digest: bytes
    writer_generation: int
    previous_revision: int | None
    candidate_revision: int

    def __post_init__(self) -> None:
        require_digest(self.state_key_digest, "state_key_digest")
        require_positive(self.writer_generation, "writer_generation")
        expected = (
            1 if self.previous_revision is None else require_positive(self.previous_revision, "previous_revision") + 1
        )
        if require_positive(self.candidate_revision, "candidate_revision") != expected:
            raise MssqlReceiptContractError("checkpoint transition must be adjacent")


@dataclass(frozen=True, slots=True)
class MssqlGenerationAuthorityClaimV1:
    authority_id: UUID
    authority_kind: str
    effect_key: bytes
    payload_digest: bytes

    def __post_init__(self) -> None:
        require_uuid(self.authority_id, "authority_id")
        if self.authority_kind not in {"initial_cutover", "rebaseline", "empty_refresh"}:
            raise MssqlReceiptContractError("generation authority kind is unsupported")
        require_digest(self.effect_key, "effect_key")
        require_digest(self.payload_digest, "payload_digest")


__all__ = [
    name
    for name in globals()
    if name.startswith("Mssql")
    or name.startswith("build_")
    or name
    in {
        "CONTRACT_VERSION",
        "MAX_SQL_BIGINT",
        "ReceiptKind",
        "SourceMode",
        "WriterMode",
        "canonical_hash",
        "deterministic_receipt_id",
        "require_count",
        "require_digest",
        "require_positive",
        "require_uuid",
    }
]
