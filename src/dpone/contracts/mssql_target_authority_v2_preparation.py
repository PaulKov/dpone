"""Immutable preparation contracts for MSSQL target authority V2.

These contracts describe target-local staging transitions only.  They contain
no database session or source-reader behavior, so retries can reconstruct the
exact sealed effect without reopening PostgreSQL.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any, cast
from uuid import UUID

from dpone.contracts.mssql_target_authority_v2_identity import (
    MssqlArtifactAuthorityV2,
    MssqlBusinessKeyV2,
    MssqlGenerationAuthorityClaimV1,
    MssqlReceiptContractError,
    MssqlSealedIntentV2,
    MssqlTargetIdentityV2,
    MssqlXminCheckpointTransitionV2,
    ReceiptKind,
    WriterMode,
    require_count,
    require_digest,
    require_positive,
    require_uuid,
)
from dpone.contracts.mssql_target_authority_v2_models import (
    MssqlBatchEffectDraftV2,
    MssqlEffectRequestV2,
    MssqlXminEffectDraftV2,
)
from dpone.contracts.mssql_target_authority_v2_receipts import MssqlEffectReceiptHeaderV2

EFFECT_REQUEST_CODEC_VERSION = "dpone-mssql-effect-request-v2-json-v1"
_PAYLOAD_DOMAIN = b"dpone-r1-effect-request-v2\0"


@dataclass(frozen=True, slots=True)
class MssqlOpenStagingArtifactV2:
    """Identity registered before BCP or runtime may populate one artifact."""

    target_binding_uuid: UUID
    effect_key: bytes
    operation_epoch: int
    owner_id_digest: bytes
    artifact_id: UUID
    artifact_kind: str
    object_uuid: UUID
    object_id: int
    physical_token: UUID
    catalog_digest: bytes
    retention_until: datetime

    def __post_init__(self) -> None:
        for name in ("target_binding_uuid", "artifact_id", "object_uuid", "physical_token"):
            require_uuid(getattr(self, name), name)
        for name in ("effect_key", "owner_id_digest", "catalog_digest"):
            require_digest(getattr(self, name), name)
        require_positive(self.operation_epoch, "operation_epoch")
        require_positive(self.object_id, "object_id")
        if self.artifact_kind not in {"batch_payload", "xmin_delta", "xmin_complete_keys"}:
            raise MssqlReceiptContractError("staging artifact kind is unsupported")
        if self.retention_until.tzinfo is None or self.retention_until.utcoffset() is None:
            raise MssqlReceiptContractError("retention_until must be timezone-aware")


@dataclass(frozen=True, slots=True)
class MssqlUnsealedArtifactAuthorityV2:
    """Expected seal facts before SQL Server supplies its permission digest."""

    artifact_id: UUID
    artifact_kind: str
    object_uuid: UUID
    object_id: int
    physical_token: UUID
    catalog_digest: bytes
    row_count: int
    payload_bytes: int
    ordered_logical_digest: bytes
    permission_contract_digest: None = None


@dataclass(frozen=True, slots=True)
class MssqlArtifactSealRequestV2:
    """Logical proof that SQL Server must bind to an OPEN staging object."""

    opened: MssqlOpenStagingArtifactV2
    row_count: int
    payload_bytes: int
    ordered_logical_digest: bytes

    def __post_init__(self) -> None:
        require_count(self.row_count, "row_count")
        require_count(self.payload_bytes, "payload_bytes")
        require_digest(self.ordered_logical_digest, "ordered_logical_digest")

    @property
    def expected_authority(self) -> MssqlUnsealedArtifactAuthorityV2:
        item = self.opened
        return MssqlUnsealedArtifactAuthorityV2(
            item.artifact_id,
            item.artifact_kind,
            item.object_uuid,
            item.object_id,
            item.physical_token,
            item.catalog_digest,
            self.row_count,
            self.payload_bytes,
            self.ordered_logical_digest,
        )


@dataclass(frozen=True, slots=True)
class MssqlPreparedEffectRequestV2:
    """Versioned canonical payload retained beside a first-sealed intent."""

    request_payload: bytes
    request_payload_digest: bytes
    codec_version: str = EFFECT_REQUEST_CODEC_VERSION

    def __post_init__(self) -> None:
        if self.codec_version != EFFECT_REQUEST_CODEC_VERSION:
            raise MssqlReceiptContractError("sealed effect request codec is unsupported")
        if not isinstance(self.request_payload, bytes) or not self.request_payload.startswith(_PAYLOAD_DOMAIN):
            raise MssqlReceiptContractError("sealed effect request payload is invalid")
        require_digest(self.request_payload_digest, "request_payload_digest")
        if hashlib.sha256(self.request_payload).digest() != self.request_payload_digest:
            raise MssqlReceiptContractError("sealed effect request digest is invalid")

    @classmethod
    def from_request(cls, request: MssqlEffectRequestV2) -> MssqlPreparedEffectRequestV2:
        encoded = json.dumps(_encode(request), ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
        payload = _PAYLOAD_DOMAIN + encoded
        return cls(payload, hashlib.sha256(payload).digest())

    def rehydrate(self) -> MssqlEffectRequestV2:
        try:
            decoded = _decode(json.loads(self.request_payload[len(_PAYLOAD_DOMAIN) :]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise MssqlReceiptContractError("sealed effect request payload is invalid") from exc
        if not isinstance(decoded, MssqlEffectRequestV2):
            raise MssqlReceiptContractError("sealed effect request has the wrong root type")
        if self != self.from_request(decoded):
            raise MssqlReceiptContractError("sealed effect request payload is noncanonical")
        return decoded


@dataclass(frozen=True, slots=True)
class MssqlIntentSealRequestV2:
    """Atomic first-seal-wins transition for an exact effect request."""

    sealed_intent_id: UUID
    operation_epoch: int
    owner_id_digest: bytes
    retention_until: datetime
    request: MssqlEffectRequestV2

    def __post_init__(self) -> None:
        require_uuid(self.sealed_intent_id, "sealed_intent_id")
        require_positive(self.operation_epoch, "operation_epoch")
        require_digest(self.owner_id_digest, "owner_id_digest")
        if self.operation_epoch != self.request.header.operation_epoch:
            raise MssqlReceiptContractError("intent seal operation epoch differs from effect request")
        if not self.request.sealed_intent.artifacts:
            raise MssqlReceiptContractError("intent seal requires the complete artifact set")
        if self.retention_until.tzinfo is None or self.retention_until.utcoffset() is None:
            raise MssqlReceiptContractError("retention_until must be timezone-aware")

    @property
    def prepared_request(self) -> MssqlPreparedEffectRequestV2:
        return MssqlPreparedEffectRequestV2.from_request(self.request)


_TYPES = {
    item.__name__: item
    for item in (
        MssqlArtifactAuthorityV2,
        MssqlBatchEffectDraftV2,
        MssqlBusinessKeyV2,
        MssqlEffectReceiptHeaderV2,
        MssqlEffectRequestV2,
        MssqlGenerationAuthorityClaimV1,
        MssqlSealedIntentV2,
        MssqlTargetIdentityV2,
        MssqlXminCheckpointTransitionV2,
        MssqlXminEffectDraftV2,
    )
}
_ENUMS = {ReceiptKind.__name__: ReceiptKind, WriterMode.__name__: WriterMode}


def _encode(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return {
            "$type": type(value).__name__,
            "fields": [[f.name, _encode(getattr(value, f.name))] for f in fields(value)],
        }
    if isinstance(value, Enum):
        return {"$enum": type(value).__name__, "value": value.value}
    if isinstance(value, UUID):
        return {"$uuid": str(value)}
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    if isinstance(value, tuple):
        return {"$tuple": [_encode(item) for item in value]}
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise MssqlReceiptContractError(f"unsupported sealed effect request value: {type(value).__name__}")


def _decode(value: Any) -> object:
    if isinstance(value, list):
        return [_decode(item) for item in value]
    if not isinstance(value, dict):
        return value
    if "$bytes" in value:
        return bytes.fromhex(value["$bytes"])
    if "$uuid" in value:
        return UUID(value["$uuid"])
    if "$tuple" in value:
        return tuple(_decode(item) for item in value["$tuple"])
    if "$enum" in value:
        return _ENUMS[value["$enum"]](value["value"])
    cls = cast(Any, _TYPES[value["$type"]])
    return cls(**{name: _decode(item) for name, item in value["fields"]})


__all__ = [
    "EFFECT_REQUEST_CODEC_VERSION",
    "MssqlArtifactSealRequestV2",
    "MssqlIntentSealRequestV2",
    "MssqlOpenStagingArtifactV2",
    "MssqlPreparedEffectRequestV2",
    "MssqlUnsealedArtifactAuthorityV2",
]
