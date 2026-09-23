"""Immutable authority, identity, containment, and DROP operation contracts for P10g retirement."""

from __future__ import annotations

from dataclasses import InitVar, asdict, dataclass
from enum import StrEnum
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_native_chunks import NativeChunkReceipt
from dpone.contracts.mssql_native_parent_journal import NativeParentAuthority, canonical_digest
from dpone.contracts.mssql_sqlclient_native_chunk import SqlClientNativeChunkProjection
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import (
    canonical_stage_id,
    validate_native_chunk_receipt,
)
from dpone.contracts.strict_json import canonical_json_bytes

_ERROR = "mssql_native.chunk_retirement_invalid"


_JOURNAL_ISSUER = object()


def _digest(value: object) -> None:
    if type(value) is not str or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(_ERROR)


def native_chunk_object_incarnation_digest(request: NativeChunkRetirementRequest) -> str:
    """Canonical exact object identity expected before and after DROP."""
    if type(request) is not NativeChunkRetirementRequest:
        raise ValueError(_ERROR)
    return sha256(canonical_json_bytes(asdict(request.projection.object_identity))).hexdigest()


def native_chunk_parent_stage_id(projection: SqlClientNativeChunkProjection) -> str:
    """Closed parent representation for the exact projected stage incarnation."""
    if type(projection) is not SqlClientNativeChunkProjection:
        raise ValueError(_ERROR)
    return canonical_stage_id(projection.object_identity)


def native_chunk_parent_evidence(projection: SqlClientNativeChunkProjection) -> dict[str, str]:
    """Closed parent evidence; arbitrary mappings cannot authorize retirement."""
    if type(projection) is not SqlClientNativeChunkProjection:
        raise ValueError(_ERROR)
    return {
        "sqlclient_projection_sha256": projection.projection_sha256,
        "sqlclient_verification_payload_sha256": projection.verification_receipt.payload_sha256,
    }


class NativeChunkDropOutcome(StrEnum):
    """Closed DROP outcome; only SUCCEEDED permits a later absence query."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"
    NO_EFFECT = "no_effect"


class NativeChunkDropObservation(StrEnum):
    INITIAL = "initial"
    RECONCILED = "reconciled"


@dataclass(frozen=True, slots=True)
class NativeChunkDropIntent:
    operation_sha256: str
    effect_attempt: int
    proof_sha256: str

    def __post_init__(self) -> None:
        _digest(self.operation_sha256)
        _digest(self.proof_sha256)
        if type(self.effect_attempt) is not int or self.effect_attempt not in (0, 1):
            raise ValueError(_ERROR)


class NativeChunkLifecyclePhase(StrEnum):
    VERIFIED = "verified"
    CONTAINMENT_REQUIRED = "containment_required"
    CONTAINED = "contained"
    RETIREMENT_REQUIRED = "retirement_required"
    RETIRED = "retired"


@dataclass(frozen=True, slots=True)
class NativeChunkLifecycleProof:
    phase: NativeChunkLifecyclePhase
    projection_sha256: str
    authorization_sha256: str
    directory_key: str
    directory_revision: int
    authority_digest: str
    authority_fence: int
    lifecycle_revision: int
    proof_sha256: str

    def __post_init__(self) -> None:
        if type(self.phase) is not NativeChunkLifecyclePhase or type(self.directory_key) is not str:
            raise ValueError(_ERROR)
        if any(
            type(v) is not int or v < 1
            for v in (self.directory_revision, self.authority_fence, self.lifecycle_revision)
        ):
            raise ValueError(_ERROR)
        for value in (
            self.projection_sha256,
            self.authorization_sha256,
            self.authority_digest,
            self.proof_sha256,
        ):
            _digest(value)
        if self.proof_sha256 != _lifecycle_digest(self):
            raise ValueError(_ERROR)


def _lifecycle_digest(value: NativeChunkLifecycleProof) -> str:
    body = asdict(value)
    body["phase"] = value.phase.value
    body.pop("proof_sha256")
    return canonical_digest(body)


def bind_native_chunk_lifecycle(**facts: object) -> NativeChunkLifecycleProof:
    provisional = object.__new__(NativeChunkLifecycleProof)
    for name, value in facts.items():
        object.__setattr__(provisional, name, value)
    object.__setattr__(provisional, "proof_sha256", "0" * 64)
    return NativeChunkLifecycleProof(**facts, proof_sha256=_lifecycle_digest(provisional))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class NativeChunkRetirementAuthorization:
    """Self-binding record emitted from one exact settled parent journal."""

    authority: NativeParentAuthority
    parent_journal_sha256: str
    parent_identity_sha256: str
    projection_sha256: str
    chunk_receipt: NativeChunkReceipt
    authorization_sha256: str
    _issuer: InitVar[object] = None

    def __post_init__(self, _issuer: object) -> None:
        if (
            _issuer is not _JOURNAL_ISSUER
            or type(self.authority) is not NativeParentAuthority
            or type(self.chunk_receipt) is not NativeChunkReceipt
        ):
            raise ValueError(_ERROR)
        for value in (
            self.parent_journal_sha256,
            self.parent_identity_sha256,
            self.projection_sha256,
            self.authorization_sha256,
        ):
            _digest(value)
        if self.authorization_sha256 != _authorization_digest(self):
            raise ValueError(_ERROR)


def _authorization_digest(value: NativeChunkRetirementAuthorization) -> str:
    return canonical_digest(
        {
            "authority": asdict(value.authority),
            "parent_journal_sha256": value.parent_journal_sha256,
            "parent_identity_sha256": value.parent_identity_sha256,
            "projection_sha256": value.projection_sha256,
            "chunk_receipt": asdict(value.chunk_receipt),
        }
    )


def _issue_native_chunk_retirement_authorization(
    *,
    authority: NativeParentAuthority,
    parent_journal_sha256: str,
    parent_identity_sha256: str,
    projection_sha256: str,
    chunk_receipt: NativeChunkReceipt,
) -> NativeChunkRetirementAuthorization:
    facts = dict(
        authority=authority,
        parent_journal_sha256=parent_journal_sha256,
        parent_identity_sha256=parent_identity_sha256,
        projection_sha256=projection_sha256,
        chunk_receipt=chunk_receipt,
    )
    provisional = object.__new__(NativeChunkRetirementAuthorization)
    for name, value in facts.items():
        object.__setattr__(provisional, name, value)
    object.__setattr__(provisional, "authorization_sha256", "0" * 64)
    return NativeChunkRetirementAuthorization(
        authority,
        parent_journal_sha256,
        parent_identity_sha256,
        projection_sha256,
        chunk_receipt,
        _authorization_digest(provisional),
        _JOURNAL_ISSUER,
    )


@dataclass(frozen=True, slots=True)
class NativeChunkRetirementRequest:
    """Exact verified projection and settled parent cleanup authority."""

    projection: SqlClientNativeChunkProjection
    authorization: NativeChunkRetirementAuthorization

    def __post_init__(self) -> None:
        if (
            type(self.projection) is not SqlClientNativeChunkProjection
            or type(self.authorization) is not NativeChunkRetirementAuthorization
            or self.authorization.projection_sha256 != self.projection.projection_sha256
            or self.authorization.parent_identity_sha256 != self.projection.attempt.plan_sha256
        ):
            raise ValueError(_ERROR)
        expected_attempt = (
            f"{self.projection.attempt.run_id}-{self.projection.attempt.ordinal}-{self.projection.attempt.attempt}"
        )
        receipt = self.authorization.chunk_receipt
        if (
            receipt.ordinal != self.projection.attempt.ordinal
            or receipt.attempt_id != expected_attempt
            or receipt.stage_id != native_chunk_parent_stage_id(self.projection)
            or receipt.rows != self.projection.rows
            or receipt.encoded_bytes != self.projection.encoded_bytes
            or receipt.file_sha256 != self.projection.file_sha256
            or receipt.typed_digest != self.projection.typed_digest
        ):
            raise ValueError(_ERROR)
        try:
            validate_native_chunk_receipt(receipt, projection=self.projection)
        except ValueError:
            raise ValueError(_ERROR) from None

    @property
    def authority(self) -> NativeParentAuthority:
        return self.authorization.authority

    @property
    def parent_receipt(self) -> NativeChunkReceipt:
        return self.authorization.chunk_receipt

    @property
    def parent_verification_sha256(self) -> str:
        """Digest expected by the v4 parent retirement receipt."""
        return canonical_digest(asdict(self.parent_receipt))


@dataclass(frozen=True, slots=True)
class NativeChunkContainmentProof:
    """Independent proof that no process or credential channel remains."""

    projection_sha256: str
    authorization_sha256: str
    directory_key: str
    directory_revision: int
    authority_digest: str
    authority_fence: int
    lifecycle_revision: int
    process_absence_sha256: str
    credential_channel_absence_sha256: str
    proof_sha256: str

    def __post_init__(self) -> None:
        if type(self.directory_key) is not str or any(
            type(v) is not int or v < 1
            for v in (self.directory_revision, self.authority_fence, self.lifecycle_revision)
        ):
            raise ValueError(_ERROR)
        for value in (
            self.projection_sha256,
            self.authorization_sha256,
            self.authority_digest,
            self.process_absence_sha256,
            self.credential_channel_absence_sha256,
            self.proof_sha256,
        ):
            _digest(value)
        if self.proof_sha256 != _simple_digest(self):
            raise ValueError(_ERROR)


def _simple_digest(value: Any) -> str:
    body = asdict(value)
    body.pop("proof_sha256")
    for key, item in tuple(body.items()):
        if isinstance(item, UUID):
            body[key] = str(item)
        elif isinstance(item, NativeChunkDropOutcome | NativeChunkDropObservation):
            body[key] = item.value
    return canonical_digest(body)


def _bind_simple(cls: type[Any], facts: dict[str, object]) -> Any:
    provisional: Any = object.__new__(cls)
    for name, value in facts.items():
        object.__setattr__(provisional, name, value)
    object.__setattr__(provisional, "proof_sha256", "0" * 64)
    return cls(**facts, proof_sha256=_simple_digest(provisional))


def bind_native_chunk_containment(**facts: object) -> NativeChunkContainmentProof:
    return _bind_simple(NativeChunkContainmentProof, facts)


@dataclass(frozen=True, slots=True)
class NativeChunkRetirementReservation:
    """Deterministic one-shot RETIRE reservation bound to the exact request."""

    operation_id: UUID
    operation_sha256: str

    def __post_init__(self) -> None:
        if type(self.operation_id) is not UUID or not self.operation_id.int:
            raise ValueError(_ERROR)
        _digest(self.operation_sha256)

    @classmethod
    def deterministic(cls, request: NativeChunkRetirementRequest) -> NativeChunkRetirementReservation:
        if type(request) is not NativeChunkRetirementRequest:
            raise ValueError(_ERROR)
        body = canonical_json_bytes(
            {
                "schema": "dpone.sqlclient.native-chunk-retire.v1",
                "projection_sha256": request.projection.projection_sha256,
                "parent_authority_digest": request.authority.digest,
                "parent_fence": request.authority.fence,
            }
        )
        digest = sha256(body).hexdigest()
        raw = bytearray(bytes.fromhex(digest[:32]))
        raw[6] = (raw[6] & 0x0F) | 0x50
        raw[8] = (raw[8] & 0x3F) | 0x80
        return cls(UUID(bytes=bytes(raw)), digest)


@dataclass(frozen=True, slots=True)
class NativeChunkDropProof:
    """Settled or unknown result of the closed exact DROP operation."""

    operation_sha256: str
    effect_attempt: int
    observation: NativeChunkDropObservation
    outcome: NativeChunkDropOutcome
    proof_sha256: str

    def __post_init__(self) -> None:
        _digest(self.operation_sha256)
        _digest(self.proof_sha256)
        if (
            type(self.effect_attempt) is not int
            or self.effect_attempt not in (0, 1)
            or type(self.observation) is not NativeChunkDropObservation
            or type(self.outcome) is not NativeChunkDropOutcome
            or (
                self.outcome is NativeChunkDropOutcome.NO_EFFECT
                and self.observation is not NativeChunkDropObservation.RECONCILED
            )
        ):
            raise ValueError(_ERROR)
        if self.proof_sha256 != _simple_digest(self):
            raise ValueError(_ERROR)


def bind_native_chunk_drop(**facts: object) -> NativeChunkDropProof:
    return _bind_simple(NativeChunkDropProof, facts)
