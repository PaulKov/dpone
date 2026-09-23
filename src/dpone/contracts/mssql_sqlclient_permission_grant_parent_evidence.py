"""Durable parent-evidence records for restricted permission grants.

The canonical payload codecs remain re-exported here for compatibility.
"""

from dataclasses import dataclass, field
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_permission_grant_evidence_codec import *  # noqa: F403
from dpone.contracts.mssql_sqlclient_permission_grant_evidence_codec import (
    ADMISSION_SCHEMA,
    CAPS,
    ERROR,
    HELD_READY_SCHEMA,
    K,
    PermissionGrantEvidenceSubject,
    PermissionGrantHeldReadyEvidence,
    PermissionGrantParentEvidenceKind,
    _binding,
    _invalid,
    _name,
    _registration,
    _subject,
    _validate_payload,
    _wire,
    decode_permission_grant_held_ready_evidence,
    encode_permission_grant_held_ready_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import PermissionWireBinding, PermissionWireKind
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes


@dataclass(frozen=True, slots=True)
class PermissionGrantParentEvidenceRecord:
    subject: PermissionGrantEvidenceSubject
    kind: PermissionGrantParentEvidenceKind
    payload: bytes = field(repr=False)
    binding: PermissionWireBinding = field(repr=False, kw_only=True)

    def __post_init__(self) -> None:
        try:
            subject = _subject(self.subject)
            _binding(self.binding)
            if (
                type(self.kind) is not K
                or type(self.payload) is not bytes
                or not 0 < len(self.payload) <= CAPS[self.kind]
            ):
                raise _invalid()
            if subject.attempt_sha256 != attempt_identity_digest(
                self.binding.request.parent
            ) or subject.operation_sha256 != coordinator_identity_digest(self.binding.operation):
                raise _invalid()
            _validate_payload(self.kind, self.payload, self.binding)
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError, UnicodeError):
            raise _invalid() from None

    @property
    def receipt(self) -> "PermissionGrantParentEvidenceReceipt":
        PermissionGrantParentEvidenceRecord(self.subject, self.kind, self.payload, binding=self.binding)
        digest = sha256(self.payload).hexdigest()
        return PermissionGrantParentEvidenceReceipt(
            self.subject, self.kind, _name(self.subject, self.kind, digest), digest, len(self.payload)
        )


@dataclass(frozen=True, slots=True)
class PermissionGrantParentEvidenceContext:
    """Retain and revalidate the exact permission evidence trust boundary."""

    subject: PermissionGrantEvidenceSubject
    binding: object = field(repr=False)
    binding_snapshot: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        try:
            subject = _subject(self.subject)
            binding = self.binding
            snapshot = _binding(binding)
            if type(binding) is not PermissionWireBinding:
                raise _invalid()
            if subject.attempt_sha256 != attempt_identity_digest(
                binding.request.parent
            ) or subject.operation_sha256 != coordinator_identity_digest(binding.operation):
                raise _invalid()
            object.__setattr__(self, "subject", subject)
            object.__setattr__(self, "binding_snapshot", snapshot)
        except (ValueError, TypeError, AttributeError):
            raise _invalid() from None

    def record(self, value: object) -> PermissionGrantParentEvidenceRecord:
        """Reconstruct one record after checking retained and incoming bindings."""
        try:
            subject = _subject(self.subject)
            if _binding(self.binding) != self.binding_snapshot:
                raise _invalid()
            if type(value) is not PermissionGrantParentEvidenceRecord:
                raise _invalid()
            if value.subject != subject or _binding(value.binding) != self.binding_snapshot:
                raise _invalid()
            return PermissionGrantParentEvidenceRecord(value.subject, value.kind, value.payload, binding=value.binding)
        except (ValueError, TypeError, AttributeError):
            raise _invalid() from None


@dataclass(frozen=True, slots=True)
class PermissionGrantParentEvidenceReceipt:
    subject: PermissionGrantEvidenceSubject
    kind: PermissionGrantParentEvidenceKind
    relative_name: str
    payload_sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        try:
            subject = _subject(self.subject)
            if type(self.kind) is not K:
                raise _invalid()
            _hash(self.payload_sha256)
            _integer(self.byte_count, 1, CAPS[self.kind])
            if type(self.relative_name) is not str or self.relative_name != _name(
                subject, self.kind, self.payload_sha256
            ):
                raise _invalid()
        except (ValueError, TypeError, KeyError):
            raise _invalid() from None


@dataclass(frozen=True, slots=True)
class PermissionGrantParentEvidenceObservation:
    subject: PermissionGrantEvidenceSubject
    receipt: PermissionGrantParentEvidenceReceipt | None = None

    def __post_init__(self) -> None:
        try:
            subject = _subject(self.subject)
            if self.receipt is not None:
                if type(self.receipt) is not PermissionGrantParentEvidenceReceipt:
                    raise _invalid()
                receipt = PermissionGrantParentEvidenceReceipt(
                    self.receipt.subject,
                    self.receipt.kind,
                    self.receipt.relative_name,
                    self.receipt.payload_sha256,
                    self.receipt.byte_count,
                )
                if receipt.subject != subject:
                    raise _invalid()
        except (ValueError, TypeError):
            raise _invalid() from None


def registration_admission_sha256(record: PermissionGrantParentEvidenceRecord) -> str:
    """Return the already-validated registration claim for actor ordering."""
    if type(record) is not PermissionGrantParentEvidenceRecord or record.kind is not K.REGISTRATION:
        raise _invalid()
    return _registration(record.payload, record.binding)


def held_ready_bindings(record: PermissionGrantParentEvidenceRecord) -> tuple[str, bytes]:
    """Return RESULT claim and canonical HELD authority for actor ordering."""
    if type(record) is not PermissionGrantParentEvidenceRecord or record.kind is not K.HELD_READY:
        raise _invalid()
    value = decode_permission_grant_held_ready_evidence(record.payload, binding=record.binding)
    held = _wire(value.held_payload, record.binding, PermissionWireKind.HELD, 4)
    return value.permission_evidence_sha256, canonical_json_bytes(held["authority"])


def authority_bytes(record: PermissionGrantParentEvidenceRecord) -> bytes:
    """Return the canonical authority from a validated AUTHORITY record."""
    if type(record) is not PermissionGrantParentEvidenceRecord or record.kind is not K.AUTHORITY:
        raise _invalid()
    body = _wire(record.payload, record.binding, PermissionWireKind.AUTHORITY, 2)
    return canonical_json_bytes(body["authority"])


__all__ = [
    "ADMISSION_SCHEMA",
    "CAPS",
    "ERROR",
    "HELD_READY_SCHEMA",
    "K",
    "PermissionGrantEvidenceSubject",
    "PermissionGrantHeldReadyEvidence",
    "PermissionGrantParentEvidenceContext",
    "PermissionGrantParentEvidenceKind",
    "PermissionGrantParentEvidenceObservation",
    "PermissionGrantParentEvidenceReceipt",
    "PermissionGrantParentEvidenceRecord",
    "authority_bytes",
    "decode_permission_grant_held_ready_evidence",
    "encode_permission_grant_held_ready_evidence",
    "held_ready_bindings",
    "registration_admission_sha256",
]
