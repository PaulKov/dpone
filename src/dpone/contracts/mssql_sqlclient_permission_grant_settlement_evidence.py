"""Durable evidence records for permission-grant settlement."""

from dataclasses import dataclass, field
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_permission_grant_settlement_codec import (
    CAPS,
    REMOTE_SETTLEMENT_SCHEMA,
    RK,
    K,
    PermissionGrantSettlementEvidenceSubject,
    SettlementKind,
    _binding,
    _decode_local_exit,
    _invalid,
    _name,
    _subject,
    _wire,
    require_permission_grant_local_exit,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import PermissionWireBinding, PermissionWireKind
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


@dataclass(frozen=True, slots=True)
class PermissionGrantSettlementEvidenceRecord:
    subject: PermissionGrantSettlementEvidenceSubject
    kind: SettlementKind
    payload: bytes = field(repr=False)
    binding: PermissionWireBinding = field(repr=False, kw_only=True)

    def __post_init__(self) -> None:
        try:
            subject = _subject(self.subject)
            snapshot = _binding(self.binding)
            if (
                type(self.kind) not in (K, RK)
                or type(self.payload) is not bytes
                or not 0 < len(self.payload) <= CAPS[self.kind]
            ):
                raise _invalid()
            if sha256(snapshot).hexdigest() != subject.binding_sha256:
                raise _invalid()
            if self.kind is K.LOCAL_EXIT:
                require_permission_grant_local_exit(
                    _decode_local_exit(self.payload), self.binding, exact_identity=False
                )
            elif self.kind is RK.REMOTE_SETTLEMENT:
                body = strict_json_object(self.payload)
                if (
                    set(body)
                    != {
                        "schema",
                        "authority_sha256",
                        "result_sha256",
                        "local_exit_sha256",
                        "exclusion_sha256",
                    }
                    or body["schema"] != REMOTE_SETTLEMENT_SCHEMA
                ):
                    raise _invalid()
                for name in set(body) - {"schema"}:
                    _hash(body[name])
            else:
                kind = PermissionWireKind.RELEASE if self.kind is K.RELEASE_INTENT else PermissionWireKind.RELEASED
                if _wire(self.payload, self.binding, kind)["evidence_sha256"] != subject.result_sha256:
                    raise _invalid()
            if canonical_json_bytes(strict_json_object(self.payload)) != self.payload:
                raise _invalid()
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError, UnicodeError):
            raise _invalid() from None

    @property
    def receipt(self) -> "PermissionGrantSettlementEvidenceReceipt":
        PermissionGrantSettlementEvidenceRecord(self.subject, self.kind, self.payload, binding=self.binding)
        digest = sha256(self.payload).hexdigest()
        return PermissionGrantSettlementEvidenceReceipt(
            self.subject, self.kind, _name(self.subject, self.kind, digest), digest, len(self.payload)
        )


@dataclass(frozen=True, slots=True)
class PermissionGrantSettlementEvidenceReceipt:
    subject: PermissionGrantSettlementEvidenceSubject
    kind: SettlementKind
    relative_name: str
    payload_sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        try:
            subject = _subject(self.subject)
            if type(self.kind) not in (K, RK):
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
class PermissionGrantSettlementEvidenceObservation:
    subject: PermissionGrantSettlementEvidenceSubject
    receipt: PermissionGrantSettlementEvidenceReceipt | None = None

    def __post_init__(self) -> None:
        subject = _subject(self.subject)
        if self.receipt is not None:
            if type(self.receipt) is not PermissionGrantSettlementEvidenceReceipt:
                raise _invalid()
            checked = PermissionGrantSettlementEvidenceReceipt(
                self.receipt.subject,
                self.receipt.kind,
                self.receipt.relative_name,
                self.receipt.payload_sha256,
                self.receipt.byte_count,
            )
            if checked.subject != subject:
                raise _invalid()


@dataclass(frozen=True, slots=True)
class PermissionGrantSettlementEvidenceContext:
    subject: PermissionGrantSettlementEvidenceSubject
    binding: object = field(repr=False)
    binding_snapshot: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "subject", _subject(self.subject))
        object.__setattr__(self, "binding_snapshot", _binding(self.binding))

    def record(self, value: object) -> PermissionGrantSettlementEvidenceRecord:
        try:
            if (
                _binding(self.binding) != self.binding_snapshot
                or type(value) is not PermissionGrantSettlementEvidenceRecord
            ):
                raise _invalid()
            if value.subject != self.subject or _binding(value.binding) != self.binding_snapshot:
                raise _invalid()
            return PermissionGrantSettlementEvidenceRecord(
                value.subject, value.kind, value.payload, binding=value.binding
            )
        except (ValueError, TypeError, AttributeError):
            raise _invalid() from None

    def transition(
        self,
        previous: PermissionGrantSettlementEvidenceRecord | None,
        current: PermissionGrantSettlementEvidenceRecord,
    ) -> None:
        """Validate the ordered cross-record claim at the actor boundary."""
        current = self.record(current)
        if current.kind is K.RELEASED:
            if previous is None:
                raise _invalid()
            release_payloads(self.record(previous), current)
        elif current.kind is K.LOCAL_EXIT:
            decode_permission_grant_local_exit(current)


def release_payloads(
    intent: PermissionGrantSettlementEvidenceRecord,
    released: PermissionGrantSettlementEvidenceRecord,
) -> tuple[bytes, bytes]:
    if intent.kind is not K.RELEASE_INTENT or released.kind is not K.RELEASED or intent.subject != released.subject:
        raise _invalid()
    left = _wire(intent.payload, intent.binding, PermissionWireKind.RELEASE)
    right = _wire(released.payload, released.binding, PermissionWireKind.RELEASED)
    if left != right:
        raise _invalid()
    return intent.payload, released.payload


def decode_permission_grant_local_exit(record: PermissionGrantSettlementEvidenceRecord):
    if type(record) is not PermissionGrantSettlementEvidenceRecord or record.kind is not K.LOCAL_EXIT:
        raise _invalid()
    result = _decode_local_exit(record.payload)
    return require_permission_grant_local_exit(result, record.binding, exact_identity=False)
