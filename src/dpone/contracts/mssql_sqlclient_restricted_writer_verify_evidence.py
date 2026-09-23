"""Dedicated create-only evidence for the six P9a local phases."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_sqlclient_restricted_writer_verify import ERROR
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

LIMIT = 1048576


class RestrictedWriterVerifyEvidenceKind(StrEnum):
    RESERVATION = "reservation"
    LAUNCH_INTENT = "launch-intent"
    REGISTRATION = "registration"
    CREDENTIAL_INTENT = "credential-intent"
    RESULT = "result"
    LOCAL_EXIT = "local-exit"


ORDER = tuple(RestrictedWriterVerifyEvidenceKind)
SCHEMAS = {kind: f"dpone.sqlclient.restricted-writer-verify-{kind.value}-evidence.v1" for kind in ORDER}
FIELDS = {
    RestrictedWriterVerifyEvidenceKind.RESERVATION: {"request_sha256", "parent_sha256"},
    RestrictedWriterVerifyEvidenceKind.LAUNCH_INTENT: {"public_sha256", "implementation_sha256"},
    RestrictedWriterVerifyEvidenceKind.REGISTRATION: {"process_pid"},
    RestrictedWriterVerifyEvidenceKind.CREDENTIAL_INTENT: {"credential_frame"},
    RestrictedWriterVerifyEvidenceKind.RESULT: {"result_sha256", "session_authority_sha256"},
    RestrictedWriterVerifyEvidenceKind.LOCAL_EXIT: {"process_pid", "exit_code", "reaped"},
}


def _relative_name(operation_id: UUID, kind: RestrictedWriterVerifyEvidenceKind, digest: str) -> str:
    return f"tds-sqlclient-restricted-writer-verify-{operation_id}-{kind.value}-{digest}.json"


def _facts(kind: RestrictedWriterVerifyEvidenceKind, facts: dict[str, object]) -> None:
    if set(facts) != FIELDS[kind]:
        raise ValueError(ERROR)
    if kind in (RestrictedWriterVerifyEvidenceKind.RESERVATION, RestrictedWriterVerifyEvidenceKind.LAUNCH_INTENT):
        for value in facts.values():
            _hash(value)
    elif kind is RestrictedWriterVerifyEvidenceKind.REGISTRATION:
        if type(facts["process_pid"]) is not int or not 1 <= facts["process_pid"] <= 2**31 - 1:
            raise ValueError(ERROR)
    elif kind is RestrictedWriterVerifyEvidenceKind.CREDENTIAL_INTENT:
        if facts["credential_frame"] != "dpone.sqlclient.restricted-writer-verify-credentials.v1":
            raise ValueError(ERROR)
    elif kind is RestrictedWriterVerifyEvidenceKind.RESULT:
        _hash(facts["result_sha256"])
        _hash(facts["session_authority_sha256"])
    elif (
        type(facts["process_pid"]) is not int
        or not 1 <= facts["process_pid"] <= 2**31 - 1
        or type(facts["exit_code"]) is not int
        or facts["exit_code"] != 0
        or facts["reaped"] is not True
    ):
        raise ValueError(ERROR)


def evidence_payload(kind: RestrictedWriterVerifyEvidenceKind, operation_id: UUID, **facts: object) -> bytes:
    """Encode one closed nonsecret phase record."""
    if type(kind) is not RestrictedWriterVerifyEvidenceKind or type(operation_id) is not UUID or not operation_id.int:
        raise ValueError(ERROR)
    _facts(kind, facts)
    body = {"schema": SCHEMAS[kind], "operation_id": str(operation_id), **facts}
    payload = canonical_json_bytes(body)
    if not 0 < len(payload) <= LIMIT:
        raise ValueError(ERROR)
    return payload


@dataclass(frozen=True, slots=True)
class RestrictedWriterVerifyEvidenceReceipt:
    operation_id: UUID
    kind: RestrictedWriterVerifyEvidenceKind
    relative_name: str
    payload_sha256: str
    size: int

    def __post_init__(self) -> None:
        if (
            type(self.operation_id) is not UUID
            or not self.operation_id.int
            or type(self.kind) is not RestrictedWriterVerifyEvidenceKind
            or type(self.relative_name) is not str
            or self.relative_name != _relative_name(self.operation_id, self.kind, self.payload_sha256)
            or type(self.size) is not int
            or not 0 < self.size <= LIMIT
        ):
            raise ValueError(ERROR)
        _hash(self.payload_sha256)


@dataclass(frozen=True, slots=True)
class RestrictedWriterVerifyEvidenceRecord:
    operation_id: UUID
    kind: RestrictedWriterVerifyEvidenceKind
    payload: bytes

    def __post_init__(self) -> None:
        try:
            if (
                type(self.operation_id) is not UUID
                or not self.operation_id.int
                or type(self.kind) is not RestrictedWriterVerifyEvidenceKind
                or type(self.payload) is not bytes
                or not 0 < len(self.payload) <= LIMIT
            ):
                raise ValueError
            body = strict_json_object(self.payload)
            if (
                set(body) != {"schema", "operation_id", *FIELDS[self.kind]}
                or body["schema"] != SCHEMAS[self.kind]
                or body["operation_id"] != str(self.operation_id)
                or canonical_json_bytes(body) != self.payload
            ):
                raise ValueError
            _facts(self.kind, {name: body[name] for name in FIELDS[self.kind]})
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            raise ValueError(ERROR) from None

    @property
    def receipt(self) -> RestrictedWriterVerifyEvidenceReceipt:
        self.__post_init__()
        digest = sha256(self.payload).hexdigest()
        return RestrictedWriterVerifyEvidenceReceipt(
            self.operation_id,
            self.kind,
            _relative_name(self.operation_id, self.kind, digest),
            digest,
            len(self.payload),
        )


@dataclass(frozen=True, slots=True)
class RestrictedWriterVerifyEvidenceObservation:
    operation_id: UUID
    receipts: tuple[RestrictedWriterVerifyEvidenceReceipt, ...] = ()

    def __post_init__(self) -> None:
        if type(self.operation_id) is not UUID or not self.operation_id.int or type(self.receipts) is not tuple:
            raise ValueError(ERROR)
        if len(self.receipts) > len(ORDER):
            raise ValueError(ERROR)
        for receipt, kind in zip(self.receipts, ORDER, strict=False):
            if (
                type(receipt) is not RestrictedWriterVerifyEvidenceReceipt
                or receipt.operation_id != self.operation_id
                or receipt.kind is not kind
            ):
                raise ValueError(ERROR)
            receipt.__post_init__()
