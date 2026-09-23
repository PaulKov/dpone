"""Finite worker evidence kinds, byte limits and exact create-only receipts."""

from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from dpone.contracts.mssql_tds_validation import _hash, _integer

ERROR = "mssql_native.sqlclient_evidence_invalid"


class SqlClientEvidenceKind(StrEnum):
    """No CREATE, state, settlement or arbitrary extension kinds."""

    REGISTRATION = "registration"
    CREDENTIAL_INTENT = "credential_intent"
    WRITER_OBSERVATION = "writer_observation"
    GRANT_INTENT = "grant_intent"
    RESULT = "result"
    LOCAL_EXIT = "local_exit"
    VERIFICATION = "verification"


EVIDENCE_LIMITS = MappingProxyType(
    {
        kind: 2_097_152 if kind in (SqlClientEvidenceKind.REGISTRATION, SqlClientEvidenceKind.VERIFICATION) else 16_384
        for kind in SqlClientEvidenceKind
    }
)


def evidence_limit(kind: SqlClientEvidenceKind) -> int:
    """Single immutable cap owner; reject string/enum aliases."""
    if type(kind) is not SqlClientEvidenceKind:
        raise ValueError(ERROR)
    return EVIDENCE_LIMITS[kind]


def require_payload(payload: bytes, kind: SqlClientEvidenceKind) -> None:
    """Bound bytes before any JSON parsing or hashing."""
    if type(payload) is not bytes or not 0 < len(payload) <= evidence_limit(kind):
        raise ValueError(ERROR)


def evidence_name(attempt_sha256: str, kind: SqlClientEvidenceKind, payload_sha256: str) -> str:
    """Closed technical filename, without an arbitrary path component."""
    _hash(attempt_sha256)
    _hash(payload_sha256)
    evidence_limit(kind)
    return f"tds-sqlclient-{attempt_sha256}-{kind.value}-{payload_sha256}.json"


@dataclass(frozen=True, slots=True)
class SqlClientEvidenceReceipt:
    """Exact acknowledged artifact byte hash, not a semantic wire digest."""

    attempt_sha256: str
    kind: SqlClientEvidenceKind
    relative_name: str
    payload_sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        expected = evidence_name(self.attempt_sha256, self.kind, self.payload_sha256)
        _integer(self.byte_count, 1, evidence_limit(self.kind))
        if type(self.relative_name) is not str or self.relative_name != expected:
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientEvidenceObservation:
    """Only the last acknowledged receipt; late commits are not observations."""

    attempt_sha256: str
    receipt: SqlClientEvidenceReceipt | None = None

    def __post_init__(self) -> None:
        _hash(self.attempt_sha256)
        if self.receipt is not None:
            if type(self.receipt) is not SqlClientEvidenceReceipt:
                raise ValueError(ERROR)
            self.receipt.__post_init__()
            if self.receipt.attempt_sha256 != self.attempt_sha256:
                raise ValueError(ERROR)
