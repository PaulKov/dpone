"""Bounded helper-departure evidence vocabulary and subject-bound receipts.

These pure values validate shape and identity only. They neither authenticate
an evidence producer nor prove an artifact ACK, departure, or route outcome.
The parent evidence actor owns persistence and the codecs own payload parsing.
"""

from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from dpone.contracts.mssql_tds_validation import _hash, _integer

ERROR = "mssql_native.sqlclient_departure_evidence_invalid"


class SqlClientDepartureEvidenceKind(StrEnum):
    """Closed helper kinds, independent of the worker evidence vocabulary."""

    LAUNCH_INTENT = "launch_intent"
    REGISTRATION = "registration"
    CREDENTIAL_INTENT = "credential_intent"
    RESULT = "result"
    LOCAL_EXIT = "local_exit"
    EXCLUSION = "exclusion"


EVIDENCE_LIMITS = MappingProxyType(
    {
        SqlClientDepartureEvidenceKind.LAUNCH_INTENT: 131_072,
        SqlClientDepartureEvidenceKind.REGISTRATION: 32_768,
        SqlClientDepartureEvidenceKind.CREDENTIAL_INTENT: 131_072,
        SqlClientDepartureEvidenceKind.RESULT: 65_536,
        SqlClientDepartureEvidenceKind.LOCAL_EXIT: 16_384,
        SqlClientDepartureEvidenceKind.EXCLUSION: 16_384,
    }
)


def evidence_limit(kind: SqlClientDepartureEvidenceKind) -> int:
    """Return the immutable byte cap; equal strings or other enums are invalid."""
    if type(kind) is not SqlClientDepartureEvidenceKind:
        raise ValueError(ERROR)
    return EVIDENCE_LIMITS[kind]


def require_payload(payload: bytes, kind: SqlClientDepartureEvidenceKind) -> None:
    """Require nonempty exact bytes within the kind cap before any parsing."""
    if type(payload) is not bytes or not 0 < len(payload) <= evidence_limit(kind):
        raise ValueError(ERROR)


def _require_subject(helper_id: UUID, attempt_sha256: str) -> None:
    if type(helper_id) is not UUID or helper_id.int == 0:
        raise ValueError(ERROR)
    try:
        _hash(attempt_sha256)
    except ValueError:
        raise ValueError(ERROR) from None


def evidence_name(
    helper_id: UUID,
    attempt_sha256: str,
    kind: SqlClientDepartureEvidenceKind,
    payload_sha256: str,
) -> str:
    """Build the sole admitted relative name from exact technical identities."""
    _require_subject(helper_id, attempt_sha256)
    evidence_limit(kind)
    try:
        _hash(payload_sha256)
    except ValueError:
        raise ValueError(ERROR) from None
    return f"tds-sqlclient-departure-{attempt_sha256}-{helper_id}-{kind.value}-{payload_sha256}.json"


@dataclass(frozen=True, slots=True)
class SqlClientDepartureEvidenceReceipt:
    """Exact byte metadata for one helper and attempt; not an ACK authenticator.

    ``payload_sha256`` identifies raw artifact bytes, not a semantic digest.
    ``relative_name`` must match the generated name and cannot supply a path.
    """

    helper_id: UUID
    attempt_sha256: str
    kind: SqlClientDepartureEvidenceKind
    relative_name: str
    payload_sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        expected = evidence_name(self.helper_id, self.attempt_sha256, self.kind, self.payload_sha256)
        try:
            _integer(self.byte_count, 1, evidence_limit(self.kind))
        except ValueError:
            raise ValueError(ERROR) from None
        if type(self.relative_name) is not str or self.relative_name != expected:
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientDepartureEvidenceObservation:
    """Optional receipt for this helper and attempt, with nested revalidation.

    Revalidation rejects malformed receipts even if frozen construction was
    bypassed. Matching identities alone do not establish producer provenance.
    """

    helper_id: UUID
    attempt_sha256: str
    receipt: SqlClientDepartureEvidenceReceipt | None = None

    def __post_init__(self) -> None:
        _require_subject(self.helper_id, self.attempt_sha256)
        if self.receipt is not None:
            if type(self.receipt) is not SqlClientDepartureEvidenceReceipt:
                raise ValueError(ERROR)
            self.receipt.__post_init__()
            if self.receipt.helper_id != self.helper_id or self.receipt.attempt_sha256 != self.attempt_sha256:
                raise ValueError(ERROR)
