"""Values for a single-host authority; records alone never grant SQL dispatch."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from enum import StrEnum

from dpone.contracts.clickhouse_publication import PublicationRecord


class AuthorityError(RuntimeError):
    """Authority is unavailable or invalid; retain resources and stop admission."""


class AuthorityConflict(AuthorityError):
    """An existing owner or irreversible transition conflicts with this request."""


def require_text(value: str) -> None:
    """Reject ambiguous identity rather than trimming or coercing it."""
    if type(value) is not str or not value or value != value.strip() or "\x00" in value:
        raise AuthorityError("Expected a nonempty canonical identity")


def require_positive(value: int) -> None:
    if type(value) is not int or not 0 < value <= (1 << 63) - 1:
        raise AuthorityError("Expected a positive SQLite integer")


def require_identifier(value: str) -> None:
    require_text(value)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise AuthorityError("Authority requires simple SQL identifiers")


class TransportState(StrEnum):
    NOT_STARTED = "not_started"
    MAY_HAVE_SENT = "may_have_sent"
    CLOSED_WITHOUT_SEND = "closed_without_send"
    CLOSED_TERMINAL = "closed_terminal"


@dataclass(frozen=True)
class AuthoritySubject:
    """Deployment-registered server identity, independent of aliases/table UUIDs."""

    deployment_id: str
    server_id: str
    database: str
    target: str

    def __post_init__(self) -> None:
        require_text(self.deployment_id)
        require_text(self.server_id)
        require_identifier(self.database)
        require_identifier(self.target)

    @property
    def key(self) -> str:
        payload = json.dumps([self.deployment_id, self.server_id, self.database, self.target], separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True)
class OperationBinding:
    """Immutable prepublication registration, not permission to send SQL."""

    operation_id: str
    subject: AuthoritySubject
    candidate: str
    epoch: int

    def __post_init__(self) -> None:
        require_text(self.operation_id)
        require_positive(self.epoch)
        require_identifier(self.candidate)
        if not isinstance(self.subject, AuthoritySubject) or self.candidate == self.subject.target:
            raise AuthorityError("A distinct candidate and canonical subject are required")
        prefix = self.subject.deployment_id + ":"
        if not self.operation_id.startswith(prefix) or not self.operation_id[len(prefix) :].strip():
            raise AuthorityError("Operation must be namespaced by deployment")


@dataclass(frozen=True)
class DispatchGrant:
    """Volatile claimant secret; never persist in cleartext or reconstruct it."""

    operation_id: str
    epoch: int
    secret: str = field(repr=False)

    def __post_init__(self) -> None:
        require_text(self.operation_id)
        require_positive(self.epoch)
        require_text(self.secret)


@dataclass(frozen=True)
class JournalEntry:
    """Exact durable revision used for CAS; loading it grants no execution."""

    record: PublicationRecord
    revision: int

    def __post_init__(self) -> None:
        require_positive(self.revision)
        if not isinstance(self.record, PublicationRecord):
            raise AuthorityError("Expected a publication record")
