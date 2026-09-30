"""Immutable method-aware publication values; values do not confer authority."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from typing import Literal


class PublicationState(StrEnum):
    PREPARED = "prepared"
    CLAIMED = "claimed"
    COMMITTED = "committed"
    NOT_PUBLISHED = "not_published"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PublicationTable:
    """Logical evidence from the protected, pinned catalog/content producer.

    Design covers ordered columns, defaults/codecs, keys, policies and settings.
    Content uses a pinned typed multiset algorithm, not physical part checksums.
    Partitions are canonical server IDs, sorted and unique. UUIDs are opaque.
    """

    uuid: str
    design_digest: str
    content_digest: str
    rows: int
    partitions: tuple[str, ...]
    engine: str = "MergeTree"

    def __post_init__(self) -> None:
        if not self.uuid or type(self.rows) is not int or self.rows < 0:
            raise ValueError("Invalid table identity or row count")
        for digest in (self.design_digest, self.content_digest):
            if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("Expected a canonical SHA-256 digest")
        if not isinstance(self.partitions, tuple) or tuple(sorted(set(self.partitions))) != self.partitions:
            raise ValueError("Partition IDs must be a sorted unique tuple")
        if any(not p for p in self.partitions) or bool(self.rows) != bool(self.partitions):
            raise ValueError("Incomplete partition inventory")


@dataclass(frozen=True)
class PublicationObservation:
    """Fresh complete observation; flags are verified by the protected backend."""

    subject: tuple[str, str, str, str]  # endpoint identity, database, target, staging
    database_engine: str
    target: PublicationTable | None
    candidate: PublicationTable | None
    catalog_complete: bool
    side_effects_safe: bool

    def require_supported(self) -> None:
        if (
            not isinstance(self.subject, tuple)
            or len(self.subject) != 4
            or not all(isinstance(s, str) and s for s in self.subject)
            or self.subject[2] == self.subject[3]
            or self.database_engine != "Atomic"
            or self.catalog_complete is not True
            or self.side_effects_safe is not True
            or any(t.engine != "MergeTree" for t in (self.target, self.candidate) if t is not None)
        ):
            raise ValueError("Unsupported or incomplete publication observation")


@dataclass(frozen=True)
class PublicationIntent:
    operation_id: str
    before: PublicationObservation
    method: Literal["exchange", "replace_partition", "rename", "noop"]
    reason: str
    partition_id: str | None = None

    @property
    def query_id(self) -> str:
        """Stable for an operation; backend uniqueness additionally binds subject."""
        return "dpone-publication-" + sha256(self.operation_id.encode()).hexdigest()


@dataclass(frozen=True)
class PublicationRecord:
    intent: PublicationIntent
    state: PublicationState
    claim_granted: bool = False
    schema_version: str = "dpone.clickhouse.guarded-publication.v2"


@dataclass(frozen=True)
class JournalEntry:
    """Exact durable publication revision for CAS; not execution authority."""

    record: PublicationRecord
    revision: int

    def __post_init__(self) -> None:
        if type(self.revision) is not int or not 0 < self.revision <= (1 << 63) - 1:
            raise ValueError("Expected a positive SQLite revision")
        if not isinstance(self.record, PublicationRecord):
            raise ValueError("Expected a publication record")


def choose_publication(operation_id: str, observed: PublicationObservation) -> PublicationIntent:
    """Choose only from complete protected evidence; never force partition DDL."""
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("Stable operation identity is required")
    observed.require_supported()
    old, new = observed.target, observed.candidate
    if new is None or old is not None and old.uuid == new.uuid:
        raise ValueError("A distinct sealed candidate is required")
    if old is None:
        return PublicationIntent(operation_id, observed, "rename", "target_absent")
    if old.design_digest != new.design_digest:
        return PublicationIntent(operation_id, observed, "exchange", "design_changed")
    if (old.content_digest, old.rows, old.partitions) == (new.content_digest, new.rows, new.partitions):
        return PublicationIntent(operation_id, observed, "noop", "identical_snapshot")
    if not new.rows:
        return PublicationIntent(operation_id, observed, "exchange", "empty_snapshot")
    if len(new.partitions) == 1 and set(old.partitions) <= set(new.partitions):
        return PublicationIntent(
            operation_id, observed, "replace_partition", "single_complete_partition", new.partitions[0]
        )
    return PublicationIntent(operation_id, observed, "exchange", "multiple_or_different_partitions")
