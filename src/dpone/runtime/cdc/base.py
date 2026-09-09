"""Runtime CDC primitives shared by database-specific readers.

The classes in this module intentionally avoid importing optional database
clients. Reader implementations depend on connector protocols only, so importing
``dpone.runtime.cdc`` stays safe in a minimal OSS installation.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from dpone._compat import StrEnum
from dpone.readiness.cdc import CDCOffset
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact


class CDCOperation(StrEnum):
    """Canonical row-level CDC operations emitted by dpone readers."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    UPDATE_BEFORE = "update_before"


@dataclass(frozen=True, slots=True)
class CDCChange:
    """A single row-level change captured from a source database.

    ``position`` is source-specific: PostgreSQL LSN, SQL Server CDC LSN, or SQL
    Server Change Tracking version. It is deliberately stored as a string to keep
    state serialization stable across drivers and database versions.
    """

    operation: CDCOperation
    data: Mapping[str, Any]
    position: str
    source_schema: str
    source_table: str
    transaction_id: str | None = None
    sequence: int | str | None = None
    before: Mapping[str, Any] | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def is_delete(self) -> bool:
        return self.operation == CDCOperation.DELETE

    def to_row(self) -> dict[str, Any]:
        """Render the change as a sink-loadable row with dpone CDC metadata."""

        row = dict(self.data)
        row.update(
            {
                "_dpone_cdc_operation": self.operation.value,
                "_dpone_cdc_position": self.position,
                "_dpone_cdc_schema": self.source_schema,
                "_dpone_cdc_table": self.source_table,
                "_dpone_cdc_deleted": self.is_delete,
            }
        )
        if self.transaction_id is not None:
            row["_dpone_cdc_transaction_id"] = self.transaction_id
        if self.sequence is not None:
            row["_dpone_cdc_sequence"] = self.sequence
        if self.before is not None:
            row["_dpone_cdc_before"] = dict(self.before)
        for key, value in self.metadata.items():
            row[f"_dpone_cdc_{key}"] = value
        return row


@dataclass(frozen=True, slots=True)
class CDCBatch:
    """A finite CDC read result plus its durable next offset."""

    changes: tuple[CDCChange, ...]
    next_offset: CDCOffset | None
    high_watermark: str | None = None

    @property
    def row_count(self) -> int:
        return len(self.changes)

    def to_rows(self) -> tuple[dict[str, Any], ...]:
        return tuple(change.to_row() for change in self.changes)

    def to_artifact(self) -> InMemoryRowsArtifact:
        """Expose the CDC batch through the regular dpone sink artifact API."""

        return InMemoryRowsArtifact(self.to_rows())


class CDCReader(Protocol):
    """Minimal contract implemented by all live CDC readers."""

    def setup(self) -> None:
        """Create or enable source-side CDC prerequisites."""

    def read_batch(self, *, start_offset: CDCOffset | None = None, max_changes: int = 10000) -> CDCBatch:
        """Read a bounded batch of changes and return the next durable offset."""


def latest_position(changes: Iterable[CDCChange], fallback: str | None = None) -> str | None:
    """Return the position of the last change, preserving a fallback watermark."""

    latest = fallback
    for change in changes:
        latest = change.position
    return latest
