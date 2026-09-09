"""Partition-level transfer checkpoint models.

The models in this module are intentionally storage-neutral. They describe
what must be persisted by an existing state/load lineage backend, but do not
introduce a new state backend or vendor dependency.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class PartitionCheckpointStatus(StrEnum):
    """Lifecycle state for one native-transfer partition."""

    PLANNED = "planned"
    EXPORTED = "exported"
    LOADED = "loaded"
    FINALIZED = "finalized"
    COMMITTED = "committed"
    FAILED = "failed"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_transfer_partition_id(
    *,
    source_table: str,
    target_table: str,
    strategy: str,
    query_hash: str,
    schema_hash: str,
    partition_bounds: dict[str, Any],
) -> str:
    """Build a deterministic SHA-256 partition identity.

    The identity is stable across retries and changes whenever query text,
    schema shape or partition bounds change.
    """

    payload = {
        "partition_bounds": partition_bounds,
        "query_hash": query_hash,
        "schema_hash": schema_hash,
        "source_table": source_table,
        "strategy": strategy,
        "target_table": target_table,
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PartitionCheckpoint:
    """State snapshot for one exported/loaded/finalized partition."""

    transfer_partition_id: str
    status: PartitionCheckpointStatus
    query_hash: str
    schema_hash: str
    source_table: str
    target_table: str
    partition_bounds: dict[str, Any]
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    rows_exported: int | None = None
    bytes_exported: int | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def can_skip(self, *, query_hash: str, schema_hash: str) -> bool:
        """Return true when this checkpoint safely represents committed work."""

        return (
            self.status is PartitionCheckpointStatus.COMMITTED
            and self.query_hash == query_hash
            and self.schema_hash == schema_hash
        )
