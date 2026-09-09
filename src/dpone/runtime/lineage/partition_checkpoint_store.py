"""Persistent partition checkpoint stores.

The checkpoint model is storage-neutral; this module provides a small,
append-only JSONL implementation for local certification, CI evidence and
state-backend adapter tests. Production state backends can implement the same
protocol without coupling native-transfer logic to a concrete database.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus


class PartitionCheckpointDecodeError(ValueError):
    """Fail closed when a persisted checkpoint counter is malformed."""

    code = "partition_checkpoint_counter_invalid"

    def __init__(self, *, field: str) -> None:
        self.field = field
        super().__init__(f"{self.code}: {field}")


class PartitionCheckpointStore(Protocol):
    """Persistence boundary for native-transfer partition checkpoints."""

    def upsert(self, checkpoint: PartitionCheckpoint) -> None:
        """Persist a checkpoint transition."""

    def upsert_many(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        """Persist checkpoint transitions in order."""

    def list_latest(self) -> tuple[PartitionCheckpoint, ...]:
        """Return the latest checkpoint per partition id."""

    def safe_to_skip(self, *, query_hash: str, schema_hash: str) -> tuple[PartitionCheckpoint, ...]:
        """Return committed checkpoints that match the current transfer hashes."""

    def summary(self) -> dict[str, int]:
        """Return latest checkpoint counts by status."""


@dataclass(frozen=True)
class JsonlPartitionCheckpointStore:
    """Append-only JSONL checkpoint store.

    The file stores every transition for auditability. ``list_latest()`` builds
    the latest-state view by transfer partition id, preserving first-seen
    partition order for stable reports.
    """

    path: Path | str

    def upsert(self, checkpoint: PartitionCheckpoint) -> None:
        file_path = Path(self.path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_checkpoint_to_payload(checkpoint), ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")

    def upsert_many(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        for checkpoint in checkpoints:
            self.upsert(checkpoint)

    def list_latest(self) -> tuple[PartitionCheckpoint, ...]:
        latest: dict[str, PartitionCheckpoint] = {}
        for checkpoint in self._iter_all():
            latest[checkpoint.transfer_partition_id] = checkpoint
        return tuple(latest.values())

    def safe_to_skip(self, *, query_hash: str, schema_hash: str) -> tuple[PartitionCheckpoint, ...]:
        return tuple(
            checkpoint
            for checkpoint in self.list_latest()
            if checkpoint.can_skip(query_hash=query_hash, schema_hash=schema_hash)
        )

    def summary(self) -> dict[str, int]:
        counts = {status.value: 0 for status in PartitionCheckpointStatus}
        for checkpoint in self.list_latest():
            counts[checkpoint.status.value] += 1
        return counts

    def _iter_all(self) -> tuple[PartitionCheckpoint, ...]:
        file_path = Path(self.path)
        if not file_path.exists():
            return ()
        checkpoints: list[PartitionCheckpoint] = []
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if stripped:
                    checkpoints.append(_checkpoint_from_payload(json.loads(stripped)))
        return tuple(checkpoints)


def _checkpoint_to_payload(checkpoint: PartitionCheckpoint) -> dict[str, object]:
    return {
        "transfer_partition_id": checkpoint.transfer_partition_id,
        "status": checkpoint.status.value,
        "query_hash": checkpoint.query_hash,
        "schema_hash": checkpoint.schema_hash,
        "source_table": checkpoint.source_table,
        "target_table": checkpoint.target_table,
        "partition_bounds": checkpoint.partition_bounds,
        "started_at": checkpoint.started_at.isoformat(),
        "completed_at": checkpoint.completed_at.isoformat() if checkpoint.completed_at else None,
        "rows_exported": checkpoint.rows_exported,
        "bytes_exported": checkpoint.bytes_exported,
        "diagnostics": checkpoint.diagnostics,
    }


def _checkpoint_from_payload(payload: dict[str, object]) -> PartitionCheckpoint:
    completed_at = payload.get("completed_at")
    return PartitionCheckpoint(
        transfer_partition_id=str(payload["transfer_partition_id"]),
        status=PartitionCheckpointStatus(str(payload["status"])),
        query_hash=str(payload["query_hash"]),
        schema_hash=str(payload["schema_hash"]),
        source_table=str(payload["source_table"]),
        target_table=str(payload["target_table"]),
        partition_bounds=dict(payload["partition_bounds"]),  # type: ignore[arg-type]
        started_at=datetime.fromisoformat(str(payload["started_at"])),
        completed_at=datetime.fromisoformat(str(completed_at)) if completed_at else None,
        rows_exported=_optional_non_negative_int(payload, field="rows_exported"),
        bytes_exported=_optional_non_negative_int(payload, field="bytes_exported"),
        diagnostics=dict(payload.get("diagnostics") or {}),
    )


def _optional_non_negative_int(payload: dict[str, object], *, field: str) -> int | None:
    if field not in payload:
        raise PartitionCheckpointDecodeError(field=field)
    value = payload[field]
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PartitionCheckpointDecodeError(field=field)
    return value
