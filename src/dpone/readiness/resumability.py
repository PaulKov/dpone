"""Resumable partition manifest primitives.

A partition manifest is intentionally small JSON so it can live in local files,
object storage, or be mirrored into a state table. It records partition ranges,
artifacts, row counts, and retryable failures without coupling to a connector.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone._compat import UTC, StrEnum
from dpone.runtime.partitioning import RangePartition, RangePartitioner


def _now() -> str:
    return datetime.now(UTC).isoformat()


class PartitionRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PartitionCheckpoint:
    partition_index: int
    lower_bound: int
    upper_bound: int
    include_upper: bool
    status: PartitionRunStatus = PartitionRunStatus.PENDING
    rows_extracted: int = 0
    rows_loaded: int = 0
    artifact_path: str | None = None
    checksum: str | None = None
    error: str | None = None
    attempts: int = 0
    updated_at: str = field(default_factory=_now)

    @classmethod
    def from_partition(cls, partition: RangePartition) -> PartitionCheckpoint:
        return cls(
            partition_index=partition.index,
            lower_bound=partition.lower_bound,
            upper_bound=partition.upper_bound,
            include_upper=partition.include_upper,
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> PartitionCheckpoint:
        payload = dict(payload)
        payload["status"] = PartitionRunStatus(payload["status"])
        return cls(**payload)


@dataclass
class PartitionManifest:
    run_id: str
    process_name: str
    source: str
    target: str
    created_at: str
    checkpoints: list[PartitionCheckpoint]
    version: int = 1

    def mark_running(self, partition_index: int) -> None:
        checkpoint = self._checkpoint(partition_index)
        checkpoint.status = PartitionRunStatus.RUNNING
        checkpoint.attempts += 1
        checkpoint.error = None
        checkpoint.updated_at = _now()

    def mark_success(
        self,
        partition_index: int,
        *,
        rows_extracted: int = 0,
        rows_loaded: int = 0,
        artifact_path: str | None = None,
        checksum: str | None = None,
    ) -> None:
        checkpoint = self._checkpoint(partition_index)
        checkpoint.status = PartitionRunStatus.SUCCESS
        checkpoint.rows_extracted = int(rows_extracted)
        checkpoint.rows_loaded = int(rows_loaded)
        checkpoint.artifact_path = artifact_path
        checkpoint.checksum = checksum
        checkpoint.error = None
        checkpoint.updated_at = _now()

    def mark_failed(self, partition_index: int, error: str) -> None:
        checkpoint = self._checkpoint(partition_index)
        checkpoint.status = PartitionRunStatus.FAILED
        checkpoint.error = error
        checkpoint.updated_at = _now()

    def retryable_partitions(self) -> list[PartitionCheckpoint]:
        return [item for item in self.checkpoints if item.status == PartitionRunStatus.FAILED]

    @property
    def is_complete(self) -> bool:
        return bool(self.checkpoints) and all(item.status == PartitionRunStatus.SUCCESS for item in self.checkpoints)

    @property
    def completion_ratio(self) -> float:
        if not self.checkpoints:
            return 0.0
        done = sum(1 for item in self.checkpoints if item.status == PartitionRunStatus.SUCCESS)
        return done / len(self.checkpoints)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for checkpoint in payload["checkpoints"]:
            checkpoint["status"] = str(checkpoint["status"])
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> PartitionManifest:
        raw = dict(payload)
        raw["checkpoints"] = [PartitionCheckpoint.from_dict(item) for item in raw["checkpoints"]]
        return cls(**raw)

    def _checkpoint(self, partition_index: int) -> PartitionCheckpoint:
        for checkpoint in self.checkpoints:
            if checkpoint.partition_index == partition_index:
                return checkpoint
        raise KeyError(f"Unknown partition index: {partition_index}")


class JsonPartitionManifestStore:
    """File-backed manifest store for local, CI, and OSS users."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def create_from_partitioner(
        self,
        *,
        run_id: str,
        process_name: str,
        source: str,
        target: str,
        partitioner: RangePartitioner,
    ) -> PartitionManifest:
        manifest = PartitionManifest(
            run_id=run_id,
            process_name=process_name,
            source=source,
            target=target,
            created_at=_now(),
            checkpoints=[PartitionCheckpoint.from_partition(item) for item in partitioner.partitions()],
        )
        self.save(manifest)
        return manifest

    def save(self, manifest: PartitionManifest) -> None:
        path = self._path(manifest.run_id)
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp_path.replace(path)

    def load(self, run_id: str) -> PartitionManifest:
        return PartitionManifest.from_dict(json.loads(self._path(run_id).read_text(encoding="utf-8")))

    def _path(self, run_id: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in run_id)
        return self.directory / f"{safe}.json"
