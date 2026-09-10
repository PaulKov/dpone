"""Immutable physical-chunk identities for one acquired ClickHouse stream."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class NativeChunkLimits:
    """Encoded/IPC bounds; SQL allocation is an observed stop threshold."""

    max_total_encoded_bytes: int
    stage_allocated_bytes_stop_threshold: int
    max_rows: int = 65536
    max_bytes: int = 16777216
    max_row_bytes: int = 1048576
    max_pending: int = 2
    max_staging_tables: int = 1024
    parallelism: int = 1

    def __post_init__(self) -> None:
        caps = {"max_rows": 1000000, "max_bytes": 1073741824, "max_pending": 64, "parallelism": 64}
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if type(value) is not int or value < 1 or value > caps.get(name, value):
                raise ValueError(f"mssql_native.invalid_limit:{name}")
        if self.max_row_bytes > self.max_bytes:
            raise ValueError("mssql_native.max_row_bytes_exceeds_chunk")

    @property
    def spool_payload_bound(self) -> int:
        """Payload reservation excluding separately accounted format/receipt files."""
        return (self.parallelism + self.max_pending + 1) * self.max_bytes


@dataclass(frozen=True)
class NativeChunkPlan:
    """Stable bindings; the source query ID is not a reusable snapshot token."""

    run_id: str
    target_id: str
    source_query_id: str
    window_fingerprint: str
    schema_fingerprint: str
    wire_fingerprint: str


@dataclass(frozen=True)
class EncodedNativeFile:
    """Sealed bounded bytes, counted and digested before vendor ingestion."""

    path: Path
    ordinal: int
    rows: int
    encoded_bytes: int
    file_sha256: str
    typed_digest: str


@dataclass(frozen=True)
class NativeChunkReceipt:
    """Independently verified owned staging attempt and consumed-file authority."""

    ordinal: int
    attempt_id: str
    stage_id: str
    rows: int
    encoded_bytes: int
    file_sha256: str
    typed_digest: str
    consumed_part_evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NativeStageComplete:
    """Source-free authority created only after EOF and contiguous verification."""

    receipts: tuple[NativeChunkReceipt, ...]
    rows: int
    receipt_digest: str
    observations: tuple[dict[str, Any], ...] = ()
    metadata_digest: str = ""
