"""Immutable physical-chunk identities for one acquired ClickHouse stream."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

LEGACY_NATIVE_LIMIT_FIELDS = (
    "max_total_encoded_bytes",
    "stage_allocated_bytes_stop_threshold",
    "max_rows",
    "max_bytes",
    "max_row_bytes",
    "max_pending",
    "max_staging_tables",
    "parallelism",
)
NATIVE_STAGE_LIMIT_FIELDS = ("encoding_parallelism", "import_parallelism")


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
    encoding_parallelism: int | None = field(default=None, kw_only=True)
    import_parallelism: int | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        caps = {
            "max_rows": 1000000,
            "max_bytes": 1073741824,
            "max_pending": 64,
            "parallelism": 64,
            **dict.fromkeys(NATIVE_STAGE_LIMIT_FIELDS, 64),
        }
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if name in NATIVE_STAGE_LIMIT_FIELDS and value is None:
                continue
            if type(value) is not int or value < 1 or value > caps.get(name, value):
                raise ValueError(f"mssql_native.invalid_limit:{name}")
        if self.max_row_bytes > self.max_bytes:
            raise ValueError("mssql_native.max_row_bytes_exceeds_chunk")

    @property
    def effective_encoding_parallelism(self) -> int:
        """Resolve the CPU worker count independently of import concurrency."""
        return self.parallelism if self.encoding_parallelism is None else self.encoding_parallelism

    @property
    def effective_import_parallelism(self) -> int:
        """Resolve concurrent import/verify tasks, not all target connections."""
        return self.parallelism if self.import_parallelism is None else self.import_parallelism

    @property
    def retained_work_capacity(self) -> int:
        """Shared ceiling for encoding, sealed files, importing and retries."""
        return max(self.effective_encoding_parallelism, self.effective_import_parallelism) + self.max_pending

    def to_dict(self) -> dict[str, int]:
        """Preserve the exact legacy durable record for legacy-effective settings.

        Extended records contain both resolved counts. Authored fallback remains
        significant; journal readers never rewrite previously persisted records.
        """
        values = {name: getattr(self, name) for name in LEGACY_NATIVE_LIMIT_FIELDS}
        if (self.effective_encoding_parallelism, self.effective_import_parallelism) != (self.parallelism,) * 2:
            values.update(
                encoding_parallelism=self.effective_encoding_parallelism,
                import_parallelism=self.effective_import_parallelism,
            )
        return values

    @property
    def spool_payload_bound(self) -> int:
        """Payload reservation excluding separately accounted format/receipt files."""
        return (self.retained_work_capacity + 1) * self.max_bytes


@dataclass(frozen=True)
class NativeChunkPlan:
    """Stable bindings; the source query ID is not a reusable snapshot token."""

    run_id: str
    target_id: str
    source_query_id: str
    window_fingerprint: str
    schema_fingerprint: str
    wire_fingerprint: str
    source_read_mode: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if self.source_read_mode is not None and (
            type(self.source_read_mode) is not str or self.source_read_mode != "raw_single_query"
        ):
            raise ValueError("mssql_native.source_read_invalid")

    def to_dict(self) -> dict[str, str]:
        """Preserve legacy identity bytes and order; absence is not a default mode."""
        identity = {
            "run_id": self.run_id,
            "target_id": self.target_id,
            "source_query_id": self.source_query_id,
            "window_fingerprint": self.window_fingerprint,
            "schema_fingerprint": self.schema_fingerprint,
            "wire_fingerprint": self.wire_fingerprint,
        }
        if self.source_read_mode is not None:
            identity["source_read_mode"] = self.source_read_mode
        return identity


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
