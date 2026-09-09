"""Connector-neutral columnar snapshot provider port."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class ColumnarSnapshotRequest:
    query: str
    schema: Sequence[tuple[str, str]]
    uri_prefix: str
    run_id: str
    target_chunk_bytes: int = 512 * 1024 * 1024
    max_chunk_bytes: int = 1024 * 1024 * 1024
    format: str = "parquet"
    compression: str = "zstd"
    options: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class ColumnarSnapshotCapability:
    provider_id: str
    certified: bool
    supported_formats: tuple[str, ...] = ("parquet",)
    supported_compression: tuple[str, ...] = ("zstd", "snappy", "none")
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    details: Mapping[str, object] | None = None

    def supports(self, request: ColumnarSnapshotRequest) -> bool:
        return (
            self.certified
            and request.format.lower() in self.supported_formats
            and request.compression.lower() in self.supported_compression
            and not self.blockers
        )


class ColumnarSnapshotProvider(Protocol):
    """Source-side port for emitting bounded columnar chunks into object storage."""

    @property
    def provider_id(self) -> str:
        """Stable provider identifier for audit and decision evidence."""

    def capabilities(self, request: ColumnarSnapshotRequest) -> ColumnarSnapshotCapability:
        """Return whether the provider can produce the requested columnar snapshot."""

    def snapshot(self, request: ColumnarSnapshotRequest) -> Any:
        """Write chunks and return an object-storage staging manifest."""


__all__ = [
    "ColumnarSnapshotCapability",
    "ColumnarSnapshotProvider",
    "ColumnarSnapshotRequest",
]
