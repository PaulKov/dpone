"""Deterministic committed-row artifact codec boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_seal import ArtifactChunk


@dataclass(frozen=True, slots=True)
class SemanticRefreshParquetField:
    """One ordered payload field and its exact SQL Server physical type."""

    name: str
    mssql_type: str
    nullable: bool

    def __post_init__(self) -> None:
        if not self.name or self.name.startswith("__dpone_seal_"):
            raise ValueError("business field cannot use the reserved __dpone_seal_* namespace")
        if self.mssql_type != self.mssql_type.strip().lower():
            raise ValueError("mssql_type must be normalized lowercase text")
        if not isinstance(self.nullable, bool):
            raise ValueError("nullable must be boolean")


class SemanticRefreshArtifactCodec(Protocol):
    """Encode protected ordered rows into deterministic immutable chunks."""

    @property
    def serializer_sha256(self) -> str:
        """Return the exact frozen writer identity."""

    def schema_mapping_sha256(
        self,
        fields: tuple[SemanticRefreshParquetField, ...],
    ) -> str:
        """Return the exact ordered source-to-artifact mapping identity."""

    def encode(
        self,
        *,
        fields: tuple[SemanticRefreshParquetField, ...],
        rows: tuple[tuple[object, ...], ...],
        maximum_rows_per_chunk: int,
    ) -> tuple[ArtifactChunk, ...]:
        """Return deterministic contiguous chunks for the exact supplied rows."""


__all__ = ["SemanticRefreshArtifactCodec", "SemanticRefreshParquetField"]
