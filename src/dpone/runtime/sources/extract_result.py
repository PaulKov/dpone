"""Source extraction result contract."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.runtime.artifact_protocols import ExtractionArtifact
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority, ExtractionLifecycleReceipt
from dpone.runtime.incremental_snapshot import IncrementalSnapshotEnvelope
from dpone.type_system.source_sink.provenance import SourceColumnProvenance, SourceRelationDialect

if TYPE_CHECKING:
    from dpone.runtime.support.postgres_mssql_projection import PostgresMssqlSchemaProjection


@dataclass(frozen=True)
class ExtractResult:
    """Result returned by a source extraction strategy."""

    artifact: ExtractionArtifact
    schema: Sequence[tuple[str, str]]
    state: Any | None = None
    force_full_refresh: bool = False
    relation_schema: Sequence[tuple[str, str]] | None = None
    relation_metadata: Sequence[SourceColumnProvenance] | None = None
    relation_dialect: SourceRelationDialect | None = None
    target_projection: PostgresMssqlSchemaProjection | None = None
    snapshot_envelope: IncrementalSnapshotEnvelope[Any, Any] | None = None
    extraction_lifecycle: ExtractionLifecycleAuthority | None = None

    def __post_init__(self) -> None:
        """Preserve source timing authority without duplicating mutable state."""

        if self.extraction_lifecycle is not None:
            return
        authority = getattr(self.artifact, "extraction_lifecycle", None)
        if isinstance(authority, ExtractionLifecycleAuthority):
            object.__setattr__(self, "extraction_lifecycle", authority)

    @property
    def extraction_receipt(self) -> ExtractionLifecycleReceipt | None:
        """Return the authority's current immutable receipt snapshot."""

        authority = self.extraction_lifecycle
        return authority.receipt if authority is not None else None


__all__ = ["ExtractResult"]
