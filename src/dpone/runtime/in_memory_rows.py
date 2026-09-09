"""In-memory row extraction artifact."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.staging import StagingManager, owned_staging_handle


class InMemoryRowsArtifact(BaseExtractionArtifact):
    """Extraction artifact with rows already materialized in memory."""

    def __init__(self, rows: Iterable[Mapping[str, object]]):
        materialized_rows = list(rows)
        super().__init__(estimated_rows=len(materialized_rows))
        self._rows = materialized_rows

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            inserted = staging_manager.insert_rows(handle, self._rows)
            handle.row_count = inserted
            return handle

    def cleanup(self) -> None:
        return None


__all__ = ["InMemoryRowsArtifact"]
