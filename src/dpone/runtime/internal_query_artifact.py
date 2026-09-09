"""Internal-query extraction artifact."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.staging import StagingManager, owned_staging_handle


class InternalQueryArtifact(BaseExtractionArtifact):
    """Artifact for same-database ``INSERT INTO ... SELECT`` transfers."""

    extraction_completion_mode = "lazy"

    def __init__(
        self,
        query: str,
        *,
        estimated_rows: int | None = None,
        params: Sequence[Any] | None = None,
        extraction_lifecycle: ExtractionLifecycleAuthority | None = None,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self._query = query
        self._params = params
        if extraction_lifecycle is not None:
            self.bind_extraction_lifecycle(extraction_lifecycle)

    @property
    def query(self) -> str:
        return self._query

    @property
    def params(self) -> Sequence[Any] | None:
        return self._params

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        lifecycle = self.extraction_lifecycle
        if lifecycle is not None and lifecycle.receipt is None:
            lifecycle.acquire()
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            inserted = staging_manager.insert_from_query(handle, self._query, schema, self._params)
            handle.row_count = inserted
            if lifecycle is not None:
                lifecycle.complete()
            return handle

    def cleanup(self) -> None:
        """Release source resources; an internal query owns no external artifact."""

        return None


__all__ = ["InternalQueryArtifact"]
