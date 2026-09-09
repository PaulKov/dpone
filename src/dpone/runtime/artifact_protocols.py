from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome, ArtifactTerminalReceipt

if TYPE_CHECKING:
    from dpone.runtime.staging import StagingManager


class ExtractionArtifact(Protocol):
    """Артефакт, возвращаемый source после извлечения данных."""

    @property
    def estimated_rows(self) -> int | None:
        """Optional row estimate exposed by extraction strategies."""

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        """Материализует данные в staging приёмника и возвращает артефакт."""

    def cleanup(self) -> None:
        """Очистка временных ресурсов source (если есть)."""


class TerminalExtractionArtifact(ExtractionArtifact, Protocol):
    """Artifact that accepts a typed, idempotent terminal decision."""

    def terminate(self, outcome: ArtifactTerminalOutcome) -> ArtifactTerminalReceipt:
        """Apply one typed, idempotent terminal ownership decision."""
