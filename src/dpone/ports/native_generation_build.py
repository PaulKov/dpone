"""Explicit execution dependencies for one bootstrap-bound native dbt build."""

from __future__ import annotations

from typing import Protocol

from dpone.ports.dbt_publishing import (
    DbtCommandRunner,
    DbtExecutionEvidenceWriter,
    DbtExecutionOutcome,
    DbtProfileRenderer,
)


class NativeGenerationBuildPort(Protocol):
    """Execute bound inputs; the caller retains admission and lifetime ownership."""

    def execute(
        self,
        *,
        command_runner: DbtCommandRunner,
        profile_renderer: DbtProfileRenderer,
        evidence_writer: DbtExecutionEvidenceWriter,
    ) -> DbtExecutionOutcome: ...
