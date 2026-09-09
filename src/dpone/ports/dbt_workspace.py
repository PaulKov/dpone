"""Capabilities for workspace discovery and project-scoped compiler construction."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.dbt_workspace import DbtWorkspaceCheckReport, DbtWorkspaceDiscoveryReport, DbtWorkspaceProject
    from dpone.contracts.dbt_workspace_release import DbtWorkspaceReleaseTree
    from dpone.ports.dbt_publishing import DbtPublishCompiler


class DbtWorkspaceDiscoveryPort(Protocol):
    def discover(self, root: Path) -> DbtWorkspaceDiscoveryReport: ...


class DbtWorkspaceCompilerFactory(Protocol):
    def __call__(self, root: Path, project: DbtWorkspaceProject) -> DbtPublishCompiler: ...


class DbtWorkspaceWriter(Protocol):
    """Publish a complete verified tree once; no dependencies or deployment writes."""

    def write(self, check: DbtWorkspaceCheckReport, *, root: Path, output_dir: Path) -> DbtWorkspaceReleaseTree: ...


class DbtWorkspaceWriterFactory(Protocol):
    """Delay execution dependency construction until an admitted compile."""

    def __call__(self) -> DbtWorkspaceWriter: ...
