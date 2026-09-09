"""Canonical entry point for bounded domain-first project discovery."""

from __future__ import annotations

from pathlib import Path

from dpone.manifest.authoring import AuthoringCompiler, default_authoring_compiler
from dpone.manifest.project_config import ProjectLayout, load_project_layout, resolve_project_layout
from dpone.manifest.project_discovery_models import (
    DiscoveredWorkload,
    ProjectDiscoveryIssue,
    ProjectDiscoveryProjectionError,
    ProjectDiscoverySnapshot,
)
from dpone.manifest.project_discovery_reader import ProjectDiscoveryReader
from dpone.manifest.project_discovery_scan import ProjectDiscoveryScanner
from dpone.manifest.project_root import validate_project_root


class ProjectDiscoveryService:
    """Resolve project authority and produce one immutable discovery snapshot."""

    def __init__(
        self,
        root: str | Path,
        *,
        compiler: AuthoringCompiler | None = None,
    ) -> None:
        self._root = validate_project_root(root)
        self._reader = ProjectDiscoveryReader(self._root, compiler or default_authoring_compiler())
        self._scanner = ProjectDiscoveryScanner(self._root, self._reader)

    def discover(self, *, layout: ProjectLayout | None = None) -> ProjectDiscoverySnapshot:
        config_snapshot = None
        if layout is None:
            resolved, config_snapshot = load_project_layout(self._root)
        else:
            resolved = layout
        return self._scanner.scan(resolved, config_snapshot=config_snapshot)

    def ownership_issue(
        self,
        domain: str,
        *,
        layout: ProjectLayout | None = None,
    ) -> ProjectDiscoveryIssue | None:
        """Validate one domain authority before a scaffold writes any files."""

        resolved = layout or resolve_project_layout(self._root)
        return self._reader.ownership_issue(domain, layout_root=resolved.root)


__all__ = [
    "DiscoveredWorkload",
    "ProjectDiscoveryIssue",
    "ProjectDiscoveryProjectionError",
    "ProjectDiscoveryService",
    "ProjectDiscoverySnapshot",
]
