"""Capability-oriented ports for deterministic dbt project bundles."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.dbt_project_bundle import (
        DbtProjectBundle,
        DbtProjectBundleArtifact,
    )


class DbtProjectBundleBuilder(Protocol):
    """Capture one immutable project tree as a deterministic bundle."""

    def build(self, project_root: Path) -> DbtProjectBundleArtifact: ...


class DbtProjectBundleExtractor(Protocol):
    """Extract one validated project bundle into an isolated destination."""

    def extract(self, archive: bytes, destination: Path) -> DbtProjectBundle: ...


class DbtProjectBundleVerifier(Protocol):
    """Verify that an extracted tree still matches its immutable bundle."""

    def verify(self, archive: bytes, destination: Path) -> DbtProjectBundle: ...


class DbtProjectBundleOperations(
    DbtProjectBundleBuilder,
    DbtProjectBundleExtractor,
    DbtProjectBundleVerifier,
    Protocol,
):
    """Bundle lifecycle capabilities used by release and mirror transactions."""


__all__ = [
    "DbtProjectBundleBuilder",
    "DbtProjectBundleExtractor",
    "DbtProjectBundleOperations",
    "DbtProjectBundleVerifier",
]
