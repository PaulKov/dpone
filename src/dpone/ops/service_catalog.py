"""Thin ops service catalog used as the command/service DI boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dpone.ops.catalog_artifacts import ArtifactOpsCatalog
from dpone.ops.catalog_core import CoreOpsCatalog
from dpone.ops.catalog_release import ReleaseOpsCatalog


@dataclass(frozen=True, slots=True)
class OpsServiceCatalog:
    """Compatibility catalog over focused operational service catalogs."""

    core: CoreOpsCatalog = field(default_factory=CoreOpsCatalog.default)
    artifacts: ArtifactOpsCatalog = field(default_factory=ArtifactOpsCatalog.default)
    release: ReleaseOpsCatalog = field(default_factory=ReleaseOpsCatalog.default)

    @classmethod
    def default(cls) -> OpsServiceCatalog:
        return cls()

    def __getattr__(self, name: str) -> Any:
        for catalog in (self.core, self.artifacts, self.release):
            if hasattr(catalog, name):
                return getattr(catalog, name)
        raise AttributeError(f"{type(self).__name__!s} has no service {name!r}")


__all__ = [
    "ArtifactOpsCatalog",
    "CoreOpsCatalog",
    "OpsServiceCatalog",
    "ReleaseOpsCatalog",
]
