"""Core load-package and quarantine service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.packages import LoadPackageService
from dpone.ops.quarantine import QuarantineService


@dataclass(frozen=True, slots=True)
class CorePackageCatalog:
    """Factory catalog for load package and quarantine state services."""

    @classmethod
    def default(cls) -> CorePackageCatalog:
        return cls()

    def load_packages(self, root: str) -> LoadPackageService:
        return LoadPackageService(root)

    def quarantine(self, root: str) -> QuarantineService:
        return QuarantineService(root)


__all__ = ["CorePackageCatalog"]
