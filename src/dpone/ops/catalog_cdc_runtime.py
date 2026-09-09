"""CDC runtime-run service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.cdc.runtime_run import CdcRuntimeRunService


@dataclass(frozen=True, slots=True)
class CdcRuntimeCatalog:
    """Factory catalog for CDC runtime execution services."""

    @classmethod
    def default(cls) -> CdcRuntimeCatalog:
        return cls()

    def runtime_run(self) -> CdcRuntimeRunService:
        return CdcRuntimeRunService()


__all__ = ["CdcRuntimeCatalog"]
