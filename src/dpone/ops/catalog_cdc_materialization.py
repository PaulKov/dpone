"""CDC materialization service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.cdc.materialization import CdcMaterializationService
from dpone.ops.cdc.typed_materialization import CdcTypedMaterializationService


@dataclass(frozen=True, slots=True)
class CdcMaterializationCatalog:
    """Factory catalog for CDC materialization services."""

    @classmethod
    def default(cls) -> CdcMaterializationCatalog:
        return cls()

    def materialization(self) -> CdcMaterializationService:
        return CdcMaterializationService()

    def typed_materialization(self) -> CdcTypedMaterializationService:
        return CdcTypedMaterializationService()


__all__ = ["CdcMaterializationCatalog"]
