"""Core data-contract service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.contracts import DataContractService


@dataclass(frozen=True, slots=True)
class CoreContractCatalog:
    """Factory catalog for operational data-contract checks."""

    @classmethod
    def default(cls) -> CoreContractCatalog:
        return cls()

    def data_contracts(self) -> DataContractService:
        return DataContractService()


__all__ = ["CoreContractCatalog"]
