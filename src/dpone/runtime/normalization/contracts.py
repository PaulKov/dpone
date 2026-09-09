"""Hierarchical normalization contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.runtime.normalization.models import NormalizationResult


@dataclass(frozen=True, slots=True)
class TableContract:
    """Expected shape for one normalized table."""

    name: str
    parent: str | None = None
    required_columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class HierarchyContract:
    """Validate required normalized tables and parent/column contracts."""

    tables: tuple[TableContract, ...] = ()

    @classmethod
    def from_config(cls, config: Any) -> HierarchyContract:
        if not isinstance(config, dict):
            return cls()
        raw_tables = config.get("tables", {})
        table_contracts: list[TableContract] = []
        if isinstance(raw_tables, dict):
            for name, raw in raw_tables.items():
                if isinstance(raw, dict):
                    required = raw.get("required_columns", ())
                    if isinstance(required, str):
                        required = [required]
                    table_contracts.append(
                        TableContract(
                            name=str(name),
                            parent=str(raw["parent"]) if raw.get("parent") else None,
                            required_columns=tuple(str(item) for item in required or ()),
                        )
                    )
                else:
                    table_contracts.append(TableContract(name=str(name)))
        return cls(tuple(table_contracts))

    def validate(self, result: NormalizationResult) -> None:
        tables = result.as_mapping()
        for contract in self.tables:
            if contract.name not in tables:
                raise ValueError(f"Normalized table `{contract.name}` is required by hierarchy contract")
            if contract.parent and contract.parent not in tables:
                raise ValueError(
                    f"Normalized table `{contract.name}` requires parent table `{contract.parent}`, but it is missing"
                )
            columns = {name for name, _ in tables[contract.name].schema}
            for column in contract.required_columns:
                if column not in columns:
                    raise ValueError(f"Normalized table `{contract.name}` is missing required column `{column}`")
