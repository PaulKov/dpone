"""Catalog model for PostgreSQL→MSSQL generated-column predecessors."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


class RetainedMssqlCatalogError(ValueError):
    """Internal typed parse error translated by the projection façade."""

    def __init__(self, blocker: str, *, column: str | None = None) -> None:
        self.blocker = blocker
        self.column = column
        super().__init__(blocker)


@dataclass(frozen=True, slots=True)
class RetainedMssqlTargetColumn:
    """Catalog-proven predecessor retained by generated-column evolution."""

    name: str
    target_type: str
    nullable: bool
    collation: str | None = None


def retained_mssql_catalog(values: Sequence[Any] | None) -> dict[str, RetainedMssqlTargetColumn]:
    """Normalize one exact, collision-free target catalog by column name."""

    output: dict[str, RetainedMssqlTargetColumn] = {}
    casefolded: set[str] = set()
    for value in values or ():
        if hasattr(value, "name") and hasattr(value, "dtype"):
            name = str(value.name)
            dtype = str(value.dtype)
            nullable = bool(value.nullable)
            collation = str(value.collation) if getattr(value, "collation", None) else None
        else:
            parts = tuple(value)
            if len(parts) not in {3, 4}:
                raise RetainedMssqlCatalogError("postgres_mssql.type_contract.retained_target_catalog_invalid")
            name, dtype, nullable, *raw_collation = parts
            name = str(name)
            dtype = str(dtype)
            nullable = bool(nullable)
            collation = str(raw_collation[0]) if raw_collation and raw_collation[0] else None
        folded = name.casefold()
        if name in output or folded in casefolded:
            raise RetainedMssqlCatalogError(
                "postgres_mssql.type_contract.retained_target_catalog_collision",
                column=name,
            )
        output[name] = RetainedMssqlTargetColumn(name, dtype, nullable, collation)
        casefolded.add(folded)
    return output


__all__ = [
    "RetainedMssqlCatalogError",
    "RetainedMssqlTargetColumn",
    "retained_mssql_catalog",
]
