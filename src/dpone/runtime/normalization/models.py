"""Models returned by nested normalization services."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NormalizedTable:
    """Rows and schema for one normalized target table."""

    name: str
    schema: tuple[tuple[str, str], ...]
    rows: tuple[dict[str, object], ...]

    @property
    def row_count(self) -> int:
        return len(self.rows)


@dataclass(frozen=True, slots=True)
class NormalizationResult:
    """Complete root + child-table normalization output."""

    tables: tuple[NormalizedTable, ...]
    child_unique_keys: tuple[tuple[str, tuple[str, ...]], ...] = ()
    child_parent_tables: tuple[tuple[str, str], ...] = ()

    def table(self, name: str) -> NormalizedTable:
        for table in self.tables:
            if table.name == name:
                return table
        raise KeyError(name)

    def row_counts(self) -> dict[str, int]:
        return {table.name: table.row_count for table in self.tables}

    def as_mapping(self) -> Mapping[str, NormalizedTable]:
        return {table.name: table for table in self.tables}

    def table_keys(self) -> dict[str, tuple[str, ...]]:
        """Return configured business keys by resolved child-table name."""

        return dict(self.child_unique_keys)

    def table_parents(self) -> dict[str, str]:
        """Return the resolved direct parent for each generated child table."""

        return dict(self.child_parent_tables)
