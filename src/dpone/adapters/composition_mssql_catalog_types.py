"""Exact immutable MSSQL physical catalog vocabulary."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

COMPOSITION_COLLATION = "Latin1_General_100_BIN2"


@dataclass(frozen=True)
class CompositionColumn:
    """A built-in SQL type with exact byte length and nullability."""

    name: str
    sql_type: str
    length: int
    nullable: bool = False

    @property
    def collation(self) -> str | None:
        return COMPOSITION_COLLATION if self.sql_type in {"varchar", "nvarchar"} else None


@dataclass(frozen=True)
class CompositionKey:
    """One named primary or unique key; no filters or ignored duplicates."""

    name: str
    columns: tuple[str, ...]
    primary: bool = False


@dataclass(frozen=True)
class CompositionForeignKey:
    """A trusted, non-cascading exact association to another core table."""

    name: str
    columns: tuple[str, ...]
    target: str
    target_columns: tuple[str, ...]


@dataclass(frozen=True)
class CompositionCheck:
    """A named closed expression, separately inspected with its trust flags."""

    name: str
    expression: str


@dataclass(frozen=True)
class CompositionTable:
    """Complete ordered columns and constraints for one mandatory table."""

    name: str
    columns: tuple[CompositionColumn, ...]
    keys: tuple[CompositionKey, ...]
    foreign_keys: tuple[CompositionForeignKey, ...] = ()
    checks: tuple[CompositionCheck, ...] = ()


@dataclass(frozen=True)
class CompositionTrigger:
    """Exact internal module and its sorted SQL event set for catalog inspection."""

    name: str
    definition: str
    events: tuple[str, ...]


# Preserve historic public reflection and pickle locators.
CompositionColumn.__module__ = "dpone.adapters.composition_mssql_layout"
CompositionKey.__module__ = "dpone.adapters.composition_mssql_layout"
CompositionForeignKey.__module__ = "dpone.adapters.composition_mssql_layout"
CompositionCheck.__module__ = "dpone.adapters.composition_mssql_layout"
CompositionTable.__module__ = "dpone.adapters.composition_mssql_layout"
CompositionTrigger.__module__ = "dpone.adapters.composition_mssql_layout"


def module_sha256(definition: str) -> bytes:
    """Match HASHBYTES over SQL Server's NVARCHAR module definition bytes."""
    return sha256(definition.encode("utf-16le")).digest()


module_sha256.__module__ = "dpone.adapters.composition_mssql_gate_schema"
