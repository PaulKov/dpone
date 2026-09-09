"""Dependency-light SQL Server catalog observations and proof budgets."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SqlServerDependencyLimits:
    """Required platform budgets; no implicit defaults exist."""

    max_depth: int
    max_nodes: int
    max_edges: int
    max_definition_bytes: int

    def __post_init__(self) -> None:
        for field in ("max_depth", "max_nodes", "max_edges", "max_definition_bytes"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field} must be a positive integer")


@dataclass(frozen=True, slots=True)
class SqlServerDependencyEdge:
    """One sys.sql_expression_dependencies edge with resolution metadata."""

    referencing_object_id: int
    referenced_object_id: int | None
    referenced_server: str | None
    referenced_database: str | None
    caller_dependent: bool


@dataclass(frozen=True, slots=True)
class SqlServerObjectMetadata:
    """Minimal immutable catalog projection for one referenced SQL object."""

    object_id: int
    database: str
    schema: str
    name: str
    object_type: str
    definition: str | None
    dependency_edges: tuple[SqlServerDependencyEdge, ...]
    schema_bound: bool
    encrypted: bool
    has_computed_columns: bool


@dataclass(frozen=True, slots=True)
class SqlServerCatalogSnapshot:
    """One same-database metadata snapshot captured under DDL authority."""

    database: str
    metadata_visible: bool
    ddl_exclusivity_proven: bool
    objects: tuple[SqlServerObjectMetadata, ...]


__all__ = [
    "SqlServerCatalogSnapshot",
    "SqlServerDependencyEdge",
    "SqlServerDependencyLimits",
    "SqlServerObjectMetadata",
]
