"""Immutable schema-evolution plan models and DDL projection."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

from dpone.readiness.schema_evolution_ddl import (
    add_column_sql,
    alter_column_sql,
    normalize_dialect,
    quote_qualified_table,
)


@dataclass(frozen=True)
class ColumnDef:
    name: str
    dtype: str
    nullable: bool = True
    collation: str | None = None


class SchemaComparisonError(ValueError):
    """Stable fail-closed diagnostic for an ambiguous schema identity."""

    def __init__(self, blocker: str) -> None:
        self.blocker = blocker
        super().__init__(blocker)


@dataclass(frozen=True)
class SchemaEvolutionPolicy:
    mode: Literal["strict", "additive", "widening"] = "widening"
    allow_drop: bool = False
    on_type_change: Literal["fail", "new_column"] = "fail"
    accept_existing_nullable_target: bool = False
    new_column_prefix: str = "__dpone__nc__"
    allow_reserved_dpone_columns: bool = False
    ignored_target_columns: tuple[str, ...] = (
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        "__dpone__xmin",
    )
    protected_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class SchemaChange:
    change_type: str
    column: str
    source: ColumnDef | None = None
    target: ColumnDef | None = None
    breaking: bool = False
    reason: str = ""
    generated_column: str | None = None

    @property
    def safe_to_apply(self) -> bool:
        return not self.breaking and self.change_type in {
            "add_column",
            "add_generated_column",
            "nullability_relax",
            "type_widen",
        }


@dataclass(frozen=True)
class SchemaPlan:
    changes: list[SchemaChange]
    policy: SchemaEvolutionPolicy

    @property
    def has_breaking_changes(self) -> bool:
        return any(change.breaking for change in self.changes)

    @property
    def safe_changes(self) -> list[SchemaChange]:
        return [change for change in self.changes if change.safe_to_apply]

    @property
    def column_mapping(self) -> dict[str, str]:
        return {
            change.column: change.generated_column
            for change in self.changes
            if change.generated_column and change.change_type in {"add_generated_column", "map_to_generated_column"}
        }

    @property
    def generated_columns(self) -> dict[str, str]:
        return self.column_mapping

    def mapped_schema(
        self,
        schema: list[tuple[str, str]] | tuple[tuple[str, str], ...],
    ) -> list[tuple[str, str]]:
        mapping = {source.casefold(): target for source, target in self.column_mapping.items()}
        return [(mapping.get(column.casefold(), column), dtype) for column, dtype in schema]

    def ddl_sql(self, dialect: str, qualified_table: str) -> list[str]:
        normalized = normalize_dialect(dialect)
        table = quote_qualified_table(normalized, qualified_table)
        statements: list[str] = []
        type_alters = {change.column.casefold() for change in self.safe_changes if change.change_type == "type_widen"}
        for change in self.safe_changes:
            if change.change_type == "add_column" and change.source:
                statements.append(_add(normalized, table, change.column, change.source))
            elif change.change_type == "add_generated_column" and change.source and change.generated_column:
                generated = ColumnDef(
                    change.generated_column,
                    change.source.dtype,
                    nullable=True,
                    collation=change.source.collation,
                )
                statements.append(_add(normalized, table, change.generated_column, generated))
            elif (
                change.change_type in {"nullability_relax", "type_widen"}
                and change.source
                and not (change.change_type == "nullability_relax" and change.column.casefold() in type_alters)
            ):
                statements.append(
                    alter_column_sql(
                        normalized,
                        table,
                        change.column,
                        change.source.dtype,
                        nullable=change.source.nullable,
                        collation=change.source.collation,
                    )
                )
        return statements

    def to_dict(self) -> dict[str, object]:
        return {
            "policy": asdict(self.policy),
            "has_breaking_changes": self.has_breaking_changes,
            "column_mapping": self.column_mapping,
            "generated_columns": self.generated_columns,
            "changes": [asdict(change) for change in self.changes],
        }


def _add(dialect: str, table: str, name: str, column: ColumnDef) -> str:
    return add_column_sql(
        dialect,
        table,
        name,
        column.dtype,
        nullable=column.nullable,
        collation=column.collation,
    )


__all__ = [
    "ColumnDef",
    "SchemaChange",
    "SchemaComparisonError",
    "SchemaEvolutionPolicy",
    "SchemaPlan",
]
