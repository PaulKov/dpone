"""Immutable PostgreSQL-to-MSSQL projection models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from dpone.runtime.support.postgres_mssql_retained_catalog import (
    RetainedMssqlCatalogError,
    RetainedMssqlTargetColumn,
    retained_mssql_catalog,
)


class PostgresMssqlProjectionError(ValueError):
    """Stable fail-closed error raised before PostgreSQL row export."""

    code = "DPONE_POSTGRES_MSSQL_TYPE_CONTRACT_BLOCKED"

    def __init__(self, blocker: str, *, column: str | None = None) -> None:
        self.blocker = blocker
        self.column = column
        suffix = f":{column}" if column else ""
        super().__init__(f"{self.code}:{blocker}{suffix}")


@dataclass(frozen=True, slots=True)
class PostgresMssqlColumnProjection:
    """One immutable source-provenance and target-physical decision."""

    name: str
    source_name: str
    wire_position: int
    source_type: str
    source_native_mssql_type: str
    projected_type: str
    target_type: str
    transfer_representation: str
    requires_explicit_contract: bool
    explicit_contract_source: str | None
    nullable: bool
    source_collation: str | None
    collation: str | None

    @property
    def explicit_contract_satisfied(self) -> bool:
        return not self.requires_explicit_contract or self.explicit_contract_source is not None

    @property
    def target_name(self) -> str:
        return self.name

    @property
    def wire_name(self) -> str:
        return self.source_name


@dataclass(frozen=True, slots=True)
class PostgresMssqlSchemaProjection:
    """Ordered, lossless projection for one observed PostgreSQL relation."""

    columns: tuple[PostgresMssqlColumnProjection, ...]
    retained_target_columns: tuple[RetainedMssqlTargetColumn, ...] = ()

    @property
    def relation_schema(self) -> tuple[tuple[str, str], ...]:
        return tuple((column.source_name, column.source_type) for column in self.columns)

    @property
    def projected_schema(self) -> tuple[tuple[str, str], ...]:
        return tuple((column.wire_name, column.projected_type) for column in self.columns)

    @property
    def target_projected_schema(self) -> tuple[tuple[str, str], ...]:
        return tuple((column.target_name, column.projected_type) for column in self.columns)

    @property
    def target_schema(self) -> tuple[tuple[str, str], ...]:
        return tuple((column.name, column.target_type) for column in self.columns)

    @property
    def target_nullability(self) -> dict[str, bool]:
        return {column.name: column.nullable for column in self.columns}

    @property
    def target_collations(self) -> dict[str, str]:
        return {column.name: column.collation for column in self.columns if column.collation is not None}

    def project_target_names(
        self,
        mapping: Mapping[str, str],
        *,
        retained_catalog: Sequence[Any] | None = None,
    ) -> PostgresMssqlSchemaProjection:
        unknown = set(mapping).difference(column.name for column in self.columns)
        if unknown:
            raise PostgresMssqlProjectionError("postgres_mssql.type_contract.target_name_mapping_invalid")
        renamed = {
            column.name: mapping[column.name]
            for column in self.columns
            if column.name in mapping and mapping[column.name] != column.name
        }
        retained = list(self.retained_target_columns)
        if renamed:
            try:
                catalog = retained_mssql_catalog(retained_catalog)
            except RetainedMssqlCatalogError as exc:
                raise PostgresMssqlProjectionError(exc.blocker, column=exc.column) from exc
            for predecessor in renamed:
                if predecessor not in catalog:
                    raise PostgresMssqlProjectionError(
                        "postgres_mssql.type_contract.retained_target_catalog_required", column=predecessor
                    )
                candidate = catalog[predecessor]
                existing = next((item for item in retained if item.name == predecessor), None)
                if existing is not None and existing != candidate:
                    raise PostgresMssqlProjectionError(
                        "postgres_mssql.type_contract.retained_target_catalog_drift", column=predecessor
                    )
                if existing is None:
                    retained.append(candidate)
        columns = tuple(replace(column, name=mapping.get(column.name, column.name)) for column in self.columns)
        identities = tuple(column.name.casefold() for column in columns)
        if len(set(identities)) != len(identities):
            raise PostgresMssqlProjectionError("postgres_mssql.type_contract.target_name_collision")
        retained_identities = tuple(column.name.casefold() for column in retained)
        if len(set(retained_identities)) != len(retained_identities):
            raise PostgresMssqlProjectionError("postgres_mssql.type_contract.retained_target_name_collision")
        if set(identities).intersection(retained_identities):
            raise PostgresMssqlProjectionError("postgres_mssql.type_contract.retained_target_managed_collision")
        return PostgresMssqlSchemaProjection(columns, tuple(retained))


__all__ = [
    "PostgresMssqlColumnProjection",
    "PostgresMssqlProjectionError",
    "PostgresMssqlSchemaProjection",
    "RetainedMssqlTargetColumn",
]
