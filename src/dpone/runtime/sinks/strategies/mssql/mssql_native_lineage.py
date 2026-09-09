"""Artifact-neutral lineage projection over validated native MSSQL values.

Lineage is target metadata, not source-wire authority.  PostgreSQL file
artifacts cannot be rewritten safely after COPY, while streaming artifacts
historically carried Python-generated values.  This module defines one
projection that is applied only after every business value has reached typed
native staging.  File, batch, partition, streaming, and in-memory artifacts
therefore receive identical lineage semantics.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.lineage.options import LineageOptions
from dpone.runtime.sinks.mssql_preplanned_target_types import preplanned_mssql_target_type
from dpone.runtime.support.mssql_native_canonical import lineage_row_id_expression

LINEAGE_GENERATION_CONTRACT = "dpone.mssql.native-lineage.v1"

_TARGET_TYPES: Mapping[TechnicalColumnRole, str] = {
    TechnicalColumnRole.RUN_ID: "varchar(26)",
    TechnicalColumnRole.LOAD_ID: "varchar(26)",
    TechnicalColumnRole.LOADED_AT: "datetime2(7)",
    TechnicalColumnRole.ROW_ID: "varchar(64)",
    TechnicalColumnRole.EXTRACTED_AT: "datetime2(7)",
    TechnicalColumnRole.PARENT_ROW_ID: "varchar(64)",
    TechnicalColumnRole.ROOT_ROW_ID: "varchar(64)",
    TechnicalColumnRole.LIST_INDEX: "int",
    TechnicalColumnRole.OP: "varchar(32)",
    TechnicalColumnRole.META: "nvarchar(max)",
}

MssqlNativeLineageColumnContract = tuple[str, str, bool, str | None, str]


def resolve_mssql_native_lineage_columns(
    load_config: Any,
) -> tuple[MssqlNativeLineageColumnContract, ...]:
    """Return the pure strategy/options-derived lineage target shape.

    The transaction preplanner uses this before source I/O, while the native
    projector later supplies run-specific values.  Keeping a single function
    prevents missing-target DDL from drifting from native staging.
    """

    options = getattr(load_config, "options", {}) or {}
    policy = LineageOptions.from_config(options.get("lineage"))
    catalog = TechnicalColumnCatalog()
    return tuple(
        (
            catalog.name(role),
            preplanned_mssql_target_type(load_config, catalog.name(role), _TARGET_TYPES[role]),
            catalog.definition(role).nullable,
            None,
            role.value,
        )
        for role in policy.target_roles()
    )


@dataclass(frozen=True, slots=True)
class MssqlNativeGeneratedColumn:
    """One framework-owned native/target column and its placeholder value."""

    name: str
    target_type: str
    nullable: bool
    placeholder_sql: str
    generation_contract: str = LINEAGE_GENERATION_CONTRACT


@dataclass(frozen=True, slots=True)
class MssqlNativeLineageProjection:
    """Frozen lineage inputs resolved after source extraction completed."""

    columns: tuple[MssqlNativeGeneratedColumn, ...]
    run_id: str
    load_id: str
    extracted_at: datetime
    source_type: str
    source_schema: str
    source_table: str
    unique_key: tuple[str, ...]
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("mssql_native_lineage.version_unsupported")
        names = tuple(column.name for column in self.columns)
        if len(names) != len(set(names)):
            raise ValueError("mssql_native_lineage.column_duplicate")
        if self.columns and (not self.run_id or not self.load_id):
            raise ValueError("mssql_native_lineage.load_identity_required")
        if self.extracted_at.tzinfo is None or self.extracted_at.utcoffset() is None:
            raise ValueError("mssql_native_lineage.extracted_at_utc_required")

    @classmethod
    def resolve(
        cls,
        load_config: Any,
        lifecycle: ExtractionLifecycleReceipt,
    ) -> MssqlNativeLineageProjection:
        """Resolve the public lineage policy and immutable load identity."""

        options = getattr(load_config, "options", {}) or {}
        identity = options.get("__dpone_load_identity")
        identity = identity if isinstance(identity, Mapping) else {}
        run_id = str(identity.get("run_id") or "").strip()
        load_id = str(identity.get("load_id") or "").strip()
        extracted_at = lifecycle.extraction_started_at.astimezone(UTC)
        columns = tuple(
            _generated_column(
                name,
                target_type,
                nullable,
                role=TechnicalColumnRole(role),
                run_id=run_id,
                load_id=load_id,
                extracted_at=extracted_at,
            )
            for name, target_type, nullable, _collation, role in resolve_mssql_native_lineage_columns(load_config)
        )
        return cls(
            columns=columns,
            run_id=run_id,
            load_id=load_id,
            extracted_at=extracted_at,
            source_type=str(options.get("source_type") or ""),
            source_schema=str(getattr(load_config, "source_schema", "") or ""),
            source_table=str(getattr(load_config, "source_table", "") or ""),
            unique_key=_unique_key(getattr(load_config, "unique_key", None)),
        )

    @property
    def generation_by_name(self) -> dict[str, MssqlNativeGeneratedColumn]:
        return {column.name: column for column in self.columns}

    def assignments(
        self,
        *,
        quote_identifier: Any,
        business_schema: Sequence[tuple[str, str]],
        wire_to_target: Mapping[str, str],
        alias: str,
    ) -> tuple[str, ...]:
        """Render authoritative UPDATE assignments over native values."""

        values = self.expressions(
            business_schema=business_schema,
            wire_to_target=wire_to_target,
            value_expression=lambda column: f"{alias}.{quote_identifier(column)}",
        )
        return tuple(f"{quote_identifier(name)} = {value}" for name, value in values.items())

    def expressions(
        self,
        *,
        business_schema: Sequence[tuple[str, str]],
        wire_to_target: Mapping[str, str],
        value_expression: Callable[[str], str],
    ) -> dict[str, str]:
        """Render authoritative lineage scalars for an INSERT or UPDATE projection."""

        selected = {column.name for column in self.columns}
        values: dict[str, str] = {
            "__dpone__run_id": _unicode_literal(self.run_id),
            "__dpone__load_id": _unicode_literal(self.load_id),
            "__dpone__extracted_at": _datetime2_literal(self.extracted_at),
            # The finalizer overwrites this placeholder with one target clock
            # value under the transaction fence before any business DML.
            "__dpone__loaded_at": _datetime2_literal(self.extracted_at),
            "__dpone__parent_row_id": "NULL",
            "__dpone__list_index": "NULL",
            "__dpone__op": "NULL",
            "__dpone__meta": "NULL",
        }
        target_keys = tuple(wire_to_target.get(key, key) for key in self.unique_key)
        row_id = lineage_row_id_expression(
            business_schema,
            value_expression,
            source_type=self.source_type,
            source_schema=self.source_schema,
            source_table=self.source_table,
            unique_key=target_keys,
        )
        values["__dpone__row_id"] = row_id
        values["__dpone__root_row_id"] = row_id
        return {name: value for name, value in values.items() if name in selected}


def _generated_column(
    name: str,
    target_type: str,
    nullable: bool,
    *,
    role: TechnicalColumnRole,
    run_id: str,
    load_id: str,
    extracted_at: datetime,
) -> MssqlNativeGeneratedColumn:
    placeholders: Mapping[TechnicalColumnRole, str] = {
        TechnicalColumnRole.RUN_ID: _unicode_literal(run_id),
        TechnicalColumnRole.LOAD_ID: _unicode_literal(load_id),
        TechnicalColumnRole.LOADED_AT: _datetime2_literal(extracted_at),
        TechnicalColumnRole.EXTRACTED_AT: _datetime2_literal(extracted_at),
        TechnicalColumnRole.ROW_ID: _placeholder(target_type),
        TechnicalColumnRole.PARENT_ROW_ID: "NULL",
        TechnicalColumnRole.ROOT_ROW_ID: _placeholder(target_type),
        TechnicalColumnRole.LIST_INDEX: "NULL",
        TechnicalColumnRole.OP: "NULL",
        TechnicalColumnRole.META: "NULL",
    }
    return MssqlNativeGeneratedColumn(
        name=name,
        target_type=target_type,
        nullable=nullable,
        placeholder_sql=placeholders[role],
    )


def _placeholder(target_type: str) -> str:
    return f"CONVERT({target_type}, '')"


def _datetime2_literal(value: datetime) -> str:
    normalized = value.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="microseconds")
    return f"CONVERT(datetime2(7), '{normalized}', 126)"


def _unicode_literal(value: str) -> str:
    return "N'" + str(value).replace("'", "''") + "'"


def _unique_key(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in value)


__all__ = [
    "LINEAGE_GENERATION_CONTRACT",
    "MssqlNativeGeneratedColumn",
    "MssqlNativeLineageProjection",
    "MssqlNativeLineageColumnContract",
    "resolve_mssql_native_lineage_columns",
]
