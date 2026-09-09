"""Pure native SQL Server schema resolution for generic load strategies."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.contracts.mssql_physical_design import (
    MssqlIndexKeyContractError,
    MssqlPhysicalDesignContract,
    require_mssql_index_key_width,
)
from dpone.contracts.technical_columns import TechnicalColumnCatalog
from dpone.runtime.sinks.strategies.mssql.mssql_strategy_metadata import (
    mssql_strategy_metadata_nullability,
    mssql_strategy_metadata_types,
    resolve_mssql_native_strategy_metadata_columns,
)
from dpone.runtime.support.mssql_native_projection import (
    mssql_equality_key_collations,
    mssql_equality_keys,
    mssql_unique_keys,
    resolve_native_column_types,
)
from dpone.runtime.support.postgres_mssql_projection import (
    PostgresMssqlProjectionError,
    PostgresMssqlSchemaProjection,
)
from dpone.type_system.source_sink.provenance import SourceRelationDialect, clickhouse_type_nullable


@dataclass(frozen=True, slots=True)
class ResolvedMssqlNativeSchema:
    """One immutable physical shape reused by native and target DDL."""

    types: dict[str, str]
    source_types: dict[str, str]
    nullability: dict[str, bool]
    collations: dict[str, str]
    wire_to_target: dict[str, str]
    generated_columns: tuple[Any, ...] = ()

    @property
    def ordered_target_names(self) -> tuple[str, ...]:
        """Return exact target order with framework columns appended once."""

        names = list(self.wire_to_target.values())
        names.extend(column.name for column in self.generated_columns if column.name not in names)
        return tuple(names)

    @property
    def target_schema(self) -> tuple[tuple[str, str], ...]:
        """Ordered native/target schema, preserving source wire positions."""

        return tuple((target, self.types[target]) for target in self.ordered_target_names)

    @property
    def conversion_types(self) -> dict[str, str]:
        """Target type keyed by immutable wire identity for value validation."""

        return {wire: self.types[target] for wire, target in self.wire_to_target.items()}

    def with_lineage(
        self,
        projection: Any,
    ) -> ResolvedMssqlNativeSchema:
        """Return the same business projection plus canonical lineage shape."""

        types = dict(self.types)
        nullability = dict(self.nullability)
        collations = dict(self.collations)
        existing_generated = {column.name for column in self.generated_columns}
        if existing_generated.intersection(column.name for column in projection.columns):
            _raise("mssql_native_projection.generated_column_collision")
        for column in projection.columns:
            types[column.name] = column.target_type
            nullability[column.name] = column.nullable
            collations.pop(column.name, None)
        return ResolvedMssqlNativeSchema(
            types=types,
            source_types=dict(self.source_types),
            nullability=nullability,
            collations=collations,
            wire_to_target=dict(self.wire_to_target),
            generated_columns=(*self.generated_columns, *projection.columns),
        )


def resolve_mssql_native_schema(
    load_config: Any,
    schema: Sequence[tuple[str, str]],
    *,
    relation_schema: Sequence[tuple[str, str]] | None,
    relation_dialect: SourceRelationDialect | None,
    target_projection: PostgresMssqlSchemaProjection | None,
) -> ResolvedMssqlNativeSchema:
    """Resolve provenance, physical types, nullability and equality collation."""

    business = [(str(name), str(dtype)) for name, dtype in schema if not str(name).lower().startswith("__dpone__")]
    technical = [(str(name), str(dtype)) for name, dtype in schema if str(name).lower().startswith("__dpone__")]
    if relation_dialect == SourceRelationDialect.POSTGRES:
        if target_projection is None:
            raise PostgresMssqlProjectionError("postgres_mssql.type_contract.target_projection_required")
        if tuple(business) != target_projection.projected_schema:
            raise PostgresMssqlProjectionError("postgres_mssql.type_contract.target_projection_mismatch")
        types = {column.target_name: column.target_type for column in target_projection.columns}
        source_types = {column.wire_name: column.source_native_mssql_type for column in target_projection.columns}
        nullability = {column.target_name: column.nullable for column in target_projection.columns}
        collations = {
            column.target_name: column.collation for column in target_projection.columns if column.collation is not None
        }
        wire_to_target = {
            column.wire_name: column.target_name
            for column in sorted(target_projection.columns, key=lambda item: item.wire_position)
        }
    elif relation_schema is not None and relation_dialect is None:
        _raise("mssql_native_projection.source_relation_dialect_required")
    else:
        source_types = {name: _native_source_type(dtype) for name, dtype in business}
        types = resolve_native_column_types(load_config, tuple(source_types.items()))
        nullability = {name: _source_nullable(dtype, relation_dialect) for name, dtype in business}
        collations = {}
        wire_to_target = {name: name for name, _dtype in business}
    if technical:
        strategy_types = mssql_strategy_metadata_types(load_config)
        strategy_nullability = mssql_strategy_metadata_nullability(load_config)
        technical_types = resolve_native_column_types(
            load_config,
            technical,
            fixed_types=strategy_types,
        )
        types.update(technical_types)
        source_types.update(technical_types)
        nullability.update(
            {name: strategy_nullability.get(name, _technical_nullable(name)) for name, _dtype in technical}
        )
        wire_to_target.update({name: name for name, _dtype in technical})
    generated_columns = resolve_mssql_native_strategy_metadata_columns(load_config)
    generated_names = {column.name for column in generated_columns}
    if generated_names.intersection(types):
        _raise("mssql_native_projection.generated_column_collision")
    for column in generated_columns:
        types[column.name] = column.target_type
        nullability[column.name] = column.nullable
    explicit_nullability = _contract_nullability(load_config)
    nullability.update(
        {
            name: explicit_nullability[name]
            for wire_name, _dtype in business
            for name in (wire_to_target[wire_name],)
            if name in explicit_nullability
        }
    )
    keys = mssql_unique_keys(load_config)
    if any(key not in types for key in keys):
        _raise("mssql_native_projection.unique_key_missing_from_schema")
    if relation_dialect != SourceRelationDialect.POSTGRES:
        nullability.update({key: False for key in keys if key not in explicit_nullability})
    if any(nullability.get(key, True) for key in keys):
        _raise("mssql_native_projection.unique_key_nullable")
    _validate_index_keys(load_config, types, keys)
    collations.update(mssql_equality_key_collations(load_config, types, unique_keys=keys))
    return ResolvedMssqlNativeSchema(
        types,
        source_types,
        nullability,
        collations,
        wire_to_target,
        generated_columns,
    )


def _technical_nullable(column: str) -> bool:
    definitions = {definition.name.lower(): definition for definition in TechnicalColumnCatalog().definitions()}
    definition = definitions.get(column.lower())
    if column.lower() == "__dpone__row_hash":
        return False
    return definition.nullable if definition is not None else True


def _native_source_type(dtype: str) -> str:
    normalized = " ".join(str(dtype).strip().split())
    lowered = normalized.lower()
    for suffix in (" not nullable", " not null", " nullable", " null"):
        if lowered.endswith(suffix):
            return normalized[: -len(suffix)].strip()
    return normalized


def _source_nullable(dtype: str, dialect: SourceRelationDialect | None) -> bool:
    lowered = " ".join(str(dtype).strip().lower().split())
    if lowered.endswith((" not nullable", " not null")):
        return False
    if lowered.endswith((" nullable", " null")):
        return True
    if dialect == SourceRelationDialect.CLICKHOUSE:
        return clickhouse_type_nullable(dtype)
    return dialect != SourceRelationDialect.MSSQL


def _contract_nullability(load_config: Any) -> dict[str, bool]:
    options = getattr(load_config, "options", {}) or {}
    contract = options.get("schema_contract") if isinstance(options, dict) else None
    columns = contract.get("columns") if isinstance(contract, dict) else None
    if not isinstance(columns, dict):
        return {}
    return {
        str(name): raw["nullable"]
        for name, raw in columns.items()
        if isinstance(raw, dict) and isinstance(raw.get("nullable"), bool)
    }


def _validate_index_keys(load_config: Any, types: dict[str, str], keys: Sequence[str]) -> None:
    if not keys:
        return
    try:
        require_mssql_index_key_width(types, keys, kind="nonclustered")
        design = MssqlPhysicalDesignContract.from_options(getattr(load_config, "options", {}) or {})
        if design.active and design.primary_key:
            require_mssql_index_key_width(types, design.primary_key, kind="clustered")
    except MssqlIndexKeyContractError as exc:
        suffix = exc.code.removeprefix("mssql.index_key.")
        _raise(f"mssql_native_projection.index_key_{suffix}")


def _raise(code: str) -> None:
    from dpone.runtime.incremental_snapshot import SnapshotReconciliationError

    raise SnapshotReconciliationError(code)


__all__ = [
    "ResolvedMssqlNativeSchema",
    "mssql_equality_key_collations",
    "mssql_equality_keys",
    "mssql_unique_keys",
    "resolve_mssql_native_schema",
]
