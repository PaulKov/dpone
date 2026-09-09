"""ClickHouse physical column type resolution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.sinks.clickhouse_nullability_policy import ClickHouseNullabilityPolicy
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypeMapper

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class ClickHouseTypeDecision(Protocol):
    """Minimal ClickHouse type decision exposed by source type mappers."""

    target_type: str


class ClickHouseSourceTypeMapper(Protocol):
    """Minimal source type mapper needed by ClickHouse DDL rendering."""

    def resolve_column(self, column: str, source_type: str) -> Any:
        """Return an object exposing a ClickHouse type decision."""


@dataclass(frozen=True, slots=True)
class ClickHousePhysicalColumnTypeResolver:
    """Resolve the concrete ClickHouse type for a runtime table column."""

    sink_key: str = "clickhouse"
    nullability_policy: ClickHouseNullabilityPolicy = field(default_factory=ClickHouseNullabilityPolicy)

    def resolve(
        self,
        *,
        load_config: LoadConfig,
        type_mapper: ClickHouseSourceTypeMapper,
        column: str,
        source_type: str,
    ) -> str:
        """Return physical override when configured, otherwise mapped source type."""

        override = self.physical_override(load_config=load_config, column=column)
        if override:
            return override
        physical_type = _clickhouse_physical_type(source_type)
        if physical_type:
            return physical_type
        framework_type = _framework_column_type(column, source_type)
        if framework_type:
            return framework_type
        mapped_type = (
            _clickhouse_source_type(source_type)
            if _source_type(load_config).lower() == self.sink_key
            else _target_type(type_mapper.resolve_column(column, source_type))
        )
        return self.nullability_policy.resolve(
            load_config=load_config,
            column=column,
            mapped_type=mapped_type,
        ).target_type

    def physical_override(self, *, load_config: LoadConfig, column: str) -> str | None:
        """Return `physical_design.columns.<column>.target_type.clickhouse` if present."""

        options = load_config.options or {}
        physical_design = _as_mapping(options.get("physical_design"))
        columns = _as_mapping(physical_design.get("columns"))
        override = _as_mapping(columns.get(column) or columns.get(column.lower()))
        target_type = _as_mapping(override.get("target_type"))
        return _case_insensitive_value(target_type, self.sink_key)

    def is_nullable(self, target_type: str) -> bool:
        """Read ClickHouse ``Nullable`` wrappers through the shared dialect policy."""

        return self.nullability_policy.generic_policy.dialect.is_nullable(target_type)


DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER = ClickHousePhysicalColumnTypeResolver()


def clickhouse_column_type(
    *,
    load_config: LoadConfig,
    type_mapper: MssqlClickHouseTypeMapper,
    column: str,
    source_type: str,
) -> str:
    """Backward-compatible functional facade for simple callers."""

    return DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER.resolve(
        load_config=load_config,
        type_mapper=type_mapper,
        column=column,
        source_type=source_type,
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _case_insensitive_value(values: Mapping[str, Any], key: str) -> str | None:
    for candidate, value in values.items():
        if str(candidate).strip().lower() == key:
            return str(value)
    return None


def _source_type(load_config: LoadConfig) -> str:
    return str(_as_mapping(getattr(load_config, "options", {}) or {}).get("source_type", "")).strip()


def _clickhouse_source_type(source_type: str) -> str:
    value = str(source_type).strip()
    if not value:
        raise ValueError("ClickHouse source column type cannot be empty")
    if any(token in value for token in (";", "--", "/*", "*/", "`")):
        raise ValueError(f"Unsafe ClickHouse source column type: {value}")
    return value


def _target_type(decision: Any) -> str:
    if hasattr(decision, "target_type"):
        return str(decision.target_type)
    if hasattr(decision, "clickhouse_type"):
        return str(decision.clickhouse_type)
    raise AttributeError("ClickHouse source type mapper decision must expose target_type or clickhouse_type")


def _clickhouse_physical_type(source_type: str) -> str | None:
    value = str(source_type).strip()
    if not value or not _starts_like_clickhouse_type(value):
        return None
    root = value.split("(", 1)[0].strip().lower()
    if root in _CLICKHOUSE_PHYSICAL_ROOTS:
        return _clickhouse_source_type(value)
    return None


def _starts_like_clickhouse_type(value: str) -> bool:
    return value[:1].isupper() or value.startswith(("Nullable(", "LowCardinality("))


def _framework_column_type(column: str, source_type: str) -> str | None:
    definition = _TECHNICAL_COLUMNS.get(str(column).lower())
    if definition is None or str(source_type).strip().lower() != definition.logical_type:
        return None
    return _CLICKHOUSE_TECHNICAL_TYPES.get(definition.role)


_TECHNICAL_COLUMNS = {definition.name.lower(): definition for definition in TechnicalColumnCatalog().definitions()}

_CLICKHOUSE_TECHNICAL_TYPES = {
    TechnicalColumnRole.LOADED_AT: "DateTime64(6, 'UTC')",
    TechnicalColumnRole.UPDATED_AT: "DateTime64(6, 'UTC')",
    TechnicalColumnRole.DELETED_AT: "Nullable(DateTime64(6, 'UTC'))",
    TechnicalColumnRole.RUN_ID: "String",
    TechnicalColumnRole.LOAD_ID: "String",
    TechnicalColumnRole.ROW_ID: "String",
    TechnicalColumnRole.PARENT_ROW_ID: "Nullable(String)",
    TechnicalColumnRole.ROOT_ROW_ID: "Nullable(String)",
    TechnicalColumnRole.LIST_INDEX: "Nullable(Int64)",
    TechnicalColumnRole.OP: "Nullable(String)",
    TechnicalColumnRole.META: "Nullable(String)",
    TechnicalColumnRole.ROW_HASH: "String",
    TechnicalColumnRole.VALID_FROM_AT: "DateTime64(6, 'UTC')",
    TechnicalColumnRole.VALID_TO_AT: "Nullable(DateTime64(6, 'UTC'))",
    TechnicalColumnRole.IS_CURRENT: "Bool",
    TechnicalColumnRole.EXTRACTED_AT: "DateTime64(6, 'UTC')",
}

_CLICKHOUSE_PHYSICAL_ROOTS = {
    "array",
    "bool",
    "date",
    "date32",
    "datetime",
    "datetime64",
    "decimal",
    "decimal32",
    "decimal64",
    "decimal128",
    "decimal256",
    "enum8",
    "enum16",
    "fixedstring",
    "float32",
    "float64",
    "int8",
    "int16",
    "int32",
    "int64",
    "int128",
    "int256",
    "ipv4",
    "ipv6",
    "lowcardinality",
    "map",
    "nullable",
    "string",
    "tuple",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "uint128",
    "uint256",
    "uuid",
}


__all__ = [
    "ClickHousePhysicalColumnTypeResolver",
    "ClickHouseSourceTypeMapper",
    "ClickHouseTypeDecision",
    "DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER",
    "clickhouse_column_type",
]
