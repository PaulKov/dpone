"""Effective-key and writable-schema projection for semantic refresh pre-release."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.dbt_publish_models import DbtModelArtifact
from dpone.contracts.dbt_semantic_refresh_plan_policy import (
    SemanticRefreshWritableColumn,
    SemanticRefreshWritableRole,
)
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError
from dpone.contracts.semantic_refresh_effective_key_identity import EffectiveKeyTemplateColumn
from dpone.contracts.semantic_refresh_types import (
    DATE_DOMAIN_MAX,
    DATE_DOMAIN_MIN,
    DATETIME_DOMAIN_MAX,
    DATETIME_DOMAIN_MIN,
)


class SemanticRefreshTypeDecisionPort(Protocol):
    """Minimal lossless source-to-target type decision used by pre-release."""

    @property
    def requires_explicit_contract(self) -> bool: ...

    @property
    def lossless(self) -> bool: ...

    @property
    def target_type(self) -> str: ...


class SemanticRefreshTypeMapperPort(Protocol):
    """Resolve one normalized SQL Server type without depending on an adapter."""

    def resolve(self, source_type: str) -> SemanticRefreshTypeDecisionPort: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshPreReleaseSchemaProjection:
    """Closed effective key and writable column projection for one dbt model."""

    event_time_column: str
    effective_key_templates: tuple[EffectiveKeyTemplateColumn, ...]
    writable_columns: tuple[SemanticRefreshWritableColumn, ...]


def compile_pre_release_schema(
    model: DbtModelArtifact,
    type_mapper: SemanticRefreshTypeMapperPort,
) -> SemanticRefreshPreReleaseSchemaProjection:
    """Derive the exact key and writable schema from standard dbt contracts."""

    keys, event_time = _effective_keys(model)
    by_key = {item.name: item for item in keys}
    writable = []
    for column in model.column_contracts:
        source_type = _normalized_mssql_type(column.data_type)
        key = by_key.get(column.name)
        target_type = (
            key.target_type if key is not None else _value_target_type(source_type, column.nullable, type_mapper)
        )
        role = (
            SemanticRefreshWritableRole.EFFECTIVE_KEY_EVENT_TIME
            if column.name == event_time
            else SemanticRefreshWritableRole.EFFECTIVE_KEY
            if key is not None
            else SemanticRefreshWritableRole.MUTABLE_VALUE
        )
        writable.append(
            SemanticRefreshWritableColumn(
                column.name,
                source_type,
                target_type,
                column.nullable,
                role,
            )
        )
    return SemanticRefreshPreReleaseSchemaProjection(event_time, keys, tuple(writable))


def _effective_keys(
    model: DbtModelArtifact,
) -> tuple[tuple[EffectiveKeyTemplateColumn, ...], str]:
    if not model.unique_key or _contains_author_event_time(model.meta):
        raise SemanticRefreshContractError(
            "use only the standard dbt unique_key; event-time author knobs are forbidden"
        )
    columns = {item.name: item for item in model.column_contracts}
    if len(model.unique_key) != len(set(model.unique_key)) or any(name not in columns for name in model.unique_key):
        raise SemanticRefreshContractError("dbt unique_key differs from the enforced ordered column contract")
    keys = tuple(_effective_key(columns[name]) for name in model.unique_key)
    temporal = tuple(item.name for item in keys if item.source_type in {"date", "datetime2(6)"})
    if len(temporal) != 1:
        raise SemanticRefreshContractError("dbt unique_key must contain exactly one date or datetime2(6) key")
    return keys, temporal[0]


def _effective_key(column: object) -> EffectiveKeyTemplateColumn:
    name = str(getattr(column, "name"))
    if bool(getattr(column, "nullable")):
        raise SemanticRefreshContractError(f"effective key must be non-null: {name}")
    source = _normalized_mssql_type(str(getattr(column, "data_type")))
    if source == "date":
        return EffectiveKeyTemplateColumn(name, source, "Date", DATE_DOMAIN_MIN, DATE_DOMAIN_MAX)
    if source == "datetime2(6)":
        return EffectiveKeyTemplateColumn(
            name,
            source,
            "DateTime64(6,'UTC')",
            DATETIME_DOMAIN_MIN,
            DATETIME_DOMAIN_MAX,
        )
    return EffectiveKeyTemplateColumn(name, source, _key_target(source))


def _key_target(source: str) -> str:
    scalar = {
        "bit": "Bool",
        "tinyint": "UInt8",
        "smallint": "Int16",
        "int": "Int32",
        "bigint": "Int64",
        "uniqueidentifier": "UUID",
    }
    if source in scalar:
        return scalar[source]
    decimal = re.fullmatch(r"decimal\(([1-9][0-9]?),([0-9]|[1-9][0-9]?)\)", source)
    if decimal is not None:
        return f"Decimal({decimal.group(1)},{decimal.group(2)})"
    raise SemanticRefreshContractError(f"effective key type is unsupported: {source}")


def _value_target_type(
    source_type: str,
    nullable: bool,
    type_mapper: SemanticRefreshTypeMapperPort,
) -> str:
    decision = type_mapper.resolve(source_type)
    if decision.requires_explicit_contract or not decision.lossless:
        raise SemanticRefreshContractError(f"writable MSSQL type is not lossless: {source_type}")
    target = "DateTime64(6,'UTC')" if source_type == "datetime2(6)" else decision.target_type
    return f"Nullable({target})" if nullable else target


def _normalized_mssql_type(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.strip().casefold())
    normalized = re.sub(r"\s*([(),])\s*", r"\1", normalized)
    if not normalized:
        raise SemanticRefreshContractError("writable MSSQL type is absent")
    return normalized


def _contains_author_event_time(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            str(key).casefold() in {"event_time", "event_time_column"} or _contains_author_event_time(item)
            for key, item in value.items()
        )
    if isinstance(value, list | tuple):
        return any(_contains_author_event_time(item) for item in value)
    return False


__all__ = [
    "SemanticRefreshPreReleaseSchemaProjection",
    "SemanticRefreshTypeDecisionPort",
    "SemanticRefreshTypeMapperPort",
    "compile_pre_release_schema",
]
