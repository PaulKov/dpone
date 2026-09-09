"""Configuration helpers for the Postgres -> MSSQL refresh executor."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from .mssql_clickhouse_config import (
    bool_value,
    int_value,
    mapping,
    mssql_qualified_name,
    mssql_safe_dataset,
    optional_int,
    safe_dataset,
    safe_identifier,
    split_dataset,
    string_tuple,
)


@dataclass(frozen=True, slots=True)
class PostgresMssqlRefreshConfig:
    """Configuration for one Postgres -> MSSQL route refresh executor."""

    source_dataset: str
    target_dataset: str
    boundary_column: str
    columns: tuple[str, ...]
    query_template: str = ""
    postgres: Mapping[str, object] = field(default_factory=dict)
    mssql: Mapping[str, object] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> PostgresMssqlRefreshConfig:
        return cls(
            source_dataset=str(payload.get("source_dataset", "")),
            target_dataset=str(payload.get("target_dataset", "")),
            boundary_column=str(payload.get("boundary_column", "")),
            columns=string_tuple(payload.get("columns")),
            query_template=str(payload.get("query_template", "")),
            postgres=mapping(payload.get("postgres")),
            mssql=mapping(payload.get("mssql")),
        )

    @classmethod
    def from_json(cls, path: str | Path) -> PostgresMssqlRefreshConfig:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("executor config JSON must be an object")
        return cls.from_mapping(payload)

    def blockers(self) -> tuple[str, ...]:
        blockers: list[str] = []
        if not self.columns:
            blockers.append("postgres_mssql_refresh_executor.columns_missing")
        for column in (*self.columns, self.boundary_column):
            if not safe_identifier(column):
                blockers.append("postgres_mssql_refresh_executor.unsafe_identifier")
                break
        if not safe_dataset(self.source_dataset) or not mssql_safe_dataset(self.target_dataset):
            blockers.append("postgres_mssql_refresh_executor.unsafe_dataset")
        return tuple(dict.fromkeys(blockers))


def postgres_qualified_name(dataset: str) -> str:
    schema, table = split_dataset(dataset)
    return f"{quote_postgres_identifier(schema)}.{quote_postgres_identifier(table)}"


def quote_postgres_identifier(value: str) -> str:
    if not safe_identifier(value):
        raise ValueError(f"Unsafe Postgres identifier: {value}")
    return '"' + value.replace('"', '""') + '"'


def quote_mssql_identifier(value: str) -> str:
    if not safe_identifier(value):
        raise ValueError(f"Unsafe MSSQL identifier: {value}")
    return "[" + value.replace("]", "]]") + "]"


__all__ = [
    "PostgresMssqlRefreshConfig",
    "bool_value",
    "int_value",
    "mapping",
    "mssql_qualified_name",
    "optional_int",
    "postgres_qualified_name",
    "quote_mssql_identifier",
    "quote_postgres_identifier",
    "split_dataset",
]
