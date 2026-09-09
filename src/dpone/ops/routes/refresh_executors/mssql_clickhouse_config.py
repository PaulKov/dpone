"""Configuration helpers for the MSSQL -> ClickHouse refresh executor."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from dpone.contracts.mssql_object_name import MSSQLObjectName, mssql_dataset_is_safe

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class MssqlClickHouseRefreshConfig:
    """Configuration for one MSSQL -> ClickHouse route refresh executor."""

    source_dataset: str
    target_dataset: str
    boundary_column: str
    columns: tuple[str, ...]
    query_template: str = ""
    mssql: Mapping[str, object] = field(default_factory=dict)
    clickhouse: Mapping[str, object] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> MssqlClickHouseRefreshConfig:
        return cls(
            source_dataset=str(payload.get("source_dataset", "")),
            target_dataset=str(payload.get("target_dataset", "")),
            boundary_column=str(payload.get("boundary_column", "")),
            columns=string_tuple(payload.get("columns")),
            query_template=str(payload.get("query_template", "")),
            mssql=mapping(payload.get("mssql")),
            clickhouse=mapping(payload.get("clickhouse")),
        )

    @classmethod
    def from_json(cls, path: str | Path) -> MssqlClickHouseRefreshConfig:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("executor config JSON must be an object")
        return cls.from_mapping(payload)

    def blockers(self) -> tuple[str, ...]:
        blockers: list[str] = []
        if not self.columns:
            blockers.append("mssql_clickhouse_refresh_executor.columns_missing")
        for column in (*self.columns, self.boundary_column):
            if not safe_identifier(column):
                blockers.append("mssql_clickhouse_refresh_executor.unsafe_identifier")
                break
        if not mssql_safe_dataset(self.source_dataset) or not safe_dataset(self.target_dataset):
            blockers.append("mssql_clickhouse_refresh_executor.unsafe_dataset")
        return tuple(dict.fromkeys(blockers))


def split_dataset(value: str) -> tuple[str, str]:
    parts = tuple(part.strip() for part in str(value).split(".") if part.strip())
    if len(parts) != 2 or not all(safe_identifier(part) for part in parts):
        raise ValueError(f"Unsafe dataset identifier: {value}")
    return parts


def safe_dataset(value: str) -> bool:
    try:
        split_dataset(value)
    except ValueError:
        return False
    return True


def split_mssql_dataset(value: str) -> MSSQLObjectName:
    return MSSQLObjectName.from_dataset(value, strict=True)


def mssql_safe_dataset(value: str) -> bool:
    return mssql_dataset_is_safe(value)


def mssql_qualified_name(dataset: str) -> str:
    return split_mssql_dataset(dataset).quoted()


def safe_identifier(value: str) -> bool:
    return bool(_IDENTIFIER_RE.fullmatch(str(value)))


def clickhouse_qualified_name(dataset: str) -> str:
    database, table = split_dataset(dataset)
    return f"{quote_clickhouse_identifier(database)}.{quote_clickhouse_identifier(table)}"


def quote_clickhouse_identifier(value: str) -> str:
    if not safe_identifier(value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value}")
    return "`" + value.replace("`", "``") + "`"


def mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value else tuple()
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value if str(item))
    return tuple()


def int_value(value: object, *, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return default
    return default


def optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int_value(value, default=0)


def bool_value(value: object, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip():
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return default


__all__ = [
    "MssqlClickHouseRefreshConfig",
    "bool_value",
    "clickhouse_qualified_name",
    "int_value",
    "mapping",
    "mssql_qualified_name",
    "mssql_safe_dataset",
    "optional_int",
    "quote_clickhouse_identifier",
    "safe_dataset",
    "safe_identifier",
    "split_mssql_dataset",
    "split_dataset",
    "string_tuple",
]
