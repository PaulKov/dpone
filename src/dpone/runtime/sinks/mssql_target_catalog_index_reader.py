"""Exact SQL Server index-catalog projection for target admission."""

from __future__ import annotations

from typing import Any

from dpone.runtime.sinks.mssql_target_catalog_model import MssqlIndexState
from dpone.runtime.sinks.mssql_target_catalog_names import (
    MSSQLObjectName,
)
from dpone.runtime.sinks.mssql_target_catalog_names import (
    mssql_sys_catalog as _sys,
)


def read_indexes(connector: Any, target: MSSQLObjectName) -> tuple[MssqlIndexState, ...]:
    """Read structural and physical evidence for every target index."""

    rows = connector.get_records(
        f"""
        SELECT i.index_id, i.name, i.type_desc, i.is_unique, i.is_primary_key,
               i.is_unique_constraint, i.is_disabled, i.is_hypothetical,
               i.ignore_dup_key, i.filter_definition, i.fill_factor,
               ds.name AS data_space_name, ds.type_desc AS data_space_type_desc,
               c.name AS column_name,
               ic.key_ordinal, ic.index_column_id, ic.is_descending_key,
               ic.is_included_column
        FROM {_sys(target, "indexes")} AS i
        INNER JOIN {_sys(target, "tables")} AS tab ON tab.object_id = i.object_id
        INNER JOIN {_sys(target, "schemas")} AS s ON s.schema_id = tab.schema_id
        LEFT JOIN {_sys(target, "index_columns")} AS ic
            ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        LEFT JOIN {_sys(target, "columns")} AS c
            ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        LEFT JOIN {_sys(target, "data_spaces")} AS ds ON ds.data_space_id = i.data_space_id
        WHERE s.name = ? AND tab.name = ? AND i.index_id > 0
        ORDER BY i.index_id, ic.is_included_column, ic.key_ordinal, ic.index_column_id
        """,
        (target.schema, target.table),
        as_dict=True,
    )
    partitions = _index_partition_compression(connector, target)
    grouped: dict[int, tuple[dict[str, Any], list[str], list[bool], list[str]]] = {}
    for row in rows:
        index_id = int(row.get("index_id") or 0)
        metadata, keys, descending, included = grouped.setdefault(index_id, (dict(row), [], [], []))
        column = row.get("column_name")
        if column is None:
            continue
        if bool(row.get("is_included_column")):
            included.append(str(column))
        else:
            keys.append(str(column))
            descending.append(bool(row.get("is_descending_key")))
    return tuple(
        _index_state(metadata, keys, descending, included, partitions.get(index_id, ()))
        for index_id, (metadata, keys, descending, included) in sorted(grouped.items())
    )


def _index_state(
    metadata: dict[str, Any],
    keys: list[str],
    descending: list[bool],
    included: list[str],
    compression: tuple[str, ...],
) -> MssqlIndexState:
    return MssqlIndexState(
        name=str(metadata.get("name") or ""),
        type_desc=str(metadata.get("type_desc") or ""),
        unique=bool(metadata.get("is_unique")),
        primary_key=bool(metadata.get("is_primary_key")),
        unique_constraint=bool(metadata.get("is_unique_constraint")),
        disabled=bool(metadata.get("is_disabled")),
        hypothetical=bool(metadata.get("is_hypothetical")),
        ignore_dup_key=bool(metadata.get("ignore_dup_key")),
        filter_definition=(
            _canonical_filter_definition(str(metadata.get("filter_definition")))
            if metadata.get("filter_definition") is not None
            else None
        ),
        key_columns=tuple(keys),
        included_columns=tuple(included),
        descending_keys=tuple(descending),
        fill_factor=int(metadata.get("fill_factor") or 0),
        data_space_name=(str(metadata.get("data_space_name")) if metadata.get("data_space_name") is not None else None),
        data_space_type_desc=(
            str(metadata.get("data_space_type_desc")) if metadata.get("data_space_type_desc") is not None else None
        ),
        partition_compression=compression,
    )


def _index_partition_compression(
    connector: Any,
    target: MSSQLObjectName,
) -> dict[int, tuple[str, ...]]:
    rows = connector.get_records(
        f"""
        SELECT i.index_id, p.partition_number, p.data_compression_desc
        FROM {_sys(target, "indexes")} AS i
        INNER JOIN {_sys(target, "tables")} AS tab ON tab.object_id = i.object_id
        INNER JOIN {_sys(target, "schemas")} AS s ON s.schema_id = tab.schema_id
        INNER JOIN {_sys(target, "partitions")} AS p
            ON p.object_id = i.object_id AND p.index_id = i.index_id
        WHERE s.name = ? AND tab.name = ? AND i.index_id > 0
        ORDER BY i.index_id, p.partition_number
        """,
        (target.schema, target.table),
        as_dict=True,
    )
    grouped: dict[int, list[str]] = {}
    for row in rows:
        grouped.setdefault(int(row.get("index_id") or 0), []).append(str(row.get("data_compression_desc") or ""))
    return {index_id: tuple(values) for index_id, values in grouped.items()}


def _canonical_filter_definition(value: str) -> str:
    compact = "".join(character for character in value.casefold() if character not in "[]() \t\r\n")
    if compact in {"__dpone__is_current=1", "1=__dpone__is_current"}:
        return "[__dpone__is_current] = 1"
    return value.strip()


__all__ = ["read_indexes"]
