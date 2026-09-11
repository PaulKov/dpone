"""Strict catalog parsing and supported physical-layout fingerprinting."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from typing import Any

from dpone.contracts.native_mssql_switch import NativeSwitchObject, NativeSwitchRejected, NativeSwitchTable

_TABLE_FLAGS = "temporal_type is_memory_optimized is_filetable is_replicated has_replication_filter is_merge_published is_sync_tran_subscribed is_tracked_by_cdc is_remote_data_archive_enabled is_external ledger_type is_node is_edge filestream_data_space_id lob_data_space_id".split()
_COLUMN_FLAGS = "is_identity is_computed is_sparse is_column_set is_filestream generated_always_type is_hidden is_masked is_user_defined is_assembly_type xml_collection_id is_rowguidcol default_object_id rule_object_id".split()
_INDEX_FLAGS = "is_disabled is_hypothetical has_filter is_primary_key is_unique_constraint ignore_dup_key".split()
_TYPES = {
    "bigint",
    "int",
    "smallint",
    "tinyint",
    "bit",
    "decimal",
    "numeric",
    "float",
    "real",
    "money",
    "smallmoney",
    "date",
    "datetime2",
    "time",
    "uniqueidentifier",
    "char",
    "varchar",
    "nchar",
    "nvarchar",
    "binary",
    "varbinary",
}


def digest(value: object) -> str:
    """Hash the complete canonical catalog value, including explicit nulls."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _require(row: dict[str, Any], names: str) -> None:
    if set(names.split()) != row.keys():
        raise NativeSwitchRejected("metadata_unknown")


def _integer(value: object) -> int:
    if not isinstance(value, (int, bool)):
        raise NativeSwitchRejected("metadata_unknown")
    return int(value)


def _zero_flags(row: dict[str, Any], names: list[str]) -> bool:
    return all(_integer(row[name]) == 0 for name in names)


def _rows(value: object, *, nonempty: bool = False) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value) or (nonempty and not value):
        raise NativeSwitchRejected("metadata_unknown")
    return value


def parse_table(payload: object) -> NativeSwitchTable:
    """Reject incomplete records; retain stable reasons for known exclusions."""
    try:
        decoded = json.loads(payload) if isinstance(payload, str) else None
        rows = _rows(decoded, nonempty=True)
        if len(rows) != 1:
            raise NativeSwitchRejected("metadata_unknown")
        return _table(rows[0])
    except (KeyError, TypeError, ValueError, OverflowError, StopIteration) as error:
        if isinstance(error, NativeSwitchRejected):
            raise
        raise NativeSwitchRejected("metadata_unknown") from error


def _table(row: dict[str, Any]) -> NativeSwitchTable:
    _require(
        row,
        "object_id schema_id schema_name name created_at modified_at principal_id owner_tag columns indexes functions partitions children incoming_fks bound_dependencies security_predicates change_tracking fulltext_indexes "
        + " ".join(_TABLE_FLAGS),
    )
    for name in ("schema_name", "name", "created_at", "modified_at"):
        if not isinstance(row[name], str) or not row[name]:
            raise NativeSwitchRejected("metadata_unknown")
    for name in ("object_id", "schema_id", "principal_id"):
        if _integer(row[name]) <= 0:
            raise NativeSwitchRejected("metadata_unknown")
    if row["owner_tag"] is not None and not isinstance(row["owner_tag"], str):
        raise NativeSwitchRejected("metadata_unknown")
    issues: set[str] = set()
    if not _zero_flags(row, _TABLE_FLAGS):
        issues.add("unsupported_table_feature")
    if _rows(row["children"]) or not _zero_flags(
        row, ["incoming_fks", "bound_dependencies", "security_predicates", "change_tracking", "fulltext_indexes"]
    ):
        issues.add("unsupported_dependency")
    columns = _columns(row["columns"], issues)
    indexes = _indexes(row["indexes"], issues)
    functions = _rows(row["functions"], nonempty=True)
    if len(functions) != 1:
        raise NativeSwitchRejected("unsupported_partition_layout")
    function = functions[0]
    _require(
        function,
        "scheme_id function_id name range_right system_type_id user_type_id max_length precision scale boundaries",
    )
    for name in ("scheme_id", "function_id", "system_type_id", "user_type_id", "max_length", "precision", "scale"):
        _integer(function[name])
    if not isinstance(function["name"], str) or not function["name"]:
        raise NativeSwitchRejected("metadata_unknown")
    keys = {c["column_id"] for i in indexes for c in i["columns"] if c["partition_ordinal"] == 1}
    if len(keys) != 1 or any(i["data_space_id"] != function["scheme_id"] for i in indexes):
        raise NativeSwitchRejected("unsupported_partition_layout")
    key = next(c for c in columns if c["column_id"] in keys)
    kind = key["type_name"]
    if kind not in {"date", "datetime2"} or key["is_nullable"] or key["scale"] > 6:
        raise NativeSwitchRejected("unsupported_partition_layout")
    if any(
        key[name] != function[name] for name in ("system_type_id", "user_type_id", "max_length", "precision", "scale")
    ):
        raise NativeSwitchRejected("unsupported_partition_layout")
    boundaries = _boundaries(function["boundaries"], kind, key["scale"])
    partitions = _partitions(row["partitions"], indexes, len(boundaries) + 1)
    shape = {
        "columns": columns,
        "indexes": [{k: v for k, v in i.items() if k != "name"} for i in indexes],
        "function": function,
        "partitions": [{k: v for k, v in p.items() if k not in {"partition_id", "hobt_id"}} for p in partitions],
        "children": row["children"],
        "principal_id": row["principal_id"],
    }
    return NativeSwitchTable(
        NativeSwitchObject(row["schema_name"], row["name"], row["object_id"], row["created_at"]),
        row["owner_tag"],
        digest(shape),
        digest(row),
        function["name"],
        key["name"],
        kind,
        _integer(function["range_right"]) == 1,
        boundaries,
        tuple(sorted(issues)),
    )


def _columns(value: object, issues: set[str]) -> list[dict[str, Any]]:
    rows = _rows(value, nonempty=True)
    for row in rows:
        _require(
            row,
            "column_id name type_name system_type_id user_type_id max_length precision scale is_nullable collation_name encryption_type is_ansi_padded "
            + " ".join(_COLUMN_FLAGS),
        )
        for name in (
            "column_id",
            "system_type_id",
            "user_type_id",
            "max_length",
            "precision",
            "scale",
            "is_nullable",
            "is_ansi_padded",
        ):
            _integer(row[name])
        if not isinstance(row["name"], str) or not row["name"]:
            raise NativeSwitchRejected("metadata_unknown")
        if row["type_name"] not in _TYPES or row["max_length"] == -1 or row["system_type_id"] != row["user_type_id"]:
            issues.add("unsupported_column")
        if not _zero_flags(row, _COLUMN_FLAGS) or row["encryption_type"] is not None:
            issues.add("unsupported_column")
        if row["collation_name"] is not None and not isinstance(row["collation_name"], str):
            raise NativeSwitchRejected("metadata_unknown")
        if row["type_name"] in {"char", "varchar", "nchar", "nvarchar"} and not row["collation_name"]:
            raise NativeSwitchRejected("metadata_unknown")
    if [row["column_id"] for row in rows] != list(range(1, len(rows) + 1)):
        raise NativeSwitchRejected("unsupported_column")
    return rows


def _indexes(value: object, issues: set[str]) -> list[dict[str, Any]]:
    rows = _rows(value, nonempty=True)
    ids = [row["index_id"] for row in rows]
    if len(set(ids)) != len(ids) or len(set(ids) & {0, 1}) != 1:
        raise NativeSwitchRejected("unsupported_index")
    for row in rows:
        _require(
            row,
            "index_id name type is_unique data_space_id filter_definition fill_factor is_padded allow_row_locks allow_page_locks columns "
            + " ".join(_INDEX_FLAGS),
        )
        for name in (
            "index_id",
            "type",
            "is_unique",
            "data_space_id",
            "fill_factor",
            "is_padded",
            "allow_row_locks",
            "allow_page_locks",
        ):
            _integer(row[name])
        if row["type"] not in {0, 1, 2} or not _zero_flags(row, _INDEX_FLAGS) or row["filter_definition"] is not None:
            issues.add("unsupported_index")
        cols = _rows(row["columns"], nonempty=True)
        for column in cols:
            _require(
                column, "column_id index_column_id key_ordinal partition_ordinal is_descending_key is_included_column"
            )
            for field in column.values():
                _integer(field)
        if sum(c["partition_ordinal"] == 1 for c in cols) != 1 or any(
            c["partition_ordinal"] not in {0, 1} for c in cols
        ):
            raise NativeSwitchRejected("unsupported_partition_layout")
    return rows


def _boundaries(value: object, kind: str, scale: int) -> tuple[date | datetime, ...]:
    rows = _rows(value, nonempty=True)
    boundaries: list[date | datetime] = []
    for number, row in enumerate(rows, 1):
        _require(row, "boundary_id value")
        if row["boundary_id"] != number or not isinstance(row["value"], str):
            raise NativeSwitchRejected("unsupported_partition_layout")
        if kind == "date":
            boundaries.append(date.fromisoformat(row["value"]))
        else:
            fraction = row["value"].partition(".")[2]
            if any(digit != "0" for digit in fraction[scale:]):
                raise NativeSwitchRejected("unsupported_partition_layout")
            stamp = datetime.fromisoformat(row["value"])
            if stamp.tzinfo is not None or stamp.microsecond % 10 ** (6 - scale) != 0:
                raise NativeSwitchRejected("unsupported_partition_layout")
            boundaries.append(stamp.replace(tzinfo=UTC))
    return tuple(boundaries)


def _partitions(value: object, indexes: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    rows = _rows(value, nonempty=True)
    expected = {(i["index_id"], number) for i in indexes for number in range(1, count + 1)}
    found = []
    for row in rows:
        _require(row, "index_id partition_number partition_id hobt_id data_compression xml_compression filegroup_id")
        for field in row.values():
            _integer(field)
        if row["data_compression"] not in {0, 1, 2} or row["xml_compression"] != 0 or row["filegroup_id"] <= 0:
            raise NativeSwitchRejected("unsupported_storage")
        found.append((row["index_id"], row["partition_number"]))
    if len(found) != len(expected) or set(found) != expected:
        raise NativeSwitchRejected("unsupported_partition_layout")
    return rows
