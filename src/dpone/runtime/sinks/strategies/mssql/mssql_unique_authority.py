"""Exact SQL Server UNIQUE authority contracts and DDL rendering."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.contracts.mssql_physical_design import MssqlPhysicalDesignContract
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sinks.mssql_table_ddl import MssqlTableDdlRenderer


def unique_keys(load_config: Any) -> list[str]:
    raw = getattr(load_config, "unique_key", None)
    if raw is None:
        return []
    return [str(raw)] if isinstance(raw, str) else [str(value) for value in raw]


def _strategy_mode(load_config: Any) -> str:
    raw = getattr(load_config, "load_strategy", "")
    return str(getattr(raw, "value", raw)).strip().lower()


def _index_name(table: str, keys: Sequence[str]) -> str:
    raw = "ux_dpone_" + "_".join((table, *keys))
    compact = "".join(character if character.isalnum() or character == "_" else "_" for character in raw)
    if len(compact) <= 120:
        return compact
    return f"{compact[:107]}_{hashlib.sha256(compact.encode()).hexdigest()[:12]}"


@dataclass(frozen=True, slots=True)
class MssqlUniqueAuthorityContract:
    """Pure expected ``sys.indexes`` contract for one owned authority."""

    name: str
    key_columns: tuple[str, ...]
    filter_definition: str | None
    type_desc: str = "NONCLUSTERED"
    data_compression: str = "NONE"
    is_unique: bool = True
    is_primary_key: bool = False
    is_unique_constraint: bool = False
    ignore_dup_key: bool = False
    is_disabled: bool = False
    is_hypothetical: bool = False


@dataclass(frozen=True, slots=True)
class ObservedUniqueAuthority:
    """One complete framework-owned UNIQUE authority from ``sys`` catalogs."""

    name: str
    type_desc: str
    primary_key: bool
    unique_constraint: bool
    disabled: bool
    hypothetical: bool
    ignore_dup_key: bool
    filter_definition: str | None
    data_space_type_desc: str | None
    key_columns: tuple[str, ...]
    descending_keys: tuple[bool, ...]
    partition_count: int
    minimum_compression: str | None
    maximum_compression: str | None


def unique_authorities(rows: Sequence[Mapping[str, Any]]) -> tuple[ObservedUniqueAuthority, ...]:
    grouped: dict[int, list[Mapping[str, Any]]] = {}
    for row in rows:
        if int(row.get("key_ordinal") or 0) <= 0 or bool(row.get("is_included_column")):
            continue
        grouped.setdefault(int(row["index_id"]), []).append(row)
    output: list[ObservedUniqueAuthority] = []
    for index_rows in grouped.values():
        ordered = sorted(index_rows, key=lambda row: int(row["key_ordinal"]))
        first = ordered[0]
        output.append(
            ObservedUniqueAuthority(
                name=str(first.get("index_name") or ""),
                type_desc=str(first.get("type_desc") or "").upper(),
                primary_key=bool(first.get("is_primary_key")),
                unique_constraint=bool(first.get("is_unique_constraint")),
                disabled=bool(first.get("is_disabled")),
                hypothetical=bool(first.get("is_hypothetical")),
                ignore_dup_key=bool(first.get("ignore_dup_key")),
                filter_definition=(
                    str(first["filter_definition"]) if first.get("filter_definition") is not None else None
                ),
                data_space_type_desc=(
                    str(first["data_space_type_desc"]).upper()
                    if first.get("data_space_type_desc") is not None
                    else None
                ),
                key_columns=tuple(str(row["column_name"]) for row in ordered),
                descending_keys=tuple(bool(row.get("is_descending_key")) for row in ordered),
                partition_count=int(first.get("partition_count") or 0),
                minimum_compression=(
                    str(first["minimum_compression"]).upper() if first.get("minimum_compression") is not None else None
                ),
                maximum_compression=(
                    str(first["maximum_compression"]).upper() if first.get("maximum_compression") is not None else None
                ),
            )
        )
    return tuple(output)


def unique_authority_matches(actual: ObservedUniqueAuthority, expected: MssqlUniqueAuthorityContract) -> bool:
    expected_compression = expected.data_compression.upper()
    return (
        actual.name == expected.name
        and actual.type_desc == expected.type_desc.upper()
        and actual.primary_key is expected.is_primary_key
        and actual.unique_constraint is expected.is_unique_constraint
        and actual.disabled is expected.is_disabled
        and actual.hypothetical is expected.is_hypothetical
        and actual.ignore_dup_key is expected.ignore_dup_key
        and actual.key_columns == expected.key_columns
        and not any(actual.descending_keys)
        and _canonical_filter(actual.filter_definition) == _canonical_filter(expected.filter_definition)
        and actual.data_space_type_desc == "ROWS_FILEGROUP"
        and actual.partition_count == 1
        and actual.minimum_compression == expected_compression
        and actual.maximum_compression == expected_compression
    )


def _canonical_filter(value: str | None) -> str:
    return "".join(character for character in str(value or "").casefold() if character not in "[]() \t\r\n")


def resolve_unique_authority_contract(
    load_config: Any,
    target_column_types: Mapping[str, str],
    *,
    table: str | None = None,
) -> MssqlUniqueAuthorityContract | None:
    """Resolve one exact strategy-owned unique-index catalog contract."""

    keys = tuple(unique_keys(load_config))
    if not keys:
        return None
    physical = MssqlPhysicalDesignContract.from_options(getattr(load_config, "options", {}) or {})
    primary_key = physical.primary_key if physical.active else ()
    if _strategy_mode(load_config) == "scd2" and primary_key and set(primary_key).issubset(keys):
        raise_projection_error("scd2_physical_primary_key_conflict")
    if _strategy_mode(load_config) != "scd2" and primary_key == keys:
        return None
    for key in keys:
        if "(max)" in target_column_types.get(key, "").lower():
            raise_projection_error("unique_key_indexable_type_required")
    table_name = table or str(getattr(load_config, "target_table", ""))
    predicate = "[__dpone__is_current] = 1" if _strategy_mode(load_config) == "scd2" else None
    return MssqlUniqueAuthorityContract(
        name=_index_name(table_name, keys),
        key_columns=keys,
        filter_definition=predicate,
    )


def external_physical_primary_key_authority(
    load_config: Any,
    *,
    qualified_target: str,
) -> MssqlUniqueAuthorityContract | None:
    """Bind a declared externally provisioned PK to exact catalog evidence."""

    options = getattr(load_config, "options", {}) or {}
    physical_options = options.get("physical_design") if isinstance(options, Mapping) else None
    if not isinstance(physical_options, Mapping):
        return None
    externally_provisioned = physical_options.get("apply_runtime") is False or physical_options.get("apply") in {
        "plan_only",
        "manual_approval",
    }
    if not externally_provisioned:
        return None
    physical = MssqlPhysicalDesignContract.from_options(options)
    keys = tuple(unique_keys(load_config))
    if not keys or physical.primary_key != keys:
        return None
    return MssqlUniqueAuthorityContract(
        name=MssqlTableDdlRenderer.primary_key_name(qualified_target, keys),
        key_columns=keys,
        filter_definition=None,
        type_desc="CLUSTERED",
        data_compression=physical.storage.compression,
        is_primary_key=True,
    )


def render_unique_authority_ddl(
    load_config: Any,
    qualified_target: str,
    target_column_types: Mapping[str, str],
    *,
    table: str | None = None,
) -> str | None:
    """Render the exact strategy-owned unique index without executing DDL."""

    contract = resolve_unique_authority_contract(load_config, target_column_types, table=table)
    if contract is None:
        return None
    columns = ", ".join(_quote_identifier(key) for key in contract.key_columns)
    predicate = f" WHERE {contract.filter_definition}" if contract.filter_definition else ""
    return (
        f"CREATE UNIQUE {contract.type_desc} INDEX [{contract.name}] ON {qualified_target} "
        f"({columns}){predicate} WITH (DATA_COMPRESSION = {contract.data_compression})"
    )


def _quote_identifier(value: str) -> str:
    return "[" + str(value).replace("]", "]]") + "]"


def raise_projection_error(suffix: str) -> None:
    raise SnapshotReconciliationError(f"mssql_native_projection.{suffix}")


__all__ = [
    "MssqlUniqueAuthorityContract",
    "ObservedUniqueAuthority",
    "external_physical_primary_key_authority",
    "raise_projection_error",
    "render_unique_authority_ddl",
    "resolve_unique_authority_contract",
    "unique_authorities",
    "unique_authority_matches",
    "unique_keys",
]
