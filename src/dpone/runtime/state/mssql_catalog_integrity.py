"""Declarative SQL Server catalog integrity checks for atomic state tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from dpone.runtime.state.mssql_catalog_storage_contract import require_table_storage_integrity
from dpone.runtime.state.mssql_sql_semantics import SqlSemantic, canonical_sql_expression
from dpone.runtime.support.mssql_identifier_integrity import require_case_unambiguous_identifiers

MssqlIndexKind = Literal["primary_key", "unique_constraint", "unique_index"]


@dataclass(frozen=True, slots=True)
class MssqlIndexContract:
    """Exact safety-critical rowstore key or unique-index shape."""

    kind: MssqlIndexKind
    columns: tuple[str, ...]
    clustered: bool
    filter_expression: str | None = None


@dataclass(frozen=True, slots=True)
class MssqlForeignKeyContract:
    """Exact ordered FK relationship required by runtime DML."""

    columns: tuple[str, ...]
    referenced_schema: str
    referenced_table: str
    referenced_columns: tuple[str, ...]
    delete_action: str = "NO_ACTION"
    update_action: str = "NO_ACTION"


@dataclass(frozen=True, slots=True)
class MssqlCheckContract:
    """Semantic CHECK predicate with a stable diagnostic label."""

    label: str
    expression: str


@dataclass(frozen=True, slots=True)
class MssqlDefaultContract:
    """Default expression required for a runtime-inserted column."""

    column: str
    expression: str


@dataclass(frozen=True, slots=True)
class MssqlTableIntegrityContract:
    """Safety-critical relational and physical state-table contract."""

    indexes: tuple[MssqlIndexContract, ...] = ()
    foreign_keys: tuple[MssqlForeignKeyContract, ...] = ()
    checks: tuple[MssqlCheckContract, ...] = ()
    defaults: tuple[MssqlDefaultContract, ...] = ()
    exact_indexes: bool = False
    exact_foreign_keys: bool = False
    exact_checks: bool = False
    exact_defaults: bool = False
    require_none_compression: bool = True
    require_single_partition: bool = True


@dataclass(frozen=True, slots=True)
class _IndexState:
    kind: MssqlIndexKind | None
    columns: tuple[str, ...]
    type_desc: str
    included_columns: tuple[str, ...]
    ascending: bool
    filtered: bool
    filter_expression: SqlSemantic | None
    enabled: bool
    hypothetical: bool
    ignore_duplicate_keys: bool


@dataclass(frozen=True, slots=True)
class _ForeignKeyState:
    columns: tuple[str, ...]
    referenced_schema: str
    referenced_table: str
    referenced_columns: tuple[str, ...]
    delete_action: str
    update_action: str
    enabled: bool
    trusted: bool
    not_for_replication: bool


def require_table_catalog_integrity(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    table: str,
    contract: MssqlTableIntegrityContract,
) -> None:
    """Fail closed on relational or physical drift before source I/O."""

    prefix = f"{connector.quote_identifier(database)}." if database else ""
    label = f"{database}.{schema}.{table}"
    indexes = require_table_index_integrity(
        connector,
        database=database,
        schema=schema,
        table=table,
        indexes=contract.indexes,
    )
    if contract.exact_indexes and len(indexes) != len(contract.indexes):
        raise RuntimeError(f"mssql_external_state_index_unexpected:{label}")
    foreign_keys = _read_foreign_keys(connector, prefix, schema, table)
    for required in contract.foreign_keys:
        matching = [actual for actual in foreign_keys if _matches_foreign_key(actual, required)]
        if not matching:
            raise RuntimeError(f"mssql_external_state_foreign_key_missing:{label}")
        enforced = [actual for actual in matching if actual.enabled and actual.trusted]
        if not enforced:
            raise RuntimeError(f"mssql_external_state_foreign_key_untrusted_or_disabled:{label}")
        if not any(not actual.not_for_replication for actual in enforced):
            raise RuntimeError(f"mssql_external_state_foreign_key_not_for_replication:{label}")
    if contract.exact_foreign_keys and len(foreign_keys) != len(contract.foreign_keys):
        raise RuntimeError(f"mssql_external_state_foreign_key_unexpected:{label}")
    checks = _require_checks(connector, prefix, schema, table, label, contract.checks)
    if contract.exact_checks and len(checks) != len(contract.checks):
        raise RuntimeError(f"mssql_external_state_check_unexpected:{label}")
    defaults = _require_defaults(connector, prefix, schema, table, label, contract.defaults)
    if contract.exact_defaults and len(defaults) != len(contract.defaults):
        raise RuntimeError(f"mssql_external_state_default_unexpected:{label}")
    require_table_storage_integrity(
        connector,
        prefix=prefix,
        schema=schema,
        table=table,
        label=label,
        require_none_compression=contract.require_none_compression,
        require_single_partition=contract.require_single_partition,
    )


def require_table_index_integrity(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    table: str,
    indexes: tuple[MssqlIndexContract, ...],
) -> tuple[_IndexState, ...]:
    """Require exact semantic index shapes without relying on index names."""

    prefix = f"{connector.quote_identifier(database)}." if database else ""
    actual_indexes = _read_indexes(connector, prefix, schema, table)
    label = f"{database}.{schema}.{table}"
    for required in indexes:
        if not any(_matches_index(actual, required) for actual in actual_indexes):
            raise RuntimeError(f"mssql_external_state_index_missing:{label}:{required.kind}")
    return actual_indexes


def _read_indexes(connector: Any, prefix: str, schema: str, table: str) -> tuple[_IndexState, ...]:
    rows = connector.get_records(
        "SELECT i.index_id, i.name AS index_name, i.is_unique, i.is_primary_key, "
        "i.is_unique_constraint, i.is_disabled, i.is_hypothetical, i.has_filter, "
        "i.filter_definition, i.type_desc, i.ignore_dup_key, c.name AS column_name, ic.key_ordinal, "
        "ic.is_included_column, ic.is_descending_key "
        f"FROM {prefix}sys.indexes AS i "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = i.object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        f"INNER JOIN {prefix}sys.index_columns AS ic "
        "ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        f"INNER JOIN {prefix}sys.columns AS c "
        "ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE s.name = ? AND t.name = ? "
        "ORDER BY i.index_id, ic.key_ordinal, ic.index_column_id",
        (schema, table),
        as_dict=True,
    )
    require_case_unambiguous_identifiers(
        (row.get("column_name") for row in rows),
        error_code=f"mssql_external_state_index_identifier_case_ambiguity:{schema}.{table}",
    )
    grouped: dict[int, list[Any]] = {}
    for row in rows:
        grouped.setdefault(int(row["index_id"]), []).append(row)
    output: list[_IndexState] = []
    for index_rows in grouped.values():
        first = index_rows[0]
        key_rows = sorted(
            (row for row in index_rows if int(row.get("key_ordinal") or 0) > 0),
            key=lambda row: int(row["key_ordinal"]),
        )
        included = tuple(
            str(row.get("column_name") or "")
            for row in index_rows
            if bool(row.get("is_included_column")) or int(row.get("key_ordinal") or 0) == 0
        )
        filter_expression = _try_expression(str(first.get("filter_definition") or ""))
        output.append(
            _IndexState(
                kind=_index_kind(first),
                columns=tuple(str(row.get("column_name") or "") for row in key_rows),
                type_desc=str(first.get("type_desc") or "").upper(),
                included_columns=included,
                ascending=all(not bool(row.get("is_descending_key")) for row in key_rows),
                filtered=bool(first.get("has_filter")),
                filter_expression=filter_expression,
                enabled=not bool(first.get("is_disabled")),
                hypothetical=bool(first.get("is_hypothetical")),
                ignore_duplicate_keys=bool(first.get("ignore_dup_key")),
            )
        )
    return tuple(output)


def _index_kind(row: Any) -> MssqlIndexKind | None:
    if not bool(row.get("is_unique")):
        return None
    if bool(row.get("is_primary_key")) and not bool(row.get("is_unique_constraint")):
        return "primary_key"
    if bool(row.get("is_unique_constraint")) and not bool(row.get("is_primary_key")):
        return "unique_constraint"
    if not bool(row.get("is_primary_key")) and not bool(row.get("is_unique_constraint")):
        return "unique_index"
    return None


def _matches_index(actual: _IndexState, required: MssqlIndexContract) -> bool:
    expected_filter = (
        canonical_sql_expression(required.filter_expression) if required.filter_expression is not None else None
    )
    return bool(
        actual.kind == required.kind
        and actual.columns == required.columns
        and actual.type_desc == ("CLUSTERED" if required.clustered else "NONCLUSTERED")
        and not actual.included_columns
        and actual.ascending
        and actual.filtered == (required.filter_expression is not None)
        and actual.filter_expression == expected_filter
        and actual.enabled
        and not actual.hypothetical
        and not actual.ignore_duplicate_keys
    )


def _read_foreign_keys(connector: Any, prefix: str, schema: str, table: str) -> tuple[_ForeignKeyState, ...]:
    rows = connector.get_records(
        "SELECT fk.object_id AS constraint_object_id, fk.name AS constraint_name, "
        "fk.is_disabled, fk.is_not_trusted, fk.is_not_for_replication, fk.delete_referential_action_desc, "
        "fk.update_referential_action_desc, rs.name AS referenced_schema, "
        "rt.name AS referenced_table, pc.name AS parent_column, rc.name AS referenced_column, "
        "fkc.constraint_column_id "
        f"FROM {prefix}sys.foreign_keys AS fk "
        f"INNER JOIN {prefix}sys.tables AS pt ON pt.object_id = fk.parent_object_id "
        f"INNER JOIN {prefix}sys.schemas AS ps ON ps.schema_id = pt.schema_id "
        f"INNER JOIN {prefix}sys.tables AS rt ON rt.object_id = fk.referenced_object_id "
        f"INNER JOIN {prefix}sys.schemas AS rs ON rs.schema_id = rt.schema_id "
        f"INNER JOIN {prefix}sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id "
        f"INNER JOIN {prefix}sys.columns AS pc "
        "ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id "
        f"INNER JOIN {prefix}sys.columns AS rc "
        "ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id "
        "WHERE ps.name = ? AND pt.name = ? ORDER BY fk.object_id, fkc.constraint_column_id",
        (schema, table),
        as_dict=True,
    )
    grouped: dict[int, list[Any]] = {}
    for row in rows:
        grouped.setdefault(int(row["constraint_object_id"]), []).append(row)
    return tuple(_foreign_key_state(rows) for rows in grouped.values())


def _foreign_key_state(rows: list[Any]) -> _ForeignKeyState:
    ordered = sorted(rows, key=lambda row: int(row["constraint_column_id"]))
    first = ordered[0]
    return _ForeignKeyState(
        columns=tuple(str(row["parent_column"]) for row in ordered),
        referenced_schema=str(first["referenced_schema"]),
        referenced_table=str(first["referenced_table"]),
        referenced_columns=tuple(str(row["referenced_column"]) for row in ordered),
        delete_action=str(first.get("delete_referential_action_desc") or "").upper(),
        update_action=str(first.get("update_referential_action_desc") or "").upper(),
        enabled=not bool(first.get("is_disabled")),
        trusted=not bool(first.get("is_not_trusted")),
        not_for_replication=bool(first.get("is_not_for_replication")),
    )


def _matches_foreign_key(actual: _ForeignKeyState, required: MssqlForeignKeyContract) -> bool:
    return (
        actual.columns == required.columns
        and actual.referenced_schema == required.referenced_schema
        and actual.referenced_table == required.referenced_table
        and actual.referenced_columns == required.referenced_columns
        and actual.delete_action == required.delete_action.upper()
        and actual.update_action == required.update_action.upper()
    )


def _require_checks(
    connector: Any,
    prefix: str,
    schema: str,
    table: str,
    label: str,
    required_checks: tuple[MssqlCheckContract, ...],
) -> tuple[Any, ...]:
    rows = connector.get_records(
        "SELECT cc.name AS constraint_name, cc.definition, cc.is_disabled, cc.is_not_trusted, "
        "cc.is_not_for_replication "
        f"FROM {prefix}sys.check_constraints AS cc "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = cc.parent_object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ?",
        (schema, table),
        as_dict=True,
    )
    for required in required_checks:
        expected = canonical_sql_expression(required.expression)
        matching = [row for row in rows if _try_expression(str(row.get("definition") or "")) == expected]
        if not matching:
            raise RuntimeError(f"mssql_external_state_check_missing:{label}:{required.label}")
        enforced = [row for row in matching if not bool(row.get("is_disabled")) and not bool(row.get("is_not_trusted"))]
        if not enforced:
            raise RuntimeError(f"mssql_external_state_check_untrusted_or_disabled:{label}:{required.label}")
        if not any(not bool(row.get("is_not_for_replication")) for row in enforced):
            raise RuntimeError(f"mssql_external_state_check_not_for_replication:{label}:{required.label}")
    return tuple(rows)


def _require_defaults(
    connector: Any,
    prefix: str,
    schema: str,
    table: str,
    label: str,
    required_defaults: tuple[MssqlDefaultContract, ...],
) -> tuple[Any, ...]:
    rows = connector.get_records(
        "SELECT c.name AS column_name, dc.definition "
        f"FROM {prefix}sys.default_constraints AS dc "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = dc.parent_object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        f"INNER JOIN {prefix}sys.columns AS c "
        "ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id "
        "WHERE s.name = ? AND t.name = ?",
        (schema, table),
        as_dict=True,
    )
    for required in required_defaults:
        expected = canonical_sql_expression(required.expression)
        if not any(
            str(row.get("column_name") or "") == required.column
            and _try_expression(str(row.get("definition") or "")) == expected
            for row in rows
        ):
            raise RuntimeError(f"mssql_external_state_default_missing:{label}:{required.column}")
    return tuple(rows)


def _try_expression(value: str) -> SqlSemantic | None:
    if not value.strip():
        return None
    try:
        return canonical_sql_expression(value)
    except ValueError:
        return None


__all__ = [
    "MssqlCheckContract",
    "MssqlDefaultContract",
    "MssqlForeignKeyContract",
    "MssqlIndexContract",
    "MssqlTableIntegrityContract",
    "require_table_catalog_integrity",
    "require_table_index_integrity",
]
