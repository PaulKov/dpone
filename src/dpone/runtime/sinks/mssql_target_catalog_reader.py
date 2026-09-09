"""Exact cross-database SQL Server schema-catalog reader."""

from __future__ import annotations

from typing import Any

from dpone.runtime.sinks.mssql_target_catalog_dependencies import (
    read_foreign_keys,
    read_permissions,
    read_triggers,
)
from dpone.runtime.sinks.mssql_target_catalog_index_reader import read_indexes
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlCatalogColumnState,
    MssqlCheckConstraintState,
    MssqlSchemaCatalogSnapshot,
    MssqlTableBehaviorState,
)
from dpone.runtime.sinks.mssql_target_catalog_names import (
    MSSQLObjectName,
    quote_mssql_identifier,
)
from dpone.runtime.sinks.mssql_target_catalog_names import (
    mssql_sys_catalog as _sys,
)
from dpone.runtime.sinks.mssql_target_catalog_types import canonical_catalog_scalar


def read_schema_catalog_snapshot(owner: Any, load_config: Any) -> MssqlSchemaCatalogSnapshot:
    """Read one exact target snapshot or consume a typed test/adapter port."""

    provided = getattr(owner, "get_target_catalog_snapshot", None)
    if callable(provided):
        snapshot = provided(load_config)
        if not isinstance(snapshot, MssqlSchemaCatalogSnapshot):
            raise RuntimeError("mssql_transaction.target_catalog_snapshot_invalid")
        return snapshot
    connector = getattr(owner, "connector", None)
    if connector is None or not callable(getattr(connector, "get_records", None)):
        raise RuntimeError("mssql_transaction.target_catalog_snapshot_capability_required")
    target = MSSQLObjectName.from_parts(
        database=getattr(load_config, "target_database", None),
        schema=str(load_config.target_schema),
        table=str(load_config.target_table),
        strict=True,
    )
    if target.database is None:
        raise RuntimeError("mssql_transaction.target_database_required")
    if not _schema_exists(connector, target):
        raise RuntimeError("mssql_transaction.target_schema_preprovision_required")
    database_collation = _database_collation(connector, target.database)
    default_filegroup, row_filegroups = _row_filegroup_authority(connector, target.database)
    columns = _columns(connector, target)
    if not columns:
        exists = _table_exists(connector, target)
        return MssqlSchemaCatalogSnapshot(
            exists,
            database_collation,
            default_filegroup=default_filegroup,
            available_row_filegroups=row_filegroups,
        )
    return MssqlSchemaCatalogSnapshot(
        True,
        database_collation,
        columns=columns,
        checks=_checks(connector, target),
        indexes=read_indexes(connector, target),
        foreign_keys=read_foreign_keys(connector, target),
        triggers=read_triggers(connector, target),
        permissions=read_permissions(connector, target),
        behavior=_behavior(connector, target),
        default_filegroup=default_filegroup,
        available_row_filegroups=row_filegroups,
    )


def _database_collation(connector: Any, database: str) -> str:
    rows = connector.get_records(
        "SELECT CONVERT(nvarchar(128), DATABASEPROPERTYEX(?, 'Collation')) AS database_collation",
        (database,),
        as_dict=True,
    )
    value = rows[0].get("database_collation") if len(rows) == 1 else None
    if value is None or not str(value).strip():
        raise RuntimeError("mssql_transaction.target_database_collation_unavailable")
    return str(value)


def _row_filegroup_authority(connector: Any, database: str) -> tuple[str, tuple[str, ...]]:
    rows = connector.get_records(
        f"SELECT name AS row_filegroup, is_default "
        f"FROM {quote_mssql_identifier(database)}.sys.filegroups "
        "WHERE type_desc = N'ROWS_FILEGROUP' AND is_read_only = 0 ORDER BY data_space_id",
        as_dict=True,
    )
    names = tuple(str(row.get("row_filegroup") or "").strip() for row in rows)
    defaults = tuple(name for name, row in zip(names, rows, strict=True) if bool(row.get("is_default")))
    if not names or any(not name for name in names):
        raise RuntimeError("mssql_transaction.target_row_filegroups_unavailable")
    if len(defaults) != 1:
        raise RuntimeError("mssql_transaction.target_default_filegroup_unavailable")
    return defaults[0], names


def _table_exists(connector: Any, target: MSSQLObjectName) -> bool:
    rows = connector.get_records(
        f"SELECT 1 AS target_exists FROM {_sys(target, 'tables')} AS tab "
        f"INNER JOIN {_sys(target, 'schemas')} AS s ON s.schema_id = tab.schema_id "
        "WHERE s.name = ? AND tab.name = ?",
        (target.schema, target.table),
        as_dict=True,
    )
    return len(rows) == 1


def _behavior(connector: Any, target: MSSQLObjectName) -> MssqlTableBehaviorState:
    ledger_projection = "tab.ledger_type" if _ledger_catalog_supported(connector) else "CONVERT(int, 0)"
    rows = connector.get_records(
        f"""
        SELECT tab.temporal_type, hs.name AS history_schema, ht.name AS history_table,
               {ledger_projection} AS ledger_type,
               tab.is_memory_optimized, tab.durability_desc,
               tab.is_filetable, tab.is_node, tab.is_edge
        FROM {_sys(target, "tables")} AS tab
        INNER JOIN {_sys(target, "schemas")} AS s ON s.schema_id = tab.schema_id
        LEFT JOIN {_sys(target, "tables")} AS ht ON ht.object_id = tab.history_table_id
        LEFT JOIN {_sys(target, "schemas")} AS hs ON hs.schema_id = ht.schema_id
        WHERE s.name = ? AND tab.name = ?
        """,
        (target.schema, target.table),
        as_dict=True,
    )
    if len(rows) != 1:
        raise RuntimeError("mssql_transaction.target_behavior_catalog_unavailable")
    row = rows[0]
    return MssqlTableBehaviorState(
        temporal_type=int(row.get("temporal_type") or 0),
        history_schema=str(row.get("history_schema")) if row.get("history_schema") is not None else None,
        history_table=str(row.get("history_table")) if row.get("history_table") is not None else None,
        ledger_type=int(row.get("ledger_type") or 0),
        memory_optimized=bool(row.get("is_memory_optimized")),
        durability_desc=str(row.get("durability_desc")) if row.get("durability_desc") is not None else None,
        filetable=bool(row.get("is_filetable")),
        graph_node=bool(row.get("is_node")),
        graph_edge=bool(row.get("is_edge")),
    )


def _ledger_catalog_supported(connector: Any) -> bool:
    """Return whether this SQL Server exposes the SQL Ledger catalog surface."""

    rows = connector.get_records(
        "SELECT TRY_CONVERT(int, SERVERPROPERTY('ProductMajorVersion')) "
        "AS product_major_version, "
        "TRY_CONVERT(int, SERVERPROPERTY('EngineEdition')) AS engine_edition",
        as_dict=True,
    )
    if len(rows) != 1:
        raise RuntimeError("mssql_transaction.target_server_capabilities_unavailable")
    row = rows[0]
    major = row.get("product_major_version")
    edition = row.get("engine_edition")
    if not isinstance(major, int) or not isinstance(edition, int):
        raise RuntimeError("mssql_transaction.target_server_capabilities_unavailable")
    # SQL Ledger is available in SQL Server 2022 (major 16) and Azure SQL
    # Database/Managed Instance. Older boxed versions cannot contain a ledger
    # table and do not expose sys.tables.ledger_type at all.
    return major >= 16 or edition in {5, 8}


def _schema_exists(connector: Any, target: MSSQLObjectName) -> bool:
    rows = connector.get_records(
        f"SELECT 1 AS schema_exists FROM {_sys(target, 'schemas')} WHERE name = ?",
        (target.schema,),
        as_dict=True,
    )
    return len(rows) == 1


def _columns(connector: Any, target: MSSQLObjectName) -> tuple[MssqlCatalogColumnState, ...]:
    rows = connector.get_records(
        f"""
        SELECT c.column_id AS ordinal, c.name,
               sts.name AS system_type_schema, st.name AS system_type_name,
               uts.name AS user_type_schema, ut.name AS user_type_name,
               c.max_length, c.precision, c.scale, c.is_nullable, c.collation_name,
               c.is_identity, c.is_computed, c.is_sparse, c.is_rowguidcol,
               c.generated_always_type, dc.definition AS default_definition,
               cc.definition AS computed_definition,
               CONVERT(nvarchar(128), ic.seed_value) AS identity_seed,
               CONVERT(nvarchar(128), ic.increment_value) AS identity_increment
        FROM {_sys(target, "columns")} AS c
        INNER JOIN {_sys(target, "tables")} AS tab ON tab.object_id = c.object_id
        INNER JOIN {_sys(target, "schemas")} AS s ON s.schema_id = tab.schema_id
        INNER JOIN {_sys(target, "types")} AS ut ON ut.user_type_id = c.user_type_id
        INNER JOIN {_sys(target, "schemas")} AS uts ON uts.schema_id = ut.schema_id
        INNER JOIN {_sys(target, "types")} AS st
            ON st.user_type_id = c.system_type_id AND st.system_type_id = c.system_type_id
        INNER JOIN {_sys(target, "schemas")} AS sts ON sts.schema_id = st.schema_id
        LEFT JOIN {_sys(target, "default_constraints")} AS dc
            ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        LEFT JOIN {_sys(target, "computed_columns")} AS cc
            ON cc.object_id = c.object_id AND cc.column_id = c.column_id
        LEFT JOIN {_sys(target, "identity_columns")} AS ic
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE s.name = ? AND tab.name = ?
        ORDER BY c.column_id
        """,
        (target.schema, target.table),
        as_dict=True,
    )
    return tuple(_column(row) for row in rows)


def _checks(connector: Any, target: MSSQLObjectName) -> tuple[MssqlCheckConstraintState, ...]:
    rows = connector.get_records(
        f"""
        SELECT chk.name, chk.parent_column_id, chk.definition,
               chk.is_disabled, chk.is_not_trusted
        FROM {_sys(target, "check_constraints")} AS chk
        INNER JOIN {_sys(target, "tables")} AS tab ON tab.object_id = chk.parent_object_id
        INNER JOIN {_sys(target, "schemas")} AS s ON s.schema_id = tab.schema_id
        WHERE s.name = ? AND tab.name = ?
        ORDER BY chk.name
        """,
        (target.schema, target.table),
        as_dict=True,
    )
    return tuple(
        MssqlCheckConstraintState(
            name=str(row.get("name") or ""),
            parent_column_ordinal=int(row.get("parent_column_id") or 0),
            definition=str(row.get("definition") or ""),
            disabled=bool(row.get("is_disabled")),
            not_trusted=bool(row.get("is_not_trusted")),
        )
        for row in rows
    )


def _column(row: Any) -> MssqlCatalogColumnState:
    identity = bool(row.get("is_identity"))
    identity_seed, identity_increment = _identity_metadata(
        identity=identity,
        seed=row.get("identity_seed"),
        increment=row.get("identity_increment"),
    )
    return MssqlCatalogColumnState(
        ordinal=int(row.get("ordinal") or 0),
        name=str(row.get("name") or ""),
        system_type_schema=str(row.get("system_type_schema") or ""),
        system_type_name=str(row.get("system_type_name") or ""),
        user_type_schema=str(row.get("user_type_schema") or ""),
        user_type_name=str(row.get("user_type_name") or ""),
        max_length=int(row.get("max_length") or 0),
        precision=int(row.get("precision") or 0),
        scale=int(row.get("scale") or 0),
        nullable=bool(row.get("is_nullable")),
        collation=str(row.get("collation_name")) if row.get("collation_name") else None,
        identity=identity,
        computed=bool(row.get("is_computed")),
        sparse=bool(row.get("is_sparse")),
        rowguidcol=bool(row.get("is_rowguidcol")),
        generated_always_type=int(row.get("generated_always_type") or 0),
        default_definition=(str(row.get("default_definition")) if row.get("default_definition") is not None else None),
        computed_definition=(
            str(row.get("computed_definition")) if row.get("computed_definition") is not None else None
        ),
        identity_seed=identity_seed,
        identity_increment=identity_increment,
    )


def _identity_metadata(
    *,
    identity: bool,
    seed: Any,
    increment: Any,
) -> tuple[str | None, str | None]:
    """Normalize ODBC ``sql_variant`` identity metadata by column authority."""

    if not identity:
        # Some ODBC stacks expose a NULL from the LEFT JOIN as an empty string.
        # ``is_identity`` is the catalog authority; adapter-specific sentinels
        # must not become part of the exact target digest.
        return None, None
    normalized_seed = canonical_catalog_scalar(seed)
    normalized_increment = canonical_catalog_scalar(increment)
    if (
        normalized_seed is None
        or not normalized_seed.strip()
        or normalized_increment is None
        or not normalized_increment.strip()
    ):
        raise RuntimeError("mssql_transaction.target_identity_metadata_unavailable")
    return normalized_seed.strip(), normalized_increment.strip()


__all__ = ["read_schema_catalog_snapshot"]
