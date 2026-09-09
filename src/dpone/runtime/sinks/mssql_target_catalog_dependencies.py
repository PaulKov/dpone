"""Read SQL Server target dependencies that can alter governed DML semantics."""

from __future__ import annotations

from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName, quote_mssql_identifier
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlForeignKeyState,
    MssqlPermissionState,
    MssqlTriggerState,
)


def read_foreign_keys(connector: Any, target: MSSQLObjectName) -> tuple[MssqlForeignKeyState, ...]:
    rows = connector.get_records(
        f"""
        SELECT fk.object_id, fk.name, fk.update_referential_action_desc,
               fk.delete_referential_action_desc, fk.is_disabled,
               fk.is_not_trusted, fk.is_not_for_replication,
               s.name AS parent_schema, tab.name AS parent_table,
               rs.name AS referenced_schema, rt.name AS referenced_table,
               pc.name AS parent_column, rc.name AS referenced_column,
               fkc.constraint_column_id
        FROM {_sys(target, "foreign_keys")} AS fk
        INNER JOIN {_sys(target, "tables")} AS tab ON tab.object_id = fk.parent_object_id
        INNER JOIN {_sys(target, "schemas")} AS s ON s.schema_id = tab.schema_id
        INNER JOIN {_sys(target, "foreign_key_columns")} AS fkc
            ON fkc.constraint_object_id = fk.object_id
        INNER JOIN {_sys(target, "columns")} AS pc
            ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
        INNER JOIN {_sys(target, "tables")} AS rt ON rt.object_id = fkc.referenced_object_id
        INNER JOIN {_sys(target, "schemas")} AS rs ON rs.schema_id = rt.schema_id
        INNER JOIN {_sys(target, "columns")} AS rc
            ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
        WHERE (s.name = ? AND tab.name = ?) OR (rs.name = ? AND rt.name = ?)
        ORDER BY fk.name, fkc.constraint_column_id
        """,
        (target.schema, target.table, target.schema, target.table),
        as_dict=True,
    )
    grouped: dict[int, tuple[dict[str, Any], list[str], list[str]]] = {}
    for row in rows:
        object_id = int(row.get("object_id") or 0)
        metadata, parents, references = grouped.setdefault(object_id, (dict(row), [], []))
        parents.append(str(row.get("parent_column") or ""))
        references.append(str(row.get("referenced_column") or ""))
    return tuple(
        MssqlForeignKeyState(
            name=str(metadata.get("name") or ""),
            direction=_foreign_key_direction(metadata, target),
            parent_schema=str(metadata.get("parent_schema") or ""),
            parent_table=str(metadata.get("parent_table") or ""),
            parent_columns=tuple(parents),
            referenced_schema=str(metadata.get("referenced_schema") or ""),
            referenced_table=str(metadata.get("referenced_table") or ""),
            referenced_columns=tuple(references),
            update_action=str(metadata.get("update_referential_action_desc") or ""),
            delete_action=str(metadata.get("delete_referential_action_desc") or ""),
            disabled=bool(metadata.get("is_disabled")),
            not_trusted=bool(metadata.get("is_not_trusted")),
            not_for_replication=bool(metadata.get("is_not_for_replication")),
        )
        for _object_id, (metadata, parents, references) in sorted(grouped.items())
    )


def _foreign_key_direction(metadata: dict[str, Any], target: MSSQLObjectName) -> str:
    parent = (
        str(metadata.get("parent_schema") or "").casefold(),
        str(metadata.get("parent_table") or "").casefold(),
    )
    referenced = (
        str(metadata.get("referenced_schema") or "").casefold(),
        str(metadata.get("referenced_table") or "").casefold(),
    )
    expected = (target.schema.casefold(), target.table.casefold())
    if parent == expected and referenced == expected:
        return "self"
    return "outbound" if parent == expected else "inbound"


def read_triggers(connector: Any, target: MSSQLObjectName) -> tuple[MssqlTriggerState, ...]:
    rows = connector.get_records(
        f"""
        SELECT tr.name, tr.is_disabled, tr.is_instead_of_trigger,
               tr.is_not_for_replication, tr.is_ms_shipped,
               te.type_desc AS event_type, dp.name AS execute_as_principal,
               sm.definition, tr.object_id
        FROM {_sys(target, "triggers")} AS tr
        INNER JOIN {_sys(target, "tables")} AS tab ON tab.object_id = tr.parent_id
        INNER JOIN {_sys(target, "schemas")} AS s ON s.schema_id = tab.schema_id
        LEFT JOIN {_sys(target, "sql_modules")} AS sm ON sm.object_id = tr.object_id
        LEFT JOIN {_sys(target, "trigger_events")} AS te ON te.object_id = tr.object_id
        LEFT JOIN {_sys(target, "database_principals")} AS dp
            ON dp.principal_id = sm.execute_as_principal_id
        WHERE s.name = ? AND tab.name = ?
        ORDER BY tr.name, te.type_desc
        """,
        (target.schema, target.table),
        as_dict=True,
    )
    grouped: dict[int, tuple[dict[str, Any], list[str]]] = {}
    for row in rows:
        object_id = int(row.get("object_id") or 0)
        metadata, events = grouped.setdefault(object_id, (dict(row), []))
        if row.get("event_type") is not None:
            events.append(str(row.get("event_type") or "").upper())
    return tuple(
        MssqlTriggerState(
            name=str(metadata.get("name") or ""),
            disabled=bool(metadata.get("is_disabled")),
            instead_of=bool(metadata.get("is_instead_of_trigger")),
            not_for_replication=bool(metadata.get("is_not_for_replication")),
            system_shipped=bool(metadata.get("is_ms_shipped")),
            events=tuple(events),
            execute_as_principal=(
                str(metadata.get("execute_as_principal")) if metadata.get("execute_as_principal") is not None else None
            ),
            definition=str(metadata.get("definition")) if metadata.get("definition") is not None else None,
        )
        for _object_id, (metadata, events) in sorted(grouped.items())
    )


def read_permissions(connector: Any, target: MSSQLObjectName) -> tuple[MssqlPermissionState, ...]:
    rows = connector.get_records(
        f"""
        SELECT grantee.name AS grantee, grantee.type_desc AS grantee_type,
               perm.permission_name, perm.state_desc, c.name AS column_name
        FROM {_sys(target, "database_permissions")} AS perm
        INNER JOIN {_sys(target, "tables")} AS tab ON tab.object_id = perm.major_id
        INNER JOIN {_sys(target, "schemas")} AS s ON s.schema_id = tab.schema_id
        INNER JOIN {_sys(target, "database_principals")} AS grantee
            ON grantee.principal_id = perm.grantee_principal_id
        LEFT JOIN {_sys(target, "columns")} AS c
            ON c.object_id = perm.major_id AND c.column_id = perm.minor_id
        WHERE perm.class = 1 AND s.name = ? AND tab.name = ?
        ORDER BY grantee.name, perm.permission_name, perm.state_desc, perm.minor_id
        """,
        (target.schema, target.table),
        as_dict=True,
    )
    return tuple(
        MssqlPermissionState(
            grantee=str(row.get("grantee") or ""),
            grantee_type=str(row.get("grantee_type") or ""),
            permission_name=str(row.get("permission_name") or ""),
            state_desc=str(row.get("state_desc") or ""),
            column_name=str(row.get("column_name")) if row.get("column_name") is not None else None,
        )
        for row in rows
    )


def _sys(target: MSSQLObjectName, catalog: str) -> str:
    if target.database is None:
        raise RuntimeError("mssql_transaction.target_database_required")
    return quote_mssql_identifier(target.database) + ".sys." + quote_mssql_identifier(catalog)


__all__ = ["read_foreign_keys", "read_permissions", "read_triggers"]
