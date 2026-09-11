"""Exact bounded schema-v2 catalog audit, with no DDL or admission authority.

The external CHECK reference must come from the controlled SQL round trip.
Metadata visibility and exact modules are necessary observations; trusted
provisioning must separately exclude privilege/trigger bypass, narrower metadata
DENYs and concurrent DDL. Optional NP/gate groups and incoming gate FKs have their own audits.
"""

from __future__ import annotations

import re
from hashlib import sha256
from typing import TYPE_CHECKING

from dpone.adapters import composition_mssql_check_definitions as reference
from dpone.adapters import composition_mssql_gate_check_definitions as gate_reference
from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
from dpone.adapters.composition_mssql_gate_schema import gate_table_trigger, render_composition_mssql_login_gate
from dpone.adapters.composition_mssql_layout import (
    COMPOSITION_COLLATION,
    COMPOSITION_TABLES,
    LEGACY_COMPOSITION_OBJECTS,
    CompositionTable,
    CompositionTrigger,
    require_control_schema,
)
from dpone.adapters.composition_mssql_schema import composition_invariant_trigger_sql, render_composition_mssql_schema
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.ports.sql_connection import SqlControlCursor

if TYPE_CHECKING:
    from dpone.ports.sql_connection import SqlControlCursor
Rows = tuple[tuple[object, ...], ...]


def require_check_reference(
    tables: tuple[CompositionTable, ...],
    definitions: dict[str, str],
    metadata: dict[str, tuple[int, int]],
    collation: str,
    ddl_digest: str,
    expected_digest: str,
    reason: str,
) -> None:
    """Validate the complete generated core/gate reference before catalog I/O."""
    names = {check.name for table in tables for check in table.checks}
    if (
        type(definitions) is not dict
        or set(definitions) != names
        or type(metadata) is not dict
        or (set(metadata) != names)
        or (type(collation) is not str)
        or (re.fullmatch("[A-Za-z0-9_]{1,128}", collation) is None)
        or (type(ddl_digest) is not str)
        or (ddl_digest != expected_digest)
        or any(type(value) is not str or not 1 <= len(value) <= 65536 for value in definitions.values())
    ):
        raise CompositionAdmissionError(reason)
    try:
        if any(len(value.encode("utf-16le")) > 131072 for value in definitions.values()):
            raise CompositionAdmissionError(reason)
    except UnicodeError:
        raise CompositionAdmissionError(reason) from None
    for table in tables:
        for check in table.checks:
            value = metadata[check.name]
            if (
                type(value) is not tuple
                or len(value) != 2
                or type(value[0]) is not int
                or (not 0 <= value[0] <= len(table.columns))
                or (type(value[1]) is not int)
                or (value[1] not in (0, 1))
            ):
                raise CompositionAdmissionError(reason)


def require_check_collation(cursor: SqlControlCursor, collation: str) -> None:
    """Read the current database default independently of captured reference bytes."""
    _require(cursor, "collation", "d.collation_name FROM sys.databases d WHERE d.database_id=DB_ID();", ((collation,),))


def _require(cursor: SqlControlCursor, part: str, query: str, expected: Rows, *parameters: object) -> None:
    cursor.execute(f"SELECT TOP ({len(expected) + 1}) /* composition_schema:{part} */ " + query, *parameters)
    actual = tuple(tuple(row) for row in cursor.fetchall())
    if actual != expected or any(
        (
            type(value) is not type(wanted)
            for row, expected_row in zip(actual, expected, strict=True)
            for value, wanted in zip(row, expected_row, strict=True)
        )
    ):
        raise CompositionAdmissionError("control_schema_" + part)


def require_composition_mssql_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Audit eight complete core definitions; never connect, mutate or repair.

    The owning boundary separately requires the current protected transaction
    and service identity. Empty/stale CHECK producer references reject before
    any catalog read. Unknown driver failures expose only a fixed reason.
    """
    try:
        schema = require_control_schema(control_schema)
    except ValueError:
        raise CompositionAdmissionError("control_schema") from None
    try:
        checks, metadata = (reference.CHECK_DEFINITIONS, reference.CHECK_METADATA)
        collation = reference.CHECK_DATABASE_COLLATION
        require_check_reference(
            COMPOSITION_TABLES,
            checks,
            metadata,
            collation,
            reference.CHECK_DDL_SHA256,
            "sha256:" + sha256(render_composition_mssql_schema().encode("utf-8")).hexdigest(),
            "control_schema_reference",
        )
        require_composition_catalog_visibility(cursor, schema)
        require_check_collation(cursor, collation)
        legacy = ",".join("N'composition_" + name + "'" for name in LEGACY_COMPOSITION_OBJECTS)
        _require(
            cursor, "legacy", f"name FROM sys.objects WHERE schema_id=SCHEMA_ID(?) AND name IN ({legacy});", (), schema
        )
        for table in COMPOSITION_TABLES:
            inspect_composition_table(
                cursor,
                schema,
                table,
                checks,
                metadata,
                CompositionTrigger(
                    "composition_" + table.name + "_invariant",
                    composition_invariant_trigger_sql(schema, table.name),
                    ("DELETE", "INSERT", "UPDATE"),
                ),
            )
    except CompositionAdmissionError:
        raise
    except Exception:
        raise CompositionAdmissionError("control_schema_unavailable") from None


def require_composition_catalog_visibility(cursor: SqlControlCursor, schema: str) -> None:
    """Require complete database metadata, including policies in other schemas."""
    _require(
        cursor,
        "visibility",
        "s.name,HAS_PERMS_BY_NAME(s.name,N'SCHEMA',N'VIEW DEFINITION'),HAS_PERMS_BY_NAME(DB_NAME(),N'DATABASE',N'VIEW DEFINITION'),CASE WHEN EXISTS (SELECT 1 FROM sys.database_permissions p JOIN sys.user_token u ON u.principal_id=p.grantee_principal_id WHERE p.state='D' AND p.permission_name IN ('CONTROL','VIEW DEFINITION','VIEW SECURITY DEFINITION','VIEW PERFORMANCE DEFINITION')) THEN 1 ELSE 0 END FROM sys.schemas s WHERE s.name=?;",
        ((schema, 1, 1, 0),),
        schema,
    )


def inspect_composition_table(
    cursor: SqlControlCursor,
    schema: str,
    table: CompositionTable,
    checks: dict[str, str],
    metadata: dict[str, tuple[int, int]],
    trigger: CompositionTrigger,
) -> None:
    """Inspect fixed metadata from a closed core/gate wrapper; no admission alone."""
    name = f"[{schema}].[composition_{table.name}]"
    order = " COLLATE " + COMPOSITION_COLLATION
    _require(
        cursor,
        "table",
        "SCHEMA_NAME(t.schema_id),t.name,CONVERT(int,t.uses_ansi_nulls),HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(t.schema_id))+N'.'+QUOTENAME(t.name),N'OBJECT',N'VIEW DEFINITION'),t.temporal_type,CONVERT(int,t.is_memory_optimized),CONVERT(int,t.is_filetable),CONVERT(int,t.is_external),CONVERT(int,t.is_node),CONVERT(int,t.is_edge),t.ledger_type,CONVERT(int,t.is_dropped_ledger_table),t.durability,CONVERT(int,t.is_remote_data_archive_enabled),CONVERT(int,t.is_replicated),CONVERT(int,t.has_replication_filter),CONVERT(int,t.is_merge_published),CONVERT(int,t.is_sync_tran_subscribed),CONVERT(int,t.is_tracked_by_cdc),CASE WHEN EXISTS (SELECT 1 FROM sys.fulltext_indexes f WHERE f.object_id=t.object_id) THEN 1 ELSE 0 END,CASE WHEN EXISTS (SELECT 1 FROM sys.change_tracking_tables c WHERE c.object_id=t.object_id) THEN 1 ELSE 0 END,t.history_table_id,t.ledger_view_id FROM sys.tables t WHERE t.object_id=OBJECT_ID(?,N'U');",
        ((schema, "composition_" + table.name, 1, 1, *[0] * 17, None, None),),
        name,
    )
    columns: Rows = tuple(
        (
            (
                index,
                column.name,
                column.sql_type,
                column.length,
                column.collation,
                int(column.nullable),
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                0,
                0,
                0,
                int(column.sql_type in {"varchar", "nvarchar", "binary", "varbinary"}),
            )
            for index, column in enumerate(table.columns, 1)
        )
    )
    _require(
        cursor,
        "columns",
        "c.column_id,c.name,TYPE_NAME(c.system_type_id),c.max_length,c.collation_name,CONVERT(int,c.is_nullable),CASE WHEN c.user_type_id=c.system_type_id THEN 1 ELSE 0 END,CONVERT(int,c.is_identity),CONVERT(int,c.is_computed),c.default_object_id,c.rule_object_id,c.generated_always_type,CONVERT(int,c.is_hidden),c.encryption_type,CASE WHEN EXISTS (SELECT 1 FROM sys.masked_columns m WHERE m.object_id=c.object_id AND m.column_id=c.column_id AND m.is_masked=1) THEN 1 ELSE 0 END,CONVERT(int,c.is_sparse),CONVERT(int,c.is_column_set),CONVERT(int,c.is_filestream),CONVERT(int,c.is_rowguidcol),CONVERT(int,c.is_ansi_padded) FROM sys.columns c WHERE c.object_id=OBJECT_ID(?,N'U') ORDER BY c.column_id;",
        columns,
        name,
    )
    _keys(cursor, name, table, order)
    _constraints(cursor, schema, name, table, checks, metadata, order, trigger.name)
    _modules(cursor, schema, name, trigger, order)
    _require(
        cursor,
        "row_security",
        "object_id FROM sys.security_predicates WHERE target_object_id=OBJECT_ID(?,N'U');",
        (),
        name,
    )


def _keys(cursor: SqlControlCursor, name: str, table: CompositionTable, order: str) -> None:
    keys: Rows = tuple(
        (
            (
                key.name,
                column,
                index,
                index,
                0,
                0,
                int(key.primary),
                1,
                int(not key.primary),
                1 if key.primary else 2,
                0,
                0,
                0,
                None,
                0,
                key.name,
                "PRIMARY_KEY_CONSTRAINT" if key.primary else "UNIQUE_CONSTRAINT",
            )
            for key in sorted(table.keys, key=lambda value: value.name)
            for index, column in enumerate(key.columns, 1)
        )
    )
    _require(
        cursor,
        "keys",
        f"i.name,c.name,ic.key_ordinal,ic.index_column_id,CONVERT(int,ic.is_descending_key),CONVERT(int,ic.is_included_column),CONVERT(int,i.is_primary_key),CONVERT(int,i.is_unique),CONVERT(int,i.is_unique_constraint),i.type,CONVERT(int,i.is_disabled),CONVERT(int,i.has_filter),CONVERT(int,i.ignore_dup_key),DATALENGTH(i.filter_definition),CONVERT(int,i.is_hypothetical),kc.name,kc.type_desc FROM sys.indexes i LEFT JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id LEFT JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id LEFT JOIN sys.key_constraints kc ON kc.parent_object_id=i.object_id AND kc.unique_index_id=i.index_id WHERE i.object_id=OBJECT_ID(?,N'U') ORDER BY i.name{order},ic.index_column_id;",
        keys,
        name,
    )


def _constraints(
    cursor: SqlControlCursor,
    schema: str,
    name: str,
    table: CompositionTable,
    checks: dict[str, str],
    metadata: dict[str, tuple[int, int]],
    order: str,
    trigger: str,
) -> None:
    foreign: Rows = tuple(
        (
            (key.name, schema, index, column, schema, "composition_" + key.target, target, 0, 0, 0, 0, 0, 0)
            for key in sorted(table.foreign_keys, key=lambda value: value.name)
            for index, (column, target) in enumerate(zip(key.columns, key.target_columns, strict=True), 1)
        )
    )
    _require(
        cursor,
        "foreign_keys",
        f"f.name,SCHEMA_NAME(f.schema_id),k.constraint_column_id,pc.name,OBJECT_SCHEMA_NAME(f.referenced_object_id),rt.name,rc.name,CONVERT(int,f.is_disabled),CONVERT(int,f.is_not_trusted),f.delete_referential_action,f.update_referential_action,CONVERT(int,f.is_not_for_replication),CONVERT(int,f.is_system_named) FROM sys.foreign_keys f LEFT JOIN sys.foreign_key_columns k ON k.constraint_object_id=f.object_id LEFT JOIN sys.columns pc ON pc.object_id=k.parent_object_id AND pc.column_id=k.parent_column_id LEFT JOIN sys.tables rt ON rt.object_id=f.referenced_object_id LEFT JOIN sys.columns rc ON rc.object_id=k.referenced_object_id AND rc.column_id=k.referenced_column_id WHERE f.parent_object_id=OBJECT_ID(?,N'U') ORDER BY f.name{order},k.constraint_column_id;",
        foreign,
        name,
    )
    expected: Rows = tuple(
        (
            check.name,
            schema,
            metadata[check.name][0],
            checks[check.name],
            len(checks[check.name].encode("utf-16le")),
            0,
            0,
            0,
            metadata[check.name][1],
            0,
        )
        for check in sorted(table.checks, key=lambda value: value.name)
    )
    maximum = max((len(checks[check.name].encode("utf-16le")) for check in table.checks), default=0)
    _require(
        cursor,
        "checks",
        f"c.name,SCHEMA_NAME(c.schema_id),c.parent_column_id,CASE WHEN DATALENGTH(c.definition)<=? THEN c.definition END,DATALENGTH(c.definition),CONVERT(int,c.is_disabled),CONVERT(int,c.is_not_trusted),CONVERT(int,c.is_not_for_replication),CONVERT(int,c.uses_database_collation),CONVERT(int,c.is_system_named) FROM sys.check_constraints c WHERE c.parent_object_id=OBJECT_ID(?,N'U') ORDER BY c.name{order};",
        expected,
        maximum,
        name,
    )
    objects = [(check.name, "CHECK_CONSTRAINT", schema, 0) for check in table.checks]
    objects += [
        (key.name, "PRIMARY_KEY_CONSTRAINT" if key.primary else "UNIQUE_CONSTRAINT", schema, 0) for key in table.keys
    ]
    objects += [(key.name, "FOREIGN_KEY_CONSTRAINT", schema, 0) for key in table.foreign_keys]
    expected = tuple(sorted((*objects, (trigger, "SQL_TRIGGER", schema, 0))))
    _require(
        cursor,
        "objects",
        f"o.name,o.type_desc,SCHEMA_NAME(o.schema_id),CONVERT(int,o.is_ms_shipped) FROM sys.objects o WHERE o.parent_object_id=OBJECT_ID(?,N'U') ORDER BY o.name{order};",
        expected,
        name,
    )


def _modules(cursor: SqlControlCursor, schema: str, name: str, module: CompositionTrigger, order: str) -> None:
    trigger = module.name
    raw = module.definition.encode("utf-16le")
    _require(
        cursor,
        "triggers",
        f"t.name,OBJECT_SCHEMA_NAME(t.object_id),t.type_desc,t.parent_class,CONVERT(int,t.is_ms_shipped),CONVERT(int,t.is_disabled),CONVERT(int,t.is_instead_of_trigger),CONVERT(int,t.is_not_for_replication),CONVERT(int,m.uses_ansi_nulls),CONVERT(int,m.uses_quoted_identifier),m.execute_as_principal_id,CONVERT(int,m.is_schema_bound),CONVERT(int,m.uses_native_compilation),HASHBYTES('SHA2_256',m.definition),DATALENGTH(m.definition) FROM sys.triggers t LEFT JOIN sys.sql_modules m ON m.object_id=t.object_id WHERE t.parent_id=OBJECT_ID(?,N'U') ORDER BY t.name{order};",
        ((trigger, schema, "SQL_TRIGGER", 1, 0, 0, 0, 0, 1, 1, None, 0, 0, sha256(raw).digest(), len(raw)),),
        name,
    )
    _require(
        cursor,
        "events",
        f"t.name,e.type_desc,CONVERT(int,e.is_first),CONVERT(int,e.is_last) FROM sys.trigger_events e JOIN sys.triggers t ON t.object_id=e.object_id WHERE t.parent_id=OBJECT_ID(?,N'U') ORDER BY t.name{order},e.type_desc{order};",
        tuple((trigger, event, 0, 0) for event in module.events),
        name,
    )


__all__ = ["require_composition_mssql_schema"]


def require_composition_mssql_gate_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Audit every fixed gate table and module; LOGON policy is checked by its owner."""
    try:
        schema = require_control_schema(control_schema)
        values, metadata = (gate_reference.CHECK_DEFINITIONS, gate_reference.CHECK_METADATA)
        collation = gate_reference.CHECK_DATABASE_COLLATION
        digest = (
            "sha256:"
            + sha256(render_composition_mssql_login_gate(control_database="dpone_control").encode("utf-8")).hexdigest()
        )
        require_check_reference(
            COMPOSITION_GATE_TABLES,
            values,
            metadata,
            collation,
            gate_reference.CHECK_DDL_SHA256,
            digest,
            "login_gate_schema_reference",
        )
        require_composition_catalog_visibility(cursor, schema)
        require_check_collation(cursor, collation)
        for table in COMPOSITION_GATE_TABLES:
            inspect_composition_table(cursor, schema, table, values, metadata, gate_table_trigger(schema, table.name))
    except CompositionAdmissionError:
        raise
    except Exception:
        raise CompositionAdmissionError("login_gate_schema_unavailable") from None


require_composition_mssql_gate_schema.__module__ = "dpone.adapters.composition_mssql_gate_catalog"
