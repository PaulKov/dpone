"""External, one-time schema-v2 installation for the shared composition ledger.

An administrator installs this batch only in a fresh protected incarnation,
then provisions the authority, physical enrollments and minimum permissions.
Runtime neither executes it nor repairs or adopts an old schema. Execution
wire documents and the native-v2 database are unaffected.
"""

from __future__ import annotations

from dpone.adapters.composition_mssql_invariants import composition_invariant_trigger_sql
from dpone.adapters.composition_mssql_layout import (
    COMPOSITION_MSSQL_LEDGER_LOCK as COMPOSITION_MSSQL_LEDGER_LOCK,
)
from dpone.adapters.composition_mssql_layout import (
    COMPOSITION_MSSQL_SCHEMA_VERSION as COMPOSITION_MSSQL_SCHEMA_VERSION,
)
from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES, LEGACY_COMPOSITION_OBJECTS, CompositionTable
from dpone.adapters.composition_mssql_layout import require_control_schema as require_control_schema


def _columns(names: tuple[str, ...]) -> str:
    return ", ".join(f"[{name}]" for name in names)


def render_composition_mssql_schema(control_schema: str = "dpone_control") -> str:
    """Render eight exact tables and immutable/transition trigger modules.

    The fixed global lock remains transaction-owned and shared by both owner
    families. SQL constraints bind families and complete epoch associations;
    runtime still reopens every original with its existing decoder and hash.
    No authentication, gate, grant, automatic migration or writer is installed.
    """
    schema = require_control_schema(control_schema)

    def table(name: str) -> str:
        return f"[{schema}].[composition_{name}]"

    statements = [
        "SET XACT_ABORT ON;",
        "SET ANSI_NULLS ON;",
        "SET QUOTED_IDENTIFIER ON;",
        "BEGIN TRANSACTION;",
        f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');",
    ]
    for name in LEGACY_COMPOSITION_OBJECTS:
        statements.append(
            f"IF OBJECT_ID(N'{table(name)}') IS NOT NULL THROW 51000, 'DPONE_COMPOSITION_LEGACY_LAYOUT', 1;"
        )
    for definition in COMPOSITION_TABLES:
        statements.append(render_composition_table(definition, schema))
        module = composition_invariant_trigger_sql(schema, definition.name).replace("'", "''")
        statements.append(f"EXEC(N'{module}');")
    return "\n".join((*statements, "COMMIT TRANSACTION;", ""))


def render_composition_table(definition: CompositionTable, schema: str) -> str:
    """Render fixed internal metadata; lengths are catalog bytes, including Unicode."""
    schema = require_control_schema(schema)

    def table(name: str) -> str:
        return f"[{schema}].[composition_{name}]"

    lines = []
    for column in definition.columns:
        size = (
            f"({'max' if column.length == -1 else column.length // (2 if column.sql_type == 'nvarchar' else 1)})"
            if column.sql_type in {"varchar", "nvarchar", "binary", "varbinary"}
            else ""
        )
        collation = f" COLLATE {column.collation}" if column.collation else ""
        lines.append(
            f"    [{column.name}] {column.sql_type}{size}{collation} {'NULL' if column.nullable else 'NOT NULL'}"
        )
    for key in definition.keys:
        kind = "PRIMARY KEY CLUSTERED" if key.primary else "UNIQUE NONCLUSTERED"
        lines.append(f"    CONSTRAINT [{key.name}] {kind} ({_columns(key.columns)})")
    for foreign in definition.foreign_keys:
        lines.append(
            f"    CONSTRAINT [{foreign.name}] FOREIGN KEY ({_columns(foreign.columns)}) "
            f"REFERENCES {table(foreign.target)} ({_columns(foreign.target_columns)})"
        )
    for check in definition.checks:
        lines.append(f"    CONSTRAINT [{check.name}] CHECK ({check.expression})")
    return f"CREATE TABLE {table(definition.name)} (\n" + ",\n".join(lines) + "\n);"
