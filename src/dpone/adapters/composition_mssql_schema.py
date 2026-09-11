"""External, one-time schema-v2 installation for the shared composition ledger.

An administrator installs this batch only in a fresh protected incarnation,
then provisions the authority, physical enrollments and minimum permissions.
Runtime neither executes it nor repairs or adopts an old schema. Execution
wire documents and the native-v2 database are unaffected.
"""

from __future__ import annotations

from dpone.adapters.composition_mssql_layout import (
    COMPOSITION_MSSQL_LEDGER_LOCK,
    COMPOSITION_TABLES,
    LEGACY_COMPOSITION_OBJECTS,
    CompositionTable,
    require_control_schema,
)
from dpone.adapters.composition_mssql_layout import COMPOSITION_MSSQL_SCHEMA_VERSION as COMPOSITION_MSSQL_SCHEMA_VERSION


def _original_columns(table: CompositionTable) -> str:
    mutable = {
        "owners": {"state"},
        "domains": {"fencing_epoch", "owner_key"},
        "operations": {"state", "closed_gates_sha256", "quiescence_sha256", "outcome_evidence_sha256"},
    }.get(table.name, set())
    values = []
    for column in table.columns:
        if column.name not in mutable:
            values.append(f"[{column.name}]")
            if column.sql_type in {"varchar", "varbinary"}:
                values.append(f"DATALENGTH([{column.name}])")
    return ", ".join(values)


def _runtime_lock() -> str:
    return f"""IF XACT_STATE() <> 1 OR ISNULL(APPLOCK_MODE(N'public',
        N'{COMPOSITION_MSSQL_LEDGER_LOCK}', N'Transaction'), N'NoLock') <> N'Exclusive'
        THROW 51000, 'DPONE_COMPOSITION_WRITE_LOCK', 1;"""


def _owner_transition() -> str:
    return """IF NOT EXISTS (SELECT 1 FROM deleted)
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE state <> 'PREPARED')
            THROW 51000, 'DPONE_COMPOSITION_OWNER_INITIAL', 1;
    END
    ELSE IF EXISTS (
        SELECT 1 FROM deleted d JOIN inserted i ON i.owner_key = d.owner_key
        WHERE i.state <> d.state AND NOT (d.owner_kind = 'execution' AND (
            (d.state = 'PREPARED' AND i.state = 'ACTIVE') OR
            (d.state = 'ACTIVE' AND i.state = 'RETIRING') OR
            (d.state = 'RETIRING' AND i.state = 'RETIRED'))))
        THROW 51000, 'DPONE_COMPOSITION_OWNER_TRANSITION', 1;"""


def _domain_transition(schema: str) -> str:
    # Defined even if SQL evaluates the increment before another predicate.
    return f"""IF EXISTS (
        SELECT 1 FROM deleted d JOIN inserted i ON i.guard_id = d.guard_id
        WHERE NOT (
            (d.owner_key IS NULL AND i.owner_key IS NOT NULL
             AND CONVERT(decimal(20,0), i.fencing_epoch) = CONVERT(decimal(20,0), d.fencing_epoch) + 1
             AND EXISTS (SELECT 1 FROM [{schema}].[composition_owners] o
                 WHERE o.owner_key = i.owner_key AND o.owner_kind = 'execution' AND o.state = 'PREPARED')) OR
            (d.owner_key IS NOT NULL AND i.owner_key IS NULL AND i.fencing_epoch = d.fencing_epoch
             AND EXISTS (SELECT 1 FROM [{schema}].[composition_owners] o
                 WHERE o.owner_key = d.owner_key AND o.owner_kind = 'execution' AND o.state = 'RETIRING')) OR
            (i.fencing_epoch = d.fencing_epoch AND
             ((i.owner_key IS NOT NULL AND d.owner_key IS NOT NULL AND i.owner_key = d.owner_key)
              OR (i.owner_key IS NULL AND d.owner_key IS NULL)))))
        THROW 51000, 'DPONE_COMPOSITION_DOMAIN_TRANSITION', 1;"""


def _operation_transition() -> str:
    return """IF NOT EXISTS (SELECT 1 FROM deleted)
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE state <> 'RUNNING' OR closed_gates_sha256 IS NOT NULL
            OR quiescence_sha256 IS NOT NULL OR outcome_evidence_sha256 IS NOT NULL)
            THROW 51000, 'DPONE_COMPOSITION_OPERATION_INITIAL', 1;
    END
    ELSE IF EXISTS (
        SELECT 1 FROM deleted d JOIN inserted i ON i.operation_key = d.operation_key
        WHERE NOT (
            (i.state = d.state AND NOT EXISTS (
                SELECT i.closed_gates_sha256, i.quiescence_sha256, i.outcome_evidence_sha256
                EXCEPT SELECT d.closed_gates_sha256, d.quiescence_sha256, d.outcome_evidence_sha256)) OR
            (d.operation_family = 'execution' AND
             ((d.state = 'RUNNING' AND i.state IN ('SUCCEEDED', 'FAILED', 'COMMIT_UNKNOWN')) OR
              (d.state = 'COMMIT_UNKNOWN' AND i.state IN ('SUCCEEDED', 'FAILED'))))))
        THROW 51000, 'DPONE_COMPOSITION_OPERATION_TRANSITION', 1;"""


def composition_invariant_trigger_sql(control_schema: str, name: str) -> str:
    """Render one complete module, preserving exact UTF-16 catalog bytes.

    AFTER triggers require OUTPUT INTO followed by SELECT only after successful
    trigger completion. No qualification lifecycle transition is supplied.
    """
    schema = require_control_schema(control_schema)
    table = next((table for table in COMPOSITION_TABLES if table.name == name), None)
    if table is None:
        raise ValueError("unknown composition core table")
    if name in {"authority", "owner_domains", "operation_domains", "issued_authorities", "proofs"}:
        invariant = """IF EXISTS (SELECT 1 FROM deleted)
        THROW 51000, 'DPONE_COMPOSITION_ORIGINAL_IMMUTABLE', 1;"""
    else:
        columns = _original_columns(table)
        invariant = f"""IF EXISTS (SELECT 1 FROM deleted) AND (
        (SELECT COUNT_BIG(*) FROM inserted) <> (SELECT COUNT_BIG(*) FROM deleted) OR
        EXISTS (SELECT {columns} FROM deleted EXCEPT SELECT {columns} FROM inserted))
        THROW 51000, 'DPONE_COMPOSITION_ORIGINAL_IMMUTABLE', 1;"""
    if name == "authority":
        lock = ""
    elif name == "domains":
        lock = f"""IF NOT EXISTS (SELECT 1 FROM deleted)
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE fencing_epoch <> 0 OR owner_key IS NOT NULL)
            THROW 51000, 'DPONE_COMPOSITION_DOMAIN_INITIAL', 1;
    END
    ELSE BEGIN
        {_runtime_lock()}
    END;"""
    else:
        lock = _runtime_lock()
    transition = {
        "owners": _owner_transition(),
        "domains": _domain_transition(schema),
        "operations": _operation_transition(),
    }.get(name, "")
    return f"""CREATE TRIGGER [{schema}].[composition_{name}_invariant]
ON [{schema}].[composition_{name}] AFTER INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    {invariant}
    {lock}
    {transition}
END;"""


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


composition_invariant_trigger_sql.__module__ = "dpone.adapters.composition_mssql_invariants"
