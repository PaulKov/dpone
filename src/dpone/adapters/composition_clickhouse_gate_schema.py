"""Externally installed append-only ClickHouse principal issuance originals.

No runtime method installs, repairs or grants this schema. The exact installed
catalog and triggers must be visible to the protected SQL controller.
"""

from dpone.adapters.composition_mssql_catalog import inspect_composition_table, require_composition_catalog_visibility
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionColumn,
    CompositionKey,
    CompositionTable,
    CompositionTrigger,
)
from dpone.adapters.composition_mssql_schema import (
    COMPOSITION_MSSQL_LEDGER_LOCK,
    render_composition_table,
    require_control_schema,
)
from dpone.ports.sql_connection import SqlControlCursor

GATE_TABLES = (
    CompositionTable(
        "ch_gates",
        (
            CompositionColumn("gate_key", "varchar", 71),
            CompositionColumn("operation_key", "varchar", 71),
            CompositionColumn("login_name", "varchar", 73),
            CompositionColumn("evidence_sha256", "varchar", 71),
            CompositionColumn("evidence_document", "varbinary", -1),
        ),
        (
            CompositionKey("pk_composition_ch_gates", ("gate_key",), True),
            CompositionKey("uq_composition_ch_gate_name", ("login_name",)),
        ),
    ),
    CompositionTable(
        "ch_gate_bindings",
        (
            CompositionColumn("gate_key", "varchar", 71),
            CompositionColumn("gate_id", "uniqueidentifier", 16),
            CompositionColumn("evidence_sha256", "varchar", 71),
            CompositionColumn("evidence_document", "varbinary", -1),
        ),
        (
            CompositionKey("pk_composition_ch_gate_bindings", ("gate_key",), True),
            CompositionKey("uq_composition_ch_gate_uuid", ("gate_id",)),
        ),
    ),
    CompositionTable(
        "ch_gate_events",
        (
            CompositionColumn("gate_key", "varchar", 71),
            CompositionColumn("phase", "varchar", 8),
            CompositionColumn("evidence_sha256", "varchar", 71),
            CompositionColumn("evidence_document", "varbinary", -1),
        ),
        (CompositionKey("pk_composition_ch_gate_events", ("gate_key", "phase"), True),),
    ),
)


def gate_trigger_sql(control_schema: str, name: str) -> str:
    """SQL enforces original bytes and monotonic issuance/closure dependencies."""
    schema = require_control_schema(control_schema)
    if name not in {table.name for table in GATE_TABLES}:
        raise ValueError("unknown composition ClickHouse gate table")
    gates, bindings, events = (
        f"[{schema}].[composition_{table}]" for table in ("ch_gates", "ch_gate_bindings", "ch_gate_events")
    )
    if name == "ch_gates":
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE DATALENGTH(i.operation_key)<>71
            OR DATALENGTH(i.login_name)<>73 OR NOT EXISTS (SELECT 1 FROM [{schema}].[composition_operations] o
                WITH (HOLDLOCK) WHERE o.operation_key=i.operation_key AND o.state='RUNNING' AND o.operation_family='execution'))
            THROW 51000, 'DPONE_CH_GATE_OPERATION', 1;"""
    elif name == "ch_gate_bindings":
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE NOT EXISTS
            (SELECT 1 FROM {gates} g WITH (HOLDLOCK) WHERE g.gate_key=i.gate_key)
            OR EXISTS (SELECT 1 FROM {events} e WITH (HOLDLOCK) WHERE e.gate_key=i.gate_key))
            THROW 51000, 'DPONE_CH_GATE_BINDING', 1;"""
    else:
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE i.phase NOT IN ('ENABLING','READY','CLOSING','CLOSED')
            OR DATALENGTH(i.phase)<>LEN(i.phase) OR NOT EXISTS
                (SELECT 1 FROM {bindings} b WITH (HOLDLOCK) WHERE b.gate_key=i.gate_key)
            OR (i.phase='READY' AND (NOT EXISTS (SELECT 1 FROM {events} e WITH (HOLDLOCK)
                WHERE e.gate_key=i.gate_key AND e.phase='ENABLING') OR EXISTS
                (SELECT 1 FROM {events} e WITH (HOLDLOCK) WHERE e.gate_key=i.gate_key AND e.phase='CLOSING')))
            OR (i.phase='ENABLING' AND EXISTS (SELECT 1 FROM {events} e WITH (HOLDLOCK) WHERE e.gate_key=i.gate_key))
            OR (i.phase='CLOSED' AND (NOT EXISTS (SELECT 1 FROM {events} e WITH (HOLDLOCK)
                WHERE e.gate_key=i.gate_key AND e.phase='READY') OR NOT EXISTS
                (SELECT 1 FROM {events} e WITH (HOLDLOCK) WHERE e.gate_key=i.gate_key AND e.phase='CLOSING'))))
            THROW 51000, 'DPONE_CH_GATE_PHASE', 1;"""
    return f"""CREATE TRIGGER [{schema}].[composition_{name}_invariant]
ON [{schema}].[composition_{name}] AFTER INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted) THROW 51000, 'DPONE_CH_GATE_IMMUTABLE', 1;
    IF XACT_STATE()<>1 OR ISNULL(APPLOCK_MODE(N'public',N'{COMPOSITION_MSSQL_LEDGER_LOCK}',N'Transaction'),N'NoLock')<>N'Exclusive'
        THROW 51000, 'DPONE_CH_GATE_LOCK', 1;
    IF EXISTS (SELECT 1 FROM inserted WHERE DATALENGTH(gate_key)<>71
        OR DATALENGTH(evidence_document) NOT BETWEEN 1 AND 8388608 OR DATALENGTH(evidence_sha256)<>71
        OR evidence_sha256<>'sha256:' + LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',evidence_document),2)))
        THROW 51000, 'DPONE_CH_GATE_ORIGINAL', 1;
    {policy}
END;"""


def render_clickhouse_gate_schema(control_schema: str = "dpone_control") -> str:
    """Administrator-only additive installation; no secret, enrollment or grants."""
    schema = require_control_schema(control_schema)
    statements = ["SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; BEGIN TRANSACTION;"]
    for table in GATE_TABLES:
        statements.append(render_composition_table(table, schema))
        statements.append("EXEC(N'" + gate_trigger_sql(schema, table.name).replace("'", "''") + "');")
    return "\n".join((*statements, "COMMIT TRANSACTION;", ""))


def require_clickhouse_gate_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Audit all columns/keys/checks/triggers and visibility; no repair path."""
    schema = require_control_schema(control_schema)
    require_composition_catalog_visibility(cursor, schema)
    for table in GATE_TABLES:
        inspect_composition_table(
            cursor,
            schema,
            table,
            {},
            {},
            CompositionTrigger(
                "composition_" + table.name + "_invariant",
                gate_trigger_sql(schema, table.name),
                ("DELETE", "INSERT", "UPDATE"),
            ),
        )
