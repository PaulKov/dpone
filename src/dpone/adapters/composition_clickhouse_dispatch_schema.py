"""External append-only SQL journal for ClickHouse sends and closure barriers.

Installation is an administrator action on the protected control database.
Runtime audits exact table and trigger definitions and never installs or repairs
them. The shared transaction lock orders every claim with gate closure.
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

DISPATCH_TABLES = (
    CompositionTable(
        "ch_dispatches",
        (
            CompositionColumn("dispatch_sha256", "varchar", 71),
            CompositionColumn("claim_key", "varchar", 71),
            CompositionColumn("operation_key", "varchar", 71),
            CompositionColumn("gate_id", "uniqueidentifier", 16),
            CompositionColumn("query_id", "varchar", 128),
            CompositionColumn("dispatch_document", "varbinary", -1),
        ),
        (
            CompositionKey("pk_composition_ch_dispatches", ("dispatch_sha256",), True),
            CompositionKey("uq_composition_ch_dispatch_claim", ("claim_key",)),
            CompositionKey("uq_composition_ch_dispatch_query", ("query_id",)),
        ),
    ),
    CompositionTable(
        "ch_dispatch_terminals",
        (
            CompositionColumn("dispatch_sha256", "varchar", 71),
            CompositionColumn("terminal_sha256", "varchar", 71),
            CompositionColumn("terminal_document", "varbinary", -1),
        ),
        (CompositionKey("pk_composition_ch_dispatch_terminals", ("dispatch_sha256",), True),),
    ),
    CompositionTable(
        "ch_dispatch_closures",
        (
            CompositionColumn("gate_id", "uniqueidentifier", 16),
            CompositionColumn("phase", "varchar", 7),
            CompositionColumn("evidence_sha256", "varchar", 71),
            CompositionColumn("evidence_document", "varbinary", -1),
        ),
        (CompositionKey("pk_composition_ch_dispatch_closures", ("gate_id", "phase"), True),),
    ),
)


def dispatch_trigger_sql(control_schema: str, name: str) -> str:
    """Generate exact immutable originals and closed append dependencies.

    A healthy transaction and its exclusive ledger lock are the authority.
    Trigger nesting depth is not an additional control-ledger requirement.
    """
    schema = require_control_schema(control_schema)
    definition = next((table for table in DISPATCH_TABLES if table.name == name), None)
    if definition is None:
        raise ValueError("unknown composition dispatch table")
    digest, document = {
        "ch_dispatches": ("dispatch_sha256", "dispatch_document"),
        "ch_dispatch_terminals": ("terminal_sha256", "terminal_document"),
        "ch_dispatch_closures": ("evidence_sha256", "evidence_document"),
    }[name]
    dispatches = f"[{schema}].[composition_ch_dispatches]"
    terminals = f"[{schema}].[composition_ch_dispatch_terminals]"
    closures = f"[{schema}].[composition_ch_dispatch_closures]"
    if name == "ch_dispatches":
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE
            DATALENGTH(i.operation_key)<>71 OR DATALENGTH(i.claim_key)<>71
            OR DATALENGTH(i.query_id) NOT BETWEEN 1 AND 128
            OR NOT EXISTS (SELECT 1 FROM [{schema}].[composition_operations] o WITH (HOLDLOCK)
                WHERE o.operation_key=i.operation_key AND o.operation_family='execution' AND o.state='RUNNING')
            OR EXISTS (SELECT 1 FROM {closures} c WITH (HOLDLOCK) WHERE c.gate_id=i.gate_id))
            THROW 51000, 'DPONE_COMPOSITION_DISPATCH_CLOSED', 1;"""
    elif name == "ch_dispatch_terminals":
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE NOT EXISTS
            (SELECT 1 FROM {dispatches} d WITH (HOLDLOCK) WHERE d.dispatch_sha256=i.dispatch_sha256))
            THROW 51000, 'DPONE_COMPOSITION_DISPATCH_MISSING', 1;"""
    else:
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE NOT
            ((i.phase='CLOSING' AND DATALENGTH(i.phase)=7) OR
             (i.phase='CLOSED' AND DATALENGTH(i.phase)=6)))
            THROW 51000, 'DPONE_COMPOSITION_DISPATCH_PHASE', 1;
        IF EXISTS (SELECT 1 FROM inserted i WHERE i.phase='CLOSED' AND
            (NOT EXISTS (SELECT 1 FROM {closures} c WITH (HOLDLOCK)
                WHERE c.gate_id=i.gate_id AND c.phase='CLOSING') OR
             EXISTS (SELECT 1 FROM {dispatches} d WITH (HOLDLOCK)
                LEFT JOIN {terminals} t WITH (HOLDLOCK) ON t.dispatch_sha256=d.dispatch_sha256
                WHERE d.gate_id=i.gate_id AND t.dispatch_sha256 IS NULL)))
            THROW 51000, 'DPONE_COMPOSITION_DISPATCH_UNRESOLVED', 1;"""
    return f"""CREATE TRIGGER [{schema}].[composition_{name}_invariant]
ON [{schema}].[composition_{name}] AFTER INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51000, 'DPONE_COMPOSITION_DISPATCH_IMMUTABLE', 1;
    IF XACT_STATE()<>1
        THROW 51000, 'DPONE_COMPOSITION_DISPATCH_LOCK', 1;
    IF ISNULL(APPLOCK_MODE(N'public',
        N'{COMPOSITION_MSSQL_LEDGER_LOCK}',N'Transaction'),N'NoLock')<>N'Exclusive'
        THROW 51000, 'DPONE_COMPOSITION_DISPATCH_LOCK', 1;
    IF EXISTS (SELECT 1 FROM inserted WHERE DATALENGTH({document}) NOT BETWEEN 1 AND 8388608
        OR DATALENGTH({digest})<>71 OR {digest}<>'sha256:' +
        LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',{document}),2)))
        THROW 51000, 'DPONE_COMPOSITION_DISPATCH_ORIGINAL', 1;
    {policy}
END;"""


def render_clickhouse_dispatch_schema(control_schema: str = "dpone_control") -> str:
    """Render the additive journal; no enrollment, grants or writer are installed."""
    schema = require_control_schema(control_schema)
    statements = ["SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; BEGIN TRANSACTION;"]
    for definition in DISPATCH_TABLES:
        statements.append(render_composition_table(definition, schema))
        module = dispatch_trigger_sql(schema, definition.name).replace("'", "''")
        statements.append(f"EXEC(N'{module}');")
    return "\n".join((*statements, "COMMIT TRANSACTION;", ""))


def require_clickhouse_dispatch_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Reopen complete metadata and exact trigger bytes before journal reads."""
    schema = require_control_schema(control_schema)
    require_composition_catalog_visibility(cursor, schema)
    for table in DISPATCH_TABLES:
        inspect_composition_table(
            cursor,
            schema,
            table,
            {},
            {},
            CompositionTrigger(
                "composition_" + table.name + "_invariant",
                dispatch_trigger_sql(schema, table.name),
                ("DELETE", "INSERT", "UPDATE"),
            ),
        )
