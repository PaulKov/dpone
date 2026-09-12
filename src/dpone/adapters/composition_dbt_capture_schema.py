"""Administrator-installed append-only dbt supervisor journal and exact audit.

No runtime installation or grants are provided. The database controller owns
these tables; issued worker logins must have no access to this control schema.
"""

from dpone.adapters.composition_mssql_catalog import inspect_composition_table, require_composition_catalog_visibility
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionColumn,
    CompositionForeignKey,
    CompositionKey,
    CompositionTable,
    CompositionTrigger,
)
from dpone.adapters.composition_mssql_schema import (
    COMPOSITION_MSSQL_LEDGER_LOCK,
    render_composition_table,
    require_control_schema,
)
from dpone.contracts.composition_dbt_capture_codec import MAX_CAPTURE_DOCUMENT_BYTES
from dpone.ports.sql_connection import SqlControlCursor

DBT_CAPTURE_TABLES = (
    CompositionTable(
        "dbt_registrations",
        (
            CompositionColumn("operation_key", "varchar", 71),
            CompositionColumn("intent_sha256", "varchar", 71),
            CompositionColumn("login_sid", "varbinary", 16),
            CompositionColumn("document_sha256", "varchar", 71),
            CompositionColumn("document", "varbinary", -1),
        ),
        (CompositionKey("pk_composition_dbt_registrations", ("operation_key",), True),),
        (
            CompositionForeignKey(
                "fk_composition_dbt_registration_operation", ("operation_key",), "operations", ("operation_key",)
            ),
        ),
    ),
    CompositionTable(
        "dbt_events",
        (
            CompositionColumn("operation_key", "varchar", 71),
            CompositionColumn("phase", "varchar", 16),
            CompositionColumn("document_sha256", "varchar", 71),
            CompositionColumn("document", "varbinary", -1),
        ),
        (CompositionKey("pk_composition_dbt_events", ("operation_key", "phase"), True),),
        (
            CompositionForeignKey(
                "fk_composition_dbt_event_registration", ("operation_key",), "dbt_registrations", ("operation_key",)
            ),
        ),
    ),
)


def dbt_capture_trigger_sql(control_schema: str, name: str) -> str:
    """Render immutable originals and strict forward phase dependencies."""
    schema = require_control_schema(control_schema)
    if name not in {table.name for table in DBT_CAPTURE_TABLES}:
        raise ValueError("unknown dbt capture table")
    events = f"[{schema}].[composition_dbt_events]"
    running = f"""EXISTS (SELECT 1 FROM [{schema}].[composition_operations] o WITH (HOLDLOCK)
        JOIN [{schema}].[composition_owners] p WITH (HOLDLOCK) ON p.owner_key=o.owner_key
        JOIN [{schema}].[composition_login_gates] g WITH (HOLDLOCK) ON g.operation_key=o.operation_key
        WHERE o.operation_key=i.operation_key AND o.operation_family='execution' AND o.state='RUNNING'
        AND p.owner_kind='execution' AND p.state='ACTIVE' AND g.operation_family='execution' AND g.gate_state='READY')"""
    if name == "dbt_registrations":
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE DATALENGTH(i.intent_sha256)<>71
            OR DATALENGTH(i.login_sid)<>16 OR NOT {running}
            OR NOT EXISTS (SELECT 1 FROM [{schema}].[composition_login_gates] g WITH (HOLDLOCK)
                WHERE g.operation_key=i.operation_key AND g.login_sid=i.login_sid))
            THROW 51000, 'DPONE_DBT_REGISTRATION_AUTHORITY', 1;"""
    else:
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE NOT
            ((i.phase='DISPATCH' AND DATALENGTH(i.phase)=8) OR (i.phase='EXIT' AND DATALENGTH(i.phase)=4)
             OR (i.phase='CAPTURE' AND DATALENGTH(i.phase)=7) OR (i.phase='UNDISPATCHED' AND DATALENGTH(i.phase)=12)))
            THROW 51000, 'DPONE_DBT_CAPTURE_PHASE', 1;
        IF EXISTS (SELECT 1 FROM inserted i WHERE
            (i.phase='DISPATCH' AND (NOT {running} OR EXISTS
                (SELECT 1 FROM {events} e WITH (HOLDLOCK) WHERE e.operation_key=i.operation_key AND e.phase='UNDISPATCHED')))
            OR (i.phase='EXIT' AND NOT EXISTS
                (SELECT 1 FROM {events} e WITH (HOLDLOCK) WHERE e.operation_key=i.operation_key AND e.phase='DISPATCH'))
            OR (i.phase='CAPTURE' AND NOT EXISTS
                (SELECT 1 FROM {events} e WITH (HOLDLOCK) WHERE e.operation_key=i.operation_key AND e.phase='EXIT'))
            OR (i.phase='UNDISPATCHED' AND (EXISTS
                (SELECT 1 FROM {events} e WITH (HOLDLOCK) WHERE e.operation_key=i.operation_key AND e.phase<>'UNDISPATCHED')
                OR NOT EXISTS (SELECT 1 FROM [{schema}].[composition_login_gates] g WITH (HOLDLOCK)
                    WHERE g.operation_key=i.operation_key AND g.gate_state='CLOSED'))))
            THROW 51000, 'DPONE_DBT_CAPTURE_ORDER', 1;"""
    return f"""CREATE TRIGGER [{schema}].[composition_{name}_invariant]
ON [{schema}].[composition_{name}] AFTER INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted) OR (SELECT COUNT_BIG(*) FROM inserted)<>1
        THROW 51000, 'DPONE_DBT_CAPTURE_IMMUTABLE', 1;
    IF XACT_STATE()<>1 OR ISNULL(APPLOCK_MODE(N'public',N'{COMPOSITION_MSSQL_LEDGER_LOCK}',N'Transaction'),N'NoLock')<>N'Exclusive'
        THROW 51000, 'DPONE_DBT_CAPTURE_LOCK', 1;
    IF EXISTS (SELECT 1 FROM inserted WHERE DATALENGTH(operation_key)<>71
        OR DATALENGTH(document) NOT BETWEEN 1 AND {MAX_CAPTURE_DOCUMENT_BYTES}
        OR DATALENGTH(document_sha256)<>71 OR document_sha256<>'sha256:'+
        LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',document),2)))
        THROW 51000, 'DPONE_DBT_CAPTURE_ORIGINAL', 1;
    {policy}
END;"""


def render_dbt_capture_schema(control_schema: str = "dpone_control") -> str:
    """Render external-only provisioning; does not enroll or grant a writer."""
    schema = require_control_schema(control_schema)
    statements = ["SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; BEGIN TRANSACTION;"]
    for table in DBT_CAPTURE_TABLES:
        statements.append(render_composition_table(table, schema))
        module = dbt_capture_trigger_sql(schema, table.name).replace("'", "''")
        statements.append(f"EXEC(N'{module}');")
    return "\n".join((*statements, "COMMIT TRANSACTION;", ""))


def require_dbt_capture_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Audit complete metadata, trusted foreign keys and exact trigger originals."""
    schema = require_control_schema(control_schema)
    require_composition_catalog_visibility(cursor, schema)
    for table in DBT_CAPTURE_TABLES:
        inspect_composition_table(
            cursor,
            schema,
            table,
            {},
            {},
            CompositionTrigger(
                "composition_" + table.name + "_invariant",
                dbt_capture_trigger_sql(schema, table.name),
                ("DELETE", "INSERT", "UPDATE"),
            ),
        )
