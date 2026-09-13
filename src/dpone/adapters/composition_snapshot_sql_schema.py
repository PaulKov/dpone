"""Administrator-installed, append-only snapshot intents and transition originals.

Install alongside the protected composition ledger. This producer installs no
worker grants, enrollment or execution authority; runtime verifies exact catalog
and trigger definitions before using any journal original.
"""

from dpone.adapters.composition_mssql_catalog import (
    inspect_composition_table,
    require_composition_catalog_visibility,
)
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

SNAPSHOT_TABLES = (
    CompositionTable(
        "snapshot_intents",
        (
            CompositionColumn("intent_sha256", "varchar", 71),
            CompositionColumn("operation_key", "varchar", 71),
            CompositionColumn("write_subject_sha256", "varchar", 71),
            CompositionColumn("exchange_query_id", "varchar", 128),
            CompositionColumn("intent_document", "varbinary", -1),
        ),
        (
            CompositionKey("pk_composition_snapshot_intents", ("intent_sha256",), True),
            CompositionKey("uq_composition_snapshot_subject", ("operation_key", "write_subject_sha256")),
            CompositionKey("uq_composition_snapshot_query", ("exchange_query_id",)),
        ),
    ),
    CompositionTable(
        "snapshot_history",
        (
            CompositionColumn("intent_sha256", "varchar", 71),
            CompositionColumn("revision", "bigint", 8),
            CompositionColumn("state", "varchar", 16),
            CompositionColumn("record_sha256", "varchar", 71),
            CompositionColumn("record_document", "varbinary", -1),
        ),
        (CompositionKey("pk_composition_snapshot_history", ("intent_sha256", "revision"), True),),
    ),
)


def snapshot_trigger_sql(control_schema: str, name: str) -> str:
    """Reject destructive changes, unlocked writes, invalid hashes and history gaps."""
    schema = require_control_schema(control_schema)
    if name not in {table.name for table in SNAPSHOT_TABLES}:
        raise ValueError("unknown snapshot table")
    if name == "snapshot_intents":
        digest, document = "intent_sha256", "intent_document"
        policy = f"""IF EXISTS (SELECT 1 FROM inserted i WHERE
            DATALENGTH(i.operation_key)<>71 OR DATALENGTH(i.write_subject_sha256)<>71
            OR i.exchange_query_id<>'dpone-snapshot-'+SUBSTRING(i.intent_sha256,8,64)
            OR DATALENGTH(i.exchange_query_id)<>79
            OR NOT EXISTS (SELECT 1 FROM [{schema}].[composition_operations] o WITH (HOLDLOCK)
                WHERE o.operation_key=i.operation_key AND o.operation_family='execution' AND o.state='RUNNING'))
            THROW 51000, 'DPONE_SNAPSHOT_SUBJECT', 1;"""
    else:
        digest, document = "record_sha256", "record_document"
        policy = f"""IF (SELECT COUNT_BIG(*) FROM inserted)<>1
            THROW 51000, 'DPONE_SNAPSHOT_SINGLE_TRANSITION', 1;
        IF EXISTS (SELECT 1 FROM inserted i WHERE
            NOT EXISTS (SELECT 1 FROM [{schema}].[composition_snapshot_intents] p WITH (HOLDLOCK)
                WHERE p.intent_sha256=i.intent_sha256)
            OR NOT ((i.revision=1 AND i.state='PREPARED' AND DATALENGTH(i.state)=8) OR
                (i.revision>1 AND EXISTS
                (SELECT 1 FROM [{schema}].[composition_snapshot_history] h WITH (HOLDLOCK)
                 WHERE h.intent_sha256=i.intent_sha256 AND h.revision=i.revision-1 AND
                    ((h.state='PREPARED' AND i.state IN ('EXCHANGE_INTENT','NOT_PUBLISHED')) OR
                     (h.state IN ('EXCHANGE_INTENT','COMMIT_UNKNOWN')
                        AND i.state IN ('PUBLISHED','NOT_PUBLISHED','COMMIT_UNKNOWN')))))))
            THROW 51000, 'DPONE_SNAPSHOT_TRANSITION', 1;"""
    return f"""CREATE TRIGGER [{schema}].[composition_{name}_invariant]
ON [{schema}].[composition_{name}] AFTER INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51000, 'DPONE_SNAPSHOT_IMMUTABLE', 1;
    IF XACT_STATE()<>1 OR ISNULL(APPLOCK_MODE(N'public',
        N'{COMPOSITION_MSSQL_LEDGER_LOCK}',N'Transaction'),N'NoLock')<>N'Exclusive'
        THROW 51000, 'DPONE_SNAPSHOT_LOCK', 1;
    IF EXISTS (SELECT 1 FROM inserted WHERE DATALENGTH({document}) NOT BETWEEN 1 AND 8388608
        OR DATALENGTH({digest})<>71 OR {digest}<>'sha256:'+
        LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',{document}),2)))
        THROW 51000, 'DPONE_SNAPSHOT_ORIGINAL', 1;
    {policy}
END;"""


def render_snapshot_publication_schema(control_schema: str = "dpone_control") -> str:
    """Render additive protected tables; install once using administrator authority."""
    schema = require_control_schema(control_schema)
    statements = ["SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; BEGIN TRANSACTION;"]
    for table in SNAPSHOT_TABLES:
        statements.append(render_composition_table(table, schema))
        trigger = snapshot_trigger_sql(schema, table.name).replace("'", "''")
        statements.append(f"EXEC(N'{trigger}');")
    return "\n".join((*statements, "COMMIT TRANSACTION;", ""))


def require_snapshot_publication_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Independently verify keys, types and exact immutable trigger definitions."""
    schema = require_control_schema(control_schema)
    require_composition_catalog_visibility(cursor, schema)
    for table in SNAPSHOT_TABLES:
        inspect_composition_table(
            cursor,
            schema,
            table,
            {},
            {},
            CompositionTrigger(
                "composition_" + table.name + "_invariant",
                snapshot_trigger_sql(schema, table.name),
                ("DELETE", "INSERT", "UPDATE"),
            ),
        )
