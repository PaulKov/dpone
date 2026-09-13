"""Administrator-installed immutable capture journal; runtime never provisions."""

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
from dpone.contracts.composition_snapshot_capture import MAX_CAPTURE_METADATA_BYTES
from dpone.ports.sql_connection import SqlControlCursor

CAPTURE_TABLE = CompositionTable(
    "snapshot_captures",
    (
        CompositionColumn("operation_key", "varchar", 71),
        CompositionColumn("phase", "varchar", 17),
        CompositionColumn("subject_sha256", "varchar", 71),
        CompositionColumn("generation_uuid", "uniqueidentifier", 16),
        CompositionColumn("document_sha256", "varchar", 71),
        CompositionColumn("document", "varbinary", -1),
    ),
    (
        CompositionKey("pk_composition_snapshot_captures", ("operation_key", "phase"), True),
        CompositionKey("uq_composition_snapshot_generation", ("generation_uuid", "phase")),
    ),
    (CompositionForeignKey("fk_composition_snapshot_operation", ("operation_key",), "operations", ("operation_key",)),),
)


def capture_trigger_sql(control_schema: str) -> str:
    schema = require_control_schema(control_schema)
    table = f"[{schema}].[composition_snapshot_captures]"
    return f"""CREATE TRIGGER [{schema}].[composition_snapshot_captures_invariant]
ON {table} AFTER INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted) OR (SELECT COUNT_BIG(*) FROM inserted)<>1
        THROW 51000, 'DPONE_SNAPSHOT_IMMUTABLE', 1;
    IF XACT_STATE()<>1 OR ISNULL(APPLOCK_MODE(N'public',N'{COMPOSITION_MSSQL_LEDGER_LOCK}',N'Transaction'),N'NoLock')<>N'Exclusive'
        THROW 51000, 'DPONE_SNAPSHOT_LOCK', 1;
    IF EXISTS (SELECT 1 FROM inserted WHERE DATALENGTH(operation_key)<>71 OR DATALENGTH(subject_sha256)<>71
        OR DATALENGTH(document_sha256)<>71 OR DATALENGTH(document) NOT BETWEEN 1 AND {MAX_CAPTURE_METADATA_BYTES}
        OR document_sha256<>'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',document),2))
        OR phase NOT IN ('CLAIMED','CAPTURED','GENERATION_SEALED') OR DATALENGTH(phase)<>LEN(phase))
        THROW 51000, 'DPONE_SNAPSHOT_ORIGINAL', 1;
    IF EXISTS (SELECT 1 FROM inserted i WHERE NOT EXISTS (
        SELECT 1 FROM [{schema}].[composition_operations] o WITH (HOLDLOCK)
        JOIN [{schema}].[composition_owners] p WITH (HOLDLOCK) ON p.owner_key=o.owner_key
        WHERE o.operation_key=i.operation_key AND o.operation_family='execution' AND o.state='RUNNING'
        AND p.owner_kind='execution' AND p.state='ACTIVE') OR EXISTS (
        SELECT 1 FROM [{schema}].[composition_operation_domains] od WITH (HOLDLOCK)
        JOIN [{schema}].[composition_domains] d WITH (HOLDLOCK) ON d.guard_id=od.guard_id
        WHERE od.operation_key=i.operation_key AND (d.owner_key<>od.owner_key OR d.owner_key IS NULL OR d.fencing_epoch<>od.fencing_epoch)))
        THROW 51000, 'DPONE_SNAPSHOT_AUTHORITY', 1;
    IF EXISTS (SELECT 1 FROM inserted i WHERE
        (i.phase='CLAIMED' AND EXISTS (SELECT 1 FROM {table} e WITH (HOLDLOCK) WHERE e.operation_key=i.operation_key AND e.phase<>'CLAIMED'))
        OR (i.phase<>'CLAIMED' AND NOT EXISTS (SELECT 1 FROM {table} e WITH (HOLDLOCK)
            WHERE e.operation_key=i.operation_key AND e.phase='CLAIMED' AND e.subject_sha256=i.subject_sha256 AND e.generation_uuid=i.generation_uuid))
        OR (i.phase='GENERATION_SEALED' AND NOT EXISTS (SELECT 1 FROM {table} e WITH (HOLDLOCK)
            WHERE e.operation_key=i.operation_key AND e.phase='CAPTURED')))
        THROW 51000, 'DPONE_SNAPSHOT_ORDER', 1;
    IF EXISTS (SELECT 1 FROM inserted i WHERE i.phase='GENERATION_SEALED' AND
        (NOT EXISTS (SELECT 1 FROM [{schema}].[composition_proofs] p WITH (HOLDLOCK)
            WHERE p.operation_key=i.operation_key AND p.operation_family='execution' AND p.kind='CLOSED_GATES'
            AND p.proof_sha256=JSON_VALUE(CONVERT(varchar(max),i.document),'$.closed_gates_sha256'))
         OR NOT EXISTS (SELECT 1 FROM [{schema}].[composition_proofs] p WITH (HOLDLOCK)
            WHERE p.operation_key=i.operation_key AND p.operation_family='execution' AND p.kind='QUIESCENCE'
            AND p.proof_sha256=JSON_VALUE(CONVERT(varchar(max),i.document),'$.quiescence_sha256'))))
        THROW 51000, 'DPONE_SNAPSHOT_CLOSURE', 1;
END;"""


def render_snapshot_capture_schema(control_schema: str = "dpone_control") -> str:
    schema = require_control_schema(control_schema)
    trigger = capture_trigger_sql(schema).replace("'", "''")
    return "\n".join(
        (
            "SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; BEGIN TRANSACTION;",
            render_composition_table(CAPTURE_TABLE, schema),
            f"EXEC(N'{trigger}');",
            "COMMIT TRANSACTION;",
            "",
        )
    )


def require_snapshot_capture_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    schema = require_control_schema(control_schema)
    require_composition_catalog_visibility(cursor, schema)
    inspect_composition_table(
        cursor,
        schema,
        CAPTURE_TABLE,
        {},
        {},
        CompositionTrigger(
            "composition_snapshot_captures_invariant", capture_trigger_sql(schema), ("DELETE", "INSERT", "UPDATE")
        ),
    )
