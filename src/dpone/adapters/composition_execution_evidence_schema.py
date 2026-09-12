"""Externally installed immutable originals for composed execution proofs.

Native dbt outcomes and multi-principal closure producers retain full evidence
here. No runtime provisioning, receipt repair or permission grants are provided.
"""

from dpone.adapters.composition_mssql_catalog import inspect_composition_table, require_composition_catalog_visibility
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionColumn,
    CompositionForeignKey,
    CompositionKey,
    CompositionTable,
    CompositionTrigger,
)
from dpone.adapters.composition_mssql_layout import COMPOSITION_MSSQL_LEDGER_LOCK, require_control_schema
from dpone.adapters.composition_mssql_schema import render_composition_table
from dpone.ports.sql_connection import SqlControlCursor

EXECUTION_EVIDENCE = CompositionTable(
    "execution_evidence",
    (
        CompositionColumn("operation_key", "varchar", 71),
        CompositionColumn("kind", "varchar", 12),
        CompositionColumn("evidence_sha256", "varchar", 71),
        CompositionColumn("evidence_document", "varbinary", -1),
    ),
    (CompositionKey("pk_composition_execution_evidence", ("operation_key", "kind", "evidence_sha256"), True),),
    (
        CompositionForeignKey(
            "fk_composition_execution_evidence_operation", ("operation_key",), "operations", ("operation_key",)
        ),
    ),
)


def execution_evidence_trigger_sql(control_schema: str) -> str:
    schema = require_control_schema(control_schema)
    return f"""CREATE TRIGGER [{schema}].[composition_execution_evidence_invariant]
ON [{schema}].[composition_execution_evidence] AFTER INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted) OR (SELECT COUNT_BIG(*) FROM inserted)<>1
        THROW 51000, 'DPONE_EXECUTION_EVIDENCE_IMMUTABLE', 1;
    IF XACT_STATE()<>1
        THROW 51000, 'DPONE_EXECUTION_EVIDENCE_LOCK', 1;
    IF ISNULL(APPLOCK_MODE(N'public',N'{COMPOSITION_MSSQL_LEDGER_LOCK}',N'Transaction'),N'NoLock')<>N'Exclusive'
        THROW 51000, 'DPONE_EXECUTION_EVIDENCE_LOCK', 1;
    IF EXISTS (SELECT 1 FROM inserted i WHERE DATALENGTH(i.operation_key)<>71
        OR DATALENGTH(i.evidence_document) NOT BETWEEN 1 AND 8388608
        OR DATALENGTH(i.evidence_sha256)<>71 OR i.evidence_sha256<>'sha256:'+
        LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',i.evidence_document),2))
        OR NOT ((i.kind='OUTCOME' AND DATALENGTH(i.kind)=7)
            OR (i.kind='CLOSED_GATES' AND DATALENGTH(i.kind)=12)
            OR (i.kind='QUIESCENCE' AND DATALENGTH(i.kind)=10))
        OR NOT EXISTS (SELECT 1 FROM [{schema}].[composition_operations] o WITH (HOLDLOCK)
            WHERE o.operation_key=i.operation_key AND o.operation_family='execution'
            AND o.state IN ('RUNNING','COMMIT_UNKNOWN')))
        THROW 51000, 'DPONE_EXECUTION_EVIDENCE_ORIGINAL', 1;
END;"""


def render_execution_evidence_schema(control_schema: str = "dpone_control") -> str:
    """Render additive administrator-only installation in one transaction."""
    schema = require_control_schema(control_schema)
    module = execution_evidence_trigger_sql(schema).replace("'", "''")
    return "\n".join(
        (
            "SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; BEGIN TRANSACTION;",
            render_composition_table(EXECUTION_EVIDENCE, schema),
            f"EXEC(N'{module}');",
            "COMMIT TRANSACTION;",
            "",
        )
    )


def require_execution_evidence_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Require complete exact catalog; absence is never repaired at runtime."""
    schema = require_control_schema(control_schema)
    require_composition_catalog_visibility(cursor, schema)
    inspect_composition_table(
        cursor,
        schema,
        EXECUTION_EVIDENCE,
        {},
        {},
        CompositionTrigger(
            "composition_execution_evidence_invariant",
            execution_evidence_trigger_sql(schema),
            ("DELETE", "INSERT", "UPDATE"),
        ),
    )
