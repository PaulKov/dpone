"""Administrator-only immutable SQL enrollment for the Linux supervisor.

Runtime only audits this additive table. Provisioning/boot replacement requires
external trusted enrollment and a new explicit digest; there is no adoption API.
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

SUPERVISOR_TABLE = CompositionTable(
    "ch_supervisor_enrollments",
    (
        CompositionColumn("enrollment_sha256", "varchar", 71),
        CompositionColumn("service_id", "uniqueidentifier", 16),
        CompositionColumn("database_uuid", "uniqueidentifier", 16),
        CompositionColumn("boot_id", "uniqueidentifier", 16),
        CompositionColumn("isolation_id", "uniqueidentifier", 16),
        CompositionColumn("enrollment_document", "varbinary", -1),
    ),
    (
        CompositionKey("pk_composition_ch_supervisor", ("enrollment_sha256",), True),
        CompositionKey("uq_composition_ch_supervisor_boot", ("service_id", "database_uuid", "boot_id")),
        CompositionKey("uq_composition_ch_supervisor_isolation", ("isolation_id",)),
    ),
)


def supervisor_trigger_sql(control_schema: str) -> str:
    schema = require_control_schema(control_schema)
    return f"""CREATE TRIGGER [{schema}].[composition_ch_supervisor_enrollments_invariant]
ON [{schema}].[composition_ch_supervisor_enrollments] AFTER INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted) THROW 51000, 'DPONE_CH_SUPERVISOR_IMMUTABLE', 1;
    IF XACT_STATE()<>1 OR ISNULL(APPLOCK_MODE(N'public',N'{COMPOSITION_MSSQL_LEDGER_LOCK}',N'Transaction'),N'NoLock')<>N'Exclusive'
        THROW 51000, 'DPONE_CH_SUPERVISOR_LOCK', 1;
    IF EXISTS (SELECT 1 FROM inserted WHERE DATALENGTH(enrollment_document) NOT BETWEEN 1 AND 65536
        OR DATALENGTH(enrollment_sha256)<>71 OR enrollment_sha256<>'sha256:' +
        LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',enrollment_document),2)))
        THROW 51000, 'DPONE_CH_SUPERVISOR_ORIGINAL', 1;
END;"""


def render_clickhouse_supervisor_schema(control_schema: str = "dpone_control") -> str:
    schema = require_control_schema(control_schema)
    module = supervisor_trigger_sql(schema).replace("'", "''")
    return "\n".join(
        (
            "SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; BEGIN TRANSACTION;",
            render_composition_table(SUPERVISOR_TABLE, schema),
            f"EXEC(N'{module}');",
            "COMMIT TRANSACTION;",
            "",
        )
    )


def require_clickhouse_supervisor_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    schema = require_control_schema(control_schema)
    require_composition_catalog_visibility(cursor, schema)
    inspect_composition_table(
        cursor,
        schema,
        SUPERVISOR_TABLE,
        {},
        {},
        CompositionTrigger(
            "composition_ch_supervisor_enrollments_invariant",
            supervisor_trigger_sql(schema),
            ("DELETE", "INSERT", "UPDATE"),
        ),
    )
