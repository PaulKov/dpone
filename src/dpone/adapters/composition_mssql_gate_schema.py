"""External provisioning SQL for the dedicated v3 SQL login admission barrier.

An administrator executes these batches outside runtime, enrolls only fresh
exclusive databases, then installs the gate reader and minimum grants. Runtime
compares exact installed trigger bytes. This renderer does not certify the
engine-specific LOGON/DMV visibility race; the isolated live test is mandatory.
"""

from __future__ import annotations

from dpone.adapters.composition_mssql_catalog_types import CompositionTrigger
from dpone.adapters.composition_mssql_catalog_types import module_sha256 as module_sha256
from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
from dpone.adapters.composition_mssql_schema import render_composition_table, require_control_schema

GATE_TRIGGER = "dpone_composition_login_gate"
GATE_READER = "dpone_gate_reader"


def login_trigger_sql(control_database: str, control_schema: str) -> str:
    """Shared row locks belong to the synchronous implicit LOGON transaction."""
    database = require_control_schema(control_database)
    schema = require_control_schema(control_schema)
    return f"""CREATE TRIGGER [{GATE_TRIGGER}]
ON ALL SERVER WITH EXECUTE AS N'{GATE_READER}'
FOR LOGON AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sid varbinary(85), @canonical_name sysname, @principal_type char(1);
    BEGIN TRY
        SELECT @sid = original_security_id FROM sys.dm_exec_sessions WHERE session_id = @@SPID;
        IF @sid IS NULL THROW 51000, 'DPONE_COMPOSITION_LOGON_IDENTITY_UNAVAILABLE', 1;
        SELECT @canonical_name = name, @principal_type = type FROM sys.server_principals WHERE sid = @sid;
        IF @canonical_name IS NULL THROW 51000, 'DPONE_COMPOSITION_LOGON_IDENTITY_UNAVAILABLE', 1;
        IF LEFT(@canonical_name, 9) COLLATE Latin1_General_100_BIN2 <> N'dpone_v3_' RETURN;
        IF @principal_type <> 'S' THROW 51000, 'DPONE_COMPOSITION_LOGON_DENIED', 1;
        DECLARE @state varchar(16), @name sysname;
        SELECT @state = gate_state, @name = login_name
        FROM [{database}].[{schema}].[composition_login_gates] WITH (HOLDLOCK, ROWLOCK, NOWAIT)
        WHERE login_sid = @sid;
        IF @state IS NULL OR @state <> 'READY' OR @name COLLATE Latin1_General_100_BIN2 <>
           @canonical_name COLLATE Latin1_General_100_BIN2
            THROW 51000, 'DPONE_COMPOSITION_LOGON_DENIED', 1;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        RETURN;
    END CATCH;
END;"""


def monotonic_trigger_sql(control_schema: str) -> str:
    """SID/name/operation never change; closing is irreversible, including DELETE."""
    schema = require_control_schema(control_schema)
    return f"""CREATE TRIGGER [{schema}].[composition_login_gate_monotonic]
ON [{schema}].[composition_login_gates] AFTER UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (
        SELECT 1 FROM deleted d LEFT JOIN inserted i ON i.operation_key = d.operation_key
        WHERE i.operation_key IS NULL OR i.login_sid <> d.login_sid OR i.login_name <> d.login_name
           OR DATALENGTH(i.login_name) <> DATALENGTH(d.login_name)
           OR (i.gate_state <> d.gate_state AND NOT (
               (d.gate_state = 'JOURNALED' AND i.gate_state IN ('READY', 'CLOSING')) OR
               (d.gate_state = 'READY' AND i.gate_state = 'CLOSING') OR
               (d.gate_state = 'CLOSING' AND i.gate_state = 'CLOSED')))
           OR (d.disabled_evidence_sha256 IS NOT NULL AND
               (i.disabled_evidence_sha256 IS NULL OR i.disabled_evidence_sha256 <> d.disabled_evidence_sha256))
    ) THROW 51000, 'DPONE_COMPOSITION_GATE_IMMUTABLE', 1;
END;"""


def render_composition_mssql_login_gate(*, control_database: str, control_schema: str = "dpone_control") -> str:
    """Render additive tables and trigger batches, without logins or enrollment.

    Provision the disabled SQL gate-reader login and its control-database user
    first. Grant SELECT only on login_gates, VIEW SERVER STATE, VIEW ANY DEFINITION
    and, on SQL Server 2022+, VIEW SERVER PERFORMANCE STATE. Keep its credentials
    outside workers. The controller needs catalog/DMV visibility and login/user
    administration. Only that trusted controller can mutate these protected rows.
    All target databases and bounded roles/schemas are separately preprovisioned.
    """
    database = require_control_schema(control_database)
    schema = require_control_schema(control_schema)
    batches = [f"USE [{database}];", "SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;"]
    batches.extend(render_composition_table(table, schema) for table in COMPOSITION_GATE_TABLES)
    batches.extend(gate_table_trigger(schema, table.name).definition for table in COMPOSITION_GATE_TABLES)
    batches.append(login_trigger_sql(database, schema))
    return "\nGO\n".join((*batches, ""))


def gate_table_trigger(control_schema: str, table: str) -> CompositionTrigger:
    """Keep immutable modules and UPDATE/DELETE event sets exact after migration."""
    schema = require_control_schema(control_schema)
    if table == "login_gates":
        return CompositionTrigger(
            "composition_login_gate_monotonic", monotonic_trigger_sql(schema), ("DELETE", "UPDATE")
        )
    module, reason = {
        "mssql_enrollments": ("composition_mssql_enrollment_immutable", "ENROLLMENT"),
        "mssql_managed_schemas": ("composition_mssql_schemas_immutable", "ENROLLMENT"),
        "mssql_gate_evidence": ("composition_mssql_gate_evidence_immutable", "EVIDENCE"),
    }[table]
    definition = f"""CREATE TRIGGER [{schema}].[{module}]
ON [{schema}].[composition_{table}] AFTER UPDATE, DELETE AS
BEGIN THROW 51000, 'DPONE_COMPOSITION_{reason}_IMMUTABLE', 1; END;"""
    return CompositionTrigger(module, definition, ("DELETE", "UPDATE"))
