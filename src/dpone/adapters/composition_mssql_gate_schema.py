"""External provisioning SQL for the dedicated v3 SQL login admission barrier.

An administrator executes these batches outside runtime, enrolls only fresh
exclusive databases, then installs the gate reader and minimum grants. Runtime
compares exact installed trigger bytes. This renderer does not certify the
engine-specific LOGON/DMV visibility race; the isolated live test is mandatory.
"""

from __future__ import annotations

from hashlib import sha256

from dpone.adapters.composition_mssql_schema import require_control_schema

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
    """SID/name/attempt never change; closing is irreversible, including DELETE."""
    schema = require_control_schema(control_schema)
    return f"""CREATE TRIGGER [{schema}].[composition_login_gate_monotonic]
ON [{schema}].[composition_login_gates] AFTER UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (
        SELECT 1 FROM deleted d LEFT JOIN inserted i ON i.attempt_sha256 = d.attempt_sha256
        WHERE i.attempt_sha256 IS NULL OR i.login_sid <> d.login_sid OR i.login_name <> d.login_name
           OR DATALENGTH(i.login_name) <> DATALENGTH(d.login_name)
           OR (i.gate_state <> d.gate_state AND NOT (
               (d.gate_state = 'JOURNALED' AND i.gate_state IN ('READY', 'CLOSING')) OR
               (d.gate_state = 'READY' AND i.gate_state = 'CLOSING') OR
               (d.gate_state = 'CLOSING' AND i.gate_state = 'CLOSED')))
           OR (d.disabled_evidence_sha256 IS NOT NULL AND
               (i.disabled_evidence_sha256 IS NULL OR i.disabled_evidence_sha256 <> d.disabled_evidence_sha256))
    ) THROW 51000, 'DPONE_COMPOSITION_GATE_IMMUTABLE', 1;
END;"""


def module_sha256(definition: str) -> bytes:
    """Match HASHBYTES over SQL Server's NVARCHAR module definition bytes."""
    return sha256(definition.encode("utf-16le")).digest()


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
    return f"""USE [{database}];
GO
CREATE TABLE [{schema}].[composition_login_gates] (
    attempt_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY
        REFERENCES [{schema}].[composition_attempts](attempt_sha256),
    login_sid binary(16) NOT NULL UNIQUE,
    login_name nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE,
    gate_state varchar(16) COLLATE Latin1_General_100_BIN2 NOT NULL
        CHECK (gate_state IN ('JOURNALED', 'READY', 'CLOSING', 'CLOSED')),
    disabled_evidence_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NULL,
    CHECK (gate_state <> 'CLOSED' OR disabled_evidence_sha256 IS NOT NULL)
);
CREATE TABLE [{schema}].[composition_mssql_enrollments] (
    guard_id varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY
        REFERENCES [{schema}].[composition_domains](guard_id),
    database_name nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE,
    database_id int NOT NULL UNIQUE CHECK (database_id > 4),
    database_guid uniqueidentifier NOT NULL,
    database_create_token nvarchar(33) COLLATE Latin1_General_100_BIN2 NOT NULL,
    writer_role nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL
);
CREATE TABLE [{schema}].[composition_mssql_managed_schemas] (
    guard_id varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL
        REFERENCES [{schema}].[composition_mssql_enrollments](guard_id),
    schema_name nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    PRIMARY KEY (guard_id, schema_name)
);
CREATE TABLE [{schema}].[composition_mssql_gate_evidence] (
    evidence_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
    attempt_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL
        REFERENCES [{schema}].[composition_attempts](attempt_sha256),
    evidence_document varbinary(max) NOT NULL CHECK (DATALENGTH(evidence_document) BETWEEN 1 AND 8388608)
);
GO
{monotonic_trigger_sql(schema)}
GO
CREATE TRIGGER [{schema}].[composition_mssql_enrollment_immutable]
ON [{schema}].[composition_mssql_enrollments] AFTER UPDATE, DELETE AS
BEGIN THROW 51000, 'DPONE_COMPOSITION_ENROLLMENT_IMMUTABLE', 1; END;
GO
CREATE TRIGGER [{schema}].[composition_mssql_schemas_immutable]
ON [{schema}].[composition_mssql_managed_schemas] AFTER UPDATE, DELETE AS
BEGIN THROW 51000, 'DPONE_COMPOSITION_ENROLLMENT_IMMUTABLE', 1; END;
GO
CREATE TRIGGER [{schema}].[composition_mssql_gate_evidence_immutable]
ON [{schema}].[composition_mssql_gate_evidence] AFTER UPDATE, DELETE AS
BEGIN THROW 51000, 'DPONE_COMPOSITION_EVIDENCE_IMMUTABLE', 1; END;
GO
{login_trigger_sql(database, schema)}
GO
"""
