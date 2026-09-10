"""Bounded SQL login DDL and actual protected enrollment/privilege observations.

Only the original invocation holds the password. It is bound to the driver as an
RPC parameter and quoted inside SQL Server; neither login DDL nor exceptions
interpolate the secret in Python. Connections must disable parameter/SQL tracing.
"""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass, field

from dpone.adapters.composition_mssql_gate_schema import (
    GATE_READER,
    GATE_TRIGGER,
    login_trigger_sql,
    module_sha256,
    monotonic_trigger_sql,
)
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.dbapi_lifecycle import row
from dpone.contracts.composition_control import CompositionAdmissionError, CompositionAttemptIdentity


@dataclass(frozen=True, slots=True)
class MssqlIssuedCredentials:
    """One-time memory-only connection material; never serialize this object."""

    login_name: str
    login_sid: bytes
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        if (
            re.fullmatch(r"dpone_v3_[0-9a-f]{64}", self.login_name) is None
            or type(self.login_sid) is not bytes
            or len(self.login_sid) != 16
            or type(self.password) is not str
            or not 1 <= len(self.password) <= 128
        ):
            raise CompositionAdmissionError("issued_credentials")


def new_credentials(attempt: CompositionAttemptIdentity) -> MssqlIssuedCredentials:
    """Generate 256 random password bits plus guaranteed complexity in memory."""
    return MssqlIssuedCredentials(
        "dpone_v3_" + attempt.attempt_sha256[7:], secrets.token_bytes(16), "Dp1!" + secrets.token_urlsafe(32)
    )


@dataclass(frozen=True, slots=True)
class MssqlEnrollment:
    """Platform-pinned physical database and its exact preprovisioned role."""

    guard_id: str
    database: str
    database_id: int
    database_guid: str
    create_token: str
    writer_role: str
    schemas: tuple[str, ...]


def require_gate_policy(ledger: CompositionMssqlLedger, database: str) -> None:
    """Inspect actual module bytes, reader identity and complete DMV privileges."""
    cursor = ledger.cursor
    cursor.execute(
        "SELECT DB_NAME(), t.is_disabled, HASHBYTES('SHA2_256', CONVERT(varbinary(max), m.definition)), "
        "p.name, p.type, p.is_disabled, "
        "(SELECT COUNT(*) FROM sys.server_trigger_events e JOIN sys.server_triggers x ON x.object_id=e.object_id "
        "WHERE e.type_desc='LOGON' AND x.is_disabled=0), "
        "HAS_PERMS_BY_NAME(NULL, 'SERVER', 'VIEW SERVER STATE'), "
        "HAS_PERMS_BY_NAME(NULL, 'SERVER', 'VIEW ANY DEFINITION'), "
        "CASE WHEN CONVERT(int, SERVERPROPERTY('ProductMajorVersion')) < 16 THEN 1 "
        "ELSE HAS_PERMS_BY_NAME(NULL, 'SERVER', 'VIEW SERVER PERFORMANCE STATE') END "
        "FROM sys.server_triggers t JOIN sys.server_sql_modules m ON m.object_id=t.object_id "
        "JOIN sys.server_principals p ON p.principal_id=m.execute_as_principal_id WHERE t.name=?;",
        GATE_TRIGGER,
    )
    if row(cursor) != (
        database,
        False,
        module_sha256(login_trigger_sql(database, ledger.schema)),
        GATE_READER,
        "S",
        True,
        1,
        1,
        1,
        1,
    ):
        raise CompositionAdmissionError("login_gate_policy")
    cursor.execute(
        "SELECT t.is_disabled, HASHBYTES('SHA2_256', CONVERT(varbinary(max), m.definition)) "
        "FROM sys.triggers t JOIN sys.sql_modules m ON t.object_id=m.object_id WHERE t.object_id=OBJECT_ID(?);",
        f"[{ledger.schema}].[composition_login_gate_monotonic]",
    )
    if row(cursor) != (False, module_sha256(monotonic_trigger_sql(ledger.schema))):
        raise CompositionAdmissionError("login_gate_monotonic_policy")
    cursor.execute(
        "SELECT COUNT(*) FROM sys.databases WHERE database_id=DB_ID() AND is_trustworthy_on=0 AND is_db_chaining_on=0 "
        "AND NOT EXISTS (SELECT 1 FROM sys.configurations WHERE name='cross db ownership chaining' AND value_in_use<>0) "
        "AND NOT EXISTS (SELECT 1 FROM sys.server_principals p JOIN sys.server_role_members r "
        "ON r.member_principal_id=p.principal_id WHERE p.name=?) "
        "AND NOT EXISTS (SELECT 1 FROM sys.server_principals p JOIN sys.server_permissions x "
        "ON x.grantee_principal_id=p.principal_id WHERE p.name=? AND NOT (x.state='G' AND x.permission_name IN "
        "('CONNECT SQL','VIEW SERVER STATE','VIEW SERVER PERFORMANCE STATE','VIEW ANY DEFINITION'))) "
        "AND NOT EXISTS (SELECT 1 FROM sys.database_principals p JOIN sys.database_role_members r "
        "ON r.member_principal_id=p.principal_id WHERE p.name=?) "
        "AND NOT EXISTS (SELECT 1 FROM sys.database_principals p JOIN sys.database_permissions x "
        "ON x.grantee_principal_id=p.principal_id WHERE p.name=? AND NOT (x.state='G' AND "
        "((x.class=0 AND x.permission_name='CONNECT') OR (x.class=1 AND x.major_id=OBJECT_ID(?) "
        "AND x.permission_name='SELECT'))));",
        GATE_READER,
        GATE_READER,
        GATE_READER,
        GATE_READER,
        ledger.table("login_gates"),
    )
    if row(cursor) != (1,):
        raise CompositionAdmissionError("login_gate_permission_policy")
    cursor.execute(
        f"EXECUTE AS LOGIN = N'{GATE_READER}'; "
        "BEGIN TRY SELECT HAS_PERMS_BY_NAME(NULL, 'SERVER', 'VIEW SERVER STATE'), "
        "HAS_PERMS_BY_NAME(NULL, 'SERVER', 'VIEW ANY DEFINITION'), "
        "CASE WHEN CONVERT(int, SERVERPROPERTY('ProductMajorVersion')) < 16 THEN 1 "
        "ELSE HAS_PERMS_BY_NAME(NULL, 'SERVER', 'VIEW SERVER PERFORMANCE STATE') END, "
        "HAS_PERMS_BY_NAME(?, 'OBJECT', 'SELECT'); REVERT; END TRY BEGIN CATCH REVERT; THROW; END CATCH;",
        ledger.table("login_gates"),
    )
    if row(cursor) != (1, 1, 1, 1):
        raise CompositionAdmissionError("login_gate_reader_visibility")


def require_enrollments(
    ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity, service_id: str
) -> tuple[MssqlEnrollment, ...]:
    """Check every selected MSSQL guard against current physical identity/policy."""
    ledger.cursor.execute(
        "SELECT a.guard_id, e.database_name, e.database_id, LOWER(CONVERT(char(36), e.database_guid)), "
        "e.database_create_token, e.writer_role, d.database_id, LOWER(CONVERT(char(36), r.database_guid)), "
        "CONVERT(nvarchar(33), d.create_date, 126), d.is_trustworthy_on, d.is_db_chaining_on, d.containment, d.state, "
        "LOWER(CONVERT(char(36), g.service_id)), DB_ID(), d.owner_sid, SUSER_SID(ORIGINAL_LOGIN()) "
        f"FROM {ledger.table('attempt_domains')} a JOIN {ledger.table('domains')} g ON a.guard_id=g.guard_id "
        f"LEFT JOIN {ledger.table('mssql_enrollments')} e ON e.guard_id=a.guard_id "
        "LEFT JOIN sys.databases d ON d.name=e.database_name COLLATE DATABASE_DEFAULT "
        "LEFT JOIN sys.database_recovery_status r ON r.database_id=d.database_id "
        "WHERE a.attempt_sha256=? AND g.connector='mssql' ORDER BY a.guard_id;",
        attempt.attempt_sha256,
    )
    records = tuple(ledger.cursor.fetchall())
    if not records:
        raise CompositionAdmissionError("login_no_mssql_scope")
    result = []
    for record in records:
        (
            guard,
            name,
            dbid,
            guid,
            token,
            role,
            actual_id,
            actual_guid,
            actual_token,
            trusted,
            chained,
            contained,
            state,
            service,
            control_id,
            owner_sid,
            controller_sid,
        ) = record
        if (
            None in record
            or (dbid, guid, token) != (actual_id, actual_guid, actual_token)
            or dbid <= 4
            or dbid == control_id
            or (trusted, chained, contained, state) != (0, 0, 0, 0)
            or service != service_id
            or owner_sid != controller_sid
        ):
            raise CompositionAdmissionError("login_database_enrollment")
        require_control_schema(name)
        require_control_schema(role)
        ledger.cursor.execute(
            f"SELECT schema_name FROM {ledger.table('mssql_managed_schemas')} WITH (HOLDLOCK) "
            "WHERE guard_id=? ORDER BY schema_name;",
            guard,
        )
        schemas = tuple(value[0] for value in ledger.cursor.fetchall())
        if not schemas or len(set(schemas)) != len(schemas):
            raise CompositionAdmissionError("login_managed_schemas")
        for schema in schemas:
            require_control_schema(schema)
        enrollment = MssqlEnrollment(guard, name, dbid, guid, token, role, schemas)
        _require_database_policy(ledger, enrollment)
        result.append(enrollment)
    return tuple(result)


def _require_database_policy(ledger: CompositionMssqlLedger, enrollment: MssqlEnrollment) -> None:
    """Reject ambient writers and require the bounded role's exact permission set.

    The controller owns the database; dbo owns roles and managed objects. Ordinary bindings
    are SQL instance users with read-only direct grants, optionally db_datareader.
    No other user-defined role, executable module or assembly is admitted.
    """
    db = f"[{enrollment.database}]"
    gates = ledger.table("login_gates")
    ledger.cursor.execute(
        f"SELECT p.class, p.major_id, s.name, p.permission_name, p.state FROM {db}.sys.database_permissions p "
        f"JOIN {db}.sys.database_principals u ON p.grantee_principal_id=u.principal_id "
        f"LEFT JOIN {db}.sys.schemas s ON p.class=3 AND p.major_id=s.schema_id WHERE u.name=?;",
        enrollment.writer_role,
    )
    observed = set(tuple(value) for value in ledger.cursor.fetchall())
    expected: set[tuple[int, int, str | None, str, str]] = {
        (0, 0, None, "CREATE TABLE", "G"),
        (0, 0, None, "CREATE VIEW", "G"),
    }
    ledger.cursor.execute(f"SELECT schema_id, name FROM {db}.sys.schemas;")
    schema_ids = {name: schema_id for schema_id, name in ledger.cursor.fetchall()}
    for name in enrollment.schemas:
        if name not in schema_ids:
            raise CompositionAdmissionError("login_schema_missing")
        expected.update(
            (3, schema_ids[name], name, permission, "G")
            for permission in ("SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "ALTER", "VIEW DEFINITION")
        )
    if observed != expected:
        raise CompositionAdmissionError("login_role_permissions")
    ledger.cursor.execute(
        f"""DECLARE @role sysname = ?, @guard varchar(71) = ?;
IF NOT EXISTS (SELECT 1 FROM {db}.sys.database_principals
    WHERE name=@role AND type='R' AND is_fixed_role=0 AND owning_principal_id=1)
 OR EXISTS (SELECT 1 FROM {db}.sys.database_principals WHERE type='R' AND is_fixed_role=0 AND name NOT IN ('public', @role))
 OR EXISTS (SELECT 1 FROM {db}.sys.objects WHERE is_ms_shipped=0 AND type NOT IN ('U','V','PK','UQ','F','D','C','IT'))
 OR EXISTS (SELECT 1 FROM {db}.sys.objects WHERE is_ms_shipped=0 AND principal_id IS NOT NULL AND principal_id<>1)
 OR EXISTS (SELECT 1 FROM {db}.sys.objects o JOIN {db}.sys.schemas s ON o.schema_id=s.schema_id
    WHERE o.is_ms_shipped=0 AND NOT EXISTS (SELECT 1 FROM {ledger.table("mssql_managed_schemas")} m
        WHERE m.guard_id=@guard AND m.schema_name=s.name COLLATE Latin1_General_100_BIN2))
 OR EXISTS (SELECT 1 FROM {db}.sys.assemblies WHERE is_user_defined=1)
 OR EXISTS (SELECT 1 FROM {db}.sys.database_principals u WHERE u.principal_id>4 AND u.type<>'R'
    AND (u.type<>'S' OR u.authentication_type<>1))
 OR EXISTS (SELECT 1 FROM {db}.sys.database_permissions p
    JOIN {db}.sys.database_principals u ON p.grantee_principal_id=u.principal_id
    WHERE u.name<>@role AND u.name NOT IN ('dbo','sys','INFORMATION_SCHEMA')
      AND (p.permission_name NOT IN ('SELECT','VIEW DEFINITION','CONNECT',
          'VIEW ANY COLUMN ENCRYPTION KEY DEFINITION','VIEW ANY COLUMN MASTER KEY DEFINITION')
          OR p.state NOT IN ('G','D') OR (u.name='guest' AND p.permission_name='CONNECT' AND p.state='G')))
 OR EXISTS (SELECT 1 FROM {db}.sys.database_role_members m
    JOIN {db}.sys.database_principals u ON u.principal_id=m.member_principal_id
    JOIN {db}.sys.database_principals r ON r.principal_id=m.role_principal_id
    LEFT JOIN {gates} g ON g.login_sid=u.sid
    WHERE NOT ((r.name=@role AND g.login_sid IS NOT NULL AND u.name COLLATE Latin1_General_100_BIN2=g.login_name)
          OR (r.name='db_datareader' AND g.login_sid IS NULL)))
 OR EXISTS (SELECT 1 FROM {db}.sys.schemas s JOIN {db}.sys.database_principals u ON s.principal_id=u.principal_id
    WHERE s.schema_id<16384 AND u.principal_id>4)
 OR EXISTS (SELECT 1 FROM {db}.sys.database_principals u JOIN sys.server_principals p ON p.sid=u.sid
    LEFT JOIN {gates} g ON g.login_sid=p.sid WHERE u.principal_id>4 AND g.login_sid IS NULL
    AND (p.type<>'S' OR EXISTS (SELECT 1 FROM sys.server_role_members m WHERE m.member_principal_id=p.principal_id)
         OR EXISTS (SELECT 1 FROM sys.server_permissions x WHERE x.grantee_principal_id=p.principal_id
             AND NOT (x.permission_name IN ('CONNECT SQL','VIEW ANY DEFINITION') AND x.state='G'))))
 OR EXISTS (SELECT 1 FROM sys.server_permissions x JOIN sys.server_principals p
    ON x.grantee_principal_id=p.principal_id WHERE p.name='public'
    AND NOT (x.state='G' AND ((x.class=100 AND x.permission_name IN ('CONNECT SQL','VIEW ANY DATABASE'))
        OR (x.class=105 AND x.permission_name='CONNECT' AND x.major_id IN
            (SELECT endpoint_id FROM sys.endpoints WHERE type=2)))))
 THROW 51000, 'DPONE_COMPOSITION_DATABASE_POLICY', 1;
SELECT 1;""",
        enrollment.writer_role,
        enrollment.guard_id,
    )
    if row(ledger.cursor) != (1,):
        raise CompositionAdmissionError("login_database_policy")


def create_login(
    ledger: CompositionMssqlLedger, credentials: MssqlIssuedCredentials, enrollments: tuple[MssqlEnrollment, ...]
) -> None:
    """Issue only the already journaled principal, in the READY transaction."""
    ledger.cursor.execute(
        "DECLARE @name sysname=?, @sid binary(16)=?, @password nvarchar(128)=?; "
        "IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name=@name OR sid=@sid) "
        "THROW 51000, 'DPONE_COMPOSITION_LOGIN_EXISTS', 1; "
        "DECLARE @sql nvarchar(max)=N'CREATE LOGIN '+QUOTENAME(@name)+N' WITH PASSWORD = '+ "
        "QUOTENAME(@password, '''')+N', SID = '+CONVERT(varchar(34),@sid,1)+N', CHECK_POLICY=ON, CHECK_EXPIRATION=OFF'; "
        "EXEC sys.sp_executesql @sql;",
        credentials.login_name,
        credentials.login_sid,
        credentials.password,
    )
    for enrollment in enrollments:
        ledger.cursor.execute(
            "DECLARE @db sysname=?, @name sysname=?, @role sysname=?; "
            "DECLARE @sql nvarchar(max)=N'USE '+QUOTENAME(@db)+N'; CREATE USER '+QUOTENAME(@name)+ "
            "N' FOR LOGIN '+QUOTENAME(@name)+N'; ALTER ROLE '+QUOTENAME(@role)+N' ADD MEMBER '+QUOTENAME(@name); "
            "EXEC sys.sp_executesql @sql;",
            enrollment.database,
            credentials.login_name,
            enrollment.writer_role,
        )


def require_login(ledger: CompositionMssqlLedger, name: str, sid: bytes, *, disabled: bool) -> None:
    """Exact immutable SQL login with no server-role or elevated grant fallback."""
    ledger.cursor.execute(
        "SELECT p.name, p.sid, p.type, p.is_disabled, "
        "(SELECT COUNT(*) FROM sys.server_role_members r WHERE r.member_principal_id=p.principal_id), "
        "(SELECT COUNT(*) FROM sys.server_permissions x WHERE x.grantee_principal_id=p.principal_id "
        "AND NOT (x.permission_name='CONNECT SQL' AND x.state='G')) "
        "FROM sys.server_principals p WHERE p.name=? OR p.sid=?;",
        name,
        sid,
    )
    if tuple(tuple(value) for value in ledger.cursor.fetchall()) != ((name, sid, "S", disabled, 0, 0),):
        raise CompositionAdmissionError("login_principal_readback")


def require_worker_users(
    ledger: CompositionMssqlLedger, credentials: MssqlIssuedCredentials, enrollments: tuple[MssqlEnrollment, ...]
) -> None:
    """Read back the exact created users and single bounded role per database."""
    for enrollment in enrollments:
        db = f"[{enrollment.database}]"
        ledger.cursor.execute(
            f"SELECT u.name, u.sid, u.type, u.authentication_type, r.name FROM {db}.sys.database_principals u "
            f"LEFT JOIN {db}.sys.database_role_members m ON m.member_principal_id=u.principal_id "
            f"LEFT JOIN {db}.sys.database_principals r ON r.principal_id=m.role_principal_id "
            "WHERE u.name=? OR u.sid=?;",
            credentials.login_name,
            credentials.login_sid,
        )
        if tuple(tuple(value) for value in ledger.cursor.fetchall()) != (
            (credentials.login_name, credentials.login_sid, "S", 1, enrollment.writer_role),
        ):
            raise CompositionAdmissionError("login_user_role_readback")
