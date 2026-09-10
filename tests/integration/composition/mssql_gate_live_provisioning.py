"""External provisioning for one gate in a runner-owned disposable SQL instance.

No runtime method installs DDL or grants. This fixture keeps a pre-trigger admin
connection for recovery, enrolls only databases it creates, and never exports
passwords, DSNs, driver diagnostics or server logs. Target ownership and role
permissions follow the actual gate policy, including dbo-owned managed objects.
"""

from __future__ import annotations

import re
import secrets
from contextlib import closing
from threading import Lock
from uuid import uuid4

from dpone.adapters.composition_mssql_gate_schema import (
    GATE_READER,
    GATE_TRIGGER,
    login_trigger_sql,
    module_sha256,
    monotonic_trigger_sql,
    render_composition_mssql_login_gate,
)
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema


class SqlFailure(RuntimeError):
    """Keep only SQLSTATE and a numeric error; driver messages may carry secrets."""

    def __init__(self, code=None, *, sqlstate=None):
        self.code = code
        self.sqlstate = sqlstate
        super().__init__(f"synthetic_sql_failure:{code if code is not None else 'unclassified'}")


def failure(error):
    if isinstance(error, SqlFailure):
        return error
    arguments = getattr(error, "args", ())
    state = arguments[0] if arguments and type(arguments[0]) is str else None
    state = state if state and re.fullmatch(r"[A-Z0-9]{5}", state) else None
    codes = re.findall(r"\((\d{2,6})\)", " ".join(str(value) for value in getattr(error, "args", ())))
    return SqlFailure(int(codes[-1]) if codes else None, sqlstate=state)


class ControlDiagnostics:
    """Memory-only bounded SQL codes captured before runtime sanitization.

    Wrappers never retry, change SQL or consume rows. They rethrow the identical
    exception; only fixed operation labels, ordinals and parsed codes survive.
    """

    def __init__(self):
        self.events, self.omitted, self.connections = [], 0, 0
        self._lock = Lock()

    def factory(self, factory, *, scope):
        if scope not in {"activation", "attempt", "gate"}:
            raise ValueError("diagnostic_scope")

        def connect():
            with self._lock:
                self.connections = min(self.connections + 1, 65535)
                ordinal = self.connections
            raw = self.call(factory, scope, ordinal, 0, "connect")
            return _ObservedConnection(raw, self, scope, ordinal)

        return connect

    def call(self, operation, scope, connection, execution, stage):
        try:
            return operation()
        except Exception as error:
            with self._lock:
                if len(self.events) < 16:
                    arguments = getattr(error, "args", ())
                    state = arguments[0] if arguments and type(arguments[0]) is str else None
                    state = state if state and re.fullmatch(r"[A-Z0-9]{5}", state) else None
                    message = " ".join(value[:8192] for value in arguments[:4] if type(value) is str)
                    codes = [int(value) for value in re.findall(r"\((-?\d{1,10})\)", message)[:8]]
                    if isinstance(error, SqlFailure):
                        candidate = error.sqlstate
                        state = (
                            candidate if type(candidate) is str and re.fullmatch(r"[A-Z0-9]{5}", candidate) else None
                        )
                        codes = [error.code] if type(error.code) is int else []
                    self.events.append(
                        {
                            "scope": scope,
                            "connection_ordinal": connection,
                            "execute_ordinal": execution,
                            "operation": stage,
                            "sqlstate": state,
                            "native_codes": codes,
                        }
                    )
                else:
                    self.omitted = min(self.omitted + 1, 65535)
            raise

    def snapshot(self):
        with self._lock:
            return {"events": list(self.events), "omitted_events": self.omitted, "connection_count": self.connections}


class _ObservedConnection:
    def __init__(self, raw, diagnostics, scope, ordinal):
        self.raw, self.diagnostics, self.scope, self.ordinal = raw, diagnostics, scope, ordinal
        self.execution = 0

    def observe(self, stage, operation):
        return self.diagnostics.call(operation, self.scope, self.ordinal, self.execution, stage)

    @property
    def autocommit(self):
        return self.raw.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.observe("autocommit", lambda: setattr(self.raw, "autocommit", value))

    def cursor(self, *args, **kwargs):
        return _ObservedCursor(self.observe("cursor", lambda: self.raw.cursor(*args, **kwargs)), self)

    def __getattr__(self, name):
        value = getattr(self.raw, name)
        if name in {"commit", "rollback", "close"}:
            return lambda *args, **kwargs: self.observe(name, lambda: value(*args, **kwargs))
        return value


class _ObservedCursor:
    def __init__(self, raw, connection):
        self.raw, self.connection = raw, connection

    def execute(self, *args, **kwargs):
        self.connection.execution = min(self.connection.execution + 1, 65535)
        result = self.connection.observe("execute", lambda: self.raw.execute(*args, **kwargs))
        return self if result is self.raw else result

    def __getattr__(self, name):
        value = getattr(self.raw, name)
        if name in {"fetchone", "fetchall", "fetchmany", "nextset", "close"}:
            return lambda *args, **kwargs: self.connection.observe(name, lambda: value(*args, **kwargs))
        return value


def execute(connection, statement, *parameters):
    """Drain every batch result so later trigger/grant/REVERT statements finish."""
    try:
        with closing(connection.cursor()) as cursor:
            cursor.execute(statement, *parameters)
            rows: list[tuple[object, ...]] = []
            while True:
                if cursor.description:
                    rows.extend(tuple(value) for value in cursor.fetchall())
                if not cursor.nextset():
                    return tuple(rows)
    except SqlFailure:
        raise
    except Exception as error:
        raise failure(error) from None


def gate_batches(database, schema):
    """Split only this renderer's explicit GO separators, preserving SQL bodies."""
    return tuple(
        batch.strip()
        for batch in re.split(
            r"(?m)^GO\s*$", render_composition_mssql_login_gate(control_database=database, control_schema=schema)
        )
        if batch.strip()
    )


class ProvisionedGate:
    """One server trigger/control schema; all target names originate here."""

    schema = "gate_control"
    managed_schema = "managed"
    writer_role = "bounded_writer"

    def __init__(self, database):
        self.database = database
        self.service_id = str(uuid4())
        self.targets = set()
        self.lifeline = None
        self.controller_name = None
        self.controller_sid = None

    def table(self, name):
        return f"[{self.schema}].[composition_{name}]"

    def connect(self, *, database=None, credentials=None):
        """Refuse arbitrary endpoints/databases before opening a real connection."""
        selected = database or self.database.database
        if selected not in {self.database.database, "master", *self.targets}:
            raise RuntimeError("fixture_database_scope")
        if credentials is not None and selected not in self.targets:
            raise RuntimeError("fixture_worker_database_scope")
        connection = None
        try:
            import pyodbc

            pyodbc.pooling = False
            name = credentials.login_name if credentials else "sa"
            password = credentials.password if credentials else self.database.password
            # Both credential producers return delimiter-free generated values.
            if re.fullmatch(r"[A-Za-z0-9_]+", name) is None or re.fullmatch(r"[A-Za-z0-9_!\-]+", password) is None:
                raise RuntimeError("fixture_credential_shape")
            connection = pyodbc.connect(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=127.0.0.1,{self.database.port};"
                f"DATABASE={selected};UID={name};PWD={password};Encrypt=yes;TrustServerCertificate=yes;",
                autocommit=True,
                timeout=5,
            )
            connection.timeout = 20
            execute(connection, "SET NOCOUNT ON; SET LOCK_TIMEOUT 10000;")
            return connection
        except Exception as error:
            if connection is not None:
                connection.close()
            raise failure(error) from None

    def sql(self, statement, *parameters, database=None):
        with closing(self.connect(database=database)) as connection:
            return execute(connection, statement, *parameters)

    def recover(self, statement, *parameters):
        """Use only the reserved pre-trigger administrator for owned fault repair."""
        return execute(self.lifeline, statement, *parameters)

    def install(self):
        self.lifeline = self.connect()
        self.controller_name, self.controller_sid = self.recover(
            "SELECT ORIGINAL_LOGIN(), SUSER_SID(ORIGINAL_LOGIN());"
        )[0]
        self.recover(render_composition_mssql_schema(self.schema))
        self.recover(f"INSERT INTO {self.table('authority')} VALUES (1, 1, ?);", self.service_id)
        self.recover(
            "DECLARE @password nvarchar(128)=?; DECLARE @sql nvarchar(max)="
            f"N'CREATE LOGIN [{GATE_READER}] WITH PASSWORD='+QUOTENAME(@password, '''')+"
            "N', CHECK_POLICY=ON, CHECK_EXPIRATION=OFF'; EXEC sys.sp_executesql @sql;",
            "Dp1!" + secrets.token_urlsafe(32),
        )
        self.recover(f"ALTER LOGIN [{GATE_READER}] DISABLE; CREATE USER [{GATE_READER}] FOR LOGIN [{GATE_READER}];")
        # Server permission GRANT requires master (SQL Server error 4621).
        # Dedicated sessions leave the recovery lifeline in its control DB.
        for permission in ("VIEW SERVER STATE", "VIEW ANY DEFINITION", "VIEW SERVER PERFORMANCE STATE"):
            self.sql(f"GRANT {permission} TO [{GATE_READER}];", database="master")
        batches = gate_batches(self.database.database, self.schema)
        for batch in batches[:-1]:
            self.recover(batch)
        self.recover(
            f"GRANT CONNECT TO [{GATE_READER}]; GRANT SELECT ON OBJECT::{self.table('login_gates')} TO [{GATE_READER}];"
        )
        self.recover(
            f"CREATE TABLE [{self.schema}].[synthetic_outcomes] (evidence_sha256 varchar(71) NOT NULL PRIMARY KEY, "
            "attempt_sha256 varchar(71) NOT NULL, evidence_document varbinary(max) NOT NULL);"
        )
        self.sql(batches[-1], database="master")

    def new_target(self):
        """Create and observe fresh physical identity before any enrollment row."""
        name = "gate_target_" + uuid4().hex
        self.targets.add(name)
        self.recover(f"CREATE DATABASE [{name}];")
        self.recover(f"ALTER DATABASE [{name}] SET TRUSTWORTHY OFF; ALTER DATABASE [{name}] SET DB_CHAINING OFF;")
        self.sql(f"CREATE SCHEMA [{self.managed_schema}] AUTHORIZATION [dbo];", database=name)
        self.sql("CREATE SCHEMA [unmanaged] AUTHORIZATION [dbo];", database=name)
        self.sql(
            f"CREATE ROLE [{self.writer_role}] AUTHORIZATION [dbo]; "
            f"GRANT CREATE TABLE, CREATE VIEW TO [{self.writer_role}]; "
            f"GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES, ALTER, VIEW DEFINITION ON SCHEMA::[{self.managed_schema}] TO [{self.writer_role}]; "
            f"CREATE TABLE [{self.managed_schema}].[rows] (row_id int NOT NULL PRIMARY KEY, value varchar(32) NOT NULL);",
            database=name,
        )
        pins = self.sql(
            "SELECT d.database_id, LOWER(CONVERT(char(36), r.database_guid)), CONVERT(nvarchar(33), d.create_date, 126), d.owner_sid "
            "FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.name=?;",
            name,
        )
        assert len(pins) == 1 and pins[0][0] > 4 and pins[0][3] == self.controller_sid
        return name, pins[0]

    def observe_policy(self):
        """Retain only bounded catalog facts; never SQL bodies or driver text.

        The two permission forms are observed independently for diagnosis.
        Neither their values nor observed module hashes change runtime policy.
        """
        logon = self.sql(
            "SELECT TOP (2) DB_NAME(),t.is_disabled,DATALENGTH(m.definition),"
            "HASHBYTES('SHA2_256',CONVERT(varbinary(max),m.definition)),m.execute_as_principal_id,"
            "p.principal_id,p.sid,p.type,p.is_disabled,"
            "CASE WHEN p.name=? AND p.sid=SUSER_SID(?) THEN 1 ELSE 0 END,"
            "(SELECT COUNT(*) FROM sys.server_trigger_events e JOIN sys.server_triggers x ON x.object_id=e.object_id "
            "WHERE e.type_desc='LOGON' AND x.is_disabled=0) "
            "FROM sys.server_triggers t LEFT JOIN sys.server_sql_modules m ON m.object_id=t.object_id "
            "LEFT JOIN sys.server_principals p ON p.principal_id=m.execute_as_principal_id WHERE t.name=?;",
            GATE_READER,
            GATE_READER,
            GATE_TRIGGER,
        )
        monotonic = self.sql(
            "SELECT TOP (2) t.is_disabled,DATALENGTH(m.definition),HASHBYTES('SHA2_256',CONVERT(varbinary(max),m.definition)) "
            "FROM sys.triggers t LEFT JOIN sys.sql_modules m ON m.object_id=t.object_id WHERE t.object_id=OBJECT_ID(?);",
            f"[{self.schema}].[composition_login_gate_monotonic]",
        )
        permissions = ("VIEW SERVER STATE", "VIEW ANY DEFINITION", "VIEW SERVER PERFORMANCE STATE")
        projection = ",".join(
            f"HAS_PERMS_BY_NAME(NULL,'SERVER','{permission}'),HAS_PERMS_BY_NAME(NULL,NULL,'{permission}')"
            for permission in permissions
        )
        controller_permissions = self.sql("SELECT " + projection + ";")
        reader_permissions = self.sql(
            f"EXECUTE AS LOGIN=N'{GATE_READER}'; BEGIN TRY SELECT {projection},"
            "HAS_PERMS_BY_NAME(?,'OBJECT','SELECT'),"
            "CASE WHEN SUSER_SID()=SUSER_SID(?) THEN 1 ELSE 0 END; REVERT; END TRY BEGIN CATCH REVERT; THROW; END CATCH;",
            self.table("login_gates"),
            GATE_READER,
        )
        grants = self.sql(
            "SELECT TOP (65) x.state,x.permission_name FROM sys.server_permissions x "
            "JOIN sys.server_principals p ON p.principal_id=x.grantee_principal_id WHERE p.name=? ORDER BY x.permission_name,x.state;",
            GATE_READER,
        )

        def rows(values, columns):
            return [
                {
                    key: value.hex() if isinstance(value, bytes) else value
                    for key, value in zip(columns, row, strict=True)
                }
                for row in values
            ]

        permission_columns = tuple(
            permission.lower().replace(" ", "_") + suffix
            for permission in permissions
            for suffix in ("_server_class", "_null_class")
        )
        expected_logon = login_trigger_sql(self.database.database, self.schema)
        expected_monotonic = monotonic_trigger_sql(self.schema)
        return {
            "expected_control_database": self.database.database,
            "expected_logon_sha256": module_sha256(expected_logon).hex(),
            "expected_logon_utf16_bytes": len(expected_logon.encode("utf-16le")),
            "expected_monotonic_sha256": module_sha256(expected_monotonic).hex(),
            "expected_monotonic_utf16_bytes": len(expected_monotonic.encode("utf-16le")),
            "logon": rows(
                logon,
                (
                    "control_database",
                    "disabled",
                    "definition_utf16_bytes",
                    "sha256",
                    "execute_as_principal_id",
                    "joined_principal_id",
                    "reader_sid",
                    "reader_type",
                    "reader_disabled",
                    "reader_identity_matches",
                    "enabled_logon_trigger_count",
                ),
            ),
            "monotonic": rows(monotonic, ("disabled", "definition_utf16_bytes", "sha256")),
            "controller_permissions": rows(controller_permissions, permission_columns),
            "reader_permissions": rows(
                reader_permissions, (*permission_columns, "gate_select", "effective_sid_matches")
            ),
            "reader_server_grants": rows(grants, ("state", "permission")),
        }

    def auxiliary_owner(self):
        """A disabled disposable principal used only for the foreign-owner fault."""
        name = "fixture_" + uuid4().hex
        self.recover(
            "DECLARE @name sysname=?, @password nvarchar(128)=?; DECLARE @sql nvarchar(max)="
            "N'CREATE LOGIN '+QUOTENAME(@name)+N' WITH PASSWORD='+QUOTENAME(@password, '''')+"
            "N', CHECK_POLICY=ON, CHECK_EXPIRATION=OFF; ALTER LOGIN '+QUOTENAME(@name)+N' DISABLE'; EXEC sys.sp_executesql @sql;",
            name,
            "Dp1!" + secrets.token_urlsafe(32),
        )
        return name

    def close(self):
        if self.lifeline is not None:
            self.lifeline.close()
