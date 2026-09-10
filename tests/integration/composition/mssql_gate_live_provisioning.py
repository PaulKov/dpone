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
from uuid import uuid4

from dpone.adapters.composition_mssql_gate_schema import GATE_READER, render_composition_mssql_login_gate
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema


class SqlFailure(RuntimeError):
    """Only a numeric SQL Server error is retained; messages may carry secrets."""

    def __init__(self, code=None):
        self.code = code
        super().__init__(f"synthetic_sql_failure:{code if code is not None else 'unclassified'}")


def failure(error):
    codes = re.findall(r"\((\d{2,6})\)", " ".join(str(value) for value in getattr(error, "args", ())))
    return SqlFailure(int(codes[-1]) if codes else None)


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
