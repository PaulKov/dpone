"""Exclusively owned, opt-in SQL2022 primitive-DDL fixture resources.

The parent supplies a disposable isolated SQL host. This helper never starts a
container, creates a login, scans/drop-matches a database prefix, or uses a caller
supplied database name. SQL errors fail the test; they are not converted to skips.
"""

import math
import os
import sys
import time
from typing import Any
from uuid import uuid4

from dpone.contracts.mssql_object_name import quote_mssql_identifier

CASE_BUDGET_SECONDS = 300
STATEMENT_TIMEOUT_SECONDS = 120
CONNECT_TIMEOUT_SECONDS = 10
LOCK_TIMEOUT_MS = 30000
CLEANUP_BUDGET_SECONDS = 120


def _odbc_value(value: str) -> str:
    return "{" + value.replace("}", "}}") + "}"


class RenderingFixture:
    """Own only one fresh random database and its connections per test case."""

    def __init__(self, driver: Any) -> None:
        self.driver = driver
        self.database = "dpone_rendering_" + uuid4().hex + "]Ω"
        self.schema = "render]схема_" + uuid4().hex[:12]
        self.started = time.monotonic()
        self.deadline = self.started + CASE_BUDGET_SECONDS
        self.connection: Any = None
        self._allocated = False
        self._pin: tuple[object, ...] | None = None
        self.evidence: dict[str, object] = {}
        self.filegroup_id = 0
        self.filegroup_name = ""
        self.collation = ""

    def _connect(self, database: str) -> Any:
        # Connection material is transient memory only, never fixture evidence.
        try:
            return self.driver.connect(
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={_odbc_value(os.environ['DPONE_NATIVE_SQL_TEST_HOST'])};"
                f"DATABASE={_odbc_value(database)};UID=sa;"
                f"PWD={_odbc_value(os.environ['DPONE_NATIVE_SQL_TEST_PASSWORD'])};"
                "Encrypt=no;TrustServerCertificate=yes",
                timeout=CONNECT_TIMEOUT_SECONDS,
                autocommit=True,
            )
        except Exception:
            # Do not expose driver connection diagnostics containing a DSN.
            raise RuntimeError("isolated rendering fixture SQL connection failed") from None

    def _query(self, connection: Any, sql: str, *parameters: object) -> list[tuple[Any, ...]]:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("rendering fixture correctness budget exhausted; this is not an SLO")
        connection.timeout = min(STATEMENT_TIMEOUT_SECONDS, max(1, math.ceil(remaining)))
        cursor = connection.cursor()
        try:
            cursor.execute(sql, *parameters)
            return [] if cursor.description is None else [tuple(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def query(self, sql: str, *parameters: object) -> list[tuple[Any, ...]]:
        return self._query(self.connection, sql, *parameters)

    def install(self) -> None:
        """Require the exact engine cell, then allocate and observe its real storage."""
        admin = self._connect("master")
        try:
            self._query(admin, f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS};")
            engine = self._query(
                admin,
                "SELECT CONVERT(int,SERVERPROPERTY('ProductMajorVersion')),"
                "CONVERT(nvarchar(128),SERVERPROPERTY('ProductUpdateLevel')),"
                "CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion')),CONVERT(nvarchar(max),@@VERSION)",
            )
            assert len(engine) == 1 and engine[0][0] == 16 and engine[0][1] == "CU26", "requires SQL2022 CU26"
            self.evidence = {
                "scope": "primitive_renderer_ddl_only",
                "native_admission_exercised": False,
                "route_qualification": False,
                "engine_major": engine[0][0],
                "engine_update": engine[0][1],
                "engine_version": engine[0][2],
                "engine_description": engine[0][3],
                "pyodbc_version": self.driver.version,
                "odbc_driver_name": admin.getinfo(self.driver.SQL_DRIVER_NAME),
                "odbc_driver_version": admin.getinfo(self.driver.SQL_DRIVER_VER),
                "case_correctness_budget_seconds": CASE_BUDGET_SECONDS,
                "statement_timeout_seconds": STATEMENT_TIMEOUT_SECONDS,
                "connect_timeout_seconds": CONNECT_TIMEOUT_SECONDS,
                "lock_timeout_ms": LOCK_TIMEOUT_MS,
            }
            assert self._query(admin, "SELECT DB_ID(?)", self.database) == [(None,)], "reserved database already exists"
            # Arm only this exact absent random allocation before CREATE so a lost
            # CREATE acknowledgement still reaches owned-resource cleanup.
            self._allocated = True
            self._query(admin, f"CREATE DATABASE {quote_mssql_identifier(self.database)};")
            pin = self._query(
                admin,
                "SELECT d.database_id,CONVERT(char(36),r.database_guid) FROM sys.databases d "
                "JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.name=?",
                self.database,
            )
            assert len(pin) == 1 and pin[0][0] > 4 and pin[0][1] is not None
            self._pin = pin[0]
        finally:
            admin.close()
        self.connection = self._connect(self.database)
        self.query(f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS}; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
        self.query(f"CREATE SCHEMA {quote_mssql_identifier(self.schema)} AUTHORIZATION dbo;")
        groups = self.query(
            "SELECT data_space_id,name FROM sys.filegroups WHERE type='FG' AND is_default=1 AND is_read_only=0"
        )
        assert len(groups) == 1 and groups[0][0] > 0 and isinstance(groups[0][1], str)
        self.filegroup_id, self.filegroup_name = groups[0]
        collations = self.query("SELECT CONVERT(nvarchar(128),DATABASEPROPERTYEX(DB_NAME(),'Collation'))")
        assert len(collations) == 1 and isinstance(collations[0][0], str) and collations[0][0]
        self.collation = collations[0][0]

    def cleanup(self) -> None:
        """Close writer first, then remove only the exact reserved owned database.

        A replacement identity is left untouched. Cleanup has a separate bounded
        budget even when the test exhausted its own; failure remains visible.
        The parent also destroys its exclusively owned container after pytest.
        """
        primary = sys.exception()
        failures: list[Exception] = []
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception as exc:
                failures.append(exc)
        self.deadline = time.monotonic() + CLEANUP_BUDGET_SECONDS
        if self._allocated:
            admin = None
            try:
                admin = self._connect("master")
                self._query(admin, f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS};")
                current = self._query(
                    admin,
                    "SELECT d.database_id,CONVERT(char(36),r.database_guid) FROM sys.databases d "
                    "JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.name=?",
                    self.database,
                )
                if current:
                    if len(current) != 1 or (self._pin is not None and current[0] != self._pin):
                        raise RuntimeError("owned rendering database identity changed; cleanup refused")
                    name = quote_mssql_identifier(self.database)
                    self._query(admin, f"ALTER DATABASE {name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;")
                    self._query(admin, f"DROP DATABASE {name};")
                assert self._query(admin, "SELECT DB_ID(?)", self.database) == [(None,)]
            except Exception as exc:
                failures.append(exc)
            finally:
                if admin is not None:
                    try:
                        admin.close()
                    except Exception as exc:
                        failures.append(exc)
        if failures:
            if primary is not None:
                primary.add_note("Rendering fixture cleanup failed; parent must remove its isolated container")
            raise ExceptionGroup("Owned rendering fixture cleanup failed", failures)
