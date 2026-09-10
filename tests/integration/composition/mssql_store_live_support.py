"""Synthetic SQL component fixtures, explicitly separate from physical admission.

The runner creates a new database on its owned container. Each test provisions a
fresh schema through external table-only DDL; the runtime never installs it.
ClickHouse resources are enrollment metadata only. No terminal gate/quiescence
proofs are invented. Connections and failures intentionally hide driver detail.
"""

from __future__ import annotations

import os
import re
from contextlib import closing
from dataclasses import dataclass, field
from uuid import uuid4

import pytest

from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import (
    CompositionActivationRequest,
    CompositionOccurrenceContext,
    CompositionPhysicalResource,
    CompositionWorkloadAdmission,
)

ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


@dataclass(frozen=True, repr=False)
class OwnedDatabase:
    """Accept only the runner's loopback endpoint and exact disposable name."""

    port: int
    database: str
    password: str = field(repr=False)

    @classmethod
    def from_environment(cls, env):
        token = env.get("DPONE_COMPOSITION_RUN_TOKEN", "")
        port = env.get("DPONE_COMPOSITION_SQL_PORT", "")
        database = env.get("DPONE_COMPOSITION_SQL_DATABASE", "")
        password = env.get("MSSQL_SA_PASSWORD", "")
        if (
            env.get("DPONE_RUN_COMPOSITION_MSSQL_LIVE") != "1"
            or re.fullmatch(r"[0-9a-f]{24}", token) is None
            or database != "dpone_composition_" + token
            or not port.isdecimal()
            or not 1024 <= int(port) <= 65535
            or re.fullmatch(r"Dp1![A-Za-z0-9_-]{32,}", password) is None
        ):
            raise RuntimeError("owned_synthetic_database_required")
        return cls(int(port), database, password)

    def connect(self, *, master=False):
        """Open a fresh real DBAPI connection; never expose DSN or driver errors."""
        try:
            import pyodbc

            pyodbc.pooling = False
            connection = pyodbc.connect(
                f"DRIVER={{{ODBC_DRIVER}}};SERVER=127.0.0.1,{self.port};"
                f"DATABASE={'master' if master else self.database};UID=sa;PWD={self.password};"
                "Encrypt=yes;TrustServerCertificate=yes;",
                autocommit=True,
                timeout=5,
            )
            connection.timeout = 15
            return connection
        except Exception:
            raise RuntimeError("synthetic_sql_connection_unavailable") from None


def digest(value):
    """Synthetic document identity, never evidence of a physical permission."""
    return canonical_fingerprint({"synthetic_component_input": value})


def request(service_id):
    """Build a canonical mixed-engine metadata ledger without admitting writers."""
    workloads, resources = [], []
    for name, connector, constituent, cell in (
        ("a_native", "mssql", "native", "sqlserver_dbt_v1"),
        ("b_ordinary", "clickhouse", "standalone", "mssql_clickhouse_full_refresh_v1"),
    ):
        service = service_id if connector == "mssql" else str(uuid4())
        physical, write = digest(name + " database"), digest(name + " write")
        guard = canonical_fingerprint(
            {
                "schema": "dpone.composition-physical-domain.v1",
                "connector": connector,
                "service_id": service,
                "physical_subject_sha256": physical,
            }
        )
        workloads.append(CompositionWorkloadAdmission(name, constituent, digest(name), cell, (write,)))
        resources.append(
            CompositionPhysicalResource(guard, connector, service, physical, digest("metadata observation"), (write,))
        )
    return CompositionActivationRequest(
        CompositionOccurrenceContext(
            str(uuid4()), "synthetic", digest("release"), digest("deployment"), None, digest("runtime")
        ),
        digest("sources"),
        tuple(workloads),
        tuple(sorted(resources, key=lambda resource: resource.guard_id)),
    )


class SqlCase:
    """Provision and independently reconcile one test's isolated control schema."""

    def __init__(self, database):
        self.database = database
        self.schema = "case_" + uuid4().hex
        self.service_id = str(uuid4())
        self.request = request(self.service_id)

    def store(self, factory=None, *, service_id=None):
        return MssqlCompositionActivationStore(
            factory or self.database.connect,
            expected_service_id=service_id or self.service_id,
            control_schema=self.schema,
        )

    def table(self, name):
        return f"[{self.schema}].[composition_{name}]"

    def sql(self, statement, *parameters):
        """Execute real SQL on a separate connection and return detached rows."""
        try:
            with closing(self.database.connect()) as connection, closing(connection.cursor()) as cursor:
                cursor.execute(statement, *parameters)
                return tuple(tuple(row) for row in cursor.fetchall()) if cursor.description else ()
        except Exception:
            raise RuntimeError("synthetic_sql_statement_failed") from None

    def install(self):
        """External administrator action, intentionally outside the store API."""
        try:
            with closing(self.database.connect()) as connection, closing(connection.cursor()) as cursor:
                cursor.execute(render_composition_mssql_schema(self.schema))
                while cursor.nextset():
                    pass
        except Exception:
            raise RuntimeError("synthetic_sql_ddl_failed") from None
        self.sql(
            f"INSERT INTO {self.table('authority')} (singleton, schema_version, service_id) VALUES (1, 1, ?)",
            self.service_id,
        )
        for resource in self.request.resources:
            self.sql(
                f"INSERT INTO {self.table('domains')} "
                "(guard_id, connector, service_id, physical_subject_sha256, fencing_epoch) VALUES (?, ?, ?, ?, 0)",
                resource.guard_id,
                resource.connector,
                resource.service_id,
                resource.physical_subject_sha256,
            )

    def domains(self):
        return self.sql(
            "SELECT guard_id, connector, LOWER(CONVERT(char(36), service_id)), physical_subject_sha256, "
            f"fencing_epoch, LOWER(CONVERT(char(36), owner_activation_id)) FROM {self.table('domains')} ORDER BY guard_id"
        )

    def snapshot(self):
        """Full parent/domain SQL rows detect any partial conflict mutation."""
        return (
            self.domains(),
            self.sql(f"SELECT * FROM {self.table('activations')} ORDER BY activation_id"),
            self.sql(f"SELECT * FROM {self.table('activation_domains')} ORDER BY activation_id, guard_id"),
        )


@pytest.fixture
def sql_case():
    """Skip unless explicitly enabled; enabled-but-invalid scope is an error."""
    if os.environ.get("DPONE_RUN_COMPOSITION_MSSQL_LIVE") != "1":
        pytest.skip("requires explicit disposable Linux SQL Server component runner")
    case = SqlCase(OwnedDatabase.from_environment(os.environ))
    yield case
    # The enclosing runner owns and deletes the entire container/database even
    # if setup, assertions or corruption prevent individual schema teardown.


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Retain actual outcomes while excluding driver/credential diagnostics."""
    outcome = yield
    if not item.nodeid.startswith("tests/integration/composition/test_composition_mssql_store_live.py::"):
        return
    report = outcome.get_result()
    if report.failed:
        report.longrepr = "SQL component test failed; sensitive driver diagnostics suppressed"
    report.sections = []
