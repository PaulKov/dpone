"""Opt-in live gate fixtures and bounded synchronization on owned SQL resources.

Synthetic parent metadata is not signed route authority. Real gate/attempt APIs
and issued DBAPI sessions are exercised without running native-v2, Airflow, dbt,
PostgreSQL or ClickHouse. JUnit properties retain only bounded safe observations.
"""

from __future__ import annotations

import json
import os
import platform
import re
import time
from uuid import uuid4

import pytest
from tests.integration.composition.mssql_gate_live_provisioning import ControlDiagnostics, ProvisionedGate, SqlFailure
from tests.integration.composition.mssql_store_live_support import OwnedDatabase, digest

from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_control import (
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionOccurrenceContext,
    CompositionPhysicalResource,
    CompositionWorkloadAdmission,
)


def observation_document(payload):
    """Reject secret-bearing keys and oversized records before artifact writes."""

    def inspect(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key.lower() in {"password", "dsn", "connection_string", "environment", "parameters"}:
                    raise ValueError("unsafe_observation")
                inspect(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                inspect(item)

    inspect(payload)
    document = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(document.encode()) > 65536:
        raise ValueError("observation_budget")
    return document


def require_denied(operation):
    """Require a permission/authentication refusal, never arbitrary SQL failure."""
    with pytest.raises(SqlFailure) as caught:
        operation()
    # Microsoft error 18470 is the independently observed disabled-account refusal.
    if caught.value.code not in {229, 262, 297, 916, 2760, 15151, 15247, 15406, 18456, 18470, 17892}:
        raise caught.value  # Keep its safe SQLSTATE/code available to the report hook.
    return caught.value.code


def wait_until(predicate, *, timeout=10):
    """Wait only for a concrete observation; timeout remains a failure."""
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        assert time.monotonic() < deadline, "observation_timeout"
        time.sleep(0.05)


class CommitBoundary:
    """Real DBAPI wrapper: inject behavior only after the server commit returns."""

    def __init__(self, connection, callback):
        self.connection, self.callback = connection, callback

    @property
    def autocommit(self):
        return self.connection.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.connection.autocommit = value

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def commit(self):
        self.connection.commit()
        self.callback()


def commit_factory(environment, selected, callback):
    """Wrap one selected connection only; readbacks remain independent SQL."""
    opened = []

    def factory():
        connection = environment.connect()
        opened.append(connection)
        return CommitBoundary(connection, callback) if len(opened) == selected else connection

    return factory, opened


class GateCase:
    def __init__(self, environment, record_property, *, diagnostics=None):
        self.environment, self.record_property = environment, record_property
        self.diagnostics = diagnostics or ControlDiagnostics()
        self.record("check_catalog", environment.check_catalog())
        self.record("check_catalog_context", environment.check_catalog_context())
        self.target, self.pins = environment.new_target()
        self.connections = []
        self.credentials = None
        self.request = self._request()
        resource = self.request.resources[0]
        self.sql(
            f"INSERT INTO {self.table('domains')} (guard_id,connector,service_id,physical_subject_sha256,fencing_epoch) VALUES (?,'mssql',?,?,0);",
            resource.guard_id,
            environment.service_id,
            resource.physical_subject_sha256,
        )
        self.sql(
            f"INSERT INTO {self.table('mssql_enrollments')} VALUES (?, ?, ?, ?, ?, ?);",
            resource.guard_id,
            self.target,
            *self.pins[:3],
            environment.writer_role,
        )
        self.sql(
            f"INSERT INTO {self.table('mssql_managed_schemas')} VALUES (?, ?);",
            resource.guard_id,
            environment.managed_schema,
        )
        self.store = MssqlCompositionActivationStore(
            self.diagnostics.factory(environment.connect, scope="activation"),
            expected_service_id=environment.service_id,
            control_schema=environment.schema,
        )
        self.store.prepare(self.request)
        self.active = self.store.activate(self.request)
        workload = self.request.workloads[0]
        self.attempt = CompositionAttemptIdentity(
            self.request.request_sha256,
            workload.workload_id,
            workload.constituent_id,
            workload.pack_sha256,
            digest("SQL gate component plan"),
            "synthetic:" + uuid4().hex,
            "execute",
            1,
            -1,
            self.active.receipt.guard_epochs,
        )
        self.attempts = self.attempt_store()
        self.gate = self.login_gate()
        self.record(
            "fixture",
            {
                "target": self.target,
                "database_id": self.pins[0],
                "database_guid": self.pins[1],
                "create_token": self.pins[2],
                "owner_sid": self.pins[3].hex(),
                "activation_id": self.request.activation_id,
                "attempt_sha256": self.attempt.attempt_sha256,
            },
        )

    def _request(self):
        service = self.environment.service_id
        physical = canonical_fingerprint(
            {
                "service_id": service,
                "database_id": self.pins[0],
                "database_guid": self.pins[1],
                "create_token": self.pins[2],
            }
        )
        guard = canonical_fingerprint(
            {
                "schema": "dpone.composition-physical-domain.v1",
                "connector": "mssql",
                "service_id": service,
                "physical_subject_sha256": physical,
            }
        )
        writes = (digest("native work"), digest("ordinary metadata"))
        return CompositionActivationRequest(
            CompositionOccurrenceContext(
                str(uuid4()),
                "synthetic",
                digest(self.target),
                digest("deployment " + self.target),
                None,
                digest("runtime"),
            ),
            digest("synthetic sources"),
            (
                CompositionWorkloadAdmission(
                    "a_native", "native", digest("native pack"), "sqlserver_dbt_v1", (writes[0],)
                ),
                CompositionWorkloadAdmission(
                    "b_ordinary", "standalone", digest("ordinary pack"), "postgres_mssql_full_refresh_v1", (writes[1],)
                ),
            ),
            (
                CompositionPhysicalResource(
                    guard, "mssql", service, physical, digest("observed " + physical), tuple(sorted(writes))
                ),
            ),
        )

    def table(self, name):
        return self.environment.table(name)

    def sql(self, statement, *parameters):
        return self.environment.sql(statement, *parameters)

    def target_sql(self, statement, *parameters):
        return self.environment.sql(statement, *parameters, database=self.target)

    def attempt_store(self, factory=None):
        return MssqlCompositionAttemptStore(
            self.diagnostics.factory(factory or self.environment.connect, scope="attempt"),
            expected_service_id=self.environment.service_id,
            control_schema=self.environment.schema,
        )

    def login_gate(self, factory=None):
        return MssqlCompositionLoginGate(
            self.diagnostics.factory(factory or self.environment.connect, scope="gate"),
            expected_service_id=self.environment.service_id,
            control_database=self.environment.database.database,
            control_schema=self.environment.schema,
        )

    def issue(self):
        self.attempts.admit_once(self.attempt)
        self.credentials = self.gate.issue_once(self.attempt)
        return self.credentials

    def worker(self, credentials=None):
        credentials = credentials or self.credentials
        if credentials is None:
            raise RuntimeError("fixture_worker_credentials_required")
        connection = self.environment.connect(database=self.target, credentials=credentials)
        self.connections.append(connection)
        return connection

    def gate_row(self):
        rows = self.sql(
            f"SELECT login_sid,login_name,gate_state,disabled_evidence_sha256 FROM {self.table('login_gates')} WHERE operation_key=?;",
            self.attempt.attempt_sha256,
        )
        return rows[0] if rows else None

    def no_issuance(self):
        assert self.gate_row() is None
        assert self.sql(
            f"SELECT COUNT(*) FROM {self.table('issued_authorities')} WHERE operation_key=?;",
            self.attempt.attempt_sha256,
        ) == ((0,),)
        assert self.sql(
            "SELECT COUNT(*) FROM sys.server_principals WHERE name=?;", "dpone_v3_" + self.attempt.attempt_sha256[7:]
        ) == ((0,),)

    def record(self, name, payload):
        self.record_property("dpone.gate." + name, observation_document(payload))

    def drain_workers(self):
        for connection in self.connections:
            try:
                connection.rollback()
            except Exception:
                pass
            connection.close()
        self.connections.clear()

    def close_and_prove(self):
        self.drain_workers()
        closed = self.gate.close(self.attempt)
        quiet = self.gate.prove_quiescence(self.attempt)
        self.record("gate_proofs", {"closed": closed.to_dict(), "quiescent": quiet.to_dict()})
        return closed, quiet

    def cleanup(self):
        self.drain_workers()
        # Missing principals deliberately retain CLOSING journals. Disable any
        # present original or replacement by its immutable fixture-owned name.
        row = self.gate_row()
        if row is not None:
            name = row[1]
            self.environment.recover(
                "DECLARE @name sysname=?; IF EXISTS(SELECT 1 FROM sys.server_principals WHERE name=@name) BEGIN DECLARE @sql nvarchar(max)=N'ALTER LOGIN '+QUOTENAME(@name)+N' DISABLE'; EXEC sys.sp_executesql @sql; END;",
                name,
            )


@pytest.fixture(scope="session")
def gate_environment():
    if os.environ.get("DPONE_RUN_COMPOSITION_MSSQL_GATE_LIVE") != "1":
        pytest.skip("requires explicit disposable SQL gate runner profile")
    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
        pytest.skip("requires disposable Linux x86_64 SQL gate runner")
    environment = ProvisionedGate(OwnedDatabase.from_environment(os.environ))
    try:
        environment.install()
        yield environment
    finally:
        environment.close()


@pytest.fixture
def gate_case(gate_environment, record_property):
    record_property("dpone.gate.installed_policy", observation_document(gate_environment.observe_policy()))
    diagnostics, case = ControlDiagnostics(), None
    try:
        case = GateCase(gate_environment, record_property, diagnostics=diagnostics)
        yield case
    finally:
        try:
            if case is not None:
                case.cleanup()
        finally:
            record_property("dpone.gate.control_failures", observation_document(diagnostics.snapshot()))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Preserve outcome and safe error codes while dropping sensitive details."""
    result = yield
    if item.nodeid.split("::", 1)[0] not in {
        "tests/integration/composition/test_composition_mssql_gate_live.py",
        "tests/integration/composition/test_composition_mssql_transfer_fence_live.py",
        "tests/integration/composition/test_composition_mssql_gate_recovery_live.py",
    }:
        return
    report = result.get_result()
    if report.failed:
        error = call.excinfo.value if call.excinfo else None
        reason = "assertion_or_fixture_failure"
        if isinstance(error, SqlFailure) and type(error.code) is int:
            reason = f"sql_error_{error.code}"
        elif isinstance(error, CompositionAdmissionError) and re.fullmatch(r"[a-z0-9_]+", error.reason):
            reason = error.reason
        report.longrepr = "SQL gate component failed: " + reason
        properties = [("dpone.gate.failure", reason)]
        if isinstance(error, SqlFailure):
            state = error.sqlstate
            state = state if type(state) is str and re.fullmatch(r"[A-Z0-9]{5}", state) else None
            codes = [error.code] if type(error.code) is int and abs(error.code) <= 2147483647 else []
            properties.append(
                ("dpone.gate.failure_sql", observation_document({"sqlstate": state, "native_codes": codes}))
            )
        locations = failure_locations(error)
        if locations:
            properties.append(("dpone.gate.failure_locations", observation_document({"frames": locations})))
        # JUnit finalizes with teardown's copy of the item properties.
        item.user_properties.extend(properties)
        report.user_properties.extend(properties)
    report.sections = []


def failure_locations(error):
    """Retain at most eight owned module/line pairs from 64 traceback frames.

    Inspect only code filenames and numeric line numbers. Never inspect source,
    locals, exception arguments/messages, chained exceptions or foreign paths.
    """
    directory = os.path.dirname(os.path.abspath(__file__))
    allowed = {
        os.path.join(directory, name): name
        for name in (
            "test_composition_mssql_gate_live.py",
            "test_composition_mssql_gate_recovery_live.py",
            "mssql_gate_live_support.py",
            "test_composition_mssql_transfer_fence_live.py",
            "mssql_transfer_fence_live_support.py",
            "mssql_gate_live_provisioning.py",
            "mssql_gate_live_outcomes.py",
        )
    }
    traceback = error.__traceback__ if error is not None else None
    locations = []
    for _ in range(64):
        if traceback is None:
            break
        module = allowed.get(os.path.abspath(traceback.tb_frame.f_code.co_filename))
        if module is not None and type(traceback.tb_lineno) is int and 1 <= traceback.tb_lineno <= 100000:
            locations.append({"module": module, "line": traceback.tb_lineno})
            locations = locations[-8:]
        traceback = traceback.tb_next
    return locations
