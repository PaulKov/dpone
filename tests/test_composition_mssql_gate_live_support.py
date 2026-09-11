"""Offline fixture/evidence boundaries; doubles here never qualify live SQL."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import FunctionType, SimpleNamespace
from xml.etree import ElementTree

import pytest

from dpone.adapters.composition_mssql_gate_schema import login_trigger_sql, monotonic_trigger_sql
from dpone.contracts.composition_control import CompositionAdmissionError
from tests.integration.composition import mssql_gate_live_provisioning as provisioning
from tests.integration.composition import mssql_gate_live_support as support
from tests.integration.composition.mssql_gate_live_outcomes import OutcomeProducer
from tests.integration.composition.mssql_gate_live_provisioning import (
    ControlDiagnostics,
    ProvisionedGate,
    SqlFailure,
    execute,
    gate_batches,
)


@pytest.mark.parametrize("stage", ["execute", "fetchone", "fetchall", "nextset", "commit"])
def test_control_boundary_records_only_codes_and_rethrows_identical_error(stage):
    fault = RuntimeError("42000", "PWD=never-print; SELECT private_text; failure (229) (SQLExecDirectW)")

    def operation(*_):
        raise fault

    raw_cursor: SimpleNamespace = SimpleNamespace(
        execute=lambda *_: raw_cursor, fetchone=lambda: (1,), fetchall=lambda: [(1,)]
    )
    raw_cursor.nextset = lambda: None
    raw_connection = SimpleNamespace(cursor=lambda: raw_cursor, commit=lambda: None, autocommit=True)
    setattr(raw_connection if stage == "commit" else raw_cursor, stage, operation)
    diagnostics = ControlDiagnostics()
    connection = diagnostics.factory(lambda: raw_connection, scope="gate")()
    connection.autocommit = False
    assert raw_connection.autocommit is False
    cursor = connection.cursor()
    with pytest.raises(RuntimeError) as caught:
        cursor.execute("private SQL", "never-print")
        getattr(connection if stage == "commit" else cursor, stage)()
    assert caught.value is fault
    event = diagnostics.snapshot()["events"][0]
    assert event == {
        "scope": "gate",
        "connection_ordinal": 1,
        "execute_ordinal": 1,
        "operation": stage,
        "sqlstate": "42000",
        "native_codes": [229],
    }
    document = json.dumps(diagnostics.snapshot())
    assert "never-print" not in document and "private" not in document and "SQLExecDirectW" not in document


def test_control_boundary_keeps_results_and_committed_ack_loss_behavior():
    row, rows, commits = (b"actual", None), [(b"actual", None)], []
    raw_cursor: SimpleNamespace = SimpleNamespace(
        execute=lambda *_: raw_cursor, fetchone=lambda: row, fetchall=lambda: rows, nextset=lambda: None
    )
    raw_connection = SimpleNamespace(
        cursor=lambda: raw_cursor, commit=lambda: commits.append("committed"), autocommit=True
    )
    fault = RuntimeError("injected acknowledgement loss")

    def lost_ack():
        raise fault

    diagnostics = ControlDiagnostics()
    connection = diagnostics.factory(lambda: support.CommitBoundary(raw_connection, lost_ack), scope="gate")()
    cursor = connection.cursor()
    assert cursor.execute("SELECT observed") is cursor
    assert cursor.fetchone() is row and cursor.fetchall() is rows and cursor.nextset() is None
    with pytest.raises(RuntimeError) as caught:
        connection.commit()
    assert caught.value is fault and commits == ["committed"]
    assert diagnostics.snapshot()["events"][0]["sqlstate"] is None
    assert diagnostics.snapshot()["events"][0]["native_codes"] == []


def test_control_diagnostics_are_bounded_and_connect_failures_are_not_replayed():
    calls = []

    def connect():
        calls.append(1)
        raise RuntimeError("08001", "private endpoint failure (53)")

    diagnostics = ControlDiagnostics()
    factory = diagnostics.factory(connect, scope="attempt")
    for _ in range(40):
        with pytest.raises(RuntimeError):
            factory()
    snapshot = diagnostics.snapshot()
    assert len(calls) == 40 and len(snapshot["events"]) == 16 and snapshot["omitted_events"] == 24
    assert snapshot["events"][0]["operation"] == "connect"
    assert "private" not in json.dumps(snapshot)


def test_already_sanitized_connect_failure_preserves_safe_driver_codes():
    fault = provisioning.failure(RuntimeError("08001", "PWD=never-print (53) (SQLDriverConnect)"))
    diagnostics = ControlDiagnostics()
    factory = diagnostics.factory(lambda: (_ for _ in ()).throw(fault), scope="gate")
    with pytest.raises(SqlFailure) as caught:
        factory()
    assert caught.value is fault and provisioning.failure(fault) is fault
    event = diagnostics.snapshot()["events"][0]
    assert event["sqlstate"] == "08001" and event["native_codes"] == [53]
    assert "never-print" not in json.dumps(diagnostics.snapshot())


def test_gate_optout_precedes_any_provisioning(monkeypatch):
    monkeypatch.delenv("DPONE_RUN_COMPOSITION_MSSQL_GATE_LIVE", raising=False)
    monkeypatch.setattr(support, "ProvisionedGate", lambda *_: pytest.fail("must not provision"))
    with pytest.raises(pytest.skip.Exception, match="explicit disposable SQL gate"):
        next(getattr(support.gate_environment, "__wrapped__")())


def test_batch_boundaries_preserve_exact_policy_definitions():
    batches = gate_batches("Control", "gate_control")
    assert login_trigger_sql("Control", "gate_control") == batches[-1]
    assert monotonic_trigger_sql("gate_control") in batches
    assert all("\nGO\n" not in batch for batch in batches)


@pytest.mark.parametrize("fail_permission", [None, "VIEW ANY DEFINITION"])
def test_external_provisioning_keeps_server_grants_in_master_and_lifeline_in_control(monkeypatch, fail_permission):
    from tests.test_composition_mssql_catalog_capture_provenance import check_provisioning_connections

    check_provisioning_connections(monkeypatch, fail_permission)


def test_policy_diagnostics_preserve_observed_nulls_and_hash_mismatch_without_sql_bodies(monkeypatch):
    environment = ProvisionedGate(SimpleNamespace(database="owned_control"))
    returned = iter(
        (
            (("owned_control", False, 123, b"x" * 32, 257, 257, b"s" * 16, "S", True, 1, 1),),
            ((False, 456, b"y" * 32),),
            ((None, 1, None, 1, None, 1),),
            ((None, 1, None, 1, None, 1, 1, 1),),
            (("G", "VIEW SERVER STATE"),),
        )
    )
    monkeypatch.setattr(environment, "sql", lambda *_: next(returned))
    observed = environment.observe_policy()
    assert observed["logon"][0]["sha256"] == (b"x" * 32).hex()
    assert observed["logon"][0]["sha256"] != observed["expected_logon_sha256"]
    assert observed["logon"][0]["reader_identity_matches"] == 1
    assert observed["controller_permissions"][0]["view_server_state_server_class"] is None
    assert observed["controller_permissions"][0]["view_server_state_null_class"] == 1
    assert observed["reader_permissions"][0]["effective_sid_matches"] == 1
    document = support.observation_document(observed)
    assert "CREATE TRIGGER" not in document and "SELECT " not in document
    assert json.loads(document)["reader_server_grants"] == [{"state": "G", "permission": "VIEW SERVER STATE"}]


def test_connection_scope_rejects_unowned_database_before_driver(monkeypatch):
    environment = ProvisionedGate(SimpleNamespace(database="owned", password="never-print", port=49152))
    with pytest.raises(RuntimeError, match="fixture_database_scope"):
        environment.connect(database="business_database")


def test_worker_without_issued_credentials_cannot_fall_back_to_administrator():
    case = object.__new__(support.GateCase)
    case.credentials = None
    case.target = "synthetic_target"
    case.environment = SimpleNamespace(connect=lambda **_: pytest.fail("must not open administrator connection"))
    case.connections = []
    with pytest.raises(RuntimeError, match="fixture_worker_credentials_required"):
        support.GateCase.worker(case)


def test_gate_optin_on_unsupported_host_never_provisions(monkeypatch):
    monkeypatch.setenv("DPONE_RUN_COMPOSITION_MSSQL_GATE_LIVE", "1")
    monkeypatch.setattr(support.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(support.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(support, "ProvisionedGate", lambda *_: pytest.fail("must not provision"))
    with pytest.raises(pytest.skip.Exception, match="Linux x86_64"):
        next(getattr(support.gate_environment, "__wrapped__")())


def test_sql_failure_keeps_only_safe_code_not_driver_credentials():
    class Cursor:
        def execute(self, *_):
            raise RuntimeError("42000", "DSN=private;PWD=never-print; permission denied (229) (SQLExecDirectW)")

        def close(self):
            pass

    connection = SimpleNamespace(cursor=lambda: Cursor())
    with pytest.raises(SqlFailure) as caught:
        execute(connection, "SELECT 1")
    assert caught.value.code == 229
    assert caught.value.sqlstate == "42000"
    assert "never-print" not in str(caught.value)
    assert "private" not in str(caught.value)


@pytest.mark.parametrize(
    "code,accepted", [(229, True), (17892, True), (18470, True), (102, False), (1222, False), (53, False)]
)
def test_permission_denial_codes_accept_only_documented_refusals(code, accepted):
    fault = SqlFailure(code, sqlstate="42000")

    def operation():
        raise fault

    if accepted:
        assert support.require_denied(operation) == code
    else:
        with pytest.raises(SqlFailure) as caught:
            support.require_denied(operation)
        assert caught.value is fault


@pytest.mark.parametrize("sqlstate,expected", [("42000", "42000"), ("PWD=never-print", None)])
def test_unclassified_denial_keeps_only_safe_sql_codes_in_junit(sqlstate, expected):
    fault = SqlFailure(54321, sqlstate=sqlstate)
    with pytest.raises(SqlFailure) as caught:
        support.require_denied(lambda: (_ for _ in ()).throw(fault))
    item = SimpleNamespace(
        nodeid="tests/integration/composition/test_composition_mssql_gate_live.py::test_case", user_properties=[]
    )
    report = SimpleNamespace(failed=True, longrepr="private", user_properties=[], sections=["private"])
    hook = support.pytest_runtest_makereport(item, SimpleNamespace(excinfo=SimpleNamespace(value=caught.value)))
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(SimpleNamespace(get_result=lambda: report))
    assert caught.value is fault and report.failed
    observed = json.loads(dict(item.user_properties)["dpone.gate.failure_sql"])
    assert observed == {"sqlstate": expected, "native_codes": [54321]}
    assert "never-print" not in json.dumps(item.user_properties) and report.sections == []


@pytest.mark.parametrize("payload", [{"password": "secret"}, {"evidence": {"dsn": "secret"}}, {"rows": "x" * 65537}])
def test_observation_writer_rejects_secrets_and_unbounded_payloads(payload):
    with pytest.raises(ValueError):
        support.observation_document(payload)


def test_observations_are_canonical_and_keep_actual_counts():
    document = support.observation_document({"sessions": 0, "rows": [[1, "synthetic"]]})
    assert json.loads(document) == {"sessions": 0, "rows": [[1, "synthetic"]]}
    assert document == '{"rows":[[1,"synthetic"]],"sessions":0}'


def test_unobserved_ack_loss_cannot_create_unknown_outcome():
    producer = OutcomeProducer(SimpleNamespace())
    with pytest.raises(CompositionAdmissionError, match="synthetic_unknown_event_missing"):
        producer.record_unknown()


def test_commit_failure_is_not_relabelled_as_injected_successful_commit():
    producer = OutcomeProducer(SimpleNamespace())
    connection = SimpleNamespace(commit=lambda: (_ for _ in ()).throw(RuntimeError("secret driver failure")))
    with pytest.raises(RuntimeError, match="synthetic_commit_failed") as caught:
        producer.lose_commit_ack(connection)
    assert "secret" not in str(caught.value)
    with pytest.raises(CompositionAdmissionError, match="synthetic_unknown_event_missing"):
        producer.record_unknown()


def test_bounded_wait_cannot_turn_missing_observation_into_pass():
    with pytest.raises(AssertionError, match="observation_timeout"):
        support.wait_until(lambda: False, timeout=0.01)


def test_lock_snapshot_retains_exact_scope_and_observed_modes_without_certifying(monkeypatch):
    from tests.integration.composition import test_composition_mssql_gate_recovery_live as recovery

    queries, recorded = [], []
    table = "[gate_control].[composition_login_gates]"
    locks = (("KEY", "RangeX-X", "GRANT", 5, 1234, 1, "TRANSACTION", 88, 51),)
    responses = iter((((88, 1, 2, True),), ((5, 91),), ((1234, 1, 1, 1, True, True),), locks))

    def query(statement, *values):
        queries.append((statement, values))
        return next(responses)

    monkeypatch.setattr(recovery, "execute", lambda *_: ((51, 5, 1, 1),))
    case = SimpleNamespace(
        table=lambda _: table, sql=query, record=lambda name, payload: recorded.append((name, payload))
    )
    assert recovery._record_lock_snapshot(case, SimpleNamespace(autocommit=False), 51) is None
    assert len(recorded) == 1 and recorded[0][0] == "row_lock_snapshot"
    observed = recorded[0][1]
    assert observed["lock_type_mode_status_dbid_entity_index_owner_kind_owner_id_spid"] == locks
    assert observed["control_database_and_table_ids"] == ((5, 91),)
    lock_sql, values = queries[-1]
    assert "TOP (65)" in lock_sql and "l.request_session_id=? AND l.resource_database_id=DB_ID()" in lock_sql
    assert "p.object_id=OBJECT_ID(?)" in lock_sql and "l.resource_associated_entity_id=OBJECT_ID(?)" in lock_sql
    assert values == (51, table, table)
    assert "RangeX-X" in support.observation_document(observed)
    assert "PASS" not in support.observation_document(observed)


@pytest.mark.parametrize("count,expected", [(0, False), (1, True)])
def test_blocking_key_lock_query_requires_exact_resource_mode_session_database_and_table(count, expected):
    from tests.integration.composition import test_composition_mssql_gate_recovery_live as recovery

    queries = []

    def query(statement, *values):
        queries.append((statement, values))
        return ((count,),)

    table = "[gate_control].[composition_login_gates]"
    case = SimpleNamespace(sql=query, table=lambda _: table)
    assert recovery._exclusive_gate_key_lock(case, 58) is expected
    statement, values = queries[0]
    # This SQL predicate is the observation contract: PAGE/IX and foreign
    # sessions/databases/table partitions must never satisfy it.
    assert "resource_type='KEY'" in statement and "request_mode IN ('X','RangeX-X')" in statement
    assert "request_status='GRANT'" in statement and "request_session_id=?" in statement
    assert "resource_database_id=DB_ID()" in statement
    assert "resource_associated_entity_id IN" in statement and "WHERE object_id=OBJECT_ID(?)" in statement
    assert " OR " not in statement and values == (58, table)


@pytest.mark.parametrize("expected,state", [(((1, "actual"),), "SUCCEEDED"), (((2, "invented"),), "FAILED")])
def test_reconciliation_derives_status_and_digest_from_reopened_rows(monkeypatch, expected, state):
    from dpone.contracts.airflow_deployment import canonical_fingerprint

    queries = []

    def query(sql):
        queries.append(sql)
        return ((1, "actual"),)

    producer = OutcomeProducer(SimpleNamespace(target_sql=query))
    monkeypatch.setattr(producer, "_persist", lambda outcome, evidence: (outcome, evidence))
    outcome, evidence = producer.reconcile(expected)
    assert outcome == state and len(queries) == 1
    assert evidence["observed_rows"] == ((1, "actual"),)
    assert evidence["observed_rows_sha256"] == canonical_fingerprint({"rows": ((1, "actual"),)})


def test_failure_report_retains_only_numeric_code_and_discards_capture():
    report = SimpleNamespace(failed=True, longrepr="PWD=never-print", user_properties=[], sections=["driver output"])
    call = SimpleNamespace(excinfo=SimpleNamespace(value=SqlFailure(229)))
    item = SimpleNamespace(
        nodeid="tests/integration/composition/test_composition_mssql_gate_live.py::test_case", user_properties=[]
    )
    hook = support.pytest_runtest_makereport(item, call)
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(SimpleNamespace(get_result=lambda: report))
    assert report.longrepr == "SQL gate component failed: sql_error_229"
    assert dict(report.user_properties) == {
        "dpone.gate.failure": "sql_error_229",
        "dpone.gate.failure_sql": '{"native_codes":[229],"sqlstate":null}',
    }
    assert item.user_properties == report.user_properties
    assert report.sections == []


@pytest.mark.parametrize("owned", [True, False])
def test_failure_locations_include_only_exact_owned_module_and_line(owned):
    def fail():
        raise AssertionError("PWD=never-print; private source and locals")

    module = Path(support.__file__).with_name("test_composition_mssql_gate_live.py")
    filename = module if owned else Path("/private/unrelated") / module.name
    operation = FunctionType(fail.__code__.replace(co_filename=str(filename)), {})
    with pytest.raises(AssertionError) as caught:
        operation()
    locations = support.failure_locations(caught.value)
    expected = [{"module": module.name, "line": fail.__code__.co_firstlineno + 1}] if owned else []
    assert locations == expected
    assert "never-print" not in json.dumps(locations) and "private" not in json.dumps(locations)


def test_failure_locations_are_bounded_and_survive_teardown_property_copy():
    def recurse(depth, operation):
        if depth:
            operation(depth - 1, operation)
        raise AssertionError("secret assertion payload")

    filename = str(Path(support.__file__).with_name("test_composition_mssql_gate_recovery_live.py"))
    operation = FunctionType(recurse.__code__.replace(co_filename=filename), {})
    with pytest.raises(AssertionError) as caught:
        operation(80, operation)
    item = SimpleNamespace(
        nodeid="tests/integration/composition/test_composition_mssql_gate_recovery_live.py::test_case",
        user_properties=[],
    )
    report = SimpleNamespace(failed=True, longrepr="secret", user_properties=[], sections=["secret"])
    hook = support.pytest_runtest_makereport(item, SimpleNamespace(excinfo=SimpleNamespace(value=caught.value)))
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(SimpleNamespace(get_result=lambda: report))
    # pytest snapshots item properties again for teardown, which finalizes JUnit.
    teardown_properties = list(item.user_properties)
    assert teardown_properties == report.user_properties
    locations = json.loads(dict(teardown_properties)["dpone.gate.failure_locations"])["frames"]
    assert len(locations) == 8
    assert all(
        row["module"] == "test_composition_mssql_gate_recovery_live.py" and type(row["line"]) is int
        for row in locations
    )
    assert "secret" not in json.dumps(teardown_properties) and report.sections == []


def test_unrelated_test_failure_keeps_original_diagnostics_and_properties():
    report = SimpleNamespace(
        failed=True, longrepr="useful traceback", user_properties=[("ordinary", "value")], sections=["useful output"]
    )
    item = SimpleNamespace(nodeid="tests/test_ordinary_behavior.py::test_case")
    call = SimpleNamespace(excinfo=SimpleNamespace(value=SqlFailure(229)))
    hook = support.pytest_runtest_makereport(item, call)
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(SimpleNamespace(get_result=lambda: report))
    assert report.longrepr == "useful traceback"
    assert report.user_properties == [("ordinary", "value")]
    assert report.sections == ["useful output"]


def test_serial_gate_profile_collects_all_cases_and_optout_is_only_skip(tmp_path):
    root = Path(__file__).resolve().parents[1]
    junit = tmp_path / "junit.xml"
    environment = os.environ | {
        "DPONE_RUN_COMPOSITION_MSSQL_GATE_LIVE": "0",
        "DPONE_RUN_COMPOSITION_MSSQL_LIVE": "0",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": os.pathsep.join((str(root / "src"), str(root))),
    }
    for key in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS"):
        environment.pop(key, None)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "tests.integration.composition.mssql_gate_live_support",
            "-p",
            "no:cacheprovider",
            "tests/integration/composition/test_composition_mssql_gate_live.py",
            "tests/integration/composition/test_composition_mssql_gate_recovery_live.py",
            "-o",
            "junit_family=xunit1",
            "--junitxml",
            str(junit),
            "--tb=no",
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0
    cases = ElementTree.parse(junit).getroot().findall(".//testcase")
    assert len(cases) == 16 and len({case.attrib["name"] for case in cases}) == 16
    assert all(case.find("skipped") is not None for case in cases)
    assert all(case.find("failure") is None and case.find("error") is None for case in cases)
    for case in cases:
        skipped = case.find("skipped")
        assert skipped is not None
        assert "explicit disposable SQL gate" in skipped.attrib["message"]
