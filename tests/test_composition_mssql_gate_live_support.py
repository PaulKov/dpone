"""Offline fixture/evidence boundaries; doubles here never qualify live SQL."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree

import pytest

from dpone.adapters.composition_mssql_gate_schema import login_trigger_sql, monotonic_trigger_sql
from dpone.contracts.composition_control import CompositionAdmissionError
from tests.integration.composition import mssql_gate_live_provisioning as provisioning
from tests.integration.composition import mssql_gate_live_support as support
from tests.integration.composition.mssql_gate_live_outcomes import OutcomeProducer
from tests.integration.composition.mssql_gate_live_provisioning import (
    ProvisionedGate,
    SqlFailure,
    execute,
    gate_batches,
)


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
    environment = ProvisionedGate(SimpleNamespace(database="owned_control"))
    connections, statements = [], []

    def connect(*, database=None):
        connection = SimpleNamespace(database=database or "owned_control", closed=False)
        connection.close = lambda: setattr(connection, "closed", True)
        connections.append(connection)
        return connection

    def observe(connection, statement, *_):
        statements.append((connection.database, statement))
        if statement.startswith("GRANT VIEW") and connection.database != "master":
            raise SqlFailure(4621)
        if fail_permission and statement == f"GRANT {fail_permission} TO [dpone_gate_reader];":
            raise SqlFailure(229)
        if "ON ALL SERVER" in statement:
            assert connection.database == "master"
        if "CREATE USER" in statement or statement.startswith("GRANT CONNECT"):
            assert connection.database == "owned_control"
        return (("sa", b"controller"),) if statement.startswith("SELECT ORIGINAL_LOGIN") else ()

    monkeypatch.setattr(environment, "connect", connect)
    monkeypatch.setattr(provisioning, "execute", observe)
    if fail_permission:
        with pytest.raises(SqlFailure) as failure:
            environment.install()
        assert failure.value.code == 229
    else:
        environment.install()
    server_grants = [(database, sql) for database, sql in statements if sql.startswith("GRANT VIEW")]
    permissions = ("VIEW SERVER STATE", "VIEW ANY DEFINITION", "VIEW SERVER PERFORMANCE STATE")
    expected = permissions[:2] if fail_permission else permissions
    assert server_grants == [("master", f"GRANT {permission} TO [dpone_gate_reader];") for permission in expected]
    if not fail_permission:
        assert ("master", login_trigger_sql("owned_control", environment.schema)) in statements
    lifeline = environment.lifeline
    assert lifeline is not None and lifeline.database == "owned_control" and not lifeline.closed
    assert all(connection.closed for connection in connections if connection is not environment.lifeline)
    environment.close()


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
    assert "never-print" not in str(caught.value)
    assert "private" not in str(caught.value)


def test_syntax_failure_cannot_be_counted_as_permission_denial():
    with pytest.raises(AssertionError):
        support.require_denied(lambda: (_ for _ in ()).throw(SqlFailure(102)))


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
    item = SimpleNamespace(nodeid="tests/integration/composition/test_composition_mssql_gate_live.py::test_case")
    hook = support.pytest_runtest_makereport(item, call)
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(SimpleNamespace(get_result=lambda: report))
    assert report.longrepr == "SQL gate component failed: sql_error_229"
    assert report.user_properties == [("dpone.gate.failure", "sql_error_229")]
    assert report.sections == []


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
