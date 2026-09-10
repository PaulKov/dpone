"""Offline live-fixture boundaries; none is a SQL or cryptographic qualification."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree

import pytest

from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from tests.integration.composition import nonproduction_mssql_trust_live_support as support
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure
from tests.integration.composition.test_nonproduction_mssql_trust_live import DoomedTransactionCursor


def environment():
    token = "a" * 24
    return {
        "DPONE_RUN_COMPOSITION_MSSQL_TRUST_LIVE": "1",
        "DPONE_RUN_COMPOSITION_MSSQL_LIVE": "1",
        "DPONE_COMPOSITION_RUN_TOKEN": token,
        "DPONE_COMPOSITION_SQL_PORT": "49152",
        "DPONE_COMPOSITION_SQL_DATABASE": "dpone_composition_" + token,
        "MSSQL_SA_PASSWORD": "Dp1!" + "x" * 32,
    }


def test_optout_precedes_environment_parsing_or_connections():
    with pytest.raises(pytest.skip.Exception, match="explicit disposable SQL trust"):
        support.require_owned_database({}, system="Darwin", machine="arm64")


def test_local_arm64_never_connects_even_when_opted_in():
    with pytest.raises(pytest.skip.Exception, match="Linux x86_64"):
        support.require_owned_database(environment(), system="Darwin", machine="arm64")


@pytest.mark.parametrize(
    "field",
    [
        "DPONE_RUN_COMPOSITION_MSSQL_LIVE",
        "DPONE_COMPOSITION_RUN_TOKEN",
        "DPONE_COMPOSITION_SQL_PORT",
        "DPONE_COMPOSITION_SQL_DATABASE",
        "MSSQL_SA_PASSWORD",
    ],
)
def test_invalid_owned_runner_scope_is_failure_not_skip(field):
    values = environment()
    values[field] = "unowned_or_secret"
    with pytest.raises(RuntimeError, match="owned_synthetic_database_required") as caught:
        support.require_owned_database(values, system="Linux", machine="x86_64")
    assert "unowned_or_secret" not in str(caught.value)


def test_exact_runner_scope_is_accepted_without_opening_driver():
    value = support.require_owned_database(environment(), system="Linux", machine="x86_64")
    assert value.port == 49152 and value.database == "dpone_composition_" + "a" * 24
    assert "Dp1!" not in repr(value)


@pytest.mark.parametrize(
    "payload",
    [
        {"password": "secret"},
        {"rows": "secret"},
        {"rows": True},
        {"rows": -1},
        {"rows": 2**63},
        {"rows": [1]},
        {"policy_sha256": "secret"},
        {"sql_error": 0},
        {"unknown": 1},
    ],
)
def test_fixed_observations_reject_unbounded_or_secret_bearing_values(payload):
    with pytest.raises(ValueError):
        support.observation_document(payload)


def test_fixed_observation_preserves_actual_counts_and_digest():
    payload = {"rows": 2, "revision": 3, "policy_sha256": "sha256:" + "a" * 64}
    raw = support.observation_document(payload)
    assert json.loads(raw) == payload and len(raw) < 1024


@pytest.mark.parametrize("state", [-1, 0, 1])
def test_transaction_observations_preserve_signed_server_state(state):
    payload = {"transaction_count": 1, "xact_state": state, "lock_mode": "Exclusive", "lock_result": -999}
    assert json.loads(support.observation_document(payload)) == payload


@pytest.mark.parametrize("payload", [{"xact_state": 2}, {"xact_state": True}, {"lock_mode": "PWD=secret"}])
def test_transaction_observations_reject_unknown_states_or_raw_strings(payload):
    with pytest.raises(ValueError):
        support.observation_document(payload)


def test_transaction_snapshot_records_actual_rows_before_assertions(monkeypatch):
    properties = []
    case = support.TrustCase(SimpleNamespace(), lambda name, value: properties.append((name, value)))
    monkeypatch.setattr(support, "execute", lambda *args: ((1, -1, "Exclusive"),))
    assert case.record_transaction("transaction_after_fault", object()) == ((1, -1, "Exclusive"),)
    assert len(properties) == 1 and properties[0][0] == "dpone.trust.transaction_after_fault"
    assert json.loads(properties[0][1]) == {
        "result_rows": 1,
        "lock_mode": "Exclusive",
        "transaction_count": 1,
        "xact_state": -1,
    }


def test_transaction_snapshot_guards_transaction_owned_lock_lookup(monkeypatch):
    def observe(connection, sql, *parameters):
        assert "IF @transaction_count > 0 AND @transaction_state = 1" in sql
        assert sql.index("IF @transaction_count") < sql.index("APPLOCK_MODE(")
        assert parameters == (support.COMPOSITION_MSSQL_LEDGER_LOCK,)
        return ((0, 0, "NoLock"),)

    monkeypatch.setattr(support, "execute", observe)
    case = support.TrustCase(SimpleNamespace(), lambda *args: None)
    assert case.record_transaction("transaction_initial", object()) == ((0, 0, "NoLock"),)


class FaultCursor:
    """Result-set choreography only; never evidence that SQL executed."""

    def __init__(self, sets):
        self.sets, self.index, self.calls = sets, 0, []

    @property
    def description(self):
        return None if self.sets[self.index] is None else ("columns",)

    def execute(self, sql, *parameters):
        self.calls.append((sql, parameters))

    def fetchall(self):
        return self.sets[self.index]

    def nextset(self):
        self.index += 1
        return self.index < len(self.sets)


def test_doomed_wrapper_preserves_original_sql_parameters_and_real_result_object():
    original = "SELECT @@TRANCOUNT, XACT_STATE(), APPLOCK_MODE(N'public', ?, N'Transaction');"
    observed = [(1, -1, "Exclusive")]
    cursor = FaultCursor([None, [(2627, 1, -1)], None, observed])
    boundary = DoomedTransactionCursor(cursor, lambda *args: None)
    boundary.execute(original, support.COMPOSITION_MSSQL_LEDGER_LOCK)
    assert boundary.fetchall() is observed
    sql, parameters = cursor.calls[0]
    assert sql.count(original) == 1 and parameters == (support.COMPOSITION_MSSQL_LEDGER_LOCK,)
    assert sql.index("BEGIN CATCH") < sql.index(original) < sql.index("ROLLBACK TRANSACTION")
    assert boundary.complete and cursor.index == len(cursor.sets)
    assert boundary.fault_rows == ((2627, 1, -1),) and boundary.precondition_rows == tuple(observed)
    with pytest.raises(RuntimeError, match="fault_cursor_reuse"):
        boundary.execute(original, *parameters)
    with pytest.raises(RuntimeError, match="fault_cursor_read"):
        boundary.fetchall()
    assert len(cursor.calls) == 1


@pytest.mark.parametrize("fault", [[(102, 1, -1)], [(2627, 1, 1)], []])
def test_doomed_wrapper_requires_actual_constraint_error_and_uncommittable_state(fault):
    cursor = FaultCursor([fault, [(1, -1, "NoLock")]])
    boundary = DoomedTransactionCursor(cursor, lambda *args: None)
    with pytest.raises(RuntimeError, match="fault_not_doomed"):
        boundary.execute("SELECT XACT_STATE();", support.COMPOSITION_MSSQL_LEDGER_LOCK)
    assert not boundary.complete and boundary.precondition_rows == ()


@pytest.mark.parametrize("extra", [None, [(123,)]])
def test_doomed_wrapper_requires_complete_batch_without_extra_results(extra):
    cursor = FaultCursor([[(2627, 1, -1)], [(1, -1, "NoLock")], extra])
    boundary = DoomedTransactionCursor(cursor, lambda *args: None)
    boundary.execute("SELECT XACT_STATE();", support.COMPOSITION_MSSQL_LEDGER_LOCK)
    if extra is None:
        assert boundary.fetchall() == [(1, -1, "NoLock")] and boundary.complete
    else:
        with pytest.raises(RuntimeError, match="fault_extra_result"):
            boundary.fetchall()
        assert not boundary.complete


@pytest.mark.parametrize("operation", ["execute", "nextset"])
def test_doomed_wrapper_driver_failure_is_incomplete_and_cannot_retry(monkeypatch, operation):
    cursor = FaultCursor([[(2627, 1, -1)], [(1, -1, "NoLock")]])
    boundary = DoomedTransactionCursor(cursor, lambda *args: None)

    def fail(*args):
        raise RuntimeError("driver_failure")

    if operation == "execute":
        monkeypatch.setattr(cursor, operation, fail)
        with pytest.raises(RuntimeError, match="driver_failure"):
            boundary.execute("SELECT XACT_STATE();", support.COMPOSITION_MSSQL_LEDGER_LOCK)
    else:
        boundary.execute("SELECT XACT_STATE();", support.COMPOSITION_MSSQL_LEDGER_LOCK)
        monkeypatch.setattr(cursor, operation, fail)
        with pytest.raises(RuntimeError, match="driver_failure"):
            boundary.fetchall()
    assert not boundary.complete
    with pytest.raises(RuntimeError, match="fault_cursor_reuse"):
        boundary.execute("SELECT XACT_STATE();", support.COMPOSITION_MSSQL_LEDGER_LOCK)
    with pytest.raises(RuntimeError, match="fault_cursor_read"):
        boundary.fetchall()


def report():
    return SimpleNamespace(
        failed=True,
        skipped=False,
        longrepr="PWD=never-print",
        sections=["raw SQL"],
        user_properties=[("unsafe", "never-print"), ("dpone.trust.readback", '{"rows":1}')],
    )


@pytest.mark.parametrize(
    "error,reason",
    [
        (SqlFailure(229), "sql_error_229"),
        (NonproductionAuthorityError("trust_revision_changed"), "trust_revision_changed"),
        (NonproductionAuthorityError("private_credentials"), "assertion_or_fixture_failure"),
        (RuntimeError("never-print"), "assertion_or_fixture_failure"),
    ],
)
def test_exact_live_failure_sanitizer_keeps_only_fixed_diagnostics(error, reason):
    value = report()
    item = SimpleNamespace(nodeid=support.LIVE_MODULE + "::test_case")
    call = SimpleNamespace(excinfo=SimpleNamespace(value=error))
    support.sanitize_report(item, call, value)
    assert value.longrepr == "SQL trust component failed: " + reason
    assert value.sections == []
    assert value.user_properties == [("dpone.trust.readback", '{"rows":1}'), ("dpone.trust.failure", reason)]
    assert "never-print" not in repr(value)


def test_safe_named_property_cannot_smuggle_arbitrary_text_or_nested_values():
    value = report()
    value.user_properties = [
        ("dpone.trust.readback", '{"rows":"secret"}'),
        ("dpone.trust.failure", "secret"),
        ("dpone.trust.readback", "x" * 8193),
    ]
    support.sanitize_report(
        SimpleNamespace(nodeid=support.LIVE_MODULE + "::test_case"), SimpleNamespace(excinfo=None), value
    )
    assert value.user_properties == [("dpone.trust.failure", "assertion_or_fixture_failure")]


def test_unrelated_reports_are_not_sanitized_or_relabelled():
    value = report()
    before = repr(value)
    support.sanitize_report(
        SimpleNamespace(nodeid="tests/test_other.py::test_case"), SimpleNamespace(excinfo=None), value
    )
    assert repr(value) == before


def run_child_pytest(tmp_path, *, script=None, case=None):
    root = Path(__file__).resolve().parents[1]
    junit = tmp_path / "junit.xml"
    result = subprocess.run(
        [
            sys.executable,
            *(["-c", script] if script else ["-m", "pytest"]),
            "-p",
            "tests.integration.composition.nonproduction_mssql_trust_live_support",
            "-p",
            "no:cacheprovider",
            support.LIVE_MODULE + ("::" + case if case else ""),
            "-o",
            "junit_family=xunit1",
            "--junitxml",
            str(junit),
            "--tb=no",
        ],
        cwd=root,
        env=os.environ
        | {
            "DPONE_RUN_COMPOSITION_MSSQL_TRUST_LIVE": "0",
            "DPONE_RUN_COMPOSITION_MSSQL_LIVE": "0",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": os.pathsep.join((str(root / "src"), str(root))),
            "PYTEST_ADDOPTS": "",
            "PYTEST_PLUGINS": "",
        },
        capture_output=True,
        check=False,
        timeout=60,
    )
    return result, junit


def test_failure_diagnostics_survive_real_pytest_teardown_into_junit(tmp_path):
    script = """
import sys
import pytest
from tests.integration.composition import nonproduction_mssql_trust_live_support as support
class Fault:
    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_setup(self, item):
        support.observation_document({'password': 'never-print'})
raise SystemExit(pytest.main(sys.argv[1:], plugins=[Fault()]))
"""
    result, junit = run_child_pytest(
        tmp_path, script=script, case="test_insufficient_transaction_lock_never_returns_trust"
    )
    assert result.returncode == 1
    case = ElementTree.parse(junit).getroot().find(".//testcase")
    assert case is not None and case.find("error") is not None
    properties = {item.attrib["name"]: item.attrib["value"] for item in case.findall("properties/property")}
    assert properties["dpone.trust.failure"] == "assertion_or_fixture_failure"
    assert properties["dpone.trust.failure_location"].startswith("nonproduction_mssql_trust_live_support.py:")
    assert "never-print" not in junit.read_text()


def test_fixed_report_inventory_cannot_grow_through_duplicate_properties():
    value = report()
    value.failed = False
    value.user_properties = [("dpone.trust.readback", '{"rows":1}')] * 100
    support.sanitize_report(
        SimpleNamespace(nodeid=support.LIVE_MODULE + "::test_case"), SimpleNamespace(excinfo=None), value
    )
    assert value.user_properties == [("dpone.trust.readback", '{"rows":1}')]


def test_malformed_property_names_or_error_reasons_cannot_break_redaction(monkeypatch):
    value = report()
    value.user_properties = [([], "secret")]
    error = NonproductionAuthorityError("fixed")
    monkeypatch.setattr(error, "reason", [])
    support.sanitize_report(
        SimpleNamespace(nodeid=support.LIVE_MODULE + "::test_case"),
        SimpleNamespace(excinfo=SimpleNamespace(value=error)),
        value,
    )
    assert value.longrepr == "SQL trust component failed: assertion_or_fixture_failure"
    assert "secret" not in repr(value)


def test_owned_fault_restores_and_checks_schema_even_after_assertion(monkeypatch):
    events = []
    case = object.__new__(support.TrustCase)
    monkeypatch.setattr(case, "require_schema", lambda: events.append("schema_checked"))
    with pytest.raises(AssertionError, match="actual failure"):
        with case.fault(lambda: events.append("changed"), lambda: events.append("restored")):
            raise AssertionError("actual failure")
    assert events == ["changed", "restored", "schema_checked"]


def test_cleanup_failure_is_never_suppressed_as_success(monkeypatch):
    case = object.__new__(support.TrustCase)
    monkeypatch.setattr(case, "require_schema", lambda: None)

    def fail():
        raise RuntimeError("restoration_failed")

    with pytest.raises(RuntimeError, match="restoration_failed"):
        with case.fault(lambda: None, fail):
            pass


def test_lost_ack_injection_happens_only_after_successful_real_commit():
    events = []
    boundary = support.LostReadAcknowledgement(SimpleNamespace(commit=lambda: events.append("commit")))
    with pytest.raises(ConnectionError, match="synthetic_read_ack_lost"):
        boundary.commit()
    assert events == ["commit"] and boundary.committed is True


def test_failed_real_commit_cannot_be_counted_as_injected_ack_loss():
    def fail():
        raise RuntimeError("actual commit failure")

    boundary = support.LostReadAcknowledgement(SimpleNamespace(commit=fail))
    with pytest.raises(RuntimeError, match="actual commit failure"):
        boundary.commit()
    assert boundary.committed is False


def test_syntax_error_cannot_count_as_permission_or_append_rejection():
    with pytest.raises(AssertionError):
        support.require_sql_rejection(lambda: (_ for _ in ()).throw(SqlFailure(102)), {229, 51000})


def test_exact_nine_case_optout_runs_plugin_and_produces_only_skips(tmp_path):
    result, junit = run_child_pytest(tmp_path)
    assert result.returncode == 0
    cases = ElementTree.parse(junit).getroot().findall(".//testcase")
    expected = {
        "test_external_trust_ddl_and_original_byte_readback",
        "test_append_only_revision_rejects_update_delete_and_replay",
        "test_trust_revision_change_blocks_same_ledger_compare",
        "test_concurrent_appends_admit_one_next_revision",
        "test_invalid_original_hash_or_epoch_cannot_append",
        "test_read_rejects_changed_trigger_or_schema",
        "test_insufficient_transaction_lock_never_returns_trust",
        "test_lost_read_ack_returns_no_trusted_revision",
        "test_provisioner_can_append_without_schema_bypass",
    }
    assert len(cases) == 9 and {case.attrib["name"] for case in cases} == expected
    assert all(case.find("skipped") is not None for case in cases)
    assert all(case.find("failure") is None and case.find("error") is None for case in cases)
    assert "never-print" not in junit.read_text()
