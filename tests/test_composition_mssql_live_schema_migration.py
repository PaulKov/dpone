"""Offline migration boundaries; captures and doubles never certify live SQL."""

import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from xml.etree import ElementTree

import pytest

from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_control import CompositionAdmissionError
from dpone.contracts.composition_ownership import CompositionOwnerReference
from dpone.contracts.composition_persistence import encode_attempt_identity, encode_attempt_proof
from tests.composition_mssql_gate_helpers import SERVICE, attempt, occurrence
from tests.integration.composition import mssql_gate_live_outcomes as outcomes
from tests.integration.composition import mssql_gate_live_provisioning as provisioning
from tests.integration.composition import mssql_gate_live_support as gate
from tests.integration.composition import mssql_store_live_support as store
from tests.integration.composition import test_composition_mssql_store_live as live

CATALOG = {
    "schema": "controlled-catalog",
    "ddl_sha256": "sha256:" + "a" * 64,
    "checks": [["authority", "ck_original", "exact ( [x] )" + " " * 4096, 0, 0, 0, 0, 0, 0]],
}
CONTEXT = {
    "database": "owned",
    "compatibility_level": 160,
    "options_mask": 5496,
    "product_version": "16.0.4265.3",
    "product_version_status": "OBSERVED",
    "session_id": 51,
}


def cursor():
    return SimpleNamespace(execute=Mock(), nextset=Mock(return_value=False), description=None, close=Mock())


def test_core_originals_are_retained_before_a_later_bootstrap_failure(monkeypatch):
    sql_cursor = cursor()
    connection = SimpleNamespace(cursor=lambda: sql_cursor, close=Mock())
    case = store.SqlCase(SimpleNamespace(connect=lambda: connection, database="owned"))
    events: list[object] = []

    def capture_original(*_):
        events.append("captured")
        return CATALOG

    capture = Mock(side_effect=capture_original)
    monkeypatch.setattr(store, "capture_check_catalog", capture, raising=False)
    monkeypatch.setattr(
        store,
        "catalog_context",
        lambda selected, database: CONTEXT if selected is sql_cursor and database == "owned" else None,
        raising=False,
    )
    monkeypatch.setattr(case, "sql", Mock(side_effect=RuntimeError("later_bootstrap_failure")))
    with pytest.raises(RuntimeError, match="later_bootstrap_failure"):
        case.install(lambda name, value: events.append((name, json.loads(value))))
    assert events == [
        "captured",
        ("dpone.composition.check_catalog", CATALOG),
        ("dpone.composition.check_catalog_context", CONTEXT),
    ]
    assert sql_cursor.nextset.called and sql_cursor.close.called and connection.close.called
    assert capture.call_args.args == (sql_cursor, case.schema)


def test_gate_capture_precedes_constructor_work_that_can_fail():
    events = []
    environment = SimpleNamespace(
        check_catalog=lambda: CATALOG,
        check_catalog_context=lambda: CONTEXT,
        new_target=Mock(side_effect=RuntimeError("constructor_failure")),
    )
    with pytest.raises(RuntimeError, match="constructor_failure"):
        gate.GateCase(environment, lambda name, value: events.append((name, json.loads(value))))
    assert events == [("dpone.gate.check_catalog", CATALOG), ("dpone.gate.check_catalog_context", CONTEXT)]


def test_core_capture_budget_fails_before_property_or_bootstrap(monkeypatch):
    sql_cursor = cursor()
    connection = SimpleNamespace(cursor=lambda: sql_cursor, close=Mock())
    case = store.SqlCase(SimpleNamespace(connect=lambda: connection))
    monkeypatch.setattr(store, "capture_check_catalog", lambda *_: {"original": "x" * 65536})
    sql, record = Mock(), Mock()
    monkeypatch.setattr(case, "sql", sql)
    with pytest.raises(RuntimeError, match="^synthetic_sql_ddl_failed$"):
        case.install(record)
    record.assert_not_called()
    sql.assert_not_called()
    assert sql_cursor.close.called and connection.close.called


def test_gate_capture_uses_owned_lifeline_and_closes_cursor(monkeypatch):
    sql_cursor = cursor()
    environment = provisioning.ProvisionedGate(SimpleNamespace(database="owned"))
    monkeypatch.setattr(environment, "connect", lambda: SimpleNamespace(cursor=lambda: sql_cursor))
    monkeypatch.setattr(environment, "recover", Mock(return_value=(("controller", b"sid"),)))
    monkeypatch.setattr(environment, "sql", Mock())
    monkeypatch.setattr(provisioning, "gate_batches", lambda *_: ("DDL one", "DDL two", "server trigger"))
    capture = Mock(return_value=CATALOG)
    monkeypatch.setattr(provisioning, "capture_gate_check_catalog", capture, raising=False)
    context = Mock(return_value=CONTEXT)
    monkeypatch.setattr(provisioning, "catalog_context", context)
    sequence = Mock()
    for name, method in (("execute", sql_cursor.execute), ("catalog", capture), ("context", context)):
        sequence.attach_mock(method, name)
    environment.install()
    assert environment.check_catalog() == CATALOG
    assert environment.check_catalog_context() == CONTEXT
    capture.assert_called_once_with(sql_cursor, environment.schema)
    context.assert_called_once_with(sql_cursor, "owned")
    assert [call.args[0] for call in sql_cursor.execute.call_args_list] == ["DDL one", "DDL two"]
    assert [call[0] for call in sequence.mock_calls] == ["execute", "execute", "catalog", "context"]
    sql_cursor.close.assert_called_once_with()


@pytest.mark.parametrize("stage", [None, "disable", "mutation", "restore"])
def test_owned_invariant_is_restored_even_when_fault_setup_or_body_fails(stage):
    events = []

    def sql(statement):
        events.append(statement)
        if (stage == "disable" and statement.startswith("DISABLE")) or (
            stage == "restore" and statement.startswith("ENABLE")
        ):
            raise RuntimeError(stage)

    def inject():
        with store.invariant_fault(sql, "owned", "proofs"):
            events.append("mutation")
            if stage == "mutation":
                raise RuntimeError(stage)

    if stage:
        with pytest.raises(RuntimeError, match=stage):
            inject()
    else:
        inject()
    assert events[0] == "DISABLE TRIGGER [owned].[composition_proofs_invariant] ON [owned].[composition_proofs];"
    assert events[-1] == "ENABLE TRIGGER [owned].[composition_proofs_invariant] ON [owned].[composition_proofs];"
    assert events.count("mutation") == (0 if stage == "disable" else 1)


@pytest.mark.parametrize("schema,table", [("foreign.schema", "proofs"), ("owned", "login_gates")])
def test_fault_cannot_select_arbitrary_schema_or_module(schema, table):
    sql = Mock()
    with pytest.raises(ValueError):
        with store.invariant_fault(sql, schema, table):
            pytest.fail("invalid fault target")
    sql.assert_not_called()


@pytest.mark.parametrize("late_error", [False, True])
def test_store_sql_drains_later_results_and_surfaces_trigger_failure(late_error):
    sql_cursor = cursor()
    sql_cursor.description = ("value",)
    sql_cursor.fetchall = Mock(side_effect=[[(1,)], [(2,)]])
    sql_cursor.nextset = Mock(side_effect=[True, RuntimeError("private-driver") if late_error else False])
    connection = SimpleNamespace(cursor=lambda: sql_cursor, close=Mock())
    case = store.SqlCase(SimpleNamespace(connect=lambda: connection))
    if late_error:
        with pytest.raises(RuntimeError, match="synthetic_sql_statement_failed") as caught:
            case.sql("controlled batch")
        assert "private-driver" not in str(caught.value)
    else:
        assert case.sql("controlled batch") == ((1,), (2,))
    assert sql_cursor.nextset.call_count == 2 and sql_cursor.close.called and connection.close.called


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_direct_operation_has_one_locked_transaction_and_original_partition(monkeypatch, state):
    parent, identity = occurrence(), attempt()
    sql_cursor = cursor()
    events = []
    case = SimpleNamespace(
        schema="owned", service_id=SERVICE, request=parent.request, database=SimpleNamespace(connect=Mock())
    )

    @contextmanager
    def transaction(factory, schema, service):
        assert (factory, schema, service) == (case.database.connect, case.schema, SERVICE)
        events.append("locked")
        yield SimpleNamespace(cursor=sql_cursor, table=lambda name: "[owned].[composition_" + name + "]")
        events.append("committed")

    monkeypatch.setattr(live, "composition_control_transaction", transaction, raising=False)
    live.seed_unresolved(case, identity, state)
    calls = sql_cursor.execute.call_args_list
    assert events == ["locked", "committed"]
    assert "'RUNNING'" in calls[0].args[0] and "'execution'" in calls[0].args[0]
    owner = CompositionOwnerReference("execution", parent.request.activation_id).owner_key
    assert calls[0].args[1:] == (
        identity.attempt_sha256,
        owner,
        parent.request.request_sha256,
        identity.attempt_sha256,
        encode_attempt_identity(identity),
    )
    partitions = [call for call in calls if "INSERT INTO [owned].[composition_operation_domains]" in call.args[0]]
    assert [call.args[1:] for call in partitions] == [
        (identity.attempt_sha256, owner, guard, epoch) for guard, epoch in identity.guard_epochs
    ]
    assert len(calls) == 1 + len(partitions) + (state == "COMMIT_UNKNOWN")
    assert sql_cursor.nextset.call_count == len(calls)


def test_outcome_original_and_proof_share_locked_commit(monkeypatch):
    identity, sql_cursor, events = attempt(), cursor(), []
    sid = b"s" * 16
    environment = SimpleNamespace(connect=Mock(), schema="owned", service_id=SERVICE)
    documents = []

    def execute(statement, *parameters):
        if "synthetic_outcomes" in statement:
            documents.append(parameters[-1])

    sql_cursor.execute.side_effect = execute

    def read(statement, *_):
        if "issued_authorities" in statement:
            return (("mssql", SERVICE, "mssql-sid:" + sid.hex()),)
        if "SELECT kind" in statement:
            return (("CLOSED_GATES",), ("QUIESCENCE",))
        return ((documents[0],),)

    case = SimpleNamespace(
        environment=environment,
        attempt=identity,
        pins=(7, "physical"),
        sql=read,
        attempts=SimpleNamespace(read_exact=lambda _: SimpleNamespace(state="RUNNING")),
        gate_row=lambda: (sid, "issued", "CLOSED", None),
        record=Mock(),
        table=lambda name: "[owned].[composition_" + name + "]",
    )

    @contextmanager
    def transaction(factory, schema, service):
        assert (factory, schema, service) == (environment.connect, "owned", SERVICE)
        events.append("locked")
        yield SimpleNamespace(cursor=sql_cursor)
        events.append("committed")

    monkeypatch.setattr(outcomes, "composition_control_transaction", transaction, raising=False)
    proof = outcomes.OutcomeProducer(case)._persist("SUCCEEDED", {"rows_reconciled": True})
    assert events == ["locked", "committed"] and sql_cursor.nextset.call_count == 2
    insert = sql_cursor.execute.call_args_list[-1]
    assert "operation_family" in insert.args[0] and "'execution'" in insert.args[0]
    assert insert.args[1:] == (identity.attempt_sha256, proof.proof_sha256, encode_attempt_proof(proof))
    assert json.loads(documents[0])["attempt_sha256"] == identity.attempt_sha256


@pytest.mark.parametrize("failure_stage", ["execute", "nextset"])
def test_direct_operation_failure_rolls_back_real_boundary_without_commit(monkeypatch, failure_stage):
    sql_cursor = cursor()
    getattr(sql_cursor, failure_stage).side_effect = RuntimeError("private-driver")
    connection = SimpleNamespace(cursor=lambda: sql_cursor, close=Mock(), commit=Mock(), rollback=Mock())
    case = SimpleNamespace(
        schema="owned",
        service_id=SERVICE,
        request=occurrence().request,
        database=SimpleNamespace(connect=lambda: connection),
    )
    begin, recheck = Mock(return_value=19), Mock()
    monkeypatch.setattr(CompositionMssqlLedger, "begin", begin)
    monkeypatch.setattr(CompositionMssqlLedger, "require_transaction", recheck)
    with pytest.raises(CompositionAdmissionError) as rejected:
        live.seed_unresolved(case, attempt(), "RUNNING")
    assert rejected.value.reason == "control_operation_unknown"
    begin.assert_called_once_with(SERVICE)
    recheck.assert_not_called()
    connection.rollback.assert_called_once_with()
    connection.commit.assert_not_called()
    assert connection.autocommit is False and sql_cursor.close.called and connection.close.called


@pytest.mark.parametrize("profile", ["store", "gate"])
def test_original_catalog_survives_real_pytest_failure_and_teardown(tmp_path, profile):
    """Run the actual selected live test with a failing offline fixture boundary."""
    root = Path(__file__).resolve().parents[1]
    context = dict(CONTEXT)
    if profile == "gate":
        context.update(product_version=None, product_version_status="UNVERIFIED")
    plugin = tmp_path / "catalog_probe.py"
    plugin.write_text(
        "import json\n"
        "from types import SimpleNamespace\n"
        "from tests.integration.composition import mssql_store_live_support as store\n"
        "from tests.integration.composition import mssql_gate_live_support as gate\n"
        f"CATALOG = {CATALOG!r}\n"
        f"CONTEXT = {context!r}\n"
        "def fail(*args): raise RuntimeError('private-canary')\n"
        "def install(self, record_property=None):\n"
        "    record_property('dpone.composition.check_catalog', json.dumps(CATALOG))\n"
        "    record_property('dpone.composition.check_catalog_context', json.dumps(CONTEXT))\n"
        "    fail()\n"
        "class Environment:\n"
        "    def __init__(self, *args): pass\n"
        "    def install(self): pass\n"
        "    def close(self): pass\n"
        "    def observe_policy(self): return {}\n"
        "    def check_catalog(self): return CATALOG\n"
        "    def check_catalog_context(self): return CONTEXT\n"
        "    def new_target(self): fail()\n"
        "def pytest_configure(config):\n"
        "    store.OwnedDatabase.from_environment = lambda env: SimpleNamespace(connect=fail)\n"
        "    store.SqlCase.sql = lambda *args: ((0,),)\n"
        "    store.SqlCase.install = install\n"
        "    gate.platform.system = lambda: 'Linux'\n"
        "    gate.platform.machine = lambda: 'x86_64'\n"
        "    gate.ProvisionedGate = Environment\n"
    )
    target = (
        "test_composition_mssql_store_live.py::test_external_ddl_and_mixed_engine_lifecycle"
        if profile == "store"
        else "test_composition_mssql_gate_live.py::test_installed_gate_policy_and_reader_permissions"
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("DPONE_COMPOSITION_") and not key.startswith("DPONE_RUN_COMPOSITION_")
    }
    env.update(
        PYTHONPATH=os.pathsep.join((str(tmp_path), str(root / "src"), str(root))),
        PYTHONDONTWRITEBYTECODE="1",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        DPONE_RUN_COMPOSITION_MSSQL_LIVE="1",
        DPONE_RUN_COMPOSITION_MSSQL_GATE_LIVE="1",
    )
    for key in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS"):
        env.pop(key, None)
    junit = tmp_path / "junit.xml"
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "-p",
            "catalog_probe",
            "-p",
            f"tests.integration.composition.mssql_{profile}_live_support",
            "-p",
            "no:cacheprovider",
            "tests/integration/composition/" + target,
            "-o",
            "addopts=",
            "-o",
            "junit_family=xunit1",
            "--tb=no",
            "--show-capture=no",
            "--junitxml",
            str(junit),
        ],
        cwd=root,
        env=env,
        capture_output=True,
        timeout=60,
    )
    assert result.returncode == 1
    cases = ElementTree.parse(junit).getroot().findall(".//testcase")
    assert len(cases) == 1 and cases[0].find("skipped") is None
    assert cases[0].find("failure") is not None or cases[0].find("error") is not None
    properties = {p.attrib["name"]: p.attrib["value"] for p in cases[0].findall("properties/property")}
    prefix = "composition" if profile == "store" else "gate"
    assert json.loads(properties[f"dpone.{prefix}.check_catalog"]) == CATALOG
    assert json.loads(properties[f"dpone.{prefix}.check_catalog_context"]) == context
    assert "private-canary" not in junit.read_text()
