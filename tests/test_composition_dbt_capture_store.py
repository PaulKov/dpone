"""Protected ledger/store fault model; no live SQL or catalog certification."""

from dataclasses import replace

import pytest

from dpone.adapters.composition_dbt_capture_schema import (
    DBT_CAPTURE_TABLES,
    dbt_capture_trigger_sql,
    render_dbt_capture_schema,
)
from dpone.adapters.composition_dbt_capture_store import MssqlDbtCaptureStore
from dpone.adapters.composition_mssql_catalog_types import CompositionTrigger
from dpone.contracts.composition_dbt_outcome import (
    DbtArtifactOriginal,
    DbtCaptureError,
    DbtCaptureRecord,
    DbtChildExit,
    DbtExitRecord,
)
from dpone.contracts.strict_json import canonical_json_bytes
from tests import composition_mssql_attempt_fixtures
from tests.composition_mssql_catalog_helpers import expected_rows
from tests.composition_mssql_gate_helpers import SERVICE, attempt
from tests.composition_mssql_store_fault_model import Cursor
from tests.test_composition_dbt_capture_codec import subject

active = composition_mssql_attempt_fixtures.active


class CaptureCursor(Cursor):
    def _select_or_mutate(self, sql, parameters):
        data = self.connection.data
        if "FROM [dpone_control].[composition_dbt_registrations]" in sql:
            row = data["dbt_registrations"].get(parameters[0])
            return [] if row is None else [row]
        if "FROM [dpone_control].[composition_dbt_events]" in sql:
            return [(phase, *row) for (key, phase), row in data["dbt_events"].items() if key == parameters[0]]
        if sql.startswith("INSERT INTO [dpone_control].[composition_dbt_registrations]"):
            key, *row = parameters
            assert key not in data["dbt_registrations"]
            data["dbt_registrations"][key] = tuple(row)
            return []
        if sql.startswith("INSERT INTO [dpone_control].[composition_dbt_events]"):
            key, phase, *row = parameters
            assert (key, phase) not in data["dbt_events"]
            data["dbt_events"][key, phase] = tuple(row)
            return []
        if sql.startswith("SELECT TOP (2) login_sid"):
            return [(data["capture_sid"], "dpone_v3_" + parameters[0][7:], data["capture_gate"], None)]
        if sql.startswith("SELECT TOP (2) principal_id"):
            return [(data["capture_principal"],)]
        if sql.startswith("SELECT p.name, p.sid, p.type"):
            return [(parameters[0], data["capture_login_sid"], "S", data["capture_gate"] != "READY", 0, 0)]
        return super()._select_or_mutate(sql, parameters)


@pytest.fixture
def scenario(active, monkeypatch):
    database, attempts = active
    attempts.admit_once(attempt())
    intent, expected, original = subject()
    database.data.update(
        dbt_registrations={},
        dbt_events={},
        capture_sid=b"a" * 16,
        capture_login_sid=b"a" * 16,
        capture_principal="mssql-sid:" + "61" * 16,
        capture_gate="READY",
    )
    database.data["catalog"].update(
        expected_rows(
            "dpone_control",
            tables=DBT_CAPTURE_TABLES,
            definitions={},
            metadata={},
            trigger_for=lambda schema, name: CompositionTrigger(
                "composition_" + name + "_invariant",
                dbt_capture_trigger_sql(schema, name),
                ("DELETE", "INSERT", "UPDATE"),
            ),
        )
    )
    # Gate installation/enrollment is separately certified. Keep actual shared
    # ledger, gate row, issued SID and SQL login observations active here.
    monkeypatch.setattr(
        "dpone.adapters.composition_mssql_login_gate.require_gate_policy",
        lambda ledger, db: ledger.require_transaction(),
    )

    def connect():
        connection = database.connect()
        connection.cursor = lambda: CaptureCursor(connection)
        return connection

    verified = []

    def verifier(value):
        verified.append(value)
        return intent, expected

    store = MssqlDbtCaptureStore(
        connect, expected_service_id=SERVICE, control_database="control", source_verifier=verifier
    )
    return database, store, intent, expected, original, verified


def test_registration_derives_source_and_freshly_reads_original(scenario):
    database, store, intent, expected, _, verified = scenario
    previous = len(database.connections)
    assert store.register(intent.attempt) == intent
    assert verified == [intent.attempt]
    assert len(database.connections) == previous + 2
    assert store.load_expectation(intent.attempt) == expected
    assert all(connection.closed for connection in database.connections)


def test_dispatch_ack_loss_never_reissues_permit(scenario):
    database, store, intent, _, original, _ = scenario
    store.register(intent.attempt)
    database.fail_commit = True
    with pytest.raises(DbtCaptureError, match="capture_commit_unknown"):
        store.record_dispatch_once(intent, original)
    assert (intent.attempt.attempt_sha256, "DISPATCH") in database.data["dbt_events"]
    database.fail_commit = False
    with pytest.raises(DbtCaptureError, match="capture_dispatch_replay"):
        store.record_dispatch_once(intent, original)


def test_exit_and_capture_append_once_with_exact_recovery(scenario):
    database, store, intent, _, original, _ = scenario
    store.register(intent.attempt)
    store.record_dispatch_once(intent, original)
    exited = DbtExitRecord(intent, DbtChildExit(123, 100, 0), b"{}", original)
    store.record_exit_once(exited)
    store.record_exit_once(exited)
    captured = DbtCaptureRecord(
        intent,
        "CAPTURED",
        exited,
        tuple(
            original if role == "preflight_manifest" else DbtArtifactOriginal(role, path, b"{}")
            for role, path in intent.artifact_paths
        ),
    )
    store.capture_once(captured)
    assert store.read_exit(intent.attempt) == exited
    assert store.read_capture(intent.attempt) == captured
    assert len(database.data["dbt_events"]) == 3


@pytest.mark.parametrize("fault", ["parent", "attempt", "issued_sid", "actual_sid", "gate", "catalog"])
def test_authority_or_catalog_drift_stops_before_registration(scenario, fault):
    database, store, intent, _, _, _ = scenario
    data = database.data
    if fault == "parent":
        key = next(iter(data["owners"]))
        data["owners"][key] = (*data["owners"][key][:-1], "RETIRING")
    elif fault == "attempt":
        key = intent.attempt.attempt_sha256
        row = data["operations"][key]
        data["operations"][key] = (*row[:6], "COMMIT_UNKNOWN", *row[7:])
    elif fault == "issued_sid":
        data["capture_principal"] = "mssql-sid:" + "62" * 16
    elif fault == "actual_sid":
        data["capture_login_sid"] = b"b" * 16
    elif fault == "gate":
        data["capture_gate"] = "CLOSED"
    else:
        key = ("[dpone_control].[composition_dbt_events]", "triggers")
        data["catalog"][key] = ()
    with pytest.raises((DbtCaptureError, ValueError)):
        store.register(intent.attempt)
    assert data["dbt_registrations"] == {}


def test_exit_without_dispatch_and_conflicting_registration_reject(scenario):
    _, store, intent, expected, original, _ = scenario
    store.register(intent.attempt)
    with pytest.raises(DbtCaptureError, match="capture_phase_order"):
        store.record_exit_once(DbtExitRecord(intent, DbtChildExit(123, 100, 0), b"{}", original))
    store._verify_source = lambda attempt: (replace(intent, argv=("/other/dbt", "build")), expected)
    with pytest.raises(DbtCaptureError, match="capture_registration_conflict"):
        store.register(intent.attempt)


def test_runtime_never_installs_or_repairs_schema(scenario):
    database, store, intent, _, original, _ = scenario
    store.register(intent.attempt)
    store.record_dispatch_once(intent, original)
    assert not any("CREATE TABLE" in sql or "CREATE TRIGGER" in sql for sql, _ in database.statements)
    sql = render_dbt_capture_schema()
    assert "CREATE TABLE" in sql and "HASHBYTES" in sql and "APPLOCK_MODE" in sql
    assert "IF EXISTS (SELECT 1 FROM deleted)" in sql


def test_fresh_readback_failure_after_dispatch_commit_is_not_permission(scenario):
    database, store, intent, _, original, _ = scenario
    store.register(intent.attempt)

    def fault(db):
        if (intent.attempt.attempt_sha256, "DISPATCH") in db.data["dbt_events"]:
            raise RuntimeError("untrusted driver text")

    database.before_connect = fault
    with pytest.raises(DbtCaptureError, match="capture_control_unknown"):
        store.record_dispatch_once(intent, original)
    database.before_connect = None
    with pytest.raises(DbtCaptureError, match="capture_dispatch_replay"):
        store.record_dispatch_once(intent, original)


def test_epoch_change_after_registration_blocks_dispatch(scenario):
    database, store, intent, _, original, _ = scenario
    store.register(intent.attempt)
    key = next(iter(database.data["domains"]))
    row = database.data["domains"][key]
    database.data["domains"][key] = (*row[:3], row[3] + 1, row[4])
    with pytest.raises((DbtCaptureError, ValueError)):
        store.record_dispatch_once(intent, original)
    assert database.data["dbt_events"] == {}


def test_retained_original_hash_corruption_cannot_be_loaded(scenario):
    database, store, intent, _, _, _ = scenario
    store.register(intent.attempt)
    key = intent.attempt.attempt_sha256
    row = database.data["dbt_registrations"][key]
    database.data["dbt_registrations"][key] = (*row[:3], row[3] + b" ")
    with pytest.raises(DbtCaptureError):
        store.load_intent(intent.attempt)


def test_undispatched_requires_protected_closed_gate_and_no_dispatch(scenario):
    database, store, intent, _, original, _ = scenario
    store.register(intent.attempt)
    store._verify_closure = lambda ledger, attempt: canonical_json_bytes(
        {
            "attempt_sha256": attempt.attempt_sha256,
            "intent_sha256": intent.intent_sha256,
            "build_dispatched": False,
            "closed": True,
        }
    )
    with pytest.raises(DbtCaptureError, match="capture_undispatched_conflict"):
        store.record_undispatched(intent.attempt)
    database.data["capture_gate"] = "CLOSED"
    store.record_undispatched(intent.attempt)
    assert store.read_capture(intent.attempt).phase == "UNDISPATCHED"
    database.data["capture_gate"] = "READY"
    with pytest.raises(DbtCaptureError):
        store.record_dispatch_once(intent, original)


@pytest.fixture
def closed_undispatched(scenario):
    database, store, intent, _, _, _ = scenario
    store.register(intent.attempt)
    database.data["capture_gate"] = "CLOSED"
    closure = canonical_json_bytes(
        {
            "attempt_sha256": intent.attempt.attempt_sha256,
            "intent_sha256": intent.intent_sha256,
            "build_dispatched": False,
            "closed": True,
        }
    )
    observations = []

    def verify(ledger, attempt):
        observations.append((ledger.require_transaction(), attempt))
        return closure

    store._verify_closure = verify
    return database, store, intent, closure, observations


@pytest.mark.parametrize("lost_ack", [False, True])
def test_undispatched_exact_recovery_reobserves_closure_and_fresh_original(closed_undispatched, lost_ack):
    database, store, intent, closure, observations = closed_undispatched
    database.fail_commit = lost_ack
    if lost_ack:
        with pytest.raises(DbtCaptureError, match="capture_commit_unknown"):
            store.record_undispatched(intent.attempt)
    else:
        store.record_undispatched(intent.attempt)
    database.fail_commit = False
    original_rows = dict(database.data["dbt_events"])
    previous = len(database.connections)
    store.record_undispatched(intent.attempt)
    assert len(database.connections) == previous + 2
    assert len(observations) == 2
    assert observations[0][0] != observations[1][0]
    assert database.data["dbt_events"] == original_rows
    assert store.read_capture(intent.attempt).undispatched_closure_original == closure


@pytest.mark.parametrize("fault", ["closure", "gate", "issued_sid"])
def test_undispatched_recovery_rejects_conflicting_original_or_authority(closed_undispatched, fault):
    database, store, intent, closure, _ = closed_undispatched
    store.record_undispatched(intent.attempt)
    original_rows = dict(database.data["dbt_events"])
    if fault == "closure":
        store._verify_closure = lambda ledger, attempt: closure + b" "
    elif fault == "gate":
        database.data["capture_gate"] = "READY"
    else:
        database.data["capture_principal"] = "mssql-sid:" + "62" * 16
    with pytest.raises((DbtCaptureError, ValueError)):
        store.record_undispatched(intent.attempt)
    assert database.data["dbt_events"] == original_rows
