"""Historical SQL original validation; simulated catalog/host facts are not live proof."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.adapters import composition_clickhouse_historical_closure as module
from dpone.adapters.composition_clickhouse_gate_queries import ClickHouseLocalSupervisorObservation
from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    CompositionAttemptProof,
    composition_attempt_epoch_subject,
    encode_attempt_proof,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_composition_clickhouse_gate import SQL_SERVICE, GateCursor, completed, make_dispatch
from tests.test_composition_clickhouse_gate import active as active


@pytest.fixture
def history(active, monkeypatch):
    db, value, gate, admin, supervisor, _ = active
    body = strict_json_object(db.data["enrollments"][db.enrollment_key][-1])
    body["facts"]["linux"]["network_namespace_id"] = "12345"
    document = canonical_json_bytes(body)
    key = module.document_sha256(document)
    db.data["enrollments"] = {key: (*db.data["enrollments"][db.enrollment_key][:4], document)}
    gate._enrollment = key
    fields = {name: body[name] for name in ("service_id", "database_uuid", "boot_id", "isolation_id")}
    fields.update(enrollment_sha256=key, network_namespace_id="12345")
    original = canonical_json_bytes(
        {"schema": "dpone.composition-clickhouse-local-supervisor.v1", **fields, "facts": body["facts"]}
    )
    supervisor.value = ClickHouseLocalSupervisorObservation(
        **fields, evidence_sha256=module.document_sha256(original), evidence_document=original
    )
    observe = admin.observe

    def principal(*args, **kwargs):
        return {**observe(*args, **kwargs), "grants": (), "roles": ()}

    monkeypatch.setattr(admin, "observe", principal)
    monkeypatch.setattr(module, "require_clickhouse_supervisor_schema", lambda *_: None)
    credentials = gate.issue_once(value.attempt)
    dispatch = make_dispatch(value)
    journal = gate.journal(value.attempt, credentials.user_id)
    journal.claim_once(dispatch)
    journal.record_completed(dispatch, completed(dispatch))
    closed, quiet = gate.close(value.attempt), gate.prove_quiescence(value.attempt)
    return db, value, gate, closed, quiet, admin, supervisor


def read(history):
    db, value, *_ = history
    with composition_control_transaction(db.connect, "dpone_control", SQL_SERVICE) as ledger:
        return module.read_purpose_closure_in(ledger, value.attempt, value.target, "ingest")


def test_retained_barrier_needs_no_live_admin_host_or_new_connection(history, monkeypatch):
    db, value, gate, closed, quiet, admin, supervisor = history

    def forbidden(*args, **kwargs):
        pytest.fail("historical read attempted live observation")

    monkeypatch.setattr(admin, "observe", forbidden)
    monkeypatch.setattr(supervisor, "observe", forbidden)
    before = deepcopy(db.data)
    with composition_control_transaction(db.connect, "dpone_control", SQL_SERVICE) as ledger:
        monkeypatch.setattr(db, "connect", forbidden)
        result = module.read_purpose_closure_in(ledger, value.attempt, value.target, "ingest")
    assert result.proofs == (closed, quiet)
    assert module.document_sha256(result.evidence_document) == closed.evidence_sha256 == quiet.evidence_sha256
    assert db.data == before


@pytest.mark.parametrize("owner", ["ACTIVE", "RETIRING", "RETIRED"])
@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED"])
def test_terminal_historical_scope_reopens_exact_originals(history, owner, state):
    db, value, _, closed, quiet, *_ = history
    outcome = CompositionAttemptProof(
        "OUTCOME",
        value.attempt.attempt_sha256,
        value.attempt.activation_request_sha256,
        composition_attempt_epoch_subject(value.attempt),
        closed.authorities,
        module.document_sha256(b"outcome"),
        state,
    )
    key = value.attempt.attempt_sha256
    db.data["proofs"][key, "OUTCOME", outcome.proof_sha256] = ("execution", encode_attempt_proof(outcome))
    operation = db.data["operations"][key]
    db.data["operations"][key] = (*operation[:6], state, closed.proof_sha256, quiet.proof_sha256, outcome.proof_sha256)
    previous = db.data["owners"][operation[2]]
    db.data["owners"][operation[2]] = (*previous[:5], owner)
    if owner == "RETIRED":
        db.data["domains"] = {guard: (*row[:4], None) for guard, row in db.data["domains"].items()}
    assert read(history).proofs == (closed, quiet)


@pytest.mark.parametrize(
    "damage",
    [
        "principal",
        "revoked",
        "roles",
        "grants",
        "extra",
        "quiescence",
        "links",
        "supervisor",
        "phase",
        "proof",
        "enrollment",
        "pending",
        "dispatch_closed",
        "issued",
    ],
)
def test_missing_changed_or_partial_original_refuses_history(history, damage):
    db, value, gate, *_ = history
    key = gate._binding(value.attempt).key
    if damage in {"principal", "revoked", "roles", "grants", "extra", "quiescence", "links", "supervisor"}:
        row = db.data["ch_gate_events"][key, "CLOSED"]
        body = strict_json_object(row[-1])
        if damage == "principal":
            body["principal"]["username"] = "other"
        elif damage == "revoked":
            body["principal"]["revoked"] = 1
        elif damage == "roles":
            body["principal"]["roles"] = ["admin"]
        elif damage == "grants":
            body["principal"]["grants"] = [["INSERT", "db", "table"]]
        elif damage == "extra":
            body["extra"] = "unverified"
        elif damage == "quiescence":
            body["quiescence"] = "empty-processes-only"
        elif damage == "links":
            body["dispatch_terminals"] = []
        else:
            body["supervisor"]["original"]["facts"]["linux"]["network_namespace_id"] = "9"
        document = canonical_json_bytes(body)
        db.data["ch_gate_events"][key, "CLOSED"] = (*row[:-2], module.document_sha256(document), document)
    elif damage == "phase":
        db.data["ch_gate_events"].pop((key, "READY"))
    elif damage == "proof":
        db.data["proofs"].clear()
    elif damage == "enrollment":
        db.data["enrollments"].clear()
    elif damage == "pending":
        db.data["terminals"].clear()
    elif damage == "dispatch_closed":
        db.data["closures"] = {k: v for k, v in db.data["closures"].items() if k[1] != "CLOSED"}
    else:
        db.data["issued_authorities"].clear()
    with pytest.raises(CompositionAdmissionError, match="historical_closure"):
        read(history)


def test_other_purpose_and_target_cannot_read_ingest(history):
    db, value, *_ = history
    with composition_control_transaction(db.connect, "dpone_control", SQL_SERVICE) as ledger:
        for target, purpose in (
            (value.target, "publisher"),
            (replace(value.target, generation_table="other"), "ingest"),
        ):
            with pytest.raises(CompositionAdmissionError):
                module.read_purpose_closure_in(ledger, value.attempt, target, purpose)


def test_transaction_replacement_during_original_read_rejects(history, monkeypatch):
    original = GateCursor._select_or_mutate

    def replace_transaction(cursor, sql, parameters):
        result = original(cursor, sql, parameters)
        if "composition_ch_gate_events]" in sql:
            cursor.connection.transaction_id += 1
            cursor.connection.database.lock_owner = cursor.connection.transaction_id
        return result

    monkeypatch.setattr(GateCursor, "_select_or_mutate", replace_transaction)
    with pytest.raises(CompositionAdmissionError):
        read(history)
