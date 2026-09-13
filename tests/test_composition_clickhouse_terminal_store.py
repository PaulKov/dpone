"""Aggregate producer with explicit nested-reader doubles; no live certificate."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from dpone.adapters import composition_clickhouse_terminal_store as module
from dpone.contracts.composition_persistence import decode_attempt_proof
from dpone.contracts.composition_snapshot import SnapshotPublicationRecord
from dpone.contracts.composition_snapshot_capture import (
    SnapshotCaptureRecord,
    SnapshotCaptureSubject,
    decode_generation_seal,
)
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_clickhouse_gate import GateCursor
from tests.test_composition_clickhouse_gate import active as active
from tests.test_composition_remote_transfer_result import result_body


@pytest.fixture
def producer(active, monkeypatch):
    db, value, *_ = active
    attempt, body = result_body()
    assert attempt == value.attempt
    capture = body["capture_document"]
    subject = SnapshotCaptureSubject.from_bytes(canonical_json_bytes(capture["subject"]["document"]))
    events = {
        phase: canonical_json_bytes(capture[name]["document"])
        for phase, name in (("CLAIMED", "subject"), ("CAPTURED", "captured"), ("GENERATION_SEALED", "generation_seal"))
    }
    published = SnapshotPublicationRecord.from_bytes(
        canonical_json_bytes(body["publication_document"]), body["publication_sha256"]
    )
    purposes = {}
    for purpose in ("ingest", "publisher"):
        refs = [body[field][purpose] for field in ("closed_gates", "quiescence")]
        proofs = tuple(decode_attempt_proof(canonical_json_bytes(r["document"]), r["sha256"]) for r in refs)
        purposes[purpose] = SimpleNamespace(
            proofs=proofs, evidence_document=canonical_json_bytes(refs[0]["evidence_document"])
        )
    monkeypatch.setattr(module, "read_purpose_closure_in", lambda ledger, a, t, p: purposes[p])
    monkeypatch.setattr(
        module,
        "read_issued_authorities_in",
        lambda *args, **kwargs: tuple(
            sorted((published.intent.ingest_principal, published.intent.publisher_principal))
        ),
    )
    for purpose in ("ingest", "publisher"):
        original = body["closed_gates"][purpose]["evidence_document"]
        key, user = original["gate_key"], original["gate_id"]
        raw = canonical_json_bytes({"gate_key": key, "gate_id": user})
        db.data["ch_gate_bindings"][key] = (key, user, module.evidence_digest(raw), raw)
    db.data["execution_evidence"] = {}
    db.data["issued_authorities"][attempt.attempt_sha256] = tuple(
        (p.connector, p.service_id, p.principal_id)
        for p in sorted((published.intent.ingest_principal, published.intent.publisher_principal))
    )
    from dpone.adapters import composition_mssql_execution_evidence as evidence

    monkeypatch.setattr(evidence, "require_execution_evidence_schema", lambda *args: None)
    original_sql = GateCursor._select_or_mutate

    def sql(cursor, statement, parameters):
        if "composition_execution_evidence]" in statement:
            rows = cursor.connection.data["execution_evidence"]
            if statement.startswith("INSERT"):
                assert parameters[:3] not in rows
                rows[parameters[:3]] = parameters[3]
                return []
            return [(rows[parameters],)] if parameters in rows else []
        return original_sql(cursor, statement, parameters)

    monkeypatch.setattr(GateCursor, "_select_or_mutate", sql)
    generation = decode_generation_seal(
        subject, SnapshotCaptureRecord.from_bytes(events["CAPTURED"]), events["GENERATION_SEALED"]
    )
    files = []

    def load_generation(ledger, a, ref):
        ledger.require_transaction()
        files.append(ref)
        assert ref == generation.record_sha256
        return generation

    store = module.ClickHouseTerminalStore(
        db.connect,
        expected_service_id=db.service_id,
        target=subject.target,
        capture_store=SimpleNamespace(read_in=lambda ledger, a: (subject, events)),
        publication_store=SimpleNamespace(records_in=lambda ledger: (published,)),
        load_generation_in=load_generation,
    )
    return store, db, attempt, files


def test_persist_all_proofs_keeps_actual_receipt_running_and_result_refuses(producer):
    store, db, attempt, files = producer
    before = deepcopy(db.data)
    result = store.persist_success(attempt)
    assert len(db.data["proofs"]) == 3 and files
    assert db.data["operations"] == before["operations"]
    assert result.outcome.outcome_state == "SUCCEEDED"
    assert result.closed_gates.authorities == result.quiescence.authorities == result.outcome.authorities
    assert len(result.outcome.authorities) == 2
    assert store.read_proofs(attempt) == result
    with pytest.raises(module.CompositionAdmissionError):
        store.read_result(attempt)


def test_additional_issued_authority_blocks_before_persistence(producer, monkeypatch):
    store, db, attempt, _ = producer
    monkeypatch.setattr(module, "read_issued_authorities_in", lambda *args, **kwargs: ())
    with pytest.raises(module.CompositionAdmissionError):
        store.persist_success(attempt)
    assert not db.data["proofs"]


def test_missing_publication_blocks_before_persistence(producer):
    store, db, attempt, _ = producer
    store._publication = SimpleNamespace(records_in=lambda ledger: ())
    with pytest.raises(module.CompositionAdmissionError):
        store.persist_success(attempt)
    assert not db.data["proofs"]


def test_changed_generation_file_readback_blocks_before_persistence(producer):
    store, db, attempt, _ = producer
    store._generation = lambda *args: None
    with pytest.raises(module.CompositionAdmissionError):
        store.persist_success(attempt)
    assert not db.data["proofs"]


def test_read_result_requires_actual_finalization_and_reopens_retained_proofs(producer):
    from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore

    store, db, attempt, _ = producer
    proofs = store.persist_success(attempt)
    receipt = MssqlCompositionAttemptStore(db.connect, expected_service_id=db.service_id).finalize(
        attempt, state="SUCCEEDED", outcome_evidence_sha256=proofs.outcome.proof_sha256
    )
    assert receipt.state == "SUCCEEDED"
    assert store.read_result(attempt).rows == 2
    db.data["execution_evidence"].clear()
    with pytest.raises(module.CompositionAdmissionError):
        store.read_result(attempt)


def test_lost_commit_ack_never_returns_aggregate_ack(producer):
    store, db, attempt, _ = producer
    db.fail_commit = True
    with pytest.raises(module.CompositionAdmissionError):
        store.persist_success(attempt)
    assert db.data["operations"][attempt.attempt_sha256][6] == "RUNNING"


def test_transaction_switch_in_generation_loader_rolls_back_aggregate(producer):
    store, db, attempt, _ = producer
    original = store._generation

    def changed(ledger, attempt, ref):
        value = original(ledger, attempt, ref)
        ledger.cursor.connection.commit()
        ledger.begin(db.service_id)
        return value

    store._generation = changed
    with pytest.raises(module.CompositionAdmissionError):
        store.persist_success(attempt)
    assert not db.data["proofs"]


def test_both_issued_binding_originals_required_without_creating_missing_user(producer):
    store, db, attempt, _ = producer
    store.require_issued(attempt)
    db.data["ch_gate_bindings"].pop(next(iter(db.data["ch_gate_bindings"])))
    before = deepcopy(db.data)
    with pytest.raises(module.CompositionAdmissionError):
        store.require_issued(attempt)
    assert db.data == before
