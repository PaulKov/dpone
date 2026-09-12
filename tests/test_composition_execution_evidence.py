"""Protected proof persistence must retain originals and reject conflicting bytes."""

from types import SimpleNamespace

import pytest

from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptProof, composition_attempt_epoch_subject
from dpone.contracts.strict_json import canonical_json_bytes
from tests.composition_snapshot_helpers import intent


def test_producer_rejects_unbacked_digest_before_sql():
    from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof
    from dpone.contracts.composition_persistence import CompositionProofAuthority

    value = intent().attempt
    proof = CompositionAttemptProof(
        "OUTCOME",
        value.attempt_sha256,
        value.activation_request_sha256,
        composition_attempt_epoch_subject(value),
        (CompositionProofAuthority("mssql", "10000000-0000-4000-8000-000000000002", "mssql-sid:" + "aa" * 16),),
        "sha256:" + "0" * 64,
        "SUCCEEDED",
    )
    with pytest.raises(CompositionAdmissionError, match="execution_proof_original"):
        persist_execution_proof(SimpleNamespace(), proof, canonical_json_bytes({"observed": True}))


class Ledger:
    schema = "control"

    def __init__(self, monkeypatch):
        from dpone.adapters import composition_mssql_execution_evidence as persistence

        monkeypatch.setattr(persistence, "require_execution_evidence_schema", lambda *_: None)
        self.cursor = self
        self.records, self.rows, self.writes = {}, (), 0
        self.drop_writes = False
        self.changed = False

    def table(self, name):
        return name

    def require_transaction(self, expected=None):
        if self.changed:
            raise CompositionAdmissionError("transaction_changed")
        return 42

    def execute(self, sql, *values):
        table = "execution_evidence" if "execution_evidence" in sql else "proofs"
        key = (table, *values[:3])
        if sql.startswith("INSERT"):
            self.writes += 1
            if not self.drop_writes:
                self.records[key] = (values[3:],)
        else:
            self.rows = self.records.get(key, ())

    def fetchall(self):
        return self.rows


def subject():
    from hashlib import sha256

    from dpone.contracts.composition_persistence import CompositionProofAuthority

    value = intent().attempt
    document = canonical_json_bytes({"schema": "test.actual-observation", "attempt_sha256": value.attempt_sha256})
    return CompositionAttemptProof(
        "OUTCOME",
        value.attempt_sha256,
        value.activation_request_sha256,
        composition_attempt_epoch_subject(value),
        (CompositionProofAuthority("mssql", "10000000-0000-4000-8000-000000000002", "mssql-sid:" + "aa" * 16),),
        "sha256:" + sha256(document).hexdigest(),
        "SUCCEEDED",
    ), document


def test_retained_proof_replay_compares_originals_without_rewriting(monkeypatch):
    from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof

    ledger = Ledger(monkeypatch)
    proof, original = subject()
    persist_execution_proof(ledger, proof, original)
    assert ledger.writes == 2
    persist_execution_proof(ledger, proof, original, create=False)
    persist_execution_proof(ledger, proof, original)
    assert ledger.writes == 2


def test_fresh_missing_readback_must_not_recreate_evidence(monkeypatch):
    from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof

    ledger = Ledger(monkeypatch)
    with pytest.raises(CompositionAdmissionError, match="execution_proof_missing"):
        persist_execution_proof(ledger, *subject(), create=False)
    assert ledger.writes == 0


def test_lost_insert_or_conflicting_original_does_not_pass(monkeypatch):
    from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof

    ledger = Ledger(monkeypatch)
    proof, original = subject()
    ledger.drop_writes = True
    with pytest.raises(CompositionAdmissionError, match="execution_proof_readback"):
        persist_execution_proof(ledger, proof, original)
    ledger.drop_writes = False
    ledger.records[("execution_evidence", proof.attempt_sha256, proof.kind, proof.evidence_sha256)] = ((b"different",),)
    with pytest.raises(CompositionAdmissionError, match="execution_proof_conflict"):
        persist_execution_proof(ledger, proof, original)
    assert ledger.writes == 1
