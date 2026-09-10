"""Stateful attempt fixtures with explicit offline catalog and proof producers."""

import pytest

from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.composition_persistence import encode_attempt_proof
from dpone.contracts.composition_proof import (
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
from tests.composition_mssql_catalog_helpers import install_offline_catalog_references
from tests.composition_mssql_gate_helpers import SERVICE, attempt, occurrence
from tests.composition_mssql_store_fault_model import FaultDatabase
from tests.test_composition_activation_contract import digest


@pytest.fixture
def active(monkeypatch):
    install_offline_catalog_references(monkeypatch)
    database = FaultDatabase(occurrence().request, SERVICE)
    activation = MssqlCompositionActivationStore(database.connect, expected_service_id=SERVICE)
    activation.prepare(database.request)
    activation.activate(database.request)
    database.connections.clear()
    database.statements.clear()
    return database, MssqlCompositionAttemptStore(database.connect, expected_service_id=SERVICE)


def terminal_proofs(outcome_state="SUCCEEDED", principal=None):
    value = attempt()
    authority = principal or CompositionProofAuthority("mssql", SERVICE, "mssql-sid:" + "61" * 16)
    return tuple(
        CompositionAttemptProof(
            kind,
            value.attempt_sha256,
            value.activation_request_sha256,
            composition_attempt_epoch_subject(value),
            (authority,),
            digest(kind),
            outcome_state if kind == "OUTCOME" else None,
        )
        for kind in ("CLOSED_GATES", "QUIESCENCE", "OUTCOME")
    )


def store_proofs(database, proofs):
    """Store producer originals; the actual adapter still validates every byte/hash."""
    value = attempt()
    database.data["issued_authorities"][value.attempt_sha256] = (("mssql", SERVICE, "mssql-sid:" + "61" * 16),)
    for proof in proofs:
        database.data["proofs"][proof.attempt_sha256, proof.kind, proof.proof_sha256] = (
            "execution",
            encode_attempt_proof(proof),
        )


def set_receipt(database, state, proofs=()):
    """Inject a retained receipt, including intentionally incomplete crash state."""
    key = attempt().attempt_sha256
    record = database.data["operations"][key]
    hashes = tuple(proof.proof_sha256 for proof in proofs) if proofs else (None, None, None)
    database.data["operations"][key] = (*record[:6], state, *hashes)


def writes(database):
    return [sql for sql, _ in database.statements if "INSERT INTO" in sql or "UPDATE [dpone_control]" in sql]
