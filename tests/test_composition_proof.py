"""Proof scope/readback tests; synthetic digests are never database evidence."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    decode_attempt_identity,
    decode_attempt_proof,
    encode_attempt_identity,
    encode_attempt_proof,
)
from dpone.contracts.composition_proof import (
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
from tests.test_composition_activation_contract import digest
from tests.test_composition_attempt_fencing import attempt


def proof():
    value = attempt()
    authority = CompositionProofAuthority("mssql", "10000000-0000-4000-8000-000000000009", "mssql-sid:" + "01" * 16)
    return CompositionAttemptProof(
        "CLOSED_GATES",
        value.attempt_sha256,
        value.activation_request_sha256,
        composition_attempt_epoch_subject(value),
        (authority,),
        digest("offline observation"),
    )


def test_complete_attempt_and_proof_roundtrip():
    value = attempt()
    assert decode_attempt_identity(encode_attempt_identity(value), value.attempt_sha256) == value
    receipt = proof()
    assert decode_attempt_proof(encode_attempt_proof(receipt), receipt.proof_sha256).require_attempt(value) == receipt


@pytest.mark.parametrize("field", ["attempt_sha256", "activation_request_sha256", "guard_epochs_sha256"])
def test_proof_cannot_be_replayed_against_another_attempt_scope(field):
    with pytest.raises(CompositionAdmissionError, match="proof_attempt"):
        replace(proof(), **{field: digest("foreign")}).require_attempt(attempt())


def test_unknown_scope_field_is_rejected_even_with_recomputed_document_hash():
    body = proof().to_dict()
    body["authority_override"] = True
    document = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(CompositionAdmissionError):
        decode_attempt_proof(document, canonical_fingerprint(body))


@pytest.mark.parametrize("authorities", [(), [], (proof().authorities[0],) * 2])
def test_missing_mutable_or_duplicate_authority_closure_rejected(authorities):
    with pytest.raises(CompositionAdmissionError, match="proof_authority_closure"):
        replace(proof(), authorities=authorities)


@pytest.mark.parametrize("principal_id", ["reader_alias", "mssql-sid:" + "GG" * 16, "mssql-sid:" + "01" * 15])
def test_sql_principal_identity_is_issued_sid_not_binding_name(principal_id):
    with pytest.raises(CompositionAdmissionError, match="proof_authority"):
        replace(proof().authorities[0], principal_id=principal_id)


def test_outcome_producer_must_bind_a_terminal_state():
    with pytest.raises(CompositionAdmissionError, match="proof_outcome_state"):
        replace(proof(), kind="OUTCOME")
