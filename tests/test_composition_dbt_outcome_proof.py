"""Durable OUTCOME proof production from protected dbt observations."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dpone.adapters import composition_mssql_dbt_outcome
from dpone.adapters.composition_mssql_dbt_outcome import CompositionDbtOutcomeProofProducer
from dpone.contracts.composition_dbt_outcome import DbtCaptureError, DbtNativeOutcome
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_composition_dbt_capture import intent

SERVICE = "11111111-1111-1111-8111-111111111111"


def materialization(value) -> bytes:
    return canonical_json_bytes(
        {
            "schema": "dpone.composition-dbt-materialization-observation.v1",
            "attempt_sha256": value.attempt.attempt_sha256,
        }
    )


def producer(tmp_path, *, outcome=None, observed_intent=None, persist=None):
    value = intent(tmp_path)
    transactions = []

    @contextmanager
    def transaction():
        ledger = SimpleNamespace()
        transactions.append(ledger)
        yield ledger

    result = CompositionDbtOutcomeProofProducer(
        transaction=transaction,
        expected_service_id=SERVICE,
        intent_reader=lambda _attempt: observed_intent or value,
        outcome_observer=SimpleNamespace(
            observe=lambda _attempt: (
                outcome
                or DbtNativeOutcome(
                    "SUCCEEDED",
                    "native_invocation_verified",
                    value.intent_sha256,
                    materialization(value),
                )
            )
        ),
        persist=persist,
    )
    return value, result, transactions


def test_outcome_proof_persists_then_reopens_exact_original(tmp_path, monkeypatch) -> None:
    value, result, transactions = producer(tmp_path)
    writes = []
    monkeypatch.setattr(
        composition_mssql_dbt_outcome,
        "persist_execution_proof",
        lambda ledger, proof, document, **kwargs: writes.append((ledger, proof, strict_json_object(document), kwargs)),
    )

    proof = result.observe(value.attempt)

    assert proof.kind == "OUTCOME"
    assert proof.outcome_state == "SUCCEEDED"
    assert proof.authorities[0].principal_id == value.sql_principal_sid
    assert [item[3] for item in writes] == [{"create": True}, {"create": False}]
    assert [item[0] for item in writes] == transactions
    assert writes[0][2] == writes[1][2]
    assert writes[0][2]["materialization"]["schema"] == ("dpone.composition-dbt-materialization-observation.v1")


def test_injected_writer_owns_both_persistence_passes(tmp_path, monkeypatch) -> None:
    """A composed root may supply the protected writer instead of the default."""
    writes = []
    value, result, transactions = producer(
        tmp_path,
        persist=lambda ledger, proof, document, **kwargs: writes.append((ledger, kwargs)),
    )
    monkeypatch.setattr(
        composition_mssql_dbt_outcome,
        "persist_execution_proof",
        lambda *args, **kwargs: pytest.fail("an injected writer must not fall back"),
    )

    proof = result.observe(value.attempt)

    assert proof.kind == "OUTCOME" and proof.outcome_state == "SUCCEEDED"
    assert [item[1] for item in writes] == [{"create": True}, {"create": False}]
    assert [item[0] for item in writes] == transactions


@pytest.mark.parametrize(
    "outcome",
    [
        DbtNativeOutcome("SUCCEEDED", "native_invocation_verified", "sha256:" + "0" * 64),
        DbtNativeOutcome("FAILED", "invalid", "sha256:" + "0" * 64, b"{}"),
        DbtNativeOutcome("unknown", "invalid", "sha256:" + "0" * 64),
    ],
)
def test_outcome_proof_rejects_unbound_observation(tmp_path, outcome) -> None:
    value, result, _transactions = producer(tmp_path, outcome=outcome)

    with pytest.raises(DbtCaptureError):
        result.observe(value.attempt)
