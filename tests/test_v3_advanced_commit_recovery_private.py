"""Committed advancement preserves exact effect evidence without enabling retries."""

from dataclasses import replace
from pathlib import Path

import pytest

from dpone.ports import mssql_r1_v3 as r1
from dpone.runtime.sinks.strategies.mssql import mssql_r1_v3_effect_uow as recovery
from tests import test_postgres_mssql_r1_v3_contracts as contracts
from tests import test_postgres_mssql_r1_v3_runtime as runtime


def _attempt(epoch=1, revision=1, *, xmin=False):
    attempt = runtime._fixture(xmin=True).attempt if xmin else contracts._batch_request()[1]
    return replace(attempt, operation_epoch=epoch, operation_projection_revision=revision)


def _proof(attempt):
    result = runtime._build(attempt, [])[0].execute(attempt)
    receipt = result.receipt
    artifacts, authorities = contracts._resources(attempt.request, contracts.R1ReplayResourceStateV3.CONSUMED, receipt)
    header = receipt.header
    return r1.CommittedEffectReplayProofV3(
        recovery.proof_request_for(attempt),
        attempt.request,
        receipt,
        contracts._operation(attempt, contracts.R1ReplayOperationStateV3.COMMITTED, receipt),
        contracts.WriterHeadReplayObservationV3(
            header.candidate_writer_generation,
            header.candidate_writer_generation,
            header.candidate_head_revision,
            header.receipt_id,
            receipt.digest,
        ),
        artifacts,
        authorities,
        runtime._checkpoint(attempt, header.receipt_id),
    )


@pytest.mark.parametrize("epoch,revision", [(1, 1), (1, 2), (2, 2), (3, 4)])
@pytest.mark.parametrize("xmin", [False, True])
def test_committed_exact_or_monotonic_observation_returns_decoded_receipt(epoch, revision, xmin):
    original = _attempt(xmin=xmin)
    proof = _proof(_attempt(epoch, revision, xmin=xmin))
    result = recovery.classify_fresh_proof(original, proof)
    assert result.receipt == proof.receipt
    assert result.receipt is not proof.receipt
    assert result.retry_same_sealed_request is False


@pytest.mark.parametrize("epoch,revision", [(1, 4), (2, 1), (3, 2), (4, 3)])
def test_stale_or_insufficient_revision_blocks(epoch, revision):
    with pytest.raises(recovery.MssqlR1V3CommitOutcomeUnknown):
        recovery.classify_fresh_proof(_attempt(2, 2), _proof(_attempt(epoch, revision)))


@pytest.mark.parametrize("field", ["effect_key", "sealed_request_digest", "expected_override_digest"])
def test_foreign_proof_request_blocks(field):
    proof = _proof(_attempt(2, 2))
    object.__setattr__(proof.proof_request, field, b"x" * 32)
    with pytest.raises(recovery.MssqlR1V3CommitOutcomeUnknown):
        recovery.classify_fresh_proof(_attempt(), proof)


@pytest.mark.parametrize("field,value", [("operation_epoch", 0), ("operation_projection_revision", True)])
def test_mutated_nested_operation_is_revalidated(field, value):
    proof = _proof(_attempt())
    object.__setattr__(proof.operation, field, value)
    with pytest.raises(recovery.MssqlR1V3CommitOutcomeUnknown):
        recovery.classify_fresh_proof(_attempt(), proof)


@pytest.mark.parametrize(
    "part,field,value",
    [
        ("artifacts", "artifact_ids", ()),
        ("artifacts", "states", (contracts.R1ReplayResourceStateV3.SEALED,)),
        ("artifacts", "consuming_receipt_digests", (b"x" * 32,)),
        ("head", "head_revision", 0),
        ("head", "last_receipt_digest", b"x" * 32),
        ("authorities", "authority_ids", ()),
        ("authorities", "issuance_consuming_receipt_digest", b"x" * 32),
        ("operation", "committed_receipt_digest", b"x" * 32),
    ],
)
def test_partial_or_foreign_resources_block(part, field, value):
    proof = _proof(_attempt())
    object.__setattr__(getattr(proof, part), field, value)
    with pytest.raises(recovery.MssqlR1V3CommitOutcomeUnknown):
        recovery.classify_fresh_proof(_attempt(), proof)


def test_xmin_checkpoint_mutation_blocks():
    proof = _proof(_attempt(xmin=True))
    object.__setattr__(proof.checkpoint, "checkpoint_revision", 999)
    with pytest.raises(recovery.MssqlR1V3CommitOutcomeUnknown):
        recovery.classify_fresh_proof(_attempt(xmin=True), proof)


@pytest.mark.parametrize("advanced", [False, True])
@pytest.mark.parametrize("kind", ["unknown", "known-not-committed"])
def test_other_variants_keep_exact_original_request(kind, advanced):
    original = _attempt()
    observed = _attempt(2, 2) if advanced else original
    _, _, factory = runtime._build(observed, [], proof_kind=kind)
    session = runtime._FreshProofSession([], factory.durable, observed, kind)
    proof = session.probe(recovery.proof_request_for(observed))
    if advanced or kind == "unknown":
        with pytest.raises(recovery.MssqlR1V3CommitOutcomeUnknown):
            recovery.classify_fresh_proof(original, proof)
    else:
        assert recovery.classify_fresh_proof(original, proof).retry_same_sealed_request is True


@pytest.mark.parametrize("xmin", [False, True])
def test_lost_response_advanced_commit_uses_one_fresh_probe_without_repeat(monkeypatch, xmin):
    original = _attempt(xmin=xmin)
    proof = _proof(_attempt(2, 2, xmin=xmin))
    events = []
    uow, sessions, factory = runtime._build(original, events, fail=True)

    def probe(session, request):
        events.append("fresh_probe")
        assert request == recovery.proof_request_for(original)
        assert all(item.transaction.invalidated for item in sessions.opened)
        return proof

    monkeypatch.setattr(runtime._FreshProofSession, "probe", probe)
    result = uow.execute(original)
    assert result.outcome is recovery.MssqlR1V3EffectOutcome.COMMITTED_AFTER_FRESH_PROOF
    assert result.receipt == proof.receipt
    assert len(sessions.opened) == len(factory.opened) == 1
    assert factory.opened[0].closed
    assert events.count("batch" if not xmin else "insert") == 1
    assert events[-5:] == ["dispatch_commit", "close", "fresh_open", "fresh_probe", "fresh_close"]
    with pytest.raises(AssertionError, match="old transaction accessed"):
        sessions.opened[0].transaction.assert_active()


def test_subject_is_owned_module():
    expected = (
        Path(__file__).resolve().parents[1] / "src/dpone/runtime/sinks/strategies/mssql/mssql_r1_v3_effect_uow.py"
    )
    assert Path(recovery.__file__).resolve() == expected


def test_coherent_foreign_effect_blocks():
    with pytest.raises(recovery.MssqlR1V3CommitOutcomeUnknown):
        recovery.classify_fresh_proof(_attempt(), _proof(_attempt(xmin=True)))


def test_malformed_canonical_bytes_block(monkeypatch):
    proof = _proof(_attempt())
    monkeypatch.setattr(r1.CommittedEffectReplayProofV3, "canonical_bytes", property(lambda self: b"invalid"))
    with pytest.raises(recovery.MssqlR1V3CommitOutcomeUnknown):
        recovery.classify_fresh_proof(_attempt(), proof)
