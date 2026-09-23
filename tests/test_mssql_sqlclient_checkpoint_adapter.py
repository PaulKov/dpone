import pytest

from dpone.adapters.mssql_sqlclient_checkpoint import SqlClientCheckpointCas
from dpone.contracts.mssql_native_parent_journal import (
    NativeChunkRetirementReceipt,
    NativeParentRetirementReceipt,
)

H = "a" * 64


def _retirement() -> NativeParentRetirementReceipt:
    return NativeParentRetirementReceipt(H, (NativeChunkRetirementReceipt(0, "attempt", H, H, H, H, H, H, H, H, H),))


def test_checkpoint_receipt_binds_cas_ack() -> None:
    retirement = _retirement()
    proof = SqlClientCheckpointCas.proof(
        retirement, target_id="target", window_fingerprint="window", fence=3, revision=7
    )
    adapter = SqlClientCheckpointCas(cas=lambda *_: (7, proof), observe=lambda *_: None)
    receipt = adapter.commit(retirement, target_id="target", window_fingerprint="window", fence=3)
    assert receipt.parent_retirement_digest == retirement.digest
    assert receipt.checkpoint_cas_revision == 7


def test_lost_ack_reconciles_exact_observation_without_second_cas() -> None:
    retirement = _retirement()
    proof = SqlClientCheckpointCas.proof(
        retirement, target_id="target", window_fingerprint="window", fence=3, revision=8
    )
    calls = []

    def cas(*args):
        calls.append(args)
        raise TimeoutError

    adapter = SqlClientCheckpointCas(cas=cas, observe=lambda *_: (8, proof))
    assert (
        adapter.commit(retirement, target_id="target", window_fingerprint="window", fence=3).checkpoint_cas_revision
        == 8
    )
    assert len(calls) == 1


def test_unknown_or_wrong_checkpoint_ack_fails_closed() -> None:
    retirement = _retirement()
    adapter = SqlClientCheckpointCas(cas=lambda *_: (_ for _ in ()).throw(TimeoutError()), observe=lambda *_: None)
    with pytest.raises(RuntimeError, match="outcome_unknown"):
        adapter.commit(retirement, target_id="target", window_fingerprint="window", fence=3)
    adapter = SqlClientCheckpointCas(cas=lambda *_: (1, H), observe=lambda *_: None)
    with pytest.raises(ValueError, match="binding_mismatch"):
        adapter.commit(retirement, target_id="target", window_fingerprint="window", fence=3)
