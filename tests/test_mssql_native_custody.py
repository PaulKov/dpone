"""Stable target custody survives leases and excludes overlapping invocations."""

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.contracts.bounded_window import WindowContractError


def test_invocation_custody_covers_empty_sequential_and_parallel_chunks(tmp_path):
    store = SQLiteWindowStore(tmp_path / "custody.db", clock=lambda: 1.0)
    lease = store.acquire("target", "writer", 60)
    custody = NativeTargetCustody(store, "target")
    claim = custody.claim(lease, "a" * 64)
    assert claim.record.epoch == 1
    assert claim.recovery_only is False
    assert custody.key == "mssql-native-target-custody-v1/" + sha256(b"target").hexdigest()
    assert custody.claim(lease, "a" * 64).record == claim.record
    custody.reassert_grant(lease, "a" * 64)
    with pytest.raises(WindowContractError, match="custody_held"):
        custody.assert_available_for_v1(lease)
    with pytest.raises(WindowContractError, match="custody_held"):
        custody.claim(lease, "b" * 64)


def test_lease_expiry_retains_custody_and_matching_recovery_cannot_grant(tmp_path):
    now = [1.0]
    store = SQLiteWindowStore(tmp_path / "custody.db", clock=lambda: now[0])
    lease = store.acquire("target", "first", 2)
    custody = NativeTargetCustody(store, "target")
    custody.claim(lease, "a" * 64)
    now[0] = 4.0
    second = store.acquire("target", "second", 60)
    with pytest.raises(WindowContractError):
        custody.claim(lease, "a" * 64)
    resumed = custody.claim(second, "a" * 64)
    assert resumed.recovery_only is True
    with pytest.raises(WindowContractError, match="recovery_only"):
        custody.reassert_grant(second, "a" * 64)
    with pytest.raises(WindowContractError, match="custody_held"):
        custody.claim(second, "b" * 64)


def test_release_requires_explicit_durable_proof_and_preserves_epoch(tmp_path):
    store = SQLiteWindowStore(tmp_path / "custody.db", clock=lambda: 1.0)
    lease = store.acquire("target", "writer", 60)
    custody = NativeTargetCustody(store, "target")
    custody.claim(lease, "a" * 64)

    def missing_proof():
        raise ValueError("no publication receipt")

    with pytest.raises(ValueError, match="no publication"):
        custody.release(lease, "a" * 64, "published_cleanup", assert_release_authority=missing_proof)
    assert custody.inspect(lease).state == "held"
    cleared = custody.release(lease, "a" * 64, "empty_completion_cleanup", assert_release_authority=lambda: None)
    assert cleared.state == "clear"
    assert cleared.epoch == 1
    assert custody.claim(lease, "b" * 64).record.epoch == 2


def test_parallel_invocations_have_one_cas_winner(tmp_path):
    path = tmp_path / "custody.db"
    first_store = SQLiteWindowStore(path, clock=lambda: 1.0)
    second_store = SQLiteWindowStore(path, clock=lambda: 1.0)
    lease = first_store.acquire("target", "writer", 60)
    first = NativeTargetCustody(first_store, "target")
    second = NativeTargetCustody(second_store, "target")

    def claim(custody, key):
        try:
            return custody.claim(lease, key).record.holder_invocation_key
        except WindowContractError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda pair: claim(*pair), ((first, "a" * 64), (second, "b" * 64))))
    assert sum(value is not None for value in results) == 1
    assert first.inspect(lease).holder_invocation_key in {"a" * 64, "b" * 64}


def test_custody_rejects_malformed_persisted_projection(tmp_path):
    store = SQLiteWindowStore(tmp_path / "custody.db", clock=lambda: 1.0)
    lease = store.acquire("target", "writer", 60)
    custody = NativeTargetCustody(store, "target")
    store.save(custody.key, None, '{"state":"clear"}', lease)
    with pytest.raises(WindowContractError, match="invalid_target_custody"):
        custody.inspect(lease)
