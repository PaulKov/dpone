from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from dpone.backfill.state import (
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_PENDING,
    CHUNK_STATUS_SUCCESS,
    BackfillChunkRecord,
    BackfillLedger,
    FileBackfillStateStore,
)


def _ledger() -> BackfillLedger:
    return BackfillLedger(
        run_key="campaign-a",
        dataset="analytics.orders",
        inner_mode="partition_replace",
        chunk_config={"column": "business_date", "from": "2025-01-01", "to": "2025-01-03", "step": "1d"},
        plan_hash="plan-a",
        config_hash="config-a",
        chunks=[
            BackfillChunkRecord(index=1, start="2025-01-01", end="2025-01-02", idempotency_key="k:1"),
            BackfillChunkRecord(index=2, start="2025-01-02", end="2025-01-03", idempotency_key="k:2"),
        ],
    )


def _store(tmp_path: Path) -> FileBackfillStateStore:
    store = FileBackfillStateStore(tmp_path)
    store.save(_ledger())
    return store


def test_campaign_lock_is_exclusive_and_released(tmp_path: Path) -> None:
    store = _store(tmp_path)
    expires_at = datetime.now(UTC) + timedelta(minutes=30)

    assert store.acquire_campaign_lock("campaign-a", owner="worker-a", lease_expires_at=expires_at)
    assert not store.acquire_campaign_lock("campaign-a", owner="worker-b", lease_expires_at=expires_at)

    store.release_campaign_lock("campaign-a", owner="worker-a")

    assert store.acquire_campaign_lock("campaign-a", owner="worker-b", lease_expires_at=expires_at)


def test_expired_campaign_lock_can_be_reclaimed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime.now(UTC)

    assert store.acquire_campaign_lock("campaign-a", owner="dead-pod", lease_expires_at=now - timedelta(minutes=1))
    assert store.acquire_campaign_lock("campaign-a", owner="new-pod", lease_expires_at=now + timedelta(minutes=30))

    ledger = store.load("campaign-a")
    assert ledger is not None
    assert ledger.lock_owner == "new-pod"


def test_campaign_cancel_blocks_new_chunks(tmp_path: Path) -> None:
    store = _store(tmp_path)

    store.request_cancel("campaign-a", reason="operator requested stop", requested_by="qa")
    ledger = store.load("campaign-a")

    assert ledger is not None
    assert ledger.status == "cancel_requested"
    assert ledger.cancel_reason == "operator requested stop"
    assert not store.acquire_chunk_lease(
        "campaign-a",
        1,
        owner="worker-a",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )


def test_stale_running_chunk_becomes_retryable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime.now(UTC)
    assert store.acquire_chunk_lease("campaign-a", 1, owner="pod-a", lease_expires_at=now - timedelta(minutes=1))

    recovered = store.recover_stale_running("campaign-a", now=now)
    ledger = store.load("campaign-a")

    assert recovered == [1]
    assert ledger is not None
    assert ledger.chunk(1).status == CHUNK_STATUS_FAILED
    assert ledger.chunk(1).error == "lease_expired"


def test_successful_chunks_are_skipped_but_failed_chunks_are_retryable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    ledger = store.load("campaign-a")
    assert ledger is not None
    first = ledger.chunk(1)
    first.status = CHUNK_STATUS_SUCCESS
    second = ledger.chunk(2)
    second.status = CHUNK_STATUS_FAILED
    second.error = "previous failure"
    store.update_chunk(ledger, first)
    store.update_chunk(ledger, second)

    assert store.retryable_chunk_indexes("campaign-a") == [2]
    assert ledger.counts()[CHUNK_STATUS_PENDING] == 0
    assert ledger.counts()[CHUNK_STATUS_SUCCESS] == 1


def test_config_hash_drift_blocks_accidental_resume(tmp_path: Path) -> None:
    store = _store(tmp_path)

    assert store.load_verified("campaign-a", plan_hash="plan-a", config_hash="config-a") is not None
    try:
        store.load_verified("campaign-a", plan_hash="plan-a", config_hash="config-b")
    except ValueError as exc:
        assert "backfill campaign config hash changed" in str(exc)
    else:
        raise AssertionError("expected config hash drift to block resume")
