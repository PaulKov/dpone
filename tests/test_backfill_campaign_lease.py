from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dpone.backfill.campaign_lease import (
    RECOVERABLE_CAMPAIGN_OWNER_PREFIX,
    BackfillCampaignLease,
    BackfillCampaignLeaseError,
)
from dpone.backfill.state import BackfillLedger, FileBackfillStateStore


class _Clock:
    def __init__(self) -> None:
        # FileBackfillStateStore intentionally validates against the real UTC
        # clock, so a fixed wall-clock fixture would become a calendar time bomb.
        self.value = datetime.now(UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


def _store(tmp_path: Path) -> FileBackfillStateStore:
    store = FileBackfillStateStore(tmp_path)
    store.save(
        BackfillLedger(
            run_key="campaign-a",
            dataset="dbo.orders",
            inner_mode="incremental_append",
            chunk_config={},
        )
    )
    return store


def test_campaign_lease_renews_synchronously_and_releases_exact_owner(tmp_path: Path) -> None:
    clock = _Clock()
    store = _store(tmp_path)
    lease = BackfillCampaignLease(store, "campaign-a", now=clock)

    lease.acquire()
    acquired = store.load("campaign-a")
    assert acquired is not None
    assert str(acquired.lock_owner).startswith(RECOVERABLE_CAMPAIGN_OWNER_PREFIX)
    assert datetime.fromisoformat(str(acquired.lock_expires_at)) == clock.value + timedelta(seconds=90)

    clock.advance(29)
    lease.tick()
    assert store.load("campaign-a").lock_expires_at == acquired.lock_expires_at

    clock.advance(2)
    lease.tick()
    renewed = store.load("campaign-a")
    assert renewed is not None
    assert datetime.fromisoformat(str(renewed.lock_expires_at)) == clock.value + timedelta(seconds=90)

    lease.release()
    released = store.load("campaign-a")
    assert released is not None
    assert released.lock_owner is None
    assert released.lock_expires_at is None


def test_campaign_lease_fails_closed_after_owner_is_replaced(tmp_path: Path) -> None:
    clock = _Clock()
    store = _store(tmp_path)
    lease = BackfillCampaignLease(store, "campaign-a", now=clock)
    lease.acquire()
    ledger = store.load("campaign-a")
    assert ledger is not None
    ledger.lock_owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}replacement"
    store.save(ledger)

    with pytest.raises(BackfillCampaignLeaseError, match="DPONE_BACKFILL_CAMPAIGN_LEASE_LOST"):
        lease.tick(force=True)

    with pytest.raises(BackfillCampaignLeaseError, match="DPONE_BACKFILL_CAMPAIGN_LEASE_LOST"):
        lease.assert_healthy()


def test_campaign_lease_rejects_unsafe_timing_and_owner_versions(tmp_path: Path) -> None:
    store = _store(tmp_path)

    with pytest.raises(ValueError, match="renew_interval"):
        BackfillCampaignLease(
            store,
            "campaign-a",
            ttl=timedelta(seconds=30),
            renew_interval=timedelta(seconds=30),
        )
    with pytest.raises(ValueError, match="identity version"):
        BackfillCampaignLease(store, "campaign-a", owner="legacy-owner")


def test_live_campaign_cannot_be_stolen_but_dead_parent_expires_within_retry_window(tmp_path: Path) -> None:
    clock = _Clock()
    store = _store(tmp_path)
    first = BackfillCampaignLease(store, "campaign-a", now=clock)
    second = BackfillCampaignLease(store, "campaign-a", now=clock)
    first.acquire()

    with pytest.raises(BackfillCampaignLeaseError):
        second.acquire()

    clock.advance(5 * 60)
    expired = store.load("campaign-a")
    assert expired is not None
    expired.lock_expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    store.save(expired)
    retry = BackfillCampaignLease(store, "campaign-a", now=clock)
    retry.acquire()
    current = store.load("campaign-a")
    assert current is not None
    assert current.lock_owner == retry.owner
