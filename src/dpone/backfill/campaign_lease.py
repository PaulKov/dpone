"""Short, synchronously renewed ownership for one backfill campaign."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar
from uuid import uuid4

from dpone.backfill.process_lane_lease import (
    PROCESS_LANE_LEASE_TTL,
    PROCESS_LANE_RENEW_INTERVAL,
)

RECOVERABLE_CAMPAIGN_OWNER_PREFIX = "dpone-backfill-campaign-lease-v2:"
LEGACY_SESSION_CAMPAIGN_OWNER_PREFIX = "dpone-backfill-campaign:"
DEFAULT_CAMPAIGN_LEASE_TTL = PROCESS_LANE_LEASE_TTL
DEFAULT_CAMPAIGN_RENEW_INTERVAL = PROCESS_LANE_RENEW_INTERVAL
_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.
_T = TypeVar("_T")


class BackfillCampaignLeaseError(RuntimeError):
    """Campaign ownership could not be acquired or renewed exactly."""

    code = "DPONE_BACKFILL_CAMPAIGN_LEASE_LOST"

    def __init__(self) -> None:
        super().__init__(self.code)


class BackfillCampaignLease:
    """Own and renew a durable campaign lease from a serial event loop.

    ``tick`` never starts a thread.  A process-lane parent calls it from the
    same loop that consumes child evidence, so SQL Server driver calls remain
    serialized in that interpreter.  The MSSQL store retains a session fence
    across non-preemptible parent calls and proves that same fence before it can
    refresh an elapsed timestamp. A terminated parent whose SQL session closes
    leaves at most one short TTL behind instead of the historical hour-long
    campaign tombstone.
    """

    def __init__(
        self,
        store: Any,
        run_key: str,
        *,
        owner: str | None = None,
        ttl: timedelta = DEFAULT_CAMPAIGN_LEASE_TTL,
        renew_interval: timedelta = DEFAULT_CAMPAIGN_RENEW_INTERVAL,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if ttl.total_seconds() <= 0 or renew_interval.total_seconds() <= 0 or renew_interval >= ttl:
            raise ValueError("backfill campaign lease requires 0 < renew_interval < ttl")
        resolved_owner = owner or f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}{uuid4().hex}"
        if not resolved_owner.startswith(RECOVERABLE_CAMPAIGN_OWNER_PREFIX):
            raise ValueError("recoverable campaign lease owner has an unsupported identity version")
        if not str(run_key).strip():
            raise ValueError("backfill campaign lease requires run_key")
        self.store = store
        self.run_key = run_key
        self.owner = resolved_owner
        self.ttl = ttl
        self.renew_interval = renew_interval
        self._now = now or (lambda: datetime.now(_UTC))
        self._next_renewal: datetime | None = None
        self._acquired = False
        self._healthy = True

    def acquire(self) -> None:
        """Acquire the first finite lease or fail without doing campaign work."""

        now = self._aware_now()
        if not self.store.acquire_campaign_lock(
            self.run_key,
            owner=self.owner,
            lease_expires_at=now + self.ttl,
        ):
            self._healthy = False
            raise BackfillCampaignLeaseError()
        self._acquired = True
        self._next_renewal = now + self.renew_interval

    def tick(self, *, force: bool = False) -> None:
        """Renew when due; raise immediately after an ownership loss."""

        self.assert_healthy()
        if not self._acquired or self._next_renewal is None:
            raise BackfillCampaignLeaseError()
        now = self._aware_now()
        if not force and now < self._next_renewal:
            return
        if not self.store.renew_campaign_lock(
            self.run_key,
            owner=self.owner,
            lease_expires_at=now + self.ttl,
        ):
            self._healthy = False
            raise BackfillCampaignLeaseError()
        self._next_renewal = now + self.renew_interval

    def assert_healthy(self) -> None:
        """Block publication after any failed acquisition or renewal."""

        if not self._healthy:
            raise BackfillCampaignLeaseError()

    def run_fenced(self, operation: Callable[[], _T]) -> _T:
        """Accept one idempotent phase result only under continuous ownership.

        Vendor statements such as index creation or an atomic shadow swap may
        legitimately run longer than a heartbeat interval.  They remain
        transactionally serialized by their vendor lock; this fence renews
        immediately before the phase and rejects its result if campaign
        ownership changed while the blocking statement was in flight.
        """

        self.tick(force=True)
        result = operation()
        self.tick(force=True)
        self.assert_healthy()
        return result

    def release(self) -> None:
        """Release only this owner; a lost owner is left for finite expiry."""

        if not self._acquired:
            return
        self.store.release_campaign_lock(self.run_key, owner=self.owner)
        self._acquired = False
        self._next_renewal = None

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("backfill campaign lease clock must be timezone-aware")
        return value.astimezone(_UTC)


__all__ = [
    "DEFAULT_CAMPAIGN_LEASE_TTL",
    "DEFAULT_CAMPAIGN_RENEW_INTERVAL",
    "LEGACY_SESSION_CAMPAIGN_OWNER_PREFIX",
    "RECOVERABLE_CAMPAIGN_OWNER_PREFIX",
    "BackfillCampaignLease",
    "BackfillCampaignLeaseError",
]
