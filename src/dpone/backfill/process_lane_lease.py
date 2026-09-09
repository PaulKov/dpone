"""Internal recoverability timing shared by process-isolated backfill lanes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

PROCESS_LANE_LEASE_TTL = timedelta(seconds=90)
PROCESS_LANE_RENEW_INTERVAL = timedelta(seconds=30)
_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


def process_lane_lease_expires_at(*, now: datetime | None = None) -> datetime:
    """Return the finite parent-renewed expiry used by all process-lane leases."""

    resolved = now or datetime.now(_UTC)
    if resolved.tzinfo is None or resolved.utcoffset() is None:
        raise ValueError("backfill process-lane lease clock must be timezone-aware")
    return resolved.astimezone(_UTC) + PROCESS_LANE_LEASE_TTL


__all__ = [
    "PROCESS_LANE_LEASE_TTL",
    "PROCESS_LANE_RENEW_INTERVAL",
    "process_lane_lease_expires_at",
]
