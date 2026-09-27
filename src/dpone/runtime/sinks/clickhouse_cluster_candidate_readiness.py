"""Bounded exact-row admission for one replicated ClickHouse candidate."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence

CandidateCounts = Callable[[str, str, str], dict[str, int]]
ErrorFactory = Callable[[str, str], Exception]


def require_candidate_rows(
    error_factory: ErrorFactory,
    candidate_counts: CandidateCounts,
    cluster: str,
    database: str,
    candidate: str,
    hosts: Sequence[str],
    staged_rows: int,
    *,
    wait_seconds: float = 300,
    poll_seconds: float = 2,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Wait until every expected replica reports the exact staged row count."""
    deadline = monotonic() + wait_seconds
    while True:
        observed = candidate_counts(cluster, database, candidate)
        if set(observed) == set(hosts) and all(observed.get(host) == staged_rows for host in hosts):
            return
        if monotonic() >= deadline:
            detail = ",".join(f"{host}={observed.get(host, 'missing')}" for host in hosts)
            raise error_factory(
                "DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY",
                f"row count differs by replica observed={detail} expected={staged_rows}",
            )
        sleep(poll_seconds)


__all__ = ["CandidateCounts", "ErrorFactory", "require_candidate_rows"]
