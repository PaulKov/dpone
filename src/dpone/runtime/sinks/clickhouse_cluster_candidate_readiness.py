"""Bounded exact-row admission for one replicated ClickHouse candidate."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence

CandidateCounts = Callable[[str, str, str], dict[str, int]]
ErrorFactory = Callable[[str, str], Exception]
DEFAULT_WAIT_SECONDS = 300


def require_candidate_rows(
    error_factory: ErrorFactory,
    candidate_counts: CandidateCounts,
    cluster: str,
    database: str,
    candidate: str,
    hosts: Sequence[str],
    staged_rows: int,
    *,
    wait_seconds: float = DEFAULT_WAIT_SECONDS,
    deadline: float | None = None,
    additional_readiness: Callable[[], str | None] | None = None,
    poll_seconds: float = 2,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Wait for exact rows and optional generation/health proof within one budget.

    The additional probe may return a transient reason or raise for immutable
    identity drift. A shared deadline prevents sequential barriers resetting
    the wait budget. Driver query timeouts remain a separate responsibility.
    """
    shared_deadline = deadline is not None
    deadline = monotonic() + wait_seconds if deadline is None else deadline
    timeout_detail = "shared readiness budget expired before probe"
    while True:
        if shared_deadline and monotonic() >= deadline:
            raise error_factory("DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY", timeout_detail)
        observed = candidate_counts(cluster, database, candidate)
        pending = additional_readiness() if additional_readiness is not None else None
        rows_ready = set(observed) == set(hosts) and all(observed.get(host) == staged_rows for host in hosts)
        if rows_ready and pending is None:
            return
        detail = ",".join(f"{host}={observed.get(host, 'missing')}" for host in hosts)
        timeout_detail = (
            pending
            if rows_ready and pending
            else f"row count differs by replica observed={detail} expected={staged_rows}"
        )
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise error_factory("DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY", timeout_detail)
        sleep(min(poll_seconds, remaining))


__all__ = ["CandidateCounts", "ErrorFactory", "require_candidate_rows"]
