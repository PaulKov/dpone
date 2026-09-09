"""Bounded SQL control plane for spawned MSSQL backfill lanes."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

from dpone.backfill.process_lane_lease import PROCESS_LANE_RENEW_INTERVAL

PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS = 5
_SCOPE_ERROR = "mssql_transaction.process_lane_control_timeout_scope_required"

if not 0 < PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS < PROCESS_LANE_RENEW_INTERVAL.total_seconds():
    raise RuntimeError("mssql_transaction.process_lane_control_timeout_budget_invalid")


class MssqlProcessLaneControlScope:
    """Cap only parent coordination sessions while process lanes are active.

    Child source, staging, BCP, and publication connections are created inside
    spawned processes and never enter this scope.  The short parent timeout is
    therefore a control-plane SLA, not a bulk-transfer timeout.
    """

    def __init__(
        self,
        connectors: Iterable[Any],
        *,
        database_authorities: Iterable[Any] = (),
        fresh_session_factories: Iterable[Any] = (),
    ) -> None:
        self._connectors = tuple(_distinct_connectors(connectors))
        self._database_authorities = tuple(database_authorities)
        self._fresh_session_factories = tuple(fresh_session_factories)

    @contextmanager
    def __call__(self, state_store: Any) -> Iterator[None]:
        """Bound static authorities plus sessions owned by the ledger store."""

        with ExitStack() as stack:
            for connector in self._connectors:
                scope = getattr(connector, "bounded_query_timeout", None)
                if not callable(scope):
                    raise RuntimeError(_SCOPE_ERROR)
                stack.enter_context(scope(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS))
            for authority in self._database_authorities:
                authority_scope = getattr(authority, "bounded_database_authority_query_timeout", None)
                if not callable(authority_scope):
                    raise RuntimeError(_SCOPE_ERROR)
                stack.enter_context(authority_scope(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS))
            for factory in self._fresh_session_factories:
                fresh_scope = getattr(factory, "bounded_query_timeout", None)
                if not callable(fresh_scope):
                    raise RuntimeError(_SCOPE_ERROR)
                stack.enter_context(fresh_scope(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS))
            store_scope = getattr(state_store, "bounded_process_lane_control_timeout", None)
            if callable(store_scope):
                stack.enter_context(store_scope(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS))
            elif getattr(state_store, "dialect", None) == "mssql":
                raise RuntimeError(_SCOPE_ERROR)
            yield


def _distinct_connectors(connectors: Iterable[Any]) -> tuple[Any, ...]:
    distinct: list[Any] = []
    identities: set[int] = set()
    for connector in connectors:
        if connector is None or id(connector) in identities:
            continue
        identities.add(id(connector))
        distinct.append(connector)
    return tuple(distinct)


__all__ = [
    "MssqlProcessLaneControlScope",
    "PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS",
]
