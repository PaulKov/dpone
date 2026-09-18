"""Fenced distributed-DDL operations for the external publication adapter."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from dpone.ports.clickhouse_external_replication import (
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalDispatchPermit,
    ExternalMemberRecord,
    MemberPublicationState,
    QueueEntry,
    QueueState,
    classify_member_publication,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import desired_generation as _desired
from dpone.runtime.sinks.clickhouse_external_replication_state import fail_external as _error
from dpone.runtime.sinks.clickhouse_external_replication_state import has_predecessor as _has_predecessor
from dpone.runtime.sinks.clickhouse_external_replication_state import member as _member
from dpone.runtime.sinks.clickhouse_external_replication_state import member_ids as _ids
from dpone.runtime.sinks.clickhouse_external_replication_state import require_queue_entry


def dispatch_publication_once(
    adapter: Any, *, operation_id: str, candidate_name: str, member_ids: tuple[str, ...]
) -> None:
    record = adapter._current(operation_id).record
    if record.candidate != candidate_name or _ids(record) != member_ids:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
    if set(publication_states(adapter, record).values()) != {MemberPublicationState.PENDING}:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
    adapter._ddl.dispatch_publication(
        record,
        _take_permit(adapter, operation_id, "publication"),
        cluster=adapter._cluster,
    )


def observe_publication(adapter: Any, operation_id: str) -> Mapping[str, str]:
    record = adapter._current(operation_id).record
    entry = require_queue_entry(adapter._ddl, adapter._cluster, record, cleanup=False, fail=_error)
    states = publication_states(adapter, record)
    queue = entry.state_for(_ids(record))
    if MemberPublicationState.UNKNOWN in states.values() or queue is QueueState.UNKNOWN:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_DDL_UNKNOWN", record)
    if queue in {QueueState.TERMINAL_SUCCESS, QueueState.TERMINAL_FAILURE} and len(set(states.values())) > 1:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL", record)
    return {
        member_id: "desired" if state is MemberPublicationState.COMMITTED else "predecessor"
        for member_id, state in states.items()
    }


def dispatch_cleanup_once(adapter: Any, *, operation_id: str, member_ids: tuple[str, ...]) -> None:
    record = adapter._current(operation_id).record
    if _ids(record) != member_ids:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", record)
    if _has_predecessor(record):
        if set(cleanup_states(adapter, record).values()) != {True}:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
        adapter._ddl.drop_predecessor(
            record,
            _take_permit(adapter, operation_id, "cleanup"),
            cluster=adapter._cluster,
        )


def observe_cleanup(adapter: Any, operation_id: str) -> Mapping[str, bool]:
    record = adapter._current(operation_id).record
    if (
        record.phase is ExternalAuthorityPhase.CLEANUP_DISPATCHING
        and _has_predecessor(record)
        and require_queue_entry(adapter._ddl, adapter._cluster, record, cleanup=True, fail=_error).state_for(
            _ids(record)
        )
        is QueueState.UNKNOWN
    ):
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
    return cleanup_states(adapter, record)


def require_publication(
    adapter: Any, record: ExternalAuthorityRecord
) -> tuple[QueueEntry, tuple[ExternalMemberRecord, ...]]:
    entry = require_queue_entry(adapter._ddl, adapter._cluster, record, cleanup=False, fail=_error)
    if entry.state_for(_ids(record)) not in {QueueState.TERMINAL_SUCCESS, QueueState.TERMINAL_FAILURE}:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS", record)
    states = publication_states(adapter, record)
    if set(states.values()) != {MemberPublicationState.COMMITTED}:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL", record)
    members = tuple(replace(member, publication_state=states[member.member_id]) for member in record.members)
    return entry, members


def require_cleanup(
    adapter: Any, record: ExternalAuthorityRecord
) -> tuple[QueueEntry | None, tuple[ExternalMemberRecord, ...]]:
    entry = (
        require_queue_entry(adapter._ddl, adapter._cluster, record, cleanup=True, fail=_error)
        if _has_predecessor(record)
        else None
    )
    if entry is not None and entry.state_for(_ids(record)) not in {
        QueueState.TERMINAL_SUCCESS,
        QueueState.TERMINAL_FAILURE,
    }:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", record)
    if any(cleanup_states(adapter, record).values()):
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", record)
    return entry, tuple(replace(member, cleanup_complete=True) for member in record.members)


def publication_states(adapter: Any, record: ExternalAuthorityRecord) -> dict[str, MemberPublicationState]:
    return {
        member.member_id: classify_member_publication(
            adapter._staging.observe(member.member_id, record),
            desired=_desired(member),
            predecessor=member.predecessor,
        )
        for member in record.members
    }


def cleanup_states(adapter: Any, record: ExternalAuthorityRecord) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for member_id, state in publication_states(adapter, record).items():
        if state not in {MemberPublicationState.COMMITTED, MemberPublicationState.CLEANUP_PENDING}:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
        result[member_id] = (
            _member(record, member_id).predecessor is not None and state is MemberPublicationState.COMMITTED
        )
    return result


def _take_permit(adapter: Any, operation_id: str, action: str) -> ExternalDispatchPermit:
    permit = adapter._permits.pop((operation_id, action), None)
    if permit is None:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_UNKNOWN")
    return permit


__all__ = [
    "cleanup_states",
    "dispatch_cleanup_once",
    "dispatch_publication_once",
    "observe_cleanup",
    "observe_publication",
    "publication_states",
    "require_cleanup",
    "require_publication",
]
