"""Fenced distributed-DDL operations for the external publication adapter."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any


def dispatch_publication_once(
    adapter: Any, *, operation_id: str, candidate_name: str, member_ids: tuple[str, ...]
) -> None:
    record = adapter._current(operation_id).record
    if record.candidate != candidate_name or adapter._state_ops.member_ids(record) != member_ids:
        adapter._state_ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
    if {str(value) for value in publication_states(adapter, record).values()} != {"pending"}:
        adapter._state_ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
    adapter._ddl.dispatch_publication(
        record,
        _take_permit(adapter, operation_id, "publication"),
        cluster=adapter._cluster,
    )


def observe_publication(adapter: Any, operation_id: str) -> Mapping[str, str]:
    record = adapter._current(operation_id).record
    ops = adapter._state_ops
    entry = ops.require_queue_entry(adapter._ddl, adapter._cluster, record, cleanup=False, fail=ops.fail_external)
    states = publication_states(adapter, record)
    queue = entry.state_for(ops.member_ids(record))
    if "unknown" in {str(value) for value in states.values()} or str(queue) == "unknown":
        ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_DDL_UNKNOWN", record)
    if str(queue) in {"terminal_success", "terminal_failure"} and len(set(states.values())) > 1:
        ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL", record)
    return {member_id: "desired" if str(state) == "committed" else "predecessor" for member_id, state in states.items()}


def dispatch_cleanup_once(adapter: Any, *, operation_id: str, member_ids: tuple[str, ...]) -> None:
    record = adapter._current(operation_id).record
    ops = adapter._state_ops
    if ops.member_ids(record) != member_ids:
        ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", record)
    if ops.has_predecessor(record):
        if set(cleanup_states(adapter, record).values()) != {True}:
            ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
        adapter._ddl.drop_predecessor(
            record,
            _take_permit(adapter, operation_id, "cleanup"),
            cluster=adapter._cluster,
        )


def observe_cleanup(adapter: Any, operation_id: str) -> Mapping[str, bool]:
    record = adapter._current(operation_id).record
    ops = adapter._state_ops
    if (
        str(record.phase) == "CLEANUP_DISPATCHING"
        and ops.has_predecessor(record)
        and ops.require_queue_entry(
            adapter._ddl, adapter._cluster, record, cleanup=True, fail=ops.fail_external
        ).state_for(ops.member_ids(record))
        == "unknown"
    ):
        ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
    return cleanup_states(adapter, record)


def require_publication(adapter: Any, record: Any) -> tuple[Any, tuple[Any, ...]]:
    ops = adapter._state_ops
    entry = ops.require_queue_entry(adapter._ddl, adapter._cluster, record, cleanup=False, fail=ops.fail_external)
    if str(entry.state_for(ops.member_ids(record))) not in {"terminal_success", "terminal_failure"}:
        ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS", record)
    states = publication_states(adapter, record)
    if {str(value) for value in states.values()} != {"committed"}:
        ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL", record)
    members = tuple(replace(member, publication_state=states[member.member_id]) for member in record.members)
    return entry, members


def require_cleanup(adapter: Any, record: Any) -> tuple[Any | None, tuple[Any, ...]]:
    ops = adapter._state_ops
    entry = (
        ops.require_queue_entry(adapter._ddl, adapter._cluster, record, cleanup=True, fail=ops.fail_external)
        if ops.has_predecessor(record)
        else None
    )
    if entry is not None and str(entry.state_for(ops.member_ids(record))) not in {
        "terminal_success",
        "terminal_failure",
    }:
        ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", record)
    if any(cleanup_states(adapter, record).values()):
        ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", record)
    return entry, tuple(replace(member, cleanup_complete=True) for member in record.members)


def publication_states(adapter: Any, record: Any) -> dict[str, Any]:
    return {
        member.member_id: adapter._classify_member_publication(
            adapter._staging.observe(member.member_id, record),
            desired=adapter._state_ops.desired_generation(member),
            predecessor=member.predecessor,
        )
        for member in record.members
    }


def cleanup_states(adapter: Any, record: Any) -> dict[str, bool]:
    ops = adapter._state_ops
    result: dict[str, bool] = {}
    for member_id, state in publication_states(adapter, record).items():
        if str(state) not in {"committed", "cleanup_pending"}:
            ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
        result[member_id] = ops.member(record, member_id).predecessor is not None and str(state) == "committed"
    return result


def _take_permit(adapter: Any, operation_id: str, action: str) -> Any:
    permit = adapter._permits.pop((operation_id, action), None)
    if permit is None:
        adapter._state_ops.fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_UNKNOWN")
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
