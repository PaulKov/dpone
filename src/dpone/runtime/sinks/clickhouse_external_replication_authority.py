"""Authority-only transitions for external ClickHouse publication."""

from __future__ import annotations

import secrets
from collections.abc import Callable, Mapping
from typing import Any


def acquire_lock(
    *,
    target_key: str,
    operation_id: str,
    candidate_name: str,
    plan_sha256: str,
    members: tuple[str, ...],
    inventory_digest: str,
    read: Callable[[str], dict[str, Any] | None],
    cas: Callable[[dict[str, Any] | None, Mapping[str, Any]], dict[str, Any]],
    fail: Callable[..., Any],
) -> dict[str, Any]:
    current = read(target_key)
    if current is not None and current.get("operation_id") == operation_id:
        if (
            tuple(current["member_ids"]) != members
            or current["plan_digest"] != plan_sha256
            or current["inventory_digest"] != inventory_digest
        ):
            fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", state=current)
        return current
    if current is not None and current.get("phase") not in {"COMPLETED", "ABORTED"}:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=current)
    desired = {
        "target_key": target_key,
        "operation_id": operation_id,
        "fence_token": secrets.token_hex(16),
        "phase": "LOCKED",
        "dispatch_epoch": 0 if current is None else int(current["dispatch_epoch"]) + 1,
        "inventory_digest": inventory_digest,
        "plan_digest": plan_sha256,
        "candidate_name": candidate_name,
        "member_ids": members,
        "member_states": {member: {"state": "PENDING"} for member in members},
    }
    return cas(current, desired)


def abort_prepared(
    *,
    target_key: str,
    operation_id: str,
    read: Callable[[str], dict[str, Any] | None],
    cas: Callable[[dict[str, Any] | None, Mapping[str, Any]], dict[str, Any]],
    fail: Callable[..., Any],
    without_version: Callable[[Mapping[str, Any]], dict[str, Any]],
) -> None:
    state = read(target_key)
    if state is None or state.get("operation_id") != operation_id or state.get("phase") in {"ABORTED"}:
        return
    if state.get("phase") != "LOCKED":
        return
    if "artifact_sha256" in state or any(
        member.get("candidate_uuid") is not None for member in state.get("member_states", {}).values()
    ):
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", state=state)
    cas(state, {**without_version(state), "phase": "ABORTED"})


__all__ = ["abort_prepared", "acquire_lock"]
