"""Publication and cleanup phase transitions for the external coordinator."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from dpone.runtime.sinks.clickhouse_external_replication_state import without_version

Cas = Callable[[dict[str, Any] | None, Mapping[str, Any]], dict[str, Any]]
Fail = Callable[..., NoReturn]


def publish_phase(state: dict[str, Any], *, service: Any, cas: Cas, fail: Fail) -> dict[str, Any]:
    if state["phase"] == "STAGED":
        state = cas(
            state,
            {
                **without_version(state),
                "phase": "PUBLICATION_DISPATCHING",
                "dispatch_epoch": int(state["dispatch_epoch"]) + 1,
            },
        )
        try:
            service.dispatch_publication_once(
                operation_id=state["operation_id"],
                candidate_name=state["candidate_name"],
                member_ids=tuple(state["member_ids"]),
            )
        except Exception:
            pass
    if state["phase"] == "PUBLICATION_DISPATCHING":
        observed = service.observe_publication(state["operation_id"])
        if set(observed) != set(state["member_ids"]) or set(observed.values()) != {"desired"}:
            fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS", state=state)
        state = cas(state, {**without_version(state), "phase": "COMMITTED"})
    return state


def cleanup_phase(state: dict[str, Any], *, service: Any, cas: Cas, fail: Fail) -> dict[str, Any]:
    if state["phase"] == "COMMITTED":
        state = cas(
            state,
            {
                **without_version(state),
                "phase": "CLEANUP_DISPATCHING",
                "dispatch_epoch": int(state["dispatch_epoch"]) + 1,
            },
        )
        try:
            service.dispatch_cleanup_once(operation_id=state["operation_id"], member_ids=tuple(state["member_ids"]))
        except Exception:
            pass
    if state["phase"] == "CLEANUP_DISPATCHING":
        observed = service.observe_cleanup(state["operation_id"])
        if set(observed) != set(state["member_ids"]) or any(observed.values()):
            fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", state=state)
        state = cas(state, {**without_version(state), "phase": "COMPLETED"})
    if state["phase"] != "COMPLETED":
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STATE_INVALID", state=state)
    return state


__all__ = ["cleanup_phase", "publish_phase"]
