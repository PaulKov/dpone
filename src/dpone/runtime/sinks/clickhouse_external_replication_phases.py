"""Publication and cleanup phase transitions for the external coordinator."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from dpone.runtime.sinks.clickhouse_external_replication_state import (
    candidate_name,
    matches_generation,
    owned_observation,
    require_replayable_artifact,
    without_version,
)

Cas = Callable[[dict[str, Any] | None, Mapping[str, Any]], dict[str, Any]]
Fail = Callable[..., NoReturn]


def validate_request(request: Any, fail: Fail) -> None:
    try:
        request.validate()
    except ValueError as error:
        code = str(getattr(error, "code", "REQUEST_INVALID"))
        fail(code)


def candidate_state(candidate: Mapping[str, Any] | None) -> dict[str, Any]:
    if candidate is None:
        return {"candidate_uuid": None}
    return {
        "candidate_uuid": candidate.get("candidate_uuid"),
        "candidate_engine_full": candidate.get("engine_full"),
        "candidate_schema_sha256": candidate.get("schema_sha256"),
        "candidate_content_sha256": candidate.get("content_sha256"),
        "candidate_row_count": candidate.get("row_count"),
    }


def require_same_inputs(
    state: Mapping[str, Any],
    request: Any,
    members: tuple[str, ...],
    inventory_digest: str,
    artifact_binding_id: str | None,
    fail: Fail,
) -> None:
    if tuple(state["member_ids"]) != members or state["plan_digest"] != request.plan_sha256:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", state=state)
    if state["inventory_digest"] != inventory_digest:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", state=state)
    if "artifact_sha256" in state and state["artifact_sha256"] != request.artifact.sha256:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED", state=state)
    if artifact_binding_id is not None and state.get("artifact_binding_id") != artifact_binding_id:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED", state=state)


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
        pending = service.observe_cleanup(state["operation_id"])
        if set(pending) != set(state["member_ids"]):
            fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", state=state)
        if not any(pending.values()):
            return cas(state, {**without_version(state), "phase": "COMPLETED"})
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


__all__ = [
    "candidate_name",
    "candidate_state",
    "cleanup_phase",
    "matches_generation",
    "owned_observation",
    "publish_phase",
    "require_replayable_artifact",
    "require_same_inputs",
    "validate_request",
    "without_version",
]
