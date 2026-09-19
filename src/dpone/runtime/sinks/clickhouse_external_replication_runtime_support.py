"""Shared validation and evidence helpers for the external runtime coordinator."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from dpone.ports.clickhouse_external_replication import (
    ExternalArtifactSourcePort,
    ExternalPublicationError,
    ExternalPublicationRequest,
    ExternalReplicationReceipt,
    digest_payload,
)
from dpone.runtime.sinks import clickhouse_external_replication_phases as phase_ops


def inventory_digest(service: Any, members: tuple[str, ...]) -> str:
    provider = getattr(service, "inventory_digest", None)
    return str(provider()) if callable(provider) else digest_payload({"member_ids": members})


def receipt(service: Any, state: dict[str, Any]) -> ExternalReplicationReceipt:
    return ExternalReplicationReceipt.from_state(
        state,
        evidence_scope=str(service.evidence_scope),
        evidence_status=str(service.evidence_status),
    )


def compare_and_swap(
    service: Any,
    current: dict[str, Any] | None,
    desired: Mapping[str, Any],
    fail: Callable[..., NoReturn],
) -> dict[str, Any]:
    target_key = str(desired["target_key"])
    version = None if current is None else int(current["version"])
    try:
        return dict(service.compare_and_swap_authority(target_key, version, desired))
    except ExternalPublicationError:
        raise
    except Exception:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=current)


def require_same_inputs(
    *,
    service: Any,
    artifact_source: ExternalArtifactSourcePort | None,
    state: Mapping[str, Any],
    request: ExternalPublicationRequest,
    members: tuple[str, ...],
    fail: Callable[..., Any],
) -> None:
    binding = None if artifact_source is None or "artifact_binding_id" not in state else artifact_source.binding_id
    phase_ops.require_same_inputs(state, request, members, inventory_digest(service, members), binding, fail)


def fail(
    service: Any,
    code: str,
    *,
    state: Mapping[str, Any] | None = None,
    member_ids: tuple[str, ...] = (),
) -> NoReturn:
    evidence: dict[str, object] = {
        "evidence_scope": str(getattr(service, "evidence_scope", "runtime")),
        "evidence_status": "FAIL",
        "member_ids": list(member_ids or tuple(state.get("member_ids", ())) if state else member_ids),
    }
    if state is not None:
        evidence.update(
            target_key=state.get("target_key"),
            operation_id=state.get("operation_id"),
            phase=state.get("phase"),
        )
    raise ExternalPublicationError(code, evidence=evidence)


__all__ = ["compare_and_swap", "fail", "inventory_digest", "receipt", "require_same_inputs"]
