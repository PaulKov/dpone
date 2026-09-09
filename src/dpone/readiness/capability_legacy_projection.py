"""Fail-closed compatibility projection for the legacy certification matrix."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.capability_discovery_protocols import CapabilitySnapshotProtocol


def legacy_certification_payload(
    snapshot: CapabilitySnapshotProtocol,
) -> dict[str, Any]:
    """Preserve the historical response shape without inventing certification.

    The legacy matrix describes connector-wide capabilities, while the
    canonical discovery snapshot carries route-level evidence. Those axes are
    intentionally not collapsed: route evidence is summarized for diagnostics,
    but cannot promote a connector capability to ``pass``.
    """

    route_statuses = _route_statuses_by_connector(snapshot)
    connectors: dict[str, dict[str, dict[str, object]]] = {}
    missing_required: list[str] = []
    for connector in snapshot.connectors:
        evidence = _legacy_evidence(
            snapshot_id=snapshot.snapshot_id,
            route_statuses=route_statuses.get(connector.id, ()),
        )
        capabilities: dict[str, dict[str, object]] = {}
        for capability_id in connector.capability_ids:
            capabilities[capability_id] = {
                "connector": connector.id,
                "capability": capability_id,
                "status": "unknown",
                "evidence": evidence,
            }
            missing_required.append(f"{connector.id}.{capability_id}")
        connectors[connector.id] = capabilities
    return {
        "passed": False,
        "missing_required": missing_required,
        "connectors": connectors,
        "snapshot_id": snapshot.snapshot_id,
    }


def legacy_studio_certification_payload(
    snapshot: CapabilitySnapshotProtocol,
) -> dict[str, Any]:
    """Return the deprecated Studio envelope with historical capability keys."""

    payload = legacy_certification_payload(snapshot)
    return {
        "levels": ["certified", "experimental", "community"],
        "connectors": payload["connectors"],
        "snapshot_id": snapshot.snapshot_id,
        "passed": payload["passed"],
        "missing_required": payload["missing_required"],
    }


def legacy_certification_markdown(payload: Mapping[str, Any]) -> str:
    """Render the compatibility report without re-evaluating its semantics."""

    lines = ["| connector | capability | status | evidence |", "|---|---|---|---|"]
    connectors = payload.get("connectors")
    if isinstance(connectors, Mapping):
        for connector_id, raw_capabilities in sorted(connectors.items()):
            if not isinstance(raw_capabilities, Mapping):
                continue
            for capability_id, raw_result in sorted(raw_capabilities.items()):
                result = raw_result if isinstance(raw_result, Mapping) else {}
                lines.append(
                    f"| {connector_id} | {capability_id} | "
                    f"{result.get('status', 'unknown')} | {result.get('evidence', '')} |"
                )
    missing = payload.get("missing_required")
    if isinstance(missing, list) and missing:
        lines.extend(("", "Missing required capabilities:"))
        lines.extend(f"- {item}" for item in missing)
    return "\n".join(lines) + "\n"


def _route_statuses_by_connector(
    snapshot: CapabilitySnapshotProtocol,
) -> dict[str, tuple[str, ...]]:
    statuses: dict[str, list[str]] = {}
    for route in snapshot.routes:
        for connector_id in (route.source, route.sink):
            statuses.setdefault(connector_id, []).append(route.certification.evidence_status)
    return {connector_id: tuple(sorted(set(items))) for connector_id, items in statuses.items()}


def _legacy_evidence(*, snapshot_id: str, route_statuses: tuple[str, ...]) -> str:
    status = ",".join(route_statuses) if route_statuses else "UNVERIFIED"
    return (
        "UNVERIFIED connector capability; route evidence is a separate axis "
        f"({status}); capability snapshot {snapshot_id}"
    )


__all__ = [
    "legacy_certification_markdown",
    "legacy_certification_payload",
    "legacy_studio_certification_payload",
]
