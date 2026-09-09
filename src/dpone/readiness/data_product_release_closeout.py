"""Release closeout gate for data product reliability evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

RELEASE_CLOSEOUT_GATE_SCHEMA = "dpone.data_product_release_closeout_gate.v1"


class ReleaseCloseoutGate:
    """Combines SLO, error budget, incident and watch evidence into one receipt."""

    def evaluate(
        self,
        *,
        slo_gate: Mapping[str, Any],
        budget_gate: Mapping[str, Any],
        incident: Mapping[str, Any] | None = None,
        watch_certificate: Mapping[str, Any] | None = None,
        post_apply_certificate: Mapping[str, Any] | None = None,
        policy_gate: Mapping[str, Any] | None = None,
        profile: str = "prod_strict",
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        _check_gate("slo_gate", slo_gate, {"allowed", "warning"}, blockers, warnings)
        _check_gate("error_budget_gate", budget_gate, {"allowed", "warning"}, blockers, warnings)
        _check_incident(incident, blockers, warnings)
        _check_optional_status(
            "watch_certificate",
            watch_certificate,
            {"stable", "warning"},
            "data_product_release_closeout.watch_certificate_blocked",
            blockers,
            warnings,
        )
        _check_optional_status(
            "post_apply_certificate",
            post_apply_certificate,
            {"verified", "warning"},
            "data_product_release_closeout.post_apply_certificate_blocked",
            blockers,
            warnings,
        )
        _check_optional_status(
            "policy_gate",
            policy_gate,
            {"allowed", "warning", "waived"},
            "data_product_release_closeout.policy_gate_blocked",
            blockers,
            warnings,
        )
        if profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        payload: dict[str, Any] = {
            "schema_version": RELEASE_CLOSEOUT_GATE_SCHEMA,
            "status": "blocked" if blockers else "warning" if warnings else "allowed",
            "profile": profile,
            "product_id": slo_gate.get("product_id")
            or budget_gate.get("product_id")
            or _optional(incident, "product_id"),
            "slo_gate_id": slo_gate.get("slo_gate_id"),
            "error_budget_gate_id": budget_gate.get("error_budget_gate_id"),
            "incident_id": _optional(incident, "incident_id"),
            "watch_certificate_id": _optional(watch_certificate, "certificate_id"),
            "post_apply_certificate_id": _optional(post_apply_certificate, "certificate_id"),
            "policy_gate_id": _optional(policy_gate, "policy_gate_id"),
            "pack_id": slo_gate.get("pack_id") or budget_gate.get("pack_id") or _optional(incident, "pack_id"),
            "bundle_id": slo_gate.get("bundle_id") or budget_gate.get("bundle_id") or _optional(incident, "bundle_id"),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["release_closeout_gate_id"] = stable_fingerprint(payload)
        return payload


def _check_gate(
    label: str,
    payload: Mapping[str, Any],
    allowed: set[str],
    blockers: list[str],
    warnings: list[str],
) -> None:
    status = payload.get("status")
    if status not in allowed:
        blockers.append(f"data_product_release_closeout.{label}_blocked")
    warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))
    blockers.extend(str(item) for item in payload.get("blockers", []) if str(item) and status not in allowed)


def _check_incident(
    incident: Mapping[str, Any] | None,
    blockers: list[str],
    warnings: list[str],
) -> None:
    if not incident:
        return
    status = incident.get("status")
    if status not in {"healthy", "resolved", "warning"}:
        blockers.append("data_product_release_closeout.incident_unresolved")
    warnings.extend(str(item) for item in incident.get("warnings", []) if str(item))
    blockers.extend(str(item) for item in incident.get("blockers", []) if str(item))


def _check_optional_status(
    label: str,
    payload: Mapping[str, Any] | None,
    allowed: set[str],
    code: str,
    blockers: list[str],
    warnings: list[str],
) -> None:
    if payload is None:
        return
    if payload.get("status") not in allowed:
        blockers.append(code)
    warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))
    if label and payload.get("status") == "warning":
        warnings.append(f"data_product_release_closeout.{label}_warning")


def _optional(payload: Mapping[str, Any] | None, key: str) -> Any:
    return payload.get(key) if isinstance(payload, Mapping) else None


def _recommendations(blockers: Sequence[object], warnings: Sequence[object]) -> list[str]:
    if blockers:
        return ["Do not close the data product release until blockers are resolved."]
    if warnings:
        return ["Closeout is possible, but attach warnings to release evidence."]
    return ["Release closeout evidence is ready for registry and bundle gates."]


__all__ = ["RELEASE_CLOSEOUT_GATE_SCHEMA", "ReleaseCloseoutGate"]
