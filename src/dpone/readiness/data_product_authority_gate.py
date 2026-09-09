"""Authority gate decisions for data product approval evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from importlib import import_module
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

AUTHORITY_GATE_SCHEMA = "dpone.data_product_authority_gate.v1"
_STRICT_PROFILES = {"prod_strict", "regulated"}


class AuthorityGate:
    """Combine authority check, quorum and signatures into a release decision."""

    def evaluate(
        self,
        *,
        authority_check: Mapping[str, Any] | None = None,
        approval_quorum: Mapping[str, Any] | None = None,
        signatures: Sequence[Mapping[str, Any]] = (),
        profile: str = "prod_strict",
    ) -> dict[str, Any]:
        blockers, warnings = _gate_findings(
            authority_check=authority_check or {},
            approval_quorum=approval_quorum or {},
            signatures=signatures,
            profile=profile,
        )
        if profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        status = "blocked" if blockers else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": AUTHORITY_GATE_SCHEMA,
            "status": status,
            "profile": profile,
            "authority_check_id": (authority_check or {}).get("authority_check_id"),
            "approval_quorum_id": (approval_quorum or {}).get("approval_quorum_id"),
            "evidence_signature_ids": [
                str(item.get("evidence_signature_id"))
                for item in signatures
                if isinstance(item, Mapping) and item.get("evidence_signature_id")
            ],
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _gate_recommendations(status),
        }
        payload["authority_gate_id"] = stable_fingerprint(payload)
        return payload

    def report(self, *, gate: Mapping[str, Any]) -> dict[str, Any]:
        renderer = import_module("dpone.readiness.data_product_authority_rendering")
        return renderer.AuthorityReportRenderer().report(gate=gate)


def _gate_findings(
    *,
    authority_check: Mapping[str, Any],
    approval_quorum: Mapping[str, Any],
    signatures: Sequence[Mapping[str, Any]],
    profile: str,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if profile in _STRICT_PROFILES and not authority_check:
        blockers.append("data_product_authority.authority_check_required")
    if profile in _STRICT_PROFILES and not approval_quorum:
        blockers.append("data_product_authority.approval_quorum_required")
    _merge_gate_input(authority_check, "authority_check", blockers, warnings)
    _merge_gate_input(approval_quorum, "approval_quorum", blockers, warnings)
    if profile in _STRICT_PROFILES and not signatures:
        blockers.append("data_product_authority.evidence_signature_required")
    for signature in signatures:
        _merge_signature(signature, blockers, warnings)
    return blockers, warnings


def _merge_gate_input(payload: Mapping[str, Any], name: str, blockers: list[str], warnings: list[str]) -> None:
    if not payload:
        return
    if payload.get("status") == "blocked":
        blockers.append(f"data_product_authority.{name}_blocked")
        blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    elif payload.get("status") == "warning":
        warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))


def _merge_signature(payload: Mapping[str, Any], blockers: list[str], warnings: list[str]) -> None:
    status = str(payload.get("status") or "")
    if status not in {"signed", "verified"}:
        blockers.append("data_product_authority.evidence_signature_invalid")
        blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))


def _gate_recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Resolve authority, quorum or signature blockers before approving release evidence."]
    if status == "warning":
        return ["Review non-blocking authority warnings before regulated release closeout."]
    return []


__all__ = ["AUTHORITY_GATE_SCHEMA", "AuthorityGate"]
