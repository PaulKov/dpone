"""Incident classification for data product SLO evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

INCIDENT_REPORT_SCHEMA = "dpone.data_product_incident_report.v1"


class IncidentClassifier:
    """Maps SLO gate outcomes to deterministic incident reports."""

    def report(self, *, evaluation: Mapping[str, Any], gate: Mapping[str, Any]) -> dict[str, Any]:
        signals = _signals(evaluation, gate)
        severity = _severity(signals, evaluation)
        status = "healthy" if severity == "none" else "open"
        payload: dict[str, Any] = {
            "schema_version": INCIDENT_REPORT_SCHEMA,
            "status": status,
            "severity": severity,
            "product_id": evaluation.get("product_id") or gate.get("product_id"),
            "slo_plan_id": evaluation.get("slo_plan_id"),
            "slo_evaluation_id": evaluation.get("slo_evaluation_id"),
            "slo_gate_id": gate.get("slo_gate_id"),
            "pack_id": gate.get("pack_id") or evaluation.get("pack_id"),
            "bundle_id": gate.get("bundle_id") or evaluation.get("bundle_id"),
            "signals": signals,
            "failed_objectives": _failed_objectives(evaluation),
            "blockers": [str(item) for item in evaluation.get("blockers", []) if str(item)],
            "warnings": [str(item) for item in evaluation.get("warnings", []) if str(item)],
            "recommendations": _recommendations(severity, signals),
        }
        payload["incident_report_id"] = stable_fingerprint(payload)
        payload["markdown"] = _markdown(payload)
        return payload


def _signals(evaluation: Mapping[str, Any], gate: Mapping[str, Any]) -> list[str]:
    blockers = {str(item) for item in evaluation.get("blockers", []) if str(item)}
    warnings = {str(item) for item in evaluation.get("warnings", []) if str(item)}
    signals: list[str] = []
    if "data_product_slo.consumer_critical_failed" in blockers:
        signals.append("critical_consumer_failed")
    if "data_product_slo.freshness_breach" in blockers:
        signals.append("freshness_breach")
    if "data_product_slo.volume_breach" in blockers:
        signals.append("volume_breach")
    if gate.get("status") == "warning" or warnings:
        signals.append("warning_only")
    if gate.get("status") == "blocked" and not signals:
        signals.append("slo_gate_blocked")
    return list(dict.fromkeys(signals))


def _severity(signals: Sequence[str], evaluation: Mapping[str, Any]) -> str:
    incident = _incident_options(evaluation)
    configured = _mapping(incident.get("severity_map"))
    if "critical_consumer_failed" in signals:
        return str(configured.get("critical_consumer_failed") or "sev1")
    if "freshness_breach" in signals:
        return str(configured.get("freshness_breach") or "sev2")
    if "volume_breach" in signals:
        return str(configured.get("volume_breach") or "sev2")
    if signals:
        return str(configured.get("warning_only") or "sev3")
    return "none"


def _failed_objectives(evaluation: Mapping[str, Any]) -> list[dict[str, Any]]:
    checks = evaluation.get("checks", [])
    return [dict(item) for item in checks if isinstance(item, Mapping) and item.get("status") == "failed"]


def _incident_options(evaluation: Mapping[str, Any]) -> dict[str, Any]:
    plan = evaluation.get("plan")
    if isinstance(plan, Mapping):
        return _mapping(plan.get("incident"))
    return {}


def _recommendations(severity: str, signals: Sequence[str]) -> list[str]:
    if severity == "none":
        return ["No incident required; attach the healthy SLO report to release closeout."]
    if "critical_consumer_failed" in signals:
        return ["Route to the product owner and affected critical consumer owners before closeout."]
    return ["Investigate the failed SLO objective and attach remediation evidence to the registry."]


def _markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Incident Report",
        "",
        f"- product_id: {payload.get('product_id')}",
        f"- status: {payload.get('status')}",
        f"- severity: {payload.get('severity')}",
        f"- slo_gate_id: {payload.get('slo_gate_id')}",
        "",
        "## Signals",
        "",
    ]
    signals = payload.get("signals", [])
    lines.extend(f"- {item}" for item in signals) if signals else lines.append("- none")
    blockers = payload.get("blockers", [])
    if blockers:
        lines.extend(["", "## Blockers", "", *[f"- {item}" for item in blockers]])
    warnings = payload.get("warnings", [])
    if warnings:
        lines.extend(["", "## Warnings", "", *[f"- {item}" for item in warnings]])
    return "\n".join(lines) + "\n"


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = ["INCIDENT_REPORT_SCHEMA", "IncidentClassifier"]
