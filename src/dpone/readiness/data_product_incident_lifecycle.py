"""Data product incident lifecycle and provider payload rendering."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

INCIDENT_LIFECYCLE_SCHEMA = "dpone.data_product_incident_lifecycle.v1"
INCIDENT_ROUTE_PAYLOAD_SCHEMA = "dpone.data_product_incident_route_payload.v1"


class IncidentLifecycleReducer:
    """Maintains deterministic append-only incident lifecycle artifacts."""

    def open(
        self,
        *,
        slo_evaluation: Mapping[str, Any],
        slo_gate: Mapping[str, Any],
        budget_gate: Mapping[str, Any],
        existing: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        signals = _signals(slo_evaluation, slo_gate, budget_gate)
        severity = _severity(signals)
        base = _base(existing, slo_evaluation, slo_gate, budget_gate, severity, signals)
        event = _event("opened", actor="dpone", details={"dedupe_key": _dedupe_key(base)})
        events = _events(existing)
        if any(
            item.get("kind") == "opened" and item.get("details", {}).get("dedupe_key") == _dedupe_key(base)
            for item in events
        ):
            return _finalize(base, events)
        return _finalize(base, (*events, event))

    def ack(self, *, incident: Mapping[str, Any], actor: str) -> dict[str, Any]:
        if not str(actor).strip():
            return _blocked(incident, "data_product_incident.actor_required")
        base = _copy_base(incident)
        events = (*_events(incident), _event("acknowledged", actor=str(actor), details={}))
        return _finalize(base, events)

    def resolve(self, *, incident: Mapping[str, Any], evidence: Mapping[str, Any]) -> dict[str, Any]:
        if evidence.get("status") not in {"allowed", "healthy", "resolved", "warning"} and not evidence.get(
            "accepted_exception"
        ):
            return _blocked(incident, "data_product_incident.resolution_evidence_not_green")
        base = _copy_base(incident)
        events = (
            *_events(incident),
            _event(
                "resolved",
                actor=str(evidence.get("actor") or "dpone"),
                details={
                    "evidence_schema_version": evidence.get("schema_version"),
                    "evidence_status": evidence.get("status"),
                    "evidence_id": _evidence_id(evidence),
                },
            ),
        )
        return _finalize(base, events)


class IncidentRouterPayloadRenderer:
    """Renders provider-specific payloads without network side effects."""

    def render(self, *, incident: Mapping[str, Any], provider: str) -> dict[str, Any]:
        normalized = provider.strip().lower()
        renderers = {
            "slack": _slack_payload,
            "jira": _jira_payload,
            "pagerduty": _pagerduty_payload,
        }
        payload_body = renderers.get(normalized, _generic_payload)(incident)
        payload: dict[str, Any] = {
            "schema_version": INCIDENT_ROUTE_PAYLOAD_SCHEMA,
            "status": "rendered",
            "provider": normalized or "generic",
            "incident_id": incident.get("incident_id"),
            "product_id": incident.get("product_id"),
            "severity": incident.get("severity"),
            "payload": payload_body,
            "network_writes": [],
            "warnings": []
            if normalized in renderers
            else [f"data_product_incident_route.unsupported_provider:{provider}"],
            "blockers": [],
        }
        payload["route_payload_id"] = stable_fingerprint(payload)
        return payload


def _base(
    existing: Mapping[str, Any] | None,
    evaluation: Mapping[str, Any],
    slo_gate: Mapping[str, Any],
    budget_gate: Mapping[str, Any],
    severity: str,
    signals: Sequence[str],
) -> dict[str, Any]:
    if existing:
        return _copy_base(existing)
    return {
        "schema_version": INCIDENT_LIFECYCLE_SCHEMA,
        "product_id": evaluation.get("product_id") or slo_gate.get("product_id") or budget_gate.get("product_id"),
        "severity": severity,
        "signals": list(signals),
        "slo_evaluation_id": evaluation.get("slo_evaluation_id"),
        "slo_gate_id": slo_gate.get("slo_gate_id"),
        "error_budget_gate_id": budget_gate.get("error_budget_gate_id"),
        "pack_id": slo_gate.get("pack_id") or budget_gate.get("pack_id"),
        "bundle_id": slo_gate.get("bundle_id") or budget_gate.get("bundle_id"),
        "blockers": [],
        "warnings": [],
    }


def _copy_base(incident: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in incident.items()
        if key
        not in {
            "incident_id",
            "status",
            "events",
            "recommendations",
        }
    }


def _finalize(base: Mapping[str, Any], events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    event_list = [dict(item) for item in events]
    status = _status(event_list, base)
    payload: dict[str, Any] = {
        **dict(base),
        "status": status,
        "events": event_list,
        "recommendations": _recommendations(status, base.get("severity")),
    }
    payload["incident_id"] = stable_fingerprint({key: value for key, value in payload.items() if key != "incident_id"})
    return payload


def _blocked(incident: Mapping[str, Any], code: str) -> dict[str, Any]:
    base = _copy_base(incident)
    blockers = [str(item) for item in incident.get("blockers", []) if str(item)]
    blockers.append(code)
    base["blockers"] = list(dict.fromkeys(blockers))
    return _finalize(base, _events(incident))


def _status(events: Sequence[Mapping[str, Any]], base: Mapping[str, Any]) -> str:
    if base.get("blockers"):
        return "blocked"
    kinds = [str(item.get("kind")) for item in events]
    if "resolved" in kinds:
        return "resolved"
    if "acknowledged" in kinds:
        return "acknowledged"
    return "open" if kinds else "healthy"


def _signals(
    evaluation: Mapping[str, Any],
    slo_gate: Mapping[str, Any],
    budget_gate: Mapping[str, Any],
) -> list[str]:
    blockers = {
        str(item) for raw in (evaluation, slo_gate, budget_gate) for item in raw.get("blockers", []) if str(item)
    }
    signals: list[str] = []
    if "data_product_slo.consumer_critical_failed" in blockers:
        signals.append("critical_consumer_failed")
    if any("fast_burn_exceeded" in item for item in blockers):
        signals.append("error_budget_fast_burn")
    if any("budget_remaining_exhausted" in item for item in blockers):
        signals.append("error_budget_exhausted")
    if any("freshness_breach" in item for item in blockers):
        signals.append("freshness_breach")
    if not signals and any(raw.get("status") == "blocked" for raw in (slo_gate, budget_gate)):
        signals.append("release_gate_blocked")
    return list(dict.fromkeys(signals))


def _severity(signals: Sequence[str]) -> str:
    if "critical_consumer_failed" in signals or "error_budget_fast_burn" in signals:
        return "sev1"
    if "error_budget_exhausted" in signals or "freshness_breach" in signals:
        return "sev2"
    return "sev3" if signals else "none"


def _event(kind: str, *, actor: str, details: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": kind,
        "actor": actor,
        "recorded_at": _utc_now(),
        "details": dict(details),
    }


def _events(incident: Mapping[str, Any] | None) -> tuple[dict[str, Any], ...]:
    raw = incident.get("events", []) if incident else []
    return tuple(dict(item) for item in raw if isinstance(item, Mapping)) if isinstance(raw, list) else ()


def _dedupe_key(base: Mapping[str, Any]) -> str:
    return stable_fingerprint(
        {
            "product_id": base.get("product_id"),
            "severity": base.get("severity"),
            "signals": base.get("signals", []),
            "slo_evaluation_id": base.get("slo_evaluation_id"),
            "error_budget_gate_id": base.get("error_budget_gate_id"),
        }
    )


def _evidence_id(payload: Mapping[str, Any]) -> Any:
    for key in ("slo_gate_id", "error_budget_gate_id", "release_closeout_gate_id", "incident_id"):
        if payload.get(key):
            return payload[key]
    return None


def _slack_payload(incident: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "text": f"{incident.get('severity')} data product incident for {incident.get('product_id')}",
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": "Data product incident"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": _summary(incident)}},
        ],
    }


def _jira_payload(incident: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "fields": {
            "summary": f"{incident.get('severity')} data product incident: {incident.get('product_id')}",
            "description": _summary(incident),
            "labels": ["dpone", "data-product", str(incident.get("severity"))],
        }
    }


def _pagerduty_payload(incident: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "routing_key": "<configured-outside-dpone>",
        "event_action": "trigger" if incident.get("status") != "resolved" else "resolve",
        "dedup_key": incident.get("incident_id"),
        "payload": {
            "summary": _summary(incident),
            "severity": str(incident.get("severity") or "error").replace("sev", "critical"),
            "source": "dpone",
        },
    }


def _generic_payload(incident: Mapping[str, Any]) -> dict[str, Any]:
    return {"summary": _summary(incident), "incident_id": incident.get("incident_id")}


def _summary(incident: Mapping[str, Any]) -> str:
    signals = ", ".join(str(item) for item in incident.get("signals", [])) or "none"
    return f"product={incident.get('product_id')} status={incident.get('status')} signals={signals}"


def _recommendations(status: str, severity: object) -> list[str]:
    if status == "resolved":
        return ["Attach the resolved lifecycle artifact to release closeout evidence."]
    if status == "acknowledged":
        return ["Track mitigation and resolve with green SLO or accepted exception evidence."]
    if severity == "sev1":
        return ["Acknowledge Sev1 before release closeout and route to product owners."]
    return ["Investigate and attach lifecycle evidence before release closeout."]


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


__all__ = [
    "INCIDENT_LIFECYCLE_SCHEMA",
    "INCIDENT_ROUTE_PAYLOAD_SCHEMA",
    "IncidentLifecycleReducer",
    "IncidentRouterPayloadRenderer",
]
