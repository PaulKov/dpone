"""Provider-neutral data product remediation planning and closeout."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness import data_product_remediation_support as support
from dpone.readiness.data_product_trust_support import domain_for_kind


class DataProductRemediationPlanner:
    """Build deterministic owner-routed remediation plans from blocked evidence."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        trust_gate: Mapping[str, Any] | None = None,
        trust_snapshot: Mapping[str, Any] | None = None,
        evidence_payloads: Sequence[Mapping[str, Any]] = (),
        registry_records: Sequence[Mapping[str, Any]] = (),
        pack_id: str | None = None,
        bundle_id: str | None = None,
    ) -> dict[str, Any]:
        del registry_records
        options = support.DataProductRemediationOptions.from_manifest(manifest)
        if not options.enabled:
            return _plan_payload(options, (), (), "disabled", (), (), pack_id, bundle_id)
        refs = _evidence_refs(evidence_payloads, options.product)
        blockers = _input_blockers(options, trust_gate)
        signals = FailureSignalClassifier().classify(
            trust_gate=trust_gate or {},
            trust_snapshot=trust_snapshot or {},
            evidence_refs=refs,
            product=options.product,
        )
        actions, action_blockers = _actions(signals, options)
        blockers.extend(action_blockers)
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=[],
            profile=options.profile,
            mode=options.mode,
        )
        return _plan_payload(
            options,
            signals,
            actions,
            support.status(blockers, warnings, "ready"),
            blockers,
            warnings,
            pack_id or _first_value((trust_gate, *evidence_payloads), "pack_id"),
            bundle_id or _first_value((trust_gate, *evidence_payloads), "bundle_id"),
        )


class FailureSignalClassifier:
    """Classify blocker strings and blocked evidence refs into repair signals."""

    def classify(
        self,
        *,
        trust_gate: Mapping[str, Any],
        trust_snapshot: Mapping[str, Any],
        evidence_refs: Sequence[Mapping[str, Any]],
        product: Mapping[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        signals: list[dict[str, Any]] = []
        domains = _domains(trust_snapshot)
        for blocker in support.strings(trust_gate.get("blockers")):
            signals.extend(_signals_from_blocker(blocker, domains, product))
        for ref in evidence_refs:
            if ref.get("status") == "blocked" or ref.get("blockers"):
                signals.append(_signal_from_ref(ref, product, ref.get("blockers") or [ref.get("artifact_kind")]))
        return tuple(_dedupe_signals(signals))


class DataProductRemediationGate:
    """Validate that a remediation plan is actionable for the selected profile."""

    def evaluate(self, *, plan: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        if plan.get("status") == "disabled":
            blockers: list[str] = []
            warnings: list[str] = []
        else:
            blockers = list(support.strings(plan.get("blockers")))
            warnings = list(support.strings(plan.get("warnings")))
            if plan.get("source_blockers") and not support.mappings(plan.get("actions")):
                blockers.append("data_product_remediation.actions_required")
            for action in support.mappings(plan.get("actions")):
                blockers.extend(_action_quality_blockers(action))
            if profile == "regulated" and not _product(plan).get("owner"):
                blockers.append("data_product_remediation.owner_missing")
        blockers, warnings = support.apply_profile(blockers=blockers, warnings=warnings, profile=profile)
        payload = {
            "schema_version": support.GATE_SCHEMA,
            "status": "allowed" if plan.get("status") == "disabled" else support.status(blockers, warnings, "allowed"),
            "profile": profile,
            "product": _product(plan),
            "product_id": plan.get("product_id"),
            "pack_id": plan.get("pack_id"),
            "bundle_id": plan.get("bundle_id"),
            "remediation_plan_id": plan.get("remediation_plan_id"),
            "summary": dict(_mapping(plan.get("summary"))),
            "blockers": support.dedupe(blockers),
            "warnings": support.dedupe(warnings),
            "recommendations": _recommendations(blockers, warnings),
        }
        return support.payload_id(payload, "remediation_gate_id")


class DataProductRemediationCloseout:
    """Verify that expected fresh evidence satisfies a remediation plan."""

    def evaluate(self, *, plan: Mapping[str, Any], evidence_payloads: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        refs = _evidence_refs(evidence_payloads, _product(plan))
        by_kind = {str(ref.get("artifact_kind")): ref for ref in refs}
        blockers = list(support.strings(plan.get("blockers")))
        warnings = list(support.strings(plan.get("warnings")))
        closed = 0
        for action in support.mappings(plan.get("actions")):
            action_blockers = _closeout_blockers(action, by_kind, _fresh_required(plan))
            blockers.extend(action_blockers)
            if not action_blockers:
                closed += 1
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=warnings,
            profile=str(plan.get("profile") or "prod_strict"),
            mode=str(plan.get("mode") or "gate"),
        )
        payload = {
            "schema_version": support.CLOSEOUT_SCHEMA,
            "status": "allowed" if plan.get("status") == "disabled" else support.status(blockers, warnings, "allowed"),
            "profile": plan.get("profile"),
            "product": _product(plan),
            "product_id": plan.get("product_id"),
            "pack_id": plan.get("pack_id"),
            "bundle_id": plan.get("bundle_id"),
            "remediation_plan_id": plan.get("remediation_plan_id"),
            "summary": {"closed_actions": closed, "planned_actions": len(support.mappings(plan.get("actions")))},
            "evidence_refs": refs,
            "blockers": support.dedupe(blockers),
            "warnings": support.dedupe(warnings),
            "recommendations": _recommendations(blockers, warnings),
        }
        return support.payload_id(payload, "remediation_closeout_id")


def _signals_from_blocker(
    blocker: str, domains: Mapping[str, Mapping[str, Any]], product: Mapping[str, Any]
) -> list[dict[str, Any]]:
    prefix = "data_product_trust.domain_blocked:"
    missing = "data_product_trust.required_domain_missing:"
    if blocker.startswith(prefix):
        domain = blocker.removeprefix(prefix)
        refs = support.mappings(_mapping(domains.get(domain)).get("evidence_refs"))
        return [_signal_from_ref(ref, product, ref.get("blockers") or [blocker]) for ref in refs] or [
            _signal(domain=domain, code=blocker, product=product)
        ]
    if blocker.startswith(missing):
        return [_signal(domain=blocker.removeprefix(missing), code=blocker, product=product)]
    domain = _domain_from_code(blocker)
    return [_signal(domain=domain, code=blocker, product=product, mapped=bool(domain))]


def _signal_from_ref(ref: Mapping[str, Any], product: Mapping[str, Any], blockers: Any) -> dict[str, Any]:
    kind = str(ref.get("artifact_kind") or "")
    domain = str(ref.get("domain") or domain_for_kind(kind) or "")
    code = next(iter(support.strings(blockers)), str(ref.get("artifact_kind") or "blocked"))
    return _signal(
        domain=domain,
        code=code,
        product=product,
        owner=str(ref.get("owner") or product.get("owner") or ""),
        artifact_kind=kind,
        evidence_id=str(ref.get("evidence_id") or ""),
    )


def _signal(
    *,
    domain: str,
    code: str,
    product: Mapping[str, Any],
    owner: str = "",
    artifact_kind: str = "",
    evidence_id: str = "",
    mapped: bool = True,
) -> dict[str, Any]:
    payload = {
        "domain": domain or "unknown",
        "code": code,
        "severity": "critical",
        "owner": owner or str(product.get("owner") or ""),
        "artifact_kind": artifact_kind or support.DOMAIN_DEFAULT_KIND.get(domain, ""),
        "evidence_id": evidence_id,
        "mapped": mapped and bool(domain and domain in support.DOMAIN_COMMANDS),
    }
    return support.payload_id(payload, "signal_id")


def _actions(
    signals: Sequence[Mapping[str, Any]], options: support.DataProductRemediationOptions
) -> tuple[tuple[dict[str, Any], ...], list[str]]:
    actions: list[dict[str, Any]] = []
    blockers: list[str] = []
    for signal in signals:
        if not signal.get("mapped"):
            blockers.append(f"data_product_remediation.unmapped_signal:{signal.get('code')}")
            continue
        kind = str(signal.get("artifact_kind") or support.DOMAIN_DEFAULT_KIND.get(str(signal.get("domain")), ""))
        action = {
            "domain": signal.get("domain"),
            "owner": signal.get("owner") or options.product.get("owner"),
            "severity": signal.get("severity"),
            "repair_class": _repair_class(str(signal.get("domain"))),
            "source_signal_id": signal.get("signal_id"),
            "source_code": signal.get("code"),
            "preconditions": ["Review blocker evidence and run the command in dry-run/read-only mode first."],
            "commands": [support.DOMAIN_COMMANDS[str(signal.get("domain"))]],
            "expected_evidence": [
                {
                    "kind": kind,
                    "allowed_statuses": support.expected_statuses(kind),
                    "previous_evidence_id": signal.get("evidence_id"),
                }
            ],
        }
        actions.append(support.payload_id(action, "action_id"))
    return tuple(_dedupe_actions(actions)), blockers


def _closeout_blockers(
    action: Mapping[str, Any], evidence_by_kind: Mapping[str, Mapping[str, Any]], fresh_required: bool
) -> list[str]:
    blockers: list[str] = []
    for expected in support.mappings(action.get("expected_evidence")):
        kind = str(expected.get("kind") or "")
        ref = evidence_by_kind.get(kind)
        if not ref:
            blockers.append(f"data_product_remediation.closeout_evidence_missing:{kind}")
            continue
        if str(ref.get("status")) not in set(support.strings(expected.get("allowed_statuses"))):
            blockers.append(f"data_product_remediation.closeout_evidence_blocked:{kind}")
        if fresh_required and ref.get("evidence_id") == expected.get("previous_evidence_id"):
            blockers.append(f"data_product_remediation.closeout_stale_evidence:{kind}")
    return blockers


def _plan_payload(
    options: support.DataProductRemediationOptions,
    signals: Sequence[Mapping[str, Any]],
    actions: Sequence[Mapping[str, Any]],
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
    pack_id: str | None,
    bundle_id: str | None,
) -> dict[str, Any]:
    payload = {
        "schema_version": support.PLAN_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "product": dict(options.product),
        "product_id": options.product.get("id"),
        "pack_id": pack_id,
        "bundle_id": bundle_id,
        "signals": [dict(signal) for signal in signals],
        "actions": [dict(action) for action in actions],
        "summary": {
            "signals": len(signals),
            "actions": len(actions),
            "critical_actions": sum(1 for action in actions if action.get("severity") == "critical"),
        },
        "source_blockers": support.dedupe(str(signal.get("code")) for signal in signals),
        "blockers": support.dedupe(blockers),
        "warnings": support.dedupe(warnings),
        "recommendations": _recommendations(blockers, warnings),
    }
    payload["closeout_policy"] = {"requires_fresh_evidence": options.closeout_requires_fresh_evidence}
    return support.payload_id(payload, "remediation_plan_id")


def _input_blockers(options: support.DataProductRemediationOptions, trust_gate: Mapping[str, Any] | None) -> list[str]:
    if options.require_trust_gate and not trust_gate:
        return ["data_product_remediation.trust_gate_required"]
    return []


def _action_quality_blockers(action: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if not support.mappings(action.get("expected_evidence")):
        blockers.append("data_product_remediation.expected_evidence_missing")
    if not support.strings(action.get("commands")):
        blockers.append("data_product_remediation.command_template_missing")
    if not action.get("owner"):
        blockers.append("data_product_remediation.owner_missing")
    return blockers


def _domain_from_code(code: str) -> str:
    if code.startswith(("data_product_assertion", "data_product_assertions")):
        return "quality"
    if code.startswith(("data_product_slo", "data_product_error_budget")):
        return "reliability"
    if code.startswith("data_product_cost"):
        return "cost"
    if code.startswith("data_product_rollout"):
        return "rollout"
    return ""


def _domains(snapshot: Mapping[str, Any]) -> Mapping[str, Mapping[str, Any]]:
    return _mapping(snapshot.get("domains"))


def _evidence_refs(payloads: Sequence[Mapping[str, Any]], product: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [support.evidence_ref(payload, product) for payload in payloads if isinstance(payload, Mapping)]


def _dedupe_signals(signals: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_key = {(item.get("domain"), item.get("code"), item.get("artifact_kind")): dict(item) for item in signals}
    return sorted(by_key.values(), key=lambda item: str(item.get("signal_id") or ""))


def _dedupe_actions(actions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_key = {(item.get("domain"), item.get("source_code")): dict(item) for item in actions}
    return sorted(by_key.values(), key=lambda item: str(item.get("action_id") or ""))


def _repair_class(domain: str) -> str:
    return {"quality": "quality_assertion", "trust": "trust_evidence"}.get(domain, f"{domain}_evidence")


def _fresh_required(plan: Mapping[str, Any]) -> bool:
    policy = _mapping(plan.get("closeout_policy"))
    return bool(policy.get("requires_fresh_evidence", True))


def _first_value(payloads: Sequence[Mapping[str, Any] | None], key: str) -> str | None:
    for payload in payloads:
        if isinstance(payload, Mapping) and payload.get(key):
            return str(payload[key])
    return None


def _recommendations(blockers: Sequence[str], warnings: Sequence[str]) -> list[str]:
    if blockers:
        return ["Resolve remediation plan blockers or add a deterministic remediation catalog entry."]
    if warnings:
        return ["Review remediation warnings before release closeout."]
    return ["Run the listed domain commands, attach fresh evidence, then run remediation closeout."]


def _product(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(payload.get("product"))


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = [
    "DataProductRemediationCloseout",
    "DataProductRemediationGate",
    "DataProductRemediationPlanner",
    "FailureSignalClassifier",
]
