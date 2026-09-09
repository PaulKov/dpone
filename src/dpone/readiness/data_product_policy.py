"""Provider-neutral data product policy-as-code and gate decisions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from importlib import import_module
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

POLICY_PACK_SCHEMA = "dpone.data_product_policy_pack.v1"
POLICY_EVALUATION_SCHEMA = "dpone.data_product_policy_evaluation.v1"
POLICY_GATE_SCHEMA = "dpone.data_product_policy_gate.v1"

_PASSING_STATUSES = {
    "allowed",
    "approved",
    "certified",
    "dry_run",
    "healthy",
    "passed",
    "ready",
    "rendered",
    "resolved",
    "stable",
    "usable",
    "verified",
    "waived",
    "warning",
}
_BLOCKING_BY_PROFILE = {
    "advisory": set(),
    "stage": {"critical"},
    "prod_strict": {"critical", "high"},
    "regulated": {"critical", "high", "medium"},
}


class DataProductPolicyEvaluator:
    """Evaluates manifest policy packs against local release evidence."""

    def evaluate(
        self,
        *,
        manifest: Mapping[str, Any],
        evidence: Mapping[str, Mapping[str, Any]],
        pack_id: str | None = None,
        bundle_id: str | None = None,
    ) -> dict[str, Any]:
        product = _product(manifest)
        options = _policy_options(product)
        if not options.get("enabled"):
            return _evaluation_payload(
                status="disabled",
                product=product,
                options=options,
                packs=(),
                rules=(),
                blockers=(),
                warnings=(),
                pack_id=pack_id,
                bundle_id=bundle_id,
            )
        packs = _policy_packs(options)
        rules = tuple(_evaluate_rule(rule, evidence) for pack in packs for rule in pack.get("rules", []))
        blockers = tuple(detail for rule in rules for detail in rule.get("blockers", []))
        warnings = tuple(detail for rule in rules for detail in rule.get("warnings", []))
        status = "blocked" if blockers else "warning" if warnings else "passed"
        return _evaluation_payload(
            status=status,
            product=product,
            options=options,
            packs=packs,
            rules=rules,
            blockers=blockers,
            warnings=warnings,
            pack_id=pack_id,
            bundle_id=bundle_id,
        )


class DataProductPolicyGate:
    """Combines policy evaluation and approved waivers into a release decision."""

    def evaluate(
        self,
        *,
        evaluation: Mapping[str, Any],
        waivers: Sequence[Mapping[str, Any]] = (),
        authority_gate: Mapping[str, Any] | None = None,
        profile: str = "prod_strict",
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        waived_rules: list[str] = []
        for rule in _blocking_rules(evaluation, profile):
            covered, waiver_blockers = _valid_waiver(evaluation, rule, waivers, observed_at)
            if covered:
                authority_blockers = _authority_gate_blockers(evaluation, authority_gate, profile)
                if authority_blockers:
                    blockers.extend(authority_blockers)
                    continue
                waived_rules.append(str(rule.get("id")))
                continue
            blockers.extend(waiver_blockers)
            blockers.append(f"data_product_policy.uncovered_rule:{rule.get('id')}")
        for rule in _warning_rules(evaluation, profile):
            warnings.extend(str(item) for item in rule.get("warnings", []) if str(item))
            if rule.get("status") == "failed":
                warnings.append(f"data_product_policy.rule_warning:{rule.get('id')}")
        if profile == "advisory":
            warnings.extend(str(item) for item in evaluation.get("blockers", []) if str(item))
            blockers = []
        warnings.extend(str(item) for item in evaluation.get("warnings", []) if str(item))
        status = "blocked" if blockers else "waived" if waived_rules else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": POLICY_GATE_SCHEMA,
            "status": status,
            "profile": profile,
            "product_id": _product_id(evaluation),
            "policy_evaluation_id": evaluation.get("policy_evaluation_id"),
            "policy_pack_id": evaluation.get("policy_pack_id"),
            "authority_gate_id": authority_gate.get("authority_gate_id")
            if isinstance(authority_gate, Mapping)
            else None,
            "pack_id": evaluation.get("pack_id"),
            "bundle_id": evaluation.get("bundle_id"),
            "rules": [dict(rule) for rule in evaluation.get("rules", []) if isinstance(rule, Mapping)],
            "waived_rules": list(dict.fromkeys(waived_rules)),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _gate_recommendations(status),
        }
        payload["policy_gate_id"] = stable_fingerprint(payload)
        return payload

    def report(self, *, gate: Mapping[str, Any]) -> dict[str, Any]:
        renderer = import_module("dpone.readiness.data_product_policy_rendering")
        return renderer.PolicyReportRenderer().report(gate=gate)


def _evaluation_payload(
    *,
    status: str,
    product: Mapping[str, Any],
    options: Mapping[str, Any],
    packs: Sequence[Mapping[str, Any]],
    rules: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    warnings: Sequence[str],
    pack_id: str | None,
    bundle_id: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": POLICY_EVALUATION_SCHEMA,
        "status": status,
        "mode": str(options.get("mode") or "gate"),
        "profile": str(options.get("profile") or "prod_strict"),
        "product": _product_ref(product),
        "pack_id": pack_id,
        "bundle_id": bundle_id,
        "policy_packs": [dict(pack) for pack in packs],
        "policy_pack_id": stable_fingerprint({"packs": packs}),
        "rules": [dict(rule) for rule in rules],
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "waiver_policy": dict(options.get("waivers", {})) if isinstance(options.get("waivers"), Mapping) else {},
        "authority_policy": _authority_policy(product),
        "recommendations": _evaluation_recommendations(status),
    }
    payload["policy_evaluation_id"] = stable_fingerprint(payload)
    return payload


def _evaluate_rule(rule: Mapping[str, Any], evidence: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    rule_id = str(rule.get("id") or "unnamed")
    for kind in _strings(rule.get("require_artifacts")):
        payload = evidence.get(kind)
        if payload is None:
            blockers.append(f"data_product_policy.required_artifact_missing:{rule_id}:{kind}")
        elif str(payload.get("status")) not in _PASSING_STATUSES:
            blockers.append(f"data_product_policy.artifact_status_blocked:{rule_id}:{kind}")
            blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
        else:
            warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))
    require_status = rule.get("require_status")
    if isinstance(require_status, Mapping):
        for kind, allowed in require_status.items():
            payload = evidence.get(str(kind))
            allowed_statuses = set(_strings(allowed))
            if payload is None:
                blockers.append(f"data_product_policy.required_artifact_missing:{rule_id}:{kind}")
            elif str(payload.get("status")) not in allowed_statuses:
                blockers.append(f"data_product_policy.artifact_status_blocked:{rule_id}:{kind}")
                blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
            else:
                warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))
    status = "failed" if blockers else "warning" if warnings else "passed"
    return {
        "id": rule_id,
        "status": status,
        "severity": str(rule.get("severity") or "medium"),
        "owner": rule.get("owner"),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }


def _valid_waiver(
    evaluation: Mapping[str, Any],
    rule: Mapping[str, Any],
    waivers: Sequence[Mapping[str, Any]],
    observed_at: str | None,
) -> tuple[bool, list[str]]:
    waiver_covers = import_module("dpone.readiness.data_product_policy_waivers").waiver_covers
    blockers: list[str] = []
    for waiver in waivers:
        covered, current = waiver_covers(
            waiver=waiver,
            evaluation=evaluation,
            rule_id=str(rule.get("id")),
            observed_at=observed_at,
        )
        blockers.extend(current)
        if covered:
            return True, []
    return False, list(dict.fromkeys(blockers))


def _authority_gate_blockers(
    evaluation: Mapping[str, Any],
    authority_gate: Mapping[str, Any] | None,
    profile: str,
) -> list[str]:
    authority = evaluation.get("authority_policy")
    if profile == "advisory" or not isinstance(authority, Mapping) or not authority.get("enabled"):
        return []
    if str(authority.get("mode") or "gate") != "gate":
        return []
    if not authority_gate:
        return ["data_product_policy.authority_gate_required"]
    if authority_gate.get("status") == "blocked":
        blockers = ["data_product_policy.authority_gate_blocked"]
        blockers.extend(str(item) for item in authority_gate.get("blockers", []) if str(item))
        return blockers
    return []


def _blocking_rules(evaluation: Mapping[str, Any], profile: str) -> tuple[Mapping[str, Any], ...]:
    severities = _BLOCKING_BY_PROFILE.get(profile, _BLOCKING_BY_PROFILE["prod_strict"])
    return tuple(
        rule
        for rule in _rules(evaluation)
        if rule.get("status") == "failed" and str(rule.get("severity", "")).lower() in severities
    )


def _warning_rules(evaluation: Mapping[str, Any], profile: str) -> tuple[Mapping[str, Any], ...]:
    blocking = set(id(rule) for rule in _blocking_rules(evaluation, profile))
    return tuple(rule for rule in _rules(evaluation) if id(rule) not in blocking and rule.get("status") != "passed")


def _rules(evaluation: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    rules = evaluation.get("rules")
    return tuple(item for item in rules if isinstance(item, Mapping)) if isinstance(rules, list) else ()


def _product(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    sink = manifest.get("sink")
    options = sink.get("options") if isinstance(sink, Mapping) else {}
    product = options.get("data_product") if isinstance(options, Mapping) else {}
    return product if isinstance(product, Mapping) else {}


def _policy_options(product: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = product.get("policy")
    return raw if isinstance(raw, Mapping) else {}


def _authority_policy(product: Mapping[str, Any]) -> dict[str, Any]:
    raw = product.get("authority")
    if not isinstance(raw, Mapping):
        return {"enabled": False}
    return {
        "enabled": bool(raw.get("enabled")),
        "mode": str(raw.get("mode") or "gate"),
        "profile": str(raw.get("profile") or "prod_strict"),
    }


def _policy_packs(options: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw = options.get("packs")
    return tuple(item for item in raw if isinstance(item, Mapping)) if isinstance(raw, list) else ()


def _product_ref(product: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": product.get("id"),
        "owner": product.get("owner"),
        "tier": product.get("tier"),
        "criticality": product.get("criticality"),
    }


def _product_id(evaluation: Mapping[str, Any]) -> str | None:
    product = evaluation.get("product")
    return str(product.get("id")) if isinstance(product, Mapping) and product.get("id") else None


def _strings(raw: object) -> tuple[str, ...]:
    if isinstance(raw, str):
        return (raw,)
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list) else ()


def _evaluation_recommendations(status: str) -> list[str]:
    if status == "disabled":
        return ["Enable data_product.policy to evaluate policy-as-code evidence."]
    if status == "blocked":
        return ["Resolve failed policy rules or attach approved, unexpired waivers."]
    if status == "warning":
        return ["Review policy warnings before release closeout."]
    return ["Attach policy evaluation to bundle and registry evidence."]


def _gate_recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Do not release until policy blockers are resolved."]
    if status == "waived":
        return ["Release may continue only within the approved waiver window."]
    if status == "warning":
        return ["Attach warnings to release evidence."]
    return ["Policy gate is ready for bundle and registry evidence."]


__all__ = [
    "POLICY_EVALUATION_SCHEMA",
    "POLICY_GATE_SCHEMA",
    "POLICY_PACK_SCHEMA",
    "DataProductPolicyEvaluator",
    "DataProductPolicyGate",
]
