"""Provider-neutral compliance control mapping for data product evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.readiness import data_product_compliance_support as support
from dpone.readiness.migration_control import stable_fingerprint

COMPLIANCE_PLAN_SCHEMA = "dpone.data_product_compliance_control_plan.v1"
COMPLIANCE_EVALUATION_SCHEMA = "dpone.data_product_compliance_control_evaluation.v1"
COMPLIANCE_GATE_SCHEMA = "dpone.data_product_compliance_gate.v1"

_BLOCKING_BY_PROFILE = {
    "advisory": set(),
    "stage": {"critical"},
    "prod_strict": {"critical", "high"},
    "regulated": {"critical", "high", "medium"},
}


@dataclass(frozen=True, slots=True)
class DataProductComplianceOptions:
    enabled: bool
    mode: str
    profile: str
    stale_evidence_policy: str
    stale_after_seconds: int | None

    @classmethod
    def from_product(cls, product: Mapping[str, Any]) -> DataProductComplianceOptions:
        raw = product.get("compliance")
        options = raw if isinstance(raw, Mapping) else {}
        return cls(
            enabled=support.bool_value(options.get("enabled"), False),
            mode=str(options.get("mode") or "gate"),
            profile=str(options.get("profile") or "prod_strict"),
            stale_evidence_policy=str(options.get("stale_evidence_policy") or "block"),
            stale_after_seconds=support.optional_int(options.get("stale_after_seconds")),
        )


class ComplianceControlCatalog:
    """Normalizes manifest framework controls into stable control records."""

    def build(self, product: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
        compliance = product.get("compliance") if isinstance(product.get("compliance"), Mapping) else {}
        controls: list[dict[str, Any]] = []
        for framework in support.mappings(compliance.get("frameworks")):
            framework_id = str(framework.get("id") or "custom")
            for control in support.mappings(framework.get("controls")):
                control_id = str(control.get("id") or f"{framework_id}.unnamed")
                required = tuple(support.strings(control.get("require_artifacts")))
                allowed = support.allowed_statuses(control.get("allowed_statuses"), required)
                controls.append(
                    {
                        "framework_id": framework_id,
                        "framework_version": framework.get("version"),
                        "framework_owner": framework.get("owner"),
                        "control_id": control_id,
                        "title": str(control.get("title") or control_id),
                        "severity": str(control.get("severity") or "medium"),
                        "required_artifacts": list(required),
                        "allowed_statuses": allowed,
                    }
                )
        return tuple(controls)


class ComplianceEvidenceIndex:
    """Normalizes local bundle, registry and evidence-dir artifacts."""

    def build(self, *, evidence: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
        refs: list[dict[str, Any]] = []
        payloads: dict[str, dict[str, Any]] = {}
        for kind, payload in sorted(evidence.items()):
            if not isinstance(payload, Mapping):
                continue
            payloads[str(kind)] = dict(payload)
            refs.append(_evidence_ref(str(kind), payload))
        return {"refs": refs, "payloads": payloads}


class ComplianceControlPlanner:
    """Builds a product-bound compliance control plan."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        evidence: Mapping[str, Mapping[str, Any]],
        pack_id: str | None = None,
        bundle_id: str | None = None,
    ) -> dict[str, Any]:
        product = support.product(manifest)
        options = DataProductComplianceOptions.from_product(product)
        if not options.enabled:
            return _plan_payload(
                status="disabled",
                product=product,
                options=options,
                controls=(),
                evidence_index={"refs": [], "payloads": {}},
                blockers=(),
                warnings=(),
                pack_id=pack_id,
                bundle_id=bundle_id,
            )
        controls = ComplianceControlCatalog().build(product)
        index = ComplianceEvidenceIndex().build(evidence=evidence)
        blockers = [] if controls else ["data_product_compliance.controls_missing"]
        return _plan_payload(
            status="blocked" if blockers else "ready",
            product=product,
            options=options,
            controls=controls,
            evidence_index=index,
            blockers=blockers,
            warnings=(),
            pack_id=pack_id or support.first_value(evidence, "pack_id"),
            bundle_id=bundle_id or support.first_value(evidence, "bundle_id"),
        )


class ComplianceControlEvaluator:
    """Evaluates planned controls against normalized evidence."""

    def evaluate(self, *, plan: Mapping[str, Any], observed_at: str | None = None) -> dict[str, Any]:
        if plan.get("status") == "disabled":
            return _evaluation_payload(plan=plan, controls=(), blockers=(), warnings=(), status="disabled")
        evidence = plan.get("evidence")
        evidence_map = evidence if isinstance(evidence, Mapping) else {}
        controls = tuple(
            _evaluate_control(control, evidence_map, plan, observed_at)
            for control in support.mappings(plan.get("controls"))
        )
        blockers = tuple(item for control in controls for item in control.get("blockers", []))
        warnings = tuple(item for control in controls for item in control.get("warnings", []))
        status = "blocked" if blockers else "warning" if warnings else "passed"
        return _evaluation_payload(plan=plan, controls=controls, blockers=blockers, warnings=warnings, status=status)


class ComplianceGate:
    """Profile-aware compliance go/no-go decision."""

    def evaluate(self, *, evaluation: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        blocking = _BLOCKING_BY_PROFILE.get(profile, _BLOCKING_BY_PROFILE["prod_strict"])
        for control in support.mappings(evaluation.get("controls")):
            severity = str(control.get("severity") or "medium")
            failed = control.get("status") == "failed"
            if failed and severity in blocking:
                blockers.append(f"data_product_compliance.control_failed:{control.get('control_id')}")
                blockers.extend(str(item) for item in control.get("blockers", []) if str(item))
            elif failed:
                warnings.append(f"data_product_compliance.control_warning:{control.get('control_id')}")
                warnings.extend(str(item) for item in control.get("blockers", []) if str(item))
            warnings.extend(str(item) for item in control.get("warnings", []) if str(item))
        if profile == "regulated":
            blockers.extend(_regulated_blockers(evaluation))
        if profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        status = "blocked" if blockers else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": COMPLIANCE_GATE_SCHEMA,
            "status": status,
            "profile": profile,
            "product": dict(evaluation.get("product", {})) if isinstance(evaluation.get("product"), Mapping) else {},
            "product_id": support.product_id(evaluation),
            "compliance_evaluation_id": evaluation.get("compliance_evaluation_id"),
            "compliance_plan_id": evaluation.get("compliance_plan_id"),
            "pack_id": evaluation.get("pack_id"),
            "bundle_id": evaluation.get("bundle_id"),
            "controls": [dict(control) for control in support.mappings(evaluation.get("controls"))],
            "evidence_refs": list(evaluation.get("evidence_refs", [])),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": support.recommendations(status),
        }
        payload["compliance_gate_id"] = stable_fingerprint(payload)
        return payload


class AuditPackageRenderer:
    """Compatibility wrapper for the dedicated rendering module."""

    def render(self, *, gate: Mapping[str, Any]) -> dict[str, Any]:
        renderer = import_module("dpone.readiness.data_product_compliance_rendering")
        return renderer.AuditPackageRenderer().render(gate=gate)


def _plan_payload(
    *,
    status: str,
    product: Mapping[str, Any],
    options: DataProductComplianceOptions,
    controls: Sequence[Mapping[str, Any]],
    evidence_index: Mapping[str, Any],
    blockers: Sequence[str],
    warnings: Sequence[str],
    pack_id: str | None,
    bundle_id: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": COMPLIANCE_PLAN_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "product": support.product_ref(product),
        "pack_id": pack_id,
        "bundle_id": bundle_id,
        "stale_evidence_policy": options.stale_evidence_policy,
        "stale_after_seconds": options.stale_after_seconds,
        "controls": [dict(control) for control in controls],
        "evidence_refs": list(evidence_index.get("refs", [])),
        "evidence": dict(evidence_index.get("payloads", {})),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }
    payload["compliance_plan_id"] = stable_fingerprint(payload)
    return payload


def _evaluation_payload(
    *,
    plan: Mapping[str, Any],
    controls: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    warnings: Sequence[str],
    status: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": COMPLIANCE_EVALUATION_SCHEMA,
        "status": status,
        "mode": plan.get("mode"),
        "profile": plan.get("profile"),
        "product": dict(plan.get("product", {})) if isinstance(plan.get("product"), Mapping) else {},
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "compliance_plan_id": plan.get("compliance_plan_id"),
        "controls": [dict(control) for control in controls],
        "evidence_refs": list(plan.get("evidence_refs", [])),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }
    payload["compliance_evaluation_id"] = stable_fingerprint(payload)
    return payload


def _evaluate_control(
    control: Mapping[str, Any],
    evidence: Mapping[str, Any],
    plan: Mapping[str, Any],
    observed_at: str | None,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    refs: list[dict[str, Any]] = []
    control_id = str(control.get("control_id"))
    allowed_by_kind = control.get("allowed_statuses") if isinstance(control.get("allowed_statuses"), Mapping) else {}
    for kind in support.strings(control.get("required_artifacts")):
        payload = evidence.get(kind)
        if not isinstance(payload, Mapping):
            blockers.append(f"data_product_compliance.required_artifact_missing:{control_id}:{kind}")
            continue
        refs.append(_evidence_ref(kind, payload))
        allowed = set(support.strings(allowed_by_kind.get(kind))) or support.PASSING_STATUSES
        if str(payload.get("status")) not in allowed:
            blockers.append(f"data_product_compliance.artifact_status_blocked:{control_id}:{kind}")
        blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
        warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))
        stale = _stale_blocker(kind=kind, payload=payload, control_id=control_id, plan=plan, observed_at=observed_at)
        if stale and plan.get("stale_evidence_policy") == "block":
            blockers.append(stale)
        elif stale:
            warnings.append(stale)
    status = "failed" if blockers else "warning" if warnings else "passed"
    return {
        **dict(control),
        "status": status,
        "evidence_refs": refs,
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }


def _regulated_blockers(evaluation: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    product = evaluation.get("product") if isinstance(evaluation.get("product"), Mapping) else {}
    if not product.get("owner"):
        blockers.append("data_product_compliance.product_owner_missing")
    for control in support.mappings(evaluation.get("controls")):
        if not control.get("framework_owner"):
            blockers.append(f"data_product_compliance.framework_owner_missing:{control.get('control_id')}")
    return blockers


def _stale_blocker(
    *,
    kind: str,
    payload: Mapping[str, Any],
    control_id: str,
    plan: Mapping[str, Any],
    observed_at: str | None,
) -> str | None:
    threshold = support.optional_int(plan.get("stale_after_seconds"))
    if threshold is None or threshold < 0 or not observed_at:
        return None
    recorded = support.parse_time(payload.get("recorded_at") or payload.get("created_at") or payload.get("updated_at"))
    observed = support.parse_time(observed_at)
    if not recorded or not observed:
        return None
    return (
        f"data_product_compliance.evidence_stale:{control_id}:{kind}"
        if (observed - recorded).total_seconds() > threshold
        else None
    )


def _evidence_ref(kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in {
            "kind": kind,
            "schema_version": payload.get("schema_version"),
            "status": payload.get("status"),
            "evidence_id": support.evidence_id(payload),
            "product_id": payload.get("product_id"),
            "artifact_sha256": payload.get("artifact_sha256"),
        }.items()
        if value is not None
    }


__all__ = [
    "COMPLIANCE_EVALUATION_SCHEMA",
    "COMPLIANCE_GATE_SCHEMA",
    "COMPLIANCE_PLAN_SCHEMA",
    "AuditPackageRenderer",
    "ComplianceControlCatalog",
    "ComplianceControlEvaluator",
    "ComplianceControlPlanner",
    "ComplianceEvidenceIndex",
    "ComplianceGate",
    "DataProductComplianceOptions",
]
