"""Provider-neutral data product SLO planning, evaluation and gates."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness.data_product_slo_support import (
    mapping as _mapping,
)
from dpone.readiness.data_product_slo_support import (
    number as _number,
)
from dpone.readiness.data_product_slo_support import (
    optional as _optional,
)
from dpone.readiness.data_product_slo_support import (
    optional_string as _optional_string,
)
from dpone.readiness.data_product_slo_support import (
    recommendations as _recommendations,
)
from dpone.readiness.data_product_slo_support import (
    target as _target,
)
from dpone.readiness.data_product_slo_support import (
    target_table as _target_table,
)
from dpone.readiness.migration_control import stable_fingerprint

SLO_PLAN_SCHEMA = "dpone.data_product_slo_plan.v1"
SLO_EVALUATION_SCHEMA = "dpone.data_product_slo_evaluation.v1"
SLO_GATE_SCHEMA = "dpone.data_product_slo_gate.v1"


@dataclass(frozen=True, slots=True)
class DataProductSloOptions:
    enabled: bool
    product_id: str
    owner: str | None = None
    tier: str = "silver"
    criticality: str = "medium"
    mode: str = "gate"
    profile: str = "prod_strict"
    objectives: dict[str, Any] | None = None
    consumer_policy: dict[str, Any] | None = None
    incident: dict[str, Any] | None = None
    target: dict[str, Any] | None = None

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> DataProductSloOptions:
        sink = _mapping(manifest.get("sink"))
        options = _mapping(sink.get("options"))
        contract = _mapping(options.get("schema_contract"))
        product = _mapping(options.get("data_product"))
        slo = _mapping(product.get("slo"))
        product_id = str(product.get("id") or contract.get("id") or _target_table(sink) or "")
        return cls(
            enabled=bool(slo.get("enabled", False)),
            product_id=product_id,
            owner=_optional_string(product.get("owner")),
            tier=str(product.get("tier") or "silver"),
            criticality=str(product.get("criticality") or "medium"),
            mode=str(slo.get("mode") or "gate"),
            profile=str(slo.get("profile") or "prod_strict"),
            objectives=dict(_mapping(slo.get("objectives"))),
            consumer_policy=dict(_mapping(slo.get("consumers"))),
            incident=dict(_mapping(slo.get("incident"))),
            target=_target(sink),
        )

    def product_dict(self) -> dict[str, Any]:
        return {
            "id": self.product_id,
            "owner": self.owner,
            "tier": self.tier,
            "criticality": self.criticality,
        }


class DataProductSloPlanner:
    """Builds deterministic SLO plans from manifests and evidence receipts."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        contract_gate: Mapping[str, Any] | None = None,
        consumer_gate: Mapping[str, Any] | None = None,
        consumer_certification: Mapping[str, Any] | None = None,
        watch_certificate: Mapping[str, Any] | None = None,
        post_apply_certificate: Mapping[str, Any] | None = None,
        assertion_gate: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = DataProductSloOptions.from_manifest(manifest)
        blockers = _plan_blockers(options, contract_gate, consumer_gate)
        objectives = dict(options.objectives or {}) if options.enabled else {}
        if assertion_gate and isinstance(objectives.get("quality"), Mapping):
            objectives["quality"] = {
                **_mapping(objectives["quality"]),
                "assertion_gate": _assertion_gate_summary(assertion_gate),
            }
        status = "disabled" if not options.enabled else "blocked" if blockers else "ready"
        payload: dict[str, Any] = {
            "schema_version": SLO_PLAN_SCHEMA,
            "status": status,
            "mode": options.mode,
            "profile": options.profile,
            "product": options.product_dict(),
            "target": options.target or {},
            "objectives": objectives,
            "consumer_policy": options.consumer_policy or {},
            "incident": options.incident or {},
            "contract_gate_id": _optional(contract_gate, "contract_gate_id"),
            "contract_id": _optional(contract_gate, "contract_id"),
            "contract_version_id": _optional(contract_gate, "contract_version_id"),
            "consumer_gate_id": _optional(consumer_gate, "consumer_gate_id"),
            "consumer_gate": _consumer_summary(consumer_gate),
            "consumer_certification_id": _optional(consumer_certification, "consumer_certification_id"),
            "assertion_gate_id": _optional(assertion_gate, "assertion_gate_id"),
            "assertion_gate": _assertion_gate_summary(assertion_gate),
            "watch_certificate_id": _optional(watch_certificate, "certificate_id"),
            "post_apply_certificate_id": _optional(post_apply_certificate, "certificate_id"),
            "blockers": blockers,
            "warnings": [],
            "recommendations": _recommendations(blockers),
        }
        payload["slo_plan_id"] = stable_fingerprint(payload)
        return payload


class DataProductSloEvaluator:
    """Evaluates offline/runtime SLO evidence against one SLO plan."""

    def evaluate(
        self,
        *,
        plan: Mapping[str, Any],
        registry_records: Sequence[Mapping[str, Any]] = (),
        runtime_artifacts: Sequence[Mapping[str, Any]] = (),
        target_evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if plan.get("status") == "disabled":
            return _disabled_evaluation(plan)
        runtime = _latest_runtime(runtime_artifacts)
        objectives = _mapping(plan.get("objectives"))
        checks: list[dict[str, Any]] = []
        blockers: list[str] = [str(item) for item in plan.get("blockers", []) if str(item)]
        warnings: list[str] = [str(item) for item in plan.get("warnings", []) if str(item)]
        _evaluate_freshness(objectives, runtime, blockers, warnings, checks)
        _evaluate_volume(objectives, runtime, target_evidence, blockers, checks)
        _evaluate_latency(objectives, runtime, blockers, checks)
        _evaluate_availability(objectives, runtime, blockers, checks)
        _evaluate_quality(objectives, runtime, target_evidence, blockers, warnings, checks)
        _evaluate_consumers(plan, blockers, warnings, checks)
        _evaluate_registry_closeout(registry_records, checks)
        blockers = list(dict.fromkeys(blockers))
        warnings = list(dict.fromkeys(warnings))
        status = "blocked" if blockers else "warning" if warnings else "passed"
        payload: dict[str, Any] = {
            "schema_version": SLO_EVALUATION_SCHEMA,
            "status": status,
            "slo_plan_id": plan.get("slo_plan_id"),
            "product_id": _product_id(plan),
            "contract_id": plan.get("contract_id"),
            "contract_version_id": plan.get("contract_version_id"),
            "consumer_gate_id": plan.get("consumer_gate_id"),
            "checks": checks,
            "metrics": _metrics(runtime, target_evidence),
            "blockers": blockers,
            "warnings": warnings,
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["slo_evaluation_id"] = stable_fingerprint(payload)
        return payload


class DataProductSloGate:
    """Profile-aware go/no-go gate for SLO evaluations."""

    def evaluate(self, *, evaluation: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        blockers = [str(item) for item in evaluation.get("blockers", []) if str(item)]
        warnings = [str(item) for item in evaluation.get("warnings", []) if str(item)]
        if profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        status = "blocked" if blockers else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": SLO_GATE_SCHEMA,
            "status": status,
            "profile": profile,
            "product_id": evaluation.get("product_id"),
            "slo_plan_id": evaluation.get("slo_plan_id"),
            "slo_evaluation_id": evaluation.get("slo_evaluation_id"),
            "pack_id": evaluation.get("pack_id"),
            "bundle_id": evaluation.get("bundle_id"),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["slo_gate_id"] = stable_fingerprint(payload)
        return payload


def _plan_blockers(
    options: DataProductSloOptions,
    contract_gate: Mapping[str, Any] | None,
    consumer_gate: Mapping[str, Any] | None,
) -> list[str]:
    if not options.enabled:
        return []
    blockers: list[str] = []
    for payload, key, label in (
        (contract_gate, "contract_id", "contract_gate"),
        (consumer_gate, "contract_id", "consumer_gate"),
    ):
        if payload and payload.get(key) not in {None, options.product_id}:
            blockers.append(f"data_product_slo.{label}_contract_mismatch")
    return blockers if options.mode == "gate" else []


def _evaluate_freshness(
    objectives: Mapping[str, Any],
    runtime: Mapping[str, Any],
    blockers: list[str],
    warnings: list[str],
    checks: list[dict[str, Any]],
) -> None:
    raw = _mapping(objectives.get("freshness"))
    if not raw:
        return
    actual = _number(runtime.get("freshness_lag_seconds"))
    limit = _number(raw.get("max_lag_seconds"))
    failed = actual is None or (limit is not None and actual > limit)
    details = ("data_product_slo.freshness_breach",) if failed else ()
    blockers.extend(details)
    checks.append(_check("freshness", "failed" if failed else "passed", details, {"lag_seconds": actual}))
    if actual is None:
        warnings.append("data_product_slo.freshness_missing")


def _evaluate_volume(
    objectives: Mapping[str, Any],
    runtime: Mapping[str, Any],
    target: Mapping[str, Any] | None,
    blockers: list[str],
    checks: list[dict[str, Any]],
) -> None:
    raw = _mapping(objectives.get("volume"))
    if not raw:
        return
    rows = _number(runtime.get("rows_loaded"), runtime.get("row_count"), _optional(target, "row_count"))
    min_rows = _number(raw.get("min_rows"))
    failed = rows is None or (min_rows is not None and rows < min_rows)
    details = ("data_product_slo.volume_breach",) if failed else ()
    blockers.extend(details)
    checks.append(_check("volume", "failed" if failed else "passed", details, {"rows": rows}))


def _evaluate_latency(
    objectives: Mapping[str, Any], runtime: Mapping[str, Any], blockers: list[str], checks: list[dict[str, Any]]
) -> None:
    raw = _mapping(objectives.get("latency"))
    if not raw:
        return
    actual = _number(runtime.get("duration_ms"), runtime.get("run_duration_ms"))
    limit = _number(raw.get("max_run_duration_ms"))
    failed = actual is None or (limit is not None and actual > limit)
    details = ("data_product_slo.latency_breach",) if failed else ()
    blockers.extend(details)
    checks.append(_check("latency", "failed" if failed else "passed", details, {"duration_ms": actual}))


def _evaluate_availability(
    objectives: Mapping[str, Any], runtime: Mapping[str, Any], blockers: list[str], checks: list[dict[str, Any]]
) -> None:
    raw = _mapping(objectives.get("availability"))
    if not raw:
        return
    failed_runs = _number(runtime.get("failed_runs"))
    limit = _number(raw.get("max_failed_runs"))
    failed = failed_runs is None or (limit is not None and failed_runs > limit) or runtime.get("status") == "failed"
    details = ("data_product_slo.availability_breach",) if failed else ()
    blockers.extend(details)
    checks.append(_check("availability", "failed" if failed else "passed", details, {"failed_runs": failed_runs}))


def _evaluate_quality(
    objectives: Mapping[str, Any],
    runtime: Mapping[str, Any],
    target: Mapping[str, Any] | None,
    blockers: list[str],
    warnings: list[str],
    checks: list[dict[str, Any]],
) -> None:
    raw = _mapping(objectives.get("quality"))
    if not raw:
        return
    quality = _mapping(runtime.get("quality")) | _mapping(_optional(target, "quality"))
    details: list[str] = []
    for metric, code, limit_key in (
        ("duplicate_keys", "data_product_slo.duplicate_key_breach", "max_duplicate_keys"),
        ("null_keys", "data_product_slo.null_key_breach", "max_null_keys"),
    ):
        actual = _number(quality.get(metric))
        limit = _number(raw.get(limit_key))
        if actual is None or (limit is not None and actual > limit):
            details.append(code)
    if raw.get("typed_hash") == "strict" and quality.get("typed_hash_status") not in {None, "passed"}:
        details.append("data_product_slo.typed_hash_breach")
    if raw.get("typed_hash") == "warning" and quality.get("typed_hash_status") not in {None, "passed"}:
        warnings.append("data_product_slo.typed_hash_warning")
    if _mapping(raw.get("assertion_gate")).get("status") == "blocked":
        details.append("data_product_slo.assertion_gate_blocked")
    blockers.extend(details)
    checks.append(_check("quality", "failed" if details else "passed", tuple(details), quality))


def _evaluate_consumers(
    plan: Mapping[str, Any], blockers: list[str], warnings: list[str], checks: list[dict[str, Any]]
) -> None:
    policy = _mapping(plan.get("consumer_policy"))
    gate = _mapping(plan.get("consumer_gate"))
    details: list[str] = []
    if policy.get("require_critical_consumer_green") and int(gate.get("critical_consumers_failed") or 0) > 0:
        details.append("data_product_slo.consumer_critical_failed")
    unknown = int(gate.get("unknown_consumers") or 0)
    if unknown:
        code = "data_product_slo.unknown_consumers_present"
        if str(policy.get("unknown_consumer") or "warn") == "block":
            details.append(code)
        elif str(policy.get("unknown_consumer") or "warn") == "warn":
            warnings.append(code)
    blockers.extend(details)
    checks.append(_check("consumers", "failed" if details else "passed", tuple(details), gate))


def _evaluate_registry_closeout(records: Sequence[Mapping[str, Any]], checks: list[dict[str, Any]]) -> None:
    stages = sorted({str(item.get("stage")) for item in records if item.get("stage")})
    checks.append(_check("registry_closeout", "passed", (), {"stages": stages}))


def _disabled_evaluation(plan: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SLO_EVALUATION_SCHEMA,
        "status": "disabled",
        "slo_plan_id": plan.get("slo_plan_id"),
        "product_id": _product_id(plan),
        "checks": [],
        "metrics": {},
        "blockers": [],
        "warnings": [],
        "recommendations": ["Enable data_product.slo to evaluate product SLOs."],
    }
    payload["slo_evaluation_id"] = stable_fingerprint(payload)
    return payload


def _consumer_summary(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    summary = _mapping(_optional(payload, "summary"))
    return {
        "status": _optional(payload, "status"),
        "critical_consumers_failed": int(summary.get("critical_consumers_failed") or 0),
        "unknown_consumers": int(summary.get("unknown_consumers") or 0),
        "blockers": [str(item) for item in _optional(payload, "blockers") or [] if str(item)],
        "warnings": [str(item) for item in _optional(payload, "warnings") or [] if str(item)],
    }


def _assertion_gate_summary(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        "status": _optional(payload, "status"),
        "blockers": [str(item) for item in _optional(payload, "blockers") or [] if str(item)],
        "warnings": [str(item) for item in _optional(payload, "warnings") or [] if str(item)],
    }


def _check(name: str, status: str, details: Sequence[str], metrics: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"name": name, "status": status, "details": list(details), "metrics": dict(metrics or {})}


def _latest_runtime(items: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    return items[-1] if items else {}


def _metrics(runtime: Mapping[str, Any], target: Mapping[str, Any] | None) -> dict[str, Any]:
    return {key: value for source in (runtime, target or {}) for key, value in source.items() if key != "quality"}


def _product_id(plan: Mapping[str, Any]) -> str | None:
    product = plan.get("product")
    return str(product.get("id")) if isinstance(product, Mapping) and product.get("id") else None
