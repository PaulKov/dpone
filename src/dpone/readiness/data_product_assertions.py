"""Provider-neutral data product assertion planning, evaluation and gates."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness.data_product_assertion_checks import (
    aggregate_assertion_results,
    assertion_summary,
    evaluate_plan_assertions,
)
from dpone.readiness.data_product_assertion_imports import imported_results, present_sources
from dpone.readiness.data_product_assertion_rendering import DataProductAssertionReportRenderer
from dpone.readiness.data_product_assertion_sql import safe_select_sql
from dpone.readiness.migration_control import stable_fingerprint

ASSERTION_PLAN_SCHEMA = "dpone.data_product_assertion_plan.v1"
ASSERTION_EVALUATION_SCHEMA = "dpone.data_product_assertion_evaluation.v1"
ASSERTION_GATE_SCHEMA = "dpone.data_product_assertion_gate.v1"
_SUPPORTED_TYPES = {
    "freshness",
    "row_count",
    "null_key",
    "duplicate_key",
    "accepted_values",
    "range",
    "regex",
    "typed_hash",
    "schema",
    "sql",
    "imported",
}


@dataclass(frozen=True, slots=True)
class DataProductAssertionOptions:
    enabled: bool
    product_id: str
    owner: str | None
    tier: str
    criticality: str
    mode: str
    profile: str
    unknown_assertion_source: str
    stale_after_seconds: int
    target: dict[str, Any]
    suites: tuple[dict[str, Any], ...]
    imports: dict[str, str]

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> DataProductAssertionOptions:
        sink = _mapping(manifest.get("sink"))
        options = _mapping(sink.get("options"))
        contract = _mapping(options.get("schema_contract"))
        product = _mapping(options.get("data_product"))
        assertions = _mapping(product.get("assertions"))
        return cls(
            enabled=bool(assertions.get("enabled", False)),
            product_id=str(product.get("id") or contract.get("id") or _target_table(sink) or ""),
            owner=_optional_string(product.get("owner")),
            tier=str(product.get("tier") or "silver"),
            criticality=str(product.get("criticality") or "medium"),
            mode=str(assertions.get("mode") or "gate"),
            profile=str(assertions.get("profile") or "prod_strict"),
            unknown_assertion_source=str(assertions.get("unknown_assertion_source") or "warn"),
            stale_after_seconds=int(assertions.get("stale_after_seconds") or 86400),
            target={"sink_type": sink.get("type"), "table": _target_table(sink)},
            suites=_normal_suites(assertions.get("suites")),
            imports={str(key): str(value) for key, value in _mapping(assertions.get("imports")).items()},
        )

    def product_dict(self) -> dict[str, Any]:
        return {"id": self.product_id, "owner": self.owner, "tier": self.tier, "criticality": self.criticality}


class DataProductAssertionPlanner:
    """Build deterministic assertion plans from manifests."""

    def plan(self, *, manifest: Mapping[str, Any]) -> dict[str, Any]:
        options = DataProductAssertionOptions.from_manifest(manifest)
        blockers = _plan_blockers(options)
        status = "disabled" if not options.enabled else "blocked" if blockers else "ready"
        payload: dict[str, Any] = {
            "schema_version": ASSERTION_PLAN_SCHEMA,
            "status": status,
            "mode": options.mode,
            "profile": options.profile,
            "product": options.product_dict(),
            "target": options.target,
            "suites": list(options.suites) if options.enabled else [],
            "imports": dict(options.imports) if options.enabled else {},
            "unknown_assertion_source": options.unknown_assertion_source,
            "stale_after_seconds": options.stale_after_seconds,
            "blockers": blockers,
            "warnings": [],
            "recommendations": _recommendations(blockers),
        }
        payload["assertion_plan_id"] = stable_fingerprint(payload)
        return payload


class DataProductAssertionEvaluator:
    """Evaluate planned assertions against offline evidence and read-only target facts."""

    def evaluate(
        self,
        *,
        plan: Mapping[str, Any],
        runtime_artifacts: Sequence[Mapping[str, Any]] = (),
        target_evidence: Mapping[str, Any] | None = None,
        imported_evidence: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        if plan.get("status") == "disabled":
            return _disabled_evaluation(plan)
        runtime = runtime_artifacts[-1] if runtime_artifacts else {}
        assertions = evaluate_plan_assertions(plan, runtime, target_evidence or {})
        assertions.extend(imported_results(imported_evidence))
        blockers = [str(item) for item in plan.get("blockers", []) if str(item)]
        warnings = [str(item) for item in plan.get("warnings", []) if str(item)]
        _apply_import_policy(plan, imported_evidence, blockers, warnings)
        aggregate_assertion_results(assertions, blockers, warnings)
        summary = assertion_summary(assertions)
        status = "blocked" if blockers else "warning" if warnings or summary["skipped"] else "passed"
        payload: dict[str, Any] = {
            "schema_version": ASSERTION_EVALUATION_SCHEMA,
            "status": status,
            "assertion_plan_id": plan.get("assertion_plan_id"),
            "product_id": _product_id(plan),
            "target": dict(plan.get("target", {})) if isinstance(plan.get("target"), Mapping) else {},
            "assertions": assertions,
            "summary": summary,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["assertion_evaluation_id"] = stable_fingerprint(payload)
        return payload


class DataProductAssertionGate:
    """Profile-aware assertion gate and report facade."""

    def evaluate(self, *, evaluation: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        blockers = [str(item) for item in evaluation.get("blockers", []) if str(item)]
        warnings = [str(item) for item in evaluation.get("warnings", []) if str(item)]
        if profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        elif profile == "stage":
            blockers = [item for item in blockers if ":critical:" in item or ".import" in item]
        elif profile == "regulated" and not evaluation.get("product_id"):
            blockers.append("data_product_assertions.product_id_required")
        status = "blocked" if blockers else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": ASSERTION_GATE_SCHEMA,
            "status": status,
            "profile": profile,
            "product_id": evaluation.get("product_id"),
            "assertion_plan_id": evaluation.get("assertion_plan_id"),
            "assertion_evaluation_id": evaluation.get("assertion_evaluation_id"),
            "pack_id": evaluation.get("pack_id"),
            "bundle_id": evaluation.get("bundle_id"),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["assertion_gate_id"] = stable_fingerprint(payload)
        return payload

    def report(self, *, evaluation: Mapping[str, Any]) -> dict[str, Any]:
        return DataProductAssertionReportRenderer().report(evaluation=evaluation)


def _normal_suites(raw: object) -> tuple[dict[str, Any], ...]:
    suites: list[dict[str, Any]] = []
    for suite in raw if isinstance(raw, list) else ():
        if not isinstance(suite, Mapping):
            continue
        severity = str(suite.get("severity") or "high")
        assertions = []
        for item in suite.get("assertions", []) if isinstance(suite.get("assertions"), list) else ():
            if isinstance(item, Mapping):
                assertions.append(
                    {
                        **dict(item),
                        "id": str(item.get("id") or ""),
                        "type": str(item.get("type") or ""),
                        "severity": str(item.get("severity") or severity),
                    }
                )
        suites.append(
            {
                "id": str(suite.get("id") or ""),
                "owner": suite.get("owner"),
                "severity": severity,
                "assertions": assertions,
            }
        )
    return tuple(suites)


def _plan_blockers(options: DataProductAssertionOptions) -> list[str]:
    if not options.enabled:
        return []
    blockers: list[str] = []
    seen: set[str] = set()
    for suite in options.suites:
        suite_id = str(suite.get("id") or "unknown")
        if options.profile == "regulated" and not suite.get("owner"):
            blockers.append(f"data_product_assertions.owner_required:{suite_id}")
        for assertion in suite.get("assertions", []):
            assertion_id = str(assertion.get("id") or "")
            if assertion_id in seen:
                blockers.append(f"data_product_assertions.duplicate_assertion_id:{assertion_id}")
            seen.add(assertion_id)
            if assertion.get("type") not in _SUPPORTED_TYPES:
                blockers.append(f"data_product_assertions.unsupported_type:{assertion_id}")
            if assertion.get("type") == "sql" and not safe_select_sql(str(assertion.get("query") or "")):
                blockers.append(f"data_product_assertions.unsafe_sql:{assertion_id}")
    return blockers if options.mode == "gate" else []


def _apply_import_policy(
    plan: Mapping[str, Any], imported: Sequence[Mapping[str, Any]], blockers: list[str], warnings: list[str]
) -> None:
    imports = _mapping(plan.get("imports"))
    missing = sorted(set(imports) - present_sources(imported))
    policy = str(plan.get("unknown_assertion_source") or "warn")
    for source in missing:
        code = f"data_product_assertions.import_missing:{source}"
        if policy == "block":
            blockers.append(code)
        elif policy == "warn":
            warnings.append(code)


def _disabled_evaluation(plan: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": ASSERTION_EVALUATION_SCHEMA,
        "status": "disabled",
        "assertion_plan_id": plan.get("assertion_plan_id"),
        "product_id": _product_id(plan),
        "assertions": [],
        "summary": {"passed": 0, "failed": 0, "warning": 0, "skipped": 0},
        "blockers": [],
        "warnings": [],
        "recommendations": ["Enable data_product.assertions to evaluate assertion suites."],
    }
    payload["assertion_evaluation_id"] = stable_fingerprint(payload)
    return payload


def _target_table(sink: Mapping[str, Any]) -> str | None:
    table = sink.get("table")
    if isinstance(table, Mapping):
        schema = table.get("schema")
        name = table.get("name")
        return f"{schema}.{name}" if schema and name else str(name or schema or "")
    return str(table) if table else None


def _product_id(plan: Mapping[str, Any]) -> str | None:
    product = plan.get("product")
    return str(product.get("id")) if isinstance(product, Mapping) and product.get("id") else None


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _optional_string(raw: object) -> str | None:
    return str(raw) if raw not in {None, ""} else None


def _recommendations(blockers: Sequence[object], warnings: Sequence[object] = ()) -> list[str]:
    if blockers:
        return ["Resolve assertion blockers before closing the data product release."]
    if warnings:
        return ["Review assertion warnings and attach them to the release evidence bundle."]
    return ["Use this assertion evidence as a data product quality gate."]


__all__ = (
    "ASSERTION_EVALUATION_SCHEMA",
    "ASSERTION_GATE_SCHEMA",
    "ASSERTION_PLAN_SCHEMA",
    "DataProductAssertionEvaluator",
    "DataProductAssertionGate",
    "DataProductAssertionOptions",
    "DataProductAssertionPlanner",
)
