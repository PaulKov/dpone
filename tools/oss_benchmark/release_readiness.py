"""Benchmark release policy and release-readiness seal."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import as_int

STABLE_METRIC_GROUPS = (
    "loc_sloc",
    "top_modules",
    "coupling_cohesion",
    "solid_clean_oop",
    "coverage_confidence",
    "industrial_maintainability",
    "quality_gates",
    "evidence_trust",
    "public_evidence_integrity",
    "source_verification",
    "trust_center",
    "runtime_certification",
    "claims_ledger",
    "quality_budgets",
    "evidence_exports",
)

EXPERIMENTAL_METRIC_GROUPS = (
    "feature_parity",
    "governance_compliance",
    "security_supply_chain",
    "operational_reliability",
    "operability_tco",
    "architecture_taxonomy",
    "complexity_boundary",
    "semantic_maintainability",
    "scoring_calibration",
    "scale_readiness",
    "refactor_roi",
    "independent_validation",
    "external_analyzer_results",
    "candidate_quality_delta",
    "trend_summary",
)


def build_benchmark_release_readiness(payload: dict[str, Any]) -> dict[str, Any]:
    """Build the release go/no-go view for the public benchmark."""

    checks = _release_checks(payload)
    failed = sum(1 for check in checks if check["status"] == "failed")
    warnings = sum(1 for check in checks if check["status"] == "warning")
    status = "blocked" if failed else "watch" if warnings else "release-ready"
    score = max(0, min(100, int(round(sum(_check_points(check) for check in checks) / max(1, len(checks))))))
    run_context = payload.get("run_context") or {}
    generated_at = str(payload.get("generated_at") or run_context.get("generated_at") or "")
    return {
        "schema_version": 1,
        "release_stage": "benchmark-v3",
        "status": status,
        "recommended_action": _recommended_action(status),
        "evidence_seal": {
            "label": _seal_label(status),
            "score": score,
            "generated_at": generated_at,
            "updated_by": run_context.get("updated_by", "local"),
            "source_revision": f"{run_context.get('branch', 'local')}@{run_context.get('git_sha', 'unknown')}",
        },
        "freeze_policy": {
            "stable_metric_groups": list(STABLE_METRIC_GROUPS),
            "experimental_metric_groups": list(EXPERIMENTAL_METRIC_GROUPS),
            "policy": (
                "Benchmark v3 keeps stable code-quality, release-certification and evidence-integrity metrics contract-compatible. "
                "Experimental posture, projection and roadmap metrics can evolve with explicit version notes."
            ),
        },
        "release_checks": checks,
    }


def _release_checks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    gates = payload.get("quality_gates") or {}
    public = payload.get("public_evidence_integrity") or {}
    source = payload.get("source_verification") or {}
    trust = payload.get("evidence_trust") or {}
    center = payload.get("trust_center") or {}
    claims = payload.get("claims_ledger") or {}
    certification = payload.get("runtime_certification") or {}
    budgets = payload.get("quality_budgets") or {}
    exports = payload.get("evidence_exports") or {}
    source_summary = source.get("summary") or {}
    claim_summary = claims.get("summary") or {}
    certification_summary = certification.get("summary") or {}
    return [
        _check("quality_gates", "Quality gates", gates.get("status") == "passed", str(gates.get("status", "n/a"))),
        _check(
            "public_evidence_integrity",
            "Public evidence integrity",
            public.get("status") == "verified" and as_int(public.get("redaction_violation_count")) == 0,
            f"{public.get('status', 'n/a')}, redactions {as_int(public.get('redaction_violation_count'))}",
        ),
        _check(
            "source_verification",
            "Source citation verification",
            source.get("status") == "verified" and as_int(source_summary.get("source_health_score")) >= 95,
            f"{source.get('status', 'n/a')}, source health {as_int(source_summary.get('source_health_score'))}/100",
        ),
        _check(
            "evidence_trust",
            "Evidence trust",
            as_int(trust.get("overall_confidence_score")) >= 90,
            f"{as_int(trust.get('overall_confidence_score'))}/100 {trust.get('overall_band', 'n/a')}",
            warning_threshold=75,
        ),
        _check(
            "trust_center",
            "Customer trust center",
            center.get("status") == "verified",
            str(center.get("status", "n/a")),
            warning_values={"watch"},
        ),
        _check(
            "generated_artifacts",
            "Generated artifact manifest",
            bool(payload.get("projects")) and bool(payload.get("run_context") or payload.get("generated_at")),
            "projects and run context present",
        ),
        _check(
            "claims_ledger",
            "Claims ledger",
            as_int(claim_summary.get("unverified")) == 0,
            f"verified {as_int(claim_summary.get('verified'))}, unverified {as_int(claim_summary.get('unverified'))}",
        ),
        _check(
            "runtime_certification",
            "Runtime certification",
            as_int(certification_summary.get("failed")) == 0 and bool(certification.get("scenarios")),
            f"passed {as_int(certification_summary.get('passed'))}, failed {as_int(certification_summary.get('failed'))}",
        ),
        _check(
            "quality_budgets",
            "Quality budgets",
            budgets.get("status") != "failed",
            str(budgets.get("status", "n/a")),
            warning_values={"warning"},
        ),
        _check(
            "evidence_exports",
            "Evidence warehouse exports",
            bool(exports.get("files")),
            f"{len(exports.get('files') or [])} CSV exports",
        ),
    ]


def _check(
    check_id: str,
    label: str,
    passed: bool,
    evidence: str,
    *,
    warning_threshold: int | None = None,
    warning_values: set[str] | None = None,
) -> dict[str, Any]:
    status = "passed" if passed else "failed"
    if not passed and warning_threshold is not None:
        try:
            status = "warning" if int(evidence.split("/", 1)[0]) >= warning_threshold else "failed"
        except ValueError:
            status = "failed"
    if not passed and warning_values and evidence in warning_values:
        status = "warning"
    return {"id": check_id, "label": label, "status": status, "evidence": evidence}


def _check_points(check: dict[str, Any]) -> int:
    if check["status"] == "passed":
        return 100
    if check["status"] == "warning":
        return 70
    return 0


def _seal_label(status: str) -> str:
    if status == "release-ready":
        return "Benchmark v3 verified"
    if status == "watch":
        return "Benchmark v3 watch"
    return "Benchmark v3 blocked"


def _recommended_action(status: str) -> str:
    if status == "release-ready":
        return "Release the benchmark as v3 and use this evidence pack as the PR/release review summary."
    if status == "watch":
        return "Release only after reviewer acknowledgement of warning checks."
    return "Do not release until failed readiness checks are fixed and the benchmark is regenerated."
