"""Metric provenance catalog for benchmark evidence trust."""

from __future__ import annotations

from typing import Any

_MODE_ORDER = ("measured", "derived", "inferred", "stale", "closed-core note")

_BASE_PROVENANCE: tuple[dict[str, str], ...] = (
    {
        "metric_group": "loc_sloc",
        "mode": "measured",
        "collector": "collect_project_metrics",
        "source": "pinned source tree",
        "formula_version": "loc-sloc-v1",
    },
    {
        "metric_group": "top_modules",
        "mode": "measured",
        "collector": "top_files",
        "source": "pinned source tree",
        "formula_version": "top-modules-v1",
    },
    {
        "metric_group": "coupling_cohesion",
        "mode": "measured",
        "collector": "compute_coupling_metrics",
        "source": "static import graph",
        "formula_version": "dependency-proxy-v1",
    },
    {
        "metric_group": "solid_clean_oop",
        "mode": "derived",
        "collector": "score_quality",
        "source": "measured module size and dependency proxies",
        "formula_version": "solid-clean-oop-rubric-v1",
    },
    {
        "metric_group": "industrial_maintainability",
        "mode": "derived",
        "collector": "compute_industrial_maintainability_index",
        "source": "quality, size, coupling, cohesion, coverage and freshness",
        "formula_version": "industrial-maintainability-v1",
    },
    {
        "metric_group": "quality_gates",
        "mode": "derived",
        "collector": "evaluate_quality_gates",
        "source": "merged benchmark payload",
        "formula_version": "quality-gates-v1",
    },
    {
        "metric_group": "feature_parity",
        "mode": "inferred",
        "collector": "build_feature_parity_matrix",
        "source": "public product documentation and local dpone docs",
        "formula_version": "feature-parity-v1",
    },
    {
        "metric_group": "governance_compliance",
        "mode": "inferred",
        "collector": "build_governance_compliance_matrix",
        "source": "public docs and local evidence signals",
        "formula_version": "governance-compliance-v1",
    },
    {
        "metric_group": "security_supply_chain",
        "mode": "inferred",
        "collector": "build_security_supply_chain_matrix",
        "source": "repository files and public platform signals",
        "formula_version": "security-supply-chain-v1",
    },
    {
        "metric_group": "refactor_roi",
        "mode": "derived",
        "collector": "build_refactor_roi_roadmap",
        "source": "quality gate, complexity and architecture-risk evidence",
        "formula_version": "refactor-roi-v1",
    },
    {
        "metric_group": "semantic_maintainability",
        "mode": "derived",
        "collector": "analyze_semantic_maintainability",
        "source": "static source-shape scan, project payloads and dependency proxies",
        "formula_version": "semantic-maintainability-v1",
    },
    {
        "metric_group": "scoring_calibration",
        "mode": "derived",
        "collector": "build_scoring_calibration",
        "source": "industrial, semantic, coverage and repository-profile evidence",
        "formula_version": "scoring-calibration-v1",
    },
    {
        "metric_group": "scale_readiness",
        "mode": "derived",
        "collector": "build_scale_readiness",
        "source": "current dpone metrics, comparator repository scale and calibrated quality budgets",
        "formula_version": "scale-readiness-v1",
    },
    {
        "metric_group": "independent_validation",
        "mode": "derived",
        "collector": "build_independent_validation",
        "source": "external analyzer command metadata and injected analyzer evidence",
        "formula_version": "independent-validation-v1",
    },
    {
        "metric_group": "public_evidence_integrity",
        "mode": "derived",
        "collector": "build_public_evidence_integrity",
        "source": "public artifact redaction and claim evidence ledger",
        "formula_version": "public-evidence-integrity-v1",
    },
    {
        "metric_group": "source_verification",
        "mode": "derived",
        "collector": "build_source_verification",
        "source": "public source registry, URL/local source checks and claim-to-source matrix",
        "formula_version": "source-verification-v1",
    },
    {
        "metric_group": "benchmark_release_readiness",
        "mode": "derived",
        "collector": "build_benchmark_release_readiness",
        "source": "quality gates, evidence integrity, source verification, evidence trust and trust center status",
        "formula_version": "benchmark-release-readiness-v1",
    },
    {
        "metric_group": "runtime_certification",
        "mode": "derived",
        "collector": "build_runtime_certification",
        "source": "release context, quality gates, local docs and contract test artifacts",
        "formula_version": "runtime-certification-v1",
    },
    {
        "metric_group": "claims_ledger",
        "mode": "derived",
        "collector": "build_claims_ledger",
        "source": "JSON evidence refs, generated artifacts and public source references",
        "formula_version": "claims-ledger-v1",
    },
    {
        "metric_group": "quality_budgets",
        "mode": "derived",
        "collector": "evaluate_quality_budgets",
        "source": "docs/benchmarks/quality_budgets.yml and merged benchmark payload",
        "formula_version": "quality-budgets-v1",
    },
    {
        "metric_group": "evidence_exports",
        "mode": "derived",
        "collector": "write_evidence_exports",
        "source": "final merged benchmark payload",
        "formula_version": "evidence-warehouse-export-v1",
    },
)


def build_metric_provenance(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the compact metric provenance ledger used by docs and JSON."""

    entries = [dict(item) for item in _BASE_PROVENANCE]
    stale_count = _stale_group_count(payload.get("projects") or [])
    if stale_count:
        entries.append(
            {
                "metric_group": "stale_project_metrics",
                "mode": "stale",
                "collector": "merge_benchmark_payload",
                "source": "previous raw evidence retained after refresh failure",
                "formula_version": "freshness-merge-v1",
                "affected_groups": stale_count,
            }
        )
    closed_notes = payload.get("closed_core_notes") or []
    if closed_notes:
        entries.append(
            {
                "metric_group": "closed_core_notes",
                "mode": "closed-core note",
                "collector": "build_benchmark_payload",
                "source": "public closed-core comparator positioning",
                "formula_version": "closed-core-note-v1",
                "affected_tools": len(closed_notes),
            }
        )
    return entries


def mode_counts(provenance: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = {mode: 0 for mode in _MODE_ORDER}
    for item in provenance:
        mode = str(item.get("mode", "inferred"))
        counts[mode] = counts.get(mode, 0) + 1
    return [{"mode": mode, "count": counts.get(mode, 0)} for mode in _MODE_ORDER if counts.get(mode, 0)]


def _stale_group_count(projects: list[dict[str, Any]]) -> int:
    return sum(
        1
        for project in projects
        for group in (project.get("metric_groups") or {}).values()
        if group.get("status") == "stale"
    )
