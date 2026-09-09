"""Benchmark evidence enrichment and artifact writing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tools.oss_benchmark.architecture_taxonomy import build_architecture_taxonomy_matrix
from tools.oss_benchmark.benchmark_intelligence import (
    build_regression_summary,
    build_remediation_backlog,
    build_score_explanations,
)
from tools.oss_benchmark.budgets import evaluate_quality_budgets
from tools.oss_benchmark.candidate_delta import build_candidate_quality_delta
from tools.oss_benchmark.certification import build_runtime_certification
from tools.oss_benchmark.certification_runtime import RUNTIME_CERTIFICATION_DIR, build_runtime_certification_v2
from tools.oss_benchmark.claims import build_claims_ledger
from tools.oss_benchmark.complexity_boundary import analyze_complexity_boundary_discipline
from tools.oss_benchmark.config import (
    ASSET_DIR,
    DATA_PATH,
    DOC_PATH,
    HISTORY_PATH,
    RELEASE_READINESS_DATA_PATH,
    RELEASE_READINESS_PATH,
    ROOT,
    TRUST_CENTER_BADGE_PATH,
    TRUST_CENTER_DATA_PATH,
    TRUST_CENTER_PATH,
)
from tools.oss_benchmark.contexts import ReleaseContext
from tools.oss_benchmark.evidence_trust import build_evidence_trust_summary, write_provenance_export
from tools.oss_benchmark.exports import EXPORT_DIR, EXPORT_FILES, build_evidence_export_manifest, write_evidence_exports
from tools.oss_benchmark.external_analyzers import collect_external_analyzer_results
from tools.oss_benchmark.feature_parity import build_feature_parity_matrix
from tools.oss_benchmark.governance_compliance import build_governance_compliance_matrix
from tools.oss_benchmark.independent_validation import build_independent_validation
from tools.oss_benchmark.maintainability import enrich_payload_with_maintainability
from tools.oss_benchmark.models import ProjectMetrics
from tools.oss_benchmark.operability_tco import build_operability_tco_matrix
from tools.oss_benchmark.operational_reliability import build_operational_reliability_matrix
from tools.oss_benchmark.payload_builder import build_benchmark_payload
from tools.oss_benchmark.pr_summary import PR_SUMMARY_PATH, write_pr_summary
from tools.oss_benchmark.public_evidence_integrity import build_public_evidence_integrity, sanitize_public_payload
from tools.oss_benchmark.quality_gates import evaluate_quality_gates
from tools.oss_benchmark.refactor_roi import build_refactor_roi_roadmap
from tools.oss_benchmark.regression_gate import build_pr_regression_gate
from tools.oss_benchmark.release_delta import build_release_delta
from tools.oss_benchmark.release_readiness import build_benchmark_release_readiness
from tools.oss_benchmark.renderers.architecture import render_architecture_taxonomy_svg
from tools.oss_benchmark.renderers.calibration_svg import (
    render_normalized_vs_raw_svg,
    render_score_calibration_svg,
    render_score_sensitivity_svg,
)
from tools.oss_benchmark.renderers.evidence_svg import render_evidence_confidence_svg
from tools.oss_benchmark.renderers.governance import render_governance_compliance_svg
from tools.oss_benchmark.renderers.markdown import render_markdown
from tools.oss_benchmark.renderers.operability import render_operability_tco_svg
from tools.oss_benchmark.renderers.public_integrity_svg import render_public_evidence_integrity_svg
from tools.oss_benchmark.renderers.release_readiness import render_release_readiness_markdown
from tools.oss_benchmark.renderers.release_readiness_svg import render_release_readiness_svg
from tools.oss_benchmark.renderers.reliability import render_operational_reliability_svg
from tools.oss_benchmark.renderers.roi_svg import render_refactor_roi_svg
from tools.oss_benchmark.renderers.scale_svg import (
    render_architecture_runway_svg,
    render_quality_headroom_svg,
    render_scale_readiness_svg,
)
from tools.oss_benchmark.renderers.security import render_security_supply_chain_svg
from tools.oss_benchmark.renderers.semantic_svg import render_god_object_radar_svg, render_semantic_maintainability_svg
from tools.oss_benchmark.renderers.source_verification_svg import render_source_verification_svg
from tools.oss_benchmark.renderers.svg import (
    render_architecture_delta_svg,
    render_architecture_risk_svg,
    render_complexity_boundary_svg,
    render_feature_parity_svg,
    render_hotspots_svg,
    render_loc_sloc_svg,
    render_quadrant_svg,
    render_quality_trend_svg,
    render_scorecard_svg,
)
from tools.oss_benchmark.renderers.validation_svg import (
    render_analyzer_confidence_svg,
    render_independent_validation_svg,
)
from tools.oss_benchmark.scale_readiness import build_scale_readiness
from tools.oss_benchmark.schema import normalize_benchmark_payload_v2
from tools.oss_benchmark.scoring_calibration import build_scoring_calibration
from tools.oss_benchmark.security_supply_chain import build_security_supply_chain_matrix
from tools.oss_benchmark.semantic_maintainability import analyze_semantic_maintainability
from tools.oss_benchmark.source_verification import build_source_verification
from tools.oss_benchmark.state import RunContext, merge_benchmark_payload
from tools.oss_benchmark.trend_history import build_architecture_delta
from tools.oss_benchmark.trend_history import build_history_entry as _build_history_entry
from tools.oss_benchmark.trend_history import update_history_payload as _update_history_payload
from tools.oss_benchmark.trust_center import build_trust_center_export, write_trust_center_outputs


def write_benchmark_outputs(
    metrics: tuple[ProjectMetrics, ...],
    *,
    generated_at: str,
    run_context: RunContext,
    release_context: ReleaseContext,
    previous_payload: dict[str, Any] | None,
    requested_slugs: set[str],
    failures: dict[str, str],
    allow_stale: bool,
    baseline_payload: dict[str, Any] | None = None,
    pr_summary_path: Path | None = ROOT / PR_SUMMARY_PATH,
    external_analyzer_timeout: int = 60,
    verify_source_urls: bool = False,
    max_stale_days: int = 30,
    run_certification: bool = False,
    certification_mode: str = "reuse",
    certification_scenario: str = "all",
    max_scenario_seconds: int = 120,
) -> dict[str, Any]:
    """Build merged evidence, render public docs/assets, and return the payload."""

    refreshed = build_benchmark_payload(
        metrics,
        generated_at=generated_at,
        run_context=run_context,
        release_context=release_context,
    )
    payload = merge_benchmark_payload(
        refreshed,
        previous_payload=previous_payload,
        requested_slugs=requested_slugs,
        attempted_at=generated_at,
        failures=failures,
        allow_stale=allow_stale,
    )
    payload = normalize_benchmark_payload_v2(payload)
    payload["freshness_governance"] = build_freshness_governance(payload, max_stale_days=max_stale_days)
    payload = _enrich(
        payload,
        previous_payload,
        baseline_payload,
        generated_at,
        external_analyzer_timeout,
        verify_source_urls,
        run_certification=run_certification,
        certification_mode=certification_mode,
        certification_scenario=certification_scenario,
        allow_stale=allow_stale,
        max_scenario_seconds=max_scenario_seconds,
    )
    history = update_history_payload(payload, previous_history=load_history_payload(ROOT / HISTORY_PATH))
    payload["trend_summary"] = history.get("latest_deltas", {})
    _write_json(ROOT / DATA_PATH, payload)
    _write_json(ROOT / HISTORY_PATH, history)
    write_evidence_exports(payload)
    _write_assets(payload, history)
    _write_text(ROOT / DOC_PATH, render_markdown(payload))
    write_release_readiness_outputs(payload)
    write_trust_center_outputs(payload)
    if pr_summary_path is not None:
        write_pr_summary(payload, pr_summary_path)
    write_provenance_export(payload, artifact_paths=_benchmark_artifact_paths(pr_summary_path))
    return payload


def build_freshness_governance(payload: dict[str, Any], *, max_stale_days: int) -> dict[str, Any]:
    """Summarize stale metric groups that exceed the governance threshold."""

    warnings: list[dict[str, Any]] = []
    for project in payload.get("projects", []):
        slug = str(project.get("project_id") or project.get("spec", {}).get("slug") or "")
        for group_name, group in (project.get("metric_groups") or {}).items():
            age = group.get("stale_age_days")
            if isinstance(age, int) and age > max_stale_days:
                warnings.append({"project_id": slug, "metric_group": group_name, "stale_age_days": age})
    return {"max_stale_days": max_stale_days, "warning_count": len(warnings), "warnings": warnings}


def _enrich(
    payload: dict[str, Any],
    previous_payload: dict[str, Any] | None,
    baseline_payload: dict[str, Any] | None,
    generated_at: str,
    external_analyzer_timeout: int,
    verify_source_urls: bool,
    *,
    run_certification: bool,
    certification_mode: str,
    certification_scenario: str,
    allow_stale: bool,
    max_scenario_seconds: int,
) -> dict[str, Any]:
    payload = enrich_payload_with_maintainability(payload)
    payload["feature_parity"] = build_feature_parity_matrix()
    payload["governance_compliance"] = build_governance_compliance_matrix(payload.get("projects", []))
    payload["operability_tco"] = build_operability_tco_matrix(payload.get("projects", []))
    payload["operational_reliability"] = build_operational_reliability_matrix(payload.get("projects", []))
    payload["security_supply_chain"] = build_security_supply_chain_matrix(payload.get("projects", []))
    payload["architecture_taxonomy"] = build_architecture_taxonomy_matrix(payload.get("projects", []))
    payload["complexity_boundary"] = analyze_complexity_boundary_discipline(payload.get("projects", []))
    payload["semantic_maintainability"] = analyze_semantic_maintainability(payload.get("projects", []))
    payload["quality_budgets"] = evaluate_quality_budgets(payload)
    payload["scoring_calibration"] = build_scoring_calibration(payload)
    payload["scale_readiness"] = build_scale_readiness(payload)
    payload["release_delta"] = build_release_delta(payload, baseline_payload=baseline_payload or previous_payload)
    certification_v2 = build_runtime_certification_v2(
        previous_payload=previous_payload,
        generated_at=generated_at,
        mode=certification_mode,
        scenario=certification_scenario,
        run_certification=run_certification,
        allow_stale=allow_stale,
        max_scenario_seconds=max_scenario_seconds,
    )
    if certification_v2:
        payload["runtime_certification_v2"] = certification_v2
    payload["quality_gates"] = evaluate_quality_gates(payload)
    payload["score_explanations"] = build_score_explanations(payload)
    payload["regression_summary"] = build_regression_summary(payload, previous_payload=previous_payload)
    payload["candidate_quality_delta"] = build_candidate_quality_delta(
        payload, baseline_payload=baseline_payload or previous_payload
    )
    payload["remediation_backlog"] = build_remediation_backlog(payload)
    payload["refactor_roi"] = build_refactor_roi_roadmap(payload)
    payload["trust_center"] = build_trust_center_export(payload)
    payload["architecture_delta"] = build_architecture_delta(payload, previous_payload=previous_payload)
    payload["pr_regression_gate"] = build_pr_regression_gate(payload)
    payload["external_analyzer_results"] = collect_external_analyzer_results(
        payload.get("projects", []),
        generated_at=generated_at,
        previous_payload=previous_payload,
        timeout_seconds=external_analyzer_timeout,
    )
    payload["independent_validation"] = build_independent_validation(payload)
    payload["evidence_trust"] = build_evidence_trust_summary(payload)
    payload = sanitize_public_payload(payload)
    payload["public_evidence_integrity"] = build_public_evidence_integrity(payload)
    payload["source_verification"] = build_source_verification(
        payload, previous_payload=previous_payload, verify_urls=verify_source_urls
    )
    payload["runtime_certification"] = build_runtime_certification(payload)
    payload["claims_ledger"] = build_claims_ledger(payload)
    payload["evidence_exports"] = build_evidence_export_manifest()
    payload["benchmark_release_readiness"] = build_benchmark_release_readiness(payload)
    return payload


def _write_assets(payload: dict[str, Any], history: dict[str, Any]) -> None:
    asset_dir = ROOT / ASSET_DIR
    asset_dir.mkdir(parents=True, exist_ok=True)
    renderers = {
        "oss-quality-scorecard.svg": render_scorecard_svg(payload),
        "oss-feature-parity.svg": render_feature_parity_svg(payload),
        "oss-governance-compliance.svg": render_governance_compliance_svg(payload),
        "oss-operability-tco.svg": render_operability_tco_svg(payload),
        "oss-operational-reliability.svg": render_operational_reliability_svg(payload),
        "oss-security-supply-chain.svg": render_security_supply_chain_svg(payload),
        "oss-evidence-confidence.svg": render_evidence_confidence_svg(payload),
        "oss-public-evidence-integrity.svg": render_public_evidence_integrity_svg(payload),
        "oss-source-verification.svg": render_source_verification_svg(payload),
        "oss-release-readiness-seal.svg": render_release_readiness_svg(payload),
        "oss-architecture-taxonomy.svg": render_architecture_taxonomy_svg(payload),
        "oss-loc-sloc.svg": render_loc_sloc_svg(payload),
        "oss-coupling-cohesion-quadrant.svg": render_quadrant_svg(payload),
        "oss-module-hotspots.svg": render_hotspots_svg(payload),
        "oss-architecture-risk-heatmap.svg": render_architecture_risk_svg(payload),
        "oss-architecture-delta.svg": render_architecture_delta_svg(payload),
        "oss-complexity-boundary.svg": render_complexity_boundary_svg(payload),
        "oss-semantic-maintainability.svg": render_semantic_maintainability_svg(payload),
        "oss-god-object-radar.svg": render_god_object_radar_svg(payload),
        "oss-score-calibration.svg": render_score_calibration_svg(payload),
        "oss-score-sensitivity.svg": render_score_sensitivity_svg(payload),
        "oss-normalized-vs-raw.svg": render_normalized_vs_raw_svg(payload),
        "oss-scale-readiness.svg": render_scale_readiness_svg(payload),
        "oss-architecture-runway.svg": render_architecture_runway_svg(payload),
        "oss-quality-headroom.svg": render_quality_headroom_svg(payload),
        "oss-refactor-roi-roadmap.svg": render_refactor_roi_svg(payload),
        "oss-independent-validation.svg": render_independent_validation_svg(payload),
        "oss-analyzer-confidence.svg": render_analyzer_confidence_svg(payload),
        "oss-quality-trend.svg": render_quality_trend_svg(history),
    }
    for name, text in renderers.items():
        _write_text(asset_dir / name, text)


def load_history_payload(path: Path) -> dict[str, Any] | None:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def write_history_payload(path: Path, history: dict[str, Any]) -> None:
    _write_json(path, history)


def write_release_readiness_outputs(payload: dict[str, Any]) -> None:
    readiness = payload.get("benchmark_release_readiness") or {}
    _write_json(ROOT / RELEASE_READINESS_DATA_PATH, readiness)
    _write_text(ROOT / RELEASE_READINESS_PATH, render_release_readiness_markdown(readiness))


def update_history_payload(
    payload: dict[str, Any],
    *,
    previous_history: dict[str, Any] | None,
    max_entries: int = 30,
) -> dict[str, Any]:
    return _update_history_payload(payload, previous_history=previous_history, max_entries=max_entries)


def build_history_entry(payload: dict[str, Any]) -> dict[str, Any]:
    return _build_history_entry(payload)


def _benchmark_artifact_paths(pr_summary_path: Path | None) -> tuple[Path, ...]:
    paths = [ROOT / path for path in (DATA_PATH, DOC_PATH, HISTORY_PATH, TRUST_CENTER_PATH, TRUST_CENTER_DATA_PATH)]
    paths.extend((ROOT / TRUST_CENTER_BADGE_PATH, ROOT / RELEASE_READINESS_PATH, ROOT / RELEASE_READINESS_DATA_PATH))
    paths.extend((ROOT / ASSET_DIR / name) for name in _ASSET_NAMES)
    paths.extend((EXPORT_DIR / name) for name in EXPORT_FILES)
    paths.extend((RUNTIME_CERTIFICATION_DIR / "run-ledger.json", RUNTIME_CERTIFICATION_DIR / "contract-checks.json"))
    if pr_summary_path is not None:
        paths.append(pr_summary_path if pr_summary_path.is_absolute() else ROOT / pr_summary_path)
    return tuple(paths)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


_ASSET_NAMES = (
    "oss-quality-scorecard.svg",
    "oss-feature-parity.svg",
    "oss-governance-compliance.svg",
    "oss-operability-tco.svg",
    "oss-operational-reliability.svg",
    "oss-security-supply-chain.svg",
    "oss-evidence-confidence.svg",
    "oss-public-evidence-integrity.svg",
    "oss-source-verification.svg",
    "oss-release-readiness-seal.svg",
    "oss-architecture-taxonomy.svg",
    "oss-loc-sloc.svg",
    "oss-coupling-cohesion-quadrant.svg",
    "oss-module-hotspots.svg",
    "oss-architecture-risk-heatmap.svg",
    "oss-architecture-delta.svg",
    "oss-complexity-boundary.svg",
    "oss-semantic-maintainability.svg",
    "oss-god-object-radar.svg",
    "oss-score-calibration.svg",
    "oss-score-sensitivity.svg",
    "oss-normalized-vs-raw.svg",
    "oss-scale-readiness.svg",
    "oss-architecture-runway.svg",
    "oss-quality-headroom.svg",
    "oss-refactor-roi-roadmap.svg",
    "oss-independent-validation.svg",
    "oss-analyzer-confidence.svg",
    "oss-quality-trend.svg",
)
