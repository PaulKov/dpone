"""CLI wrapper for the reusable OSS code-quality benchmark package."""

# ruff: noqa: E402,F401,I001

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.oss_benchmark.architecture_taxonomy import build_architecture_taxonomy_matrix  # noqa: E402,F401
from tools.oss_benchmark.benchmark_intelligence import (  # noqa: E402,F401
    build_regression_summary,
    build_remediation_backlog,
    build_score_explanations,
)
from tools.oss_benchmark.budgets import evaluate_quality_budgets, load_quality_budgets  # noqa: E402,F401
from tools.oss_benchmark.candidate_delta import (  # noqa: E402,F401
    DEFAULT_QUALITY_BUDGETS,
    build_candidate_quality_delta,
)
from tools.oss_benchmark.certification import build_runtime_certification  # noqa: E402,F401
from tools.oss_benchmark.claims import build_claims_ledger  # noqa: E402,F401
from tools.oss_benchmark.collectors import (  # noqa: E402,F401
    avg_clustering,
    collect_all_projects,
    collect_project_metrics,
    compute_coupling_metrics,
    connected_components,
    count_lines,
    count_sloc,
    ensure_external_repo,
    is_dirty_repo,
    is_test_file,
    iter_source_files,
    module_name_for_file,
    parse_import_targets,
    percentile,
    score_quality,
    should_skip_path,
    slice_for_file,
    summarize_loc,
    top_files,
)
from tools.oss_benchmark.complexity_boundary import (  # noqa: E402,F401
    analyze_complexity_boundary_discipline,
    analyze_file_complexity,
    measure_generic_complexity,
    measure_python_complexity,
    project_complexity_boundary,
)
from tools.oss_benchmark.config import (  # noqa: E402,F401
    ASSET_DIR,
    BENCHMARK_DATE,
    DATA_PATH,
    DOC_PATH,
    HISTORY_PATH,
    IGNORED_DIRS,
    LOGICAL_ANCHORS,
    SOURCE_SUFFIXES,
    TEST_DIR_NAMES,
    default_project_specs,
)
from tools.oss_benchmark.core import (  # noqa: E402,F401
    build_benchmark_payload,
    build_history_entry,
    compute_architecture_risk,
    compute_coverage_confidence,
    compute_industrial_maintainability_index,
    detect_ci_evidence,
    enrich_payload_with_maintainability,
    load_history_payload,
    load_previous_payload,
    main,
    parse_args,
    project_to_jsonable,
    update_history_payload,
    write_benchmark_outputs,
    write_history_payload,
)
from tools.oss_benchmark.evidence_trust import (  # noqa: E402,F401
    build_evidence_trust_summary,
    build_metric_provenance,
    build_provenance_export,
    checksum_artifacts,
    confidence_for_project,
    run_external_loc_cross_check,
    write_provenance_export,
)
from tools.oss_benchmark.exports import (  # noqa: E402,F401
    build_evidence_export_manifest,
    write_evidence_exports,
)
from tools.oss_benchmark.external_analyzers import (  # noqa: E402,F401
    AnalyzerProcessResult,
    ClocParser,
    LizardParser,
    RadonParser,
    SubprocessAnalyzerRunner,
    TokeiParser,
    collect_external_analyzer_results,
)
from tools.oss_benchmark.feature_parity import build_feature_parity_matrix  # noqa: E402,F401
from tools.oss_benchmark.governance_compliance import build_governance_compliance_matrix  # noqa: E402,F401
from tools.oss_benchmark.independent_validation import build_independent_validation  # noqa: E402,F401
from tools.oss_benchmark.models import (  # noqa: E402,F401
    CouplingMetrics,
    FileMetric,
    LocSummary,
    ProjectMetrics,
    ProjectSpec,
    QualityScore,
    QualitySignal,
)
from tools.oss_benchmark.operability_tco import build_operability_tco_matrix  # noqa: E402,F401
from tools.oss_benchmark.operational_reliability import build_operational_reliability_matrix  # noqa: E402,F401
from tools.oss_benchmark.pr_calibration import render_scoring_calibration_pr_section  # noqa: E402,F401
from tools.oss_benchmark.pr_evidence import render_evidence_trust_pr_section  # noqa: E402,F401
from tools.oss_benchmark.pr_public_integrity import render_public_evidence_integrity_pr_section  # noqa: E402,F401
from tools.oss_benchmark.pr_release_readiness import render_release_readiness_pr_section  # noqa: E402,F401
from tools.oss_benchmark.pr_scale import render_scale_readiness_pr_section  # noqa: E402,F401
from tools.oss_benchmark.pr_semantic import render_semantic_maintainability_pr_section  # noqa: E402,F401
from tools.oss_benchmark.pr_source_verification import render_source_verification_pr_section  # noqa: E402,F401
from tools.oss_benchmark.pr_summary import (  # noqa: E402,F401
    PR_SUMMARY_PATH,
    render_pr_summary,
    write_pr_summary,
)
from tools.oss_benchmark.pr_validation import render_independent_validation_pr_section  # noqa: E402,F401
from tools.oss_benchmark.public_evidence_integrity import (  # noqa: E402,F401
    build_public_evidence_integrity,
    find_public_redaction_violations,
    sanitize_public_payload,
)
from tools.oss_benchmark.quality_gates import (  # noqa: E402,F401
    DEFAULT_QUALITY_GATE_THRESHOLDS,
    evaluate_quality_gates,
)
from tools.oss_benchmark.refactor_roi import build_refactor_roi_roadmap  # noqa: E402,F401
from tools.oss_benchmark.release_readiness import build_benchmark_release_readiness  # noqa: E402,F401
from tools.oss_benchmark.regression_gate import build_pr_regression_gate  # noqa: E402,F401
from tools.oss_benchmark.renderers.architecture import (  # noqa: E402,F401
    render_architecture_taxonomy_section,
    render_architecture_taxonomy_svg,
)
from tools.oss_benchmark.renderers.budgets import render_quality_budgets_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.calibration_svg import (  # noqa: E402,F401
    render_normalized_vs_raw_svg,
    render_score_calibration_svg,
    render_score_sensitivity_svg,
)
from tools.oss_benchmark.renderers.certification import render_runtime_certification_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.claims import render_claims_ledger_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.complexity_boundary import render_complexity_boundary_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.evidence_svg import render_evidence_confidence_svg  # noqa: E402,F401
from tools.oss_benchmark.renderers.evidence_trust import render_evidence_trust_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.exports import render_evidence_exports_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.governance import (  # noqa: E402,F401
    render_governance_compliance_section,
    render_governance_compliance_svg,
)
from tools.oss_benchmark.renderers.independent_validation import render_independent_validation_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.markdown import (  # noqa: E402,F401
    render_feature_parity_section,
    render_file_table,
    render_markdown,
)
from tools.oss_benchmark.renderers.operability import (  # noqa: E402,F401
    render_operability_tco_section,
    render_operability_tco_svg,
)
from tools.oss_benchmark.renderers.public_evidence_integrity import render_public_evidence_integrity_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.public_integrity_svg import render_public_evidence_integrity_svg  # noqa: E402,F401
from tools.oss_benchmark.renderers.refactor_roi import render_refactor_roi_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.release_readiness import (  # noqa: E402,F401
    render_release_readiness_markdown,
    render_release_readiness_section,
)
from tools.oss_benchmark.renderers.release_readiness_svg import render_release_readiness_svg  # noqa: E402,F401
from tools.oss_benchmark.renderers.reliability import (  # noqa: E402,F401
    render_operational_reliability_section,
    render_operational_reliability_svg,
)
from tools.oss_benchmark.renderers.roi_svg import render_refactor_roi_svg  # noqa: E402,F401
from tools.oss_benchmark.renderers.scale_readiness import render_scale_readiness_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.scale_svg import (  # noqa: E402,F401
    render_architecture_runway_svg,
    render_quality_headroom_svg,
    render_scale_readiness_svg,
)
from tools.oss_benchmark.renderers.scoring_calibration import render_scoring_calibration_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.security import (  # noqa: E402,F401
    render_security_supply_chain_section,
    render_security_supply_chain_svg,
)
from tools.oss_benchmark.renderers.semantic_maintainability import (  # noqa: E402,F401
    render_semantic_maintainability_section,
)
from tools.oss_benchmark.renderers.semantic_svg import (  # noqa: E402,F401
    render_god_object_radar_svg,
    render_semantic_maintainability_svg,
)
from tools.oss_benchmark.renderers.source_verification import render_source_verification_section  # noqa: E402,F401
from tools.oss_benchmark.renderers.source_verification_svg import render_source_verification_svg  # noqa: E402,F401
from tools.oss_benchmark.renderers.svg import (  # noqa: E402,F401
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
from tools.oss_benchmark.renderers.validation_svg import (  # noqa: E402,F401
    render_analyzer_confidence_svg,
    render_independent_validation_svg,
)
from tools.oss_benchmark.scale_readiness import build_scale_readiness  # noqa: E402,F401
from tools.oss_benchmark.scoring_calibration import build_scoring_calibration  # noqa: E402,F401
from tools.oss_benchmark.security_supply_chain import build_security_supply_chain_matrix  # noqa: E402,F401
from tools.oss_benchmark.semantic_maintainability import (  # noqa: E402,F401
    analyze_semantic_file,
    analyze_semantic_maintainability,
    measure_generic_semantics,
    measure_python_semantics,
    project_semantic_maintainability,
)
from tools.oss_benchmark.source_verification import (  # noqa: E402,F401
    DefaultSourceVerifier,
    SourceCheckResult,
    SourceVerifier,
    build_source_registry,
    build_source_verification,
    source_id_for,
)
from tools.oss_benchmark.state import (  # noqa: E402,F401
    METRIC_GROUPS,
    PROJECT_ORDER,
    PROJECT_STUBS,
    RunContext,
    fresh_metric_groups,
    freshness_summary,
    merge_benchmark_payload,
    project_freshness_label,
    project_freshness_status,
    run_context_to_jsonable,
    score_deltas,
)
from tools.oss_benchmark.trend_history import build_architecture_delta  # noqa: E402,F401
from tools.oss_benchmark.trust_center import (  # noqa: E402,F401
    build_trust_center_export,
    render_trust_center_badge_svg,
    render_trust_center_markdown,
    write_trust_center_outputs,
)

if __name__ == "__main__":
    raise SystemExit(main())
