"""Score calibration and anti-gaming checks for the OSS benchmark."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tools.oss_benchmark.payload_utils import as_float, as_int, project_name, project_slug

_LANGUAGE_BY_SUFFIX = {
    ".py": "python",
    ".java": "jvm",
    ".kt": "jvm",
    ".kts": "jvm",
    ".scala": "jvm",
    ".groovy": "jvm",
    ".ts": "ts_js",
    ".tsx": "ts_js",
    ".js": "ts_js",
    ".jsx": "ts_js",
}

_PROFILE_THRESHOLDS = {
    "python-framework": {"max_module_lines": 600.0, "p90_ce": 8.0, "semantic_floor": 75.0},
    "python-library": {"max_module_lines": 700.0, "p90_ce": 8.0, "semantic_floor": 72.0},
    "jvm-platform": {"max_module_lines": 1200.0, "p90_ce": 14.0, "semantic_floor": 70.0},
    "ts-js-platform": {"max_module_lines": 850.0, "p90_ce": 10.0, "semantic_floor": 70.0},
    "mixed-monorepo": {"max_module_lines": 1000.0, "p90_ce": 12.0, "semantic_floor": 70.0},
}

_SCALE_ADJUSTMENT = {
    "focused-framework": 3.0,
    "product-codebase": 1.0,
    "large-monorepo": 0.0,
    "very-large-monorepo": -2.0,
}


def build_scoring_calibration(payload: dict[str, Any]) -> dict[str, Any]:
    """Build raw-vs-normalized score calibration from merged benchmark evidence."""

    projects = [project for project in payload.get("projects", []) if not project.get("unavailable")]
    semantic = (payload.get("semantic_maintainability") or {}).get("summary") or {}
    summary: dict[str, dict[str, Any]] = {}
    sensitivity: dict[str, dict[str, Any]] = {}
    guardrails: dict[str, list[dict[str, Any]]] = {}
    cards: dict[str, dict[str, Any]] = {}

    for project in projects:
        slug = project_slug(project)
        semantic_item = semantic.get(slug) or {}
        calibrated = _calibrate_project(project, semantic_item)
        summary[slug] = calibrated["summary"]
        sensitivity[slug] = calibrated["sensitivity"]
        guardrails[slug] = calibrated["guardrails"]
        cards[slug] = calibrated["explanation_card"]

    return {
        "schema_version": 1,
        "methodology": {
            "purpose": "Separate raw maintainability scores from language/repository-size-normalized scorecards.",
            "score_inputs": [
                "industrial maintainability",
                "semantic maintainability",
                "coverage confidence",
                "module size",
                "fan-out",
                "anti-gaming guardrails",
            ],
            "normalization_note": "Profile thresholds prevent Python frameworks, JVM platforms and mixed monorepos from being compared with one flat module-size budget.",
        },
        "summary": summary,
        "sensitivity": sensitivity,
        "guardrails": guardrails,
        "explanation_cards": cards,
    }


def _calibrate_project(project: dict[str, Any], semantic: dict[str, Any]) -> dict[str, Any]:
    language_mix = _language_mix(project)
    profile = _normalization_profile(project, language_mix)
    scale = _repo_scale(project)
    raw = _raw_score(project, semantic)
    penalties = _normalization_penalties(project, semantic, profile)
    preliminary = _clamp_score(raw + _SCALE_ADJUSTMENT[scale] - sum(penalties.values()))
    guardrails = _guardrails(project, semantic, raw, preliminary)
    normalized = _clamp_score(preliminary - _guardrail_penalty(guardrails))
    guardrails = _guardrails(project, semantic, raw, normalized)
    sensitivity = _sensitivity(project, semantic, raw, profile, scale, _guardrail_penalty(guardrails))
    positives, negatives, next_action = _score_drivers(project, semantic, profile, guardrails)
    summary = {
        "name": project_name(project),
        "raw_score": raw,
        "normalized_score": normalized,
        "normalization_profile": profile,
        "repo_scale": scale,
        "calibration_band": sensitivity["rank_stability"],
        "threshold_10_percent_swing": sensitivity["threshold_10_percent_swing"],
        "language_mix": language_mix,
        "profile_thresholds": _PROFILE_THRESHOLDS[profile],
        "normalization_adjustment": round(normalized - raw, 1),
        "top_positive_drivers": positives[:3],
        "top_negative_drivers": negatives[:3],
        "next_best_action": next_action,
    }
    return {
        "summary": summary,
        "sensitivity": sensitivity,
        "guardrails": guardrails,
        "explanation_card": {
            "headline": _headline(project, summary),
            "positive_drivers": positives[:3],
            "negative_drivers": negatives[:3],
            "next_best_action": next_action,
        },
    }


def _language_mix(project: dict[str, Any]) -> dict[str, float]:
    weights = {"python": 0.0, "jvm": 0.0, "ts_js": 0.0, "other": 0.0}
    files = list(project.get("top_sloc_without_tests") or []) or list(project.get("top_loc_without_tests") or [])
    for item in files:
        suffix = Path(str(item.get("path", ""))).suffix.lower()
        language = _LANGUAGE_BY_SUFFIX.get(suffix, "other")
        weights[language] += max(1.0, as_float(item.get("sloc")) or as_float(item.get("lines")) or 1.0)
    total = sum(weights.values()) or 1.0
    return {key: round(value / total, 3) for key, value in weights.items()}


def _normalization_profile(project: dict[str, Any], mix: dict[str, float]) -> str:
    kind = str((project.get("spec") or {}).get("kind", ""))
    if kind == "local-framework" and mix.get("python", 0.0) >= 0.6:
        return "python-framework"
    if mix.get("python", 0.0) >= 0.65:
        return "python-library"
    if mix.get("jvm", 0.0) >= 0.65:
        return "jvm-platform"
    if mix.get("ts_js", 0.0) >= 0.65:
        return "ts-js-platform"
    return "mixed-monorepo"


def _repo_scale(project: dict[str, Any]) -> str:
    files = as_int((project.get("loc_without_tests") or {}).get("files"))
    sloc = as_int((project.get("loc_without_tests") or {}).get("total_sloc"))
    if files >= 8000 or sloc >= 1_500_000:
        return "very-large-monorepo"
    if files >= 2500 or sloc >= 500_000:
        return "large-monorepo"
    if files >= 700 or sloc >= 100_000:
        return "product-codebase"
    return "focused-framework"


def _raw_score(project: dict[str, Any], semantic: dict[str, Any]) -> int:
    industrial = as_float((project.get("industrial_maintainability") or {}).get("score"))
    semantic_score = as_float(semantic.get("overall_score")) or industrial
    coverage = as_float((project.get("coverage_confidence") or {}).get("score")) or 50.0
    if industrial == 0:
        quality = project.get("quality") or {}
        industrial = ((as_float(quality.get("solid")) + as_float(quality.get("clean_oop"))) / 10.0) * 100.0
    return int(round((industrial * 0.45) + (semantic_score * 0.35) + (coverage * 0.20)))


def _normalization_penalties(
    project: dict[str, Any],
    semantic: dict[str, Any],
    profile: str,
    *,
    threshold_multiplier: float = 1.0,
) -> dict[str, float]:
    thresholds = _PROFILE_THRESHOLDS[profile]
    max_module = as_float((project.get("loc_without_tests") or {}).get("max_lines"))
    p90_ce = as_float((project.get("coupling") or {}).get("p90_ce"))
    semantic_score = as_float(semantic.get("overall_score"))
    god_objects = as_int(semantic.get("god_module_count")) + as_int(semantic.get("god_class_count"))
    direct_imports = as_int(semantic.get("direct_implementation_imports"))
    max_threshold = thresholds["max_module_lines"] * threshold_multiplier
    fanout_threshold = thresholds["p90_ce"] * threshold_multiplier
    return {
        "module_size": min(24.0, _pressure(max_module, max_threshold) * 12.0),
        "fan_out": min(14.0, _pressure(p90_ce, fanout_threshold) * 8.0),
        "semantic_floor": max(0.0, thresholds["semantic_floor"] - semantic_score) * 0.45,
        "god_object": min(10.0, god_objects * 0.9),
        "direct_implementation": min(8.0, direct_imports * 0.35),
    }


def _pressure(value: float, threshold: float) -> float:
    if value <= 0 or threshold <= 0:
        return 0.0
    return max(0.0, (value / threshold) - 1.0)


def _guardrails(project: dict[str, Any], semantic: dict[str, Any], raw: int, normalized: int) -> list[dict[str, Any]]:
    files = as_int((project.get("loc_without_tests") or {}).get("files"))
    sloc = as_int((project.get("loc_without_tests") or {}).get("total_sloc"))
    avg_sloc = sloc / files if files else 0.0
    interface_density = as_float(semantic.get("interface_density"))
    direct_imports = as_int(semantic.get("direct_implementation_imports"))
    gap = normalized - raw
    return [
        {
            "id": "micro_module_pressure",
            "label": "Micro-module pressure",
            "status": "warning" if files >= 200 and avg_sloc < 45 else "passed",
            "message": "Average production SLOC per file is too low for the repo scale."
            if files >= 200 and avg_sloc < 45
            else "No micro-module gaming pressure detected.",
        },
        {
            "id": "hollow_interface_pressure",
            "label": "Hollow-interface pressure",
            "status": "warning" if interface_density > 0.30 and direct_imports == 0 else "passed",
            "message": "High interface density with no implementation pressure may indicate score-padding."
            if interface_density > 0.30 and direct_imports == 0
            else "Interface density looks proportional to implementation evidence.",
        },
        {
            "id": "score_gaming_resistance",
            "label": "Score-gaming resistance",
            "status": "warning" if abs(gap) > 8 else "passed",
            "message": f"Normalized score differs from raw score by {gap:+d} points."
            if abs(gap) > 8
            else "Normalization stays close enough to raw score to remain explainable.",
        },
    ]


def _guardrail_penalty(guardrails: list[dict[str, Any]]) -> float:
    return sum(4.0 for guardrail in guardrails if guardrail.get("status") == "warning")


def _sensitivity(
    project: dict[str, Any],
    semantic: dict[str, Any],
    raw: int,
    profile: str,
    scale: str,
    guardrail_penalty: float,
) -> dict[str, Any]:
    scores = []
    for multiplier in (0.9, 1.0, 1.1):
        penalties = _normalization_penalties(project, semantic, profile, threshold_multiplier=multiplier)
        score = _clamp_score(raw + _SCALE_ADJUSTMENT[scale] - sum(penalties.values()) - guardrail_penalty)
        scores.append(score)
    swing = max(scores) - min(scores)
    if swing <= 5:
        stability = "stable"
    elif swing <= 10:
        stability = "watch"
    else:
        stability = "volatile"
    penalties = _normalization_penalties(project, semantic, profile)
    sensitive_metric = max(penalties, key=lambda key: penalties[key])
    return {
        "threshold_10_percent_swing": swing,
        "rank_stability": stability,
        "most_sensitive_metric": sensitive_metric,
        "low_threshold_score": min(scores),
        "baseline_score": scores[1],
        "high_threshold_score": max(scores),
    }


def _score_drivers(
    project: dict[str, Any],
    semantic: dict[str, Any],
    profile: str,
    guardrails: list[dict[str, Any]],
) -> tuple[list[str], list[str], str]:
    thresholds = _PROFILE_THRESHOLDS[profile]
    loc = project.get("loc_without_tests") or {}
    coupling = project.get("coupling") or {}
    max_lines = as_float(loc.get("max_lines"))
    p90_ce = as_float(coupling.get("p90_ce"))
    god_modules = as_int(semantic.get("god_module_count"))
    dry_kiss = as_float(semantic.get("dry_kiss_score"))
    positives: list[str] = []
    negatives: list[str] = []
    if max_lines <= thresholds["max_module_lines"]:
        positives.append("Largest production module fits the normalized profile threshold.")
    else:
        negatives.append("Largest production module exceeds the normalized profile threshold.")
    if p90_ce <= thresholds["p90_ce"]:
        positives.append("P90 fan-out remains within the language/repo profile budget.")
    else:
        negatives.append("P90 fan-out exceeds the calibrated coupling budget.")
    if god_modules == 0:
        positives.append("No production god modules detected.")
    else:
        negatives.append(f"{god_modules} production god modules remain above the calibrated threshold.")
    if dry_kiss and dry_kiss < 75:
        negatives.append("DRY/KISS responsibility pressure is still visible in semantic evidence.")
    negatives.extend(item["message"] for item in guardrails if item.get("status") == "warning")
    if not negatives:
        negatives.append("No critical calibration penalty exceeded threshold.")
    return positives, negatives, _next_action(negatives)


def _next_action(negatives: list[str]) -> str:
    first = negatives[0] if negatives else ""
    if "module" in first:
        return "Split the largest production modules behind the existing runtime contracts."
    if "fan-out" in first:
        return "Introduce a thinner application service or port to reduce cross-slice fan-out."
    if "DRY/KISS" in first:
        return "Extract repeated branching into strategy contracts with one responsibility per class."
    return "Keep adding connector breadth behind existing contracts while preserving module and fan-out gates."


def _headline(project: dict[str, Any], summary: dict[str, Any]) -> str:
    return (
        f"{project_name(project)} calibrates at {summary['normalized_score']} from raw "
        f"{summary['raw_score']} under the {summary['normalization_profile']} profile."
    )


def _clamp_score(value: float) -> int:
    return int(round(max(0.0, min(100.0, value))))
