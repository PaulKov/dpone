"""Evidence-bounded feature parity matrix for data-integration platforms."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.feature_catalog import (
    DIMENSIONS,
    LEVEL_POINTS,
    TOOLS,
    FeatureDimension,
    FeatureRating,
    FeatureTool,
)
from tools.oss_benchmark.feature_closed_core_ratings import closed_core_feature_ratings
from tools.oss_benchmark.feature_core_ratings import core_feature_ratings
from tools.oss_benchmark.feature_legacy_ratings import legacy_feature_ratings

__all__ = [
    "DIMENSIONS",
    "LEVEL_POINTS",
    "TOOLS",
    "FeatureDimension",
    "FeatureRating",
    "FeatureTool",
    "build_feature_parity_matrix",
]


def build_feature_parity_matrix() -> dict[str, Any]:
    """Build the public feature parity matrix from isolated rating sources."""

    entries = _ratings()
    return {
        "schema_version": 1,
        "methodology": (
            "Evidence-bounded feature comparison from public official documentation and local dpone docs. "
            "Scores are qualitative product-surface indicators, not performance benchmarks or commercial claims."
        ),
        "levels": LEVEL_POINTS,
        "tools": [tool.__dict__ for tool in TOOLS],
        "dimensions": [dimension.__dict__ for dimension in DIMENSIONS],
        "entries": [entry.__dict__ for entry in entries],
        "summary": {tool.slug: _summarize_tool(tool, entries) for tool in TOOLS},
    }


def _ratings() -> tuple[FeatureRating, ...]:
    return (
        *core_feature_ratings(),
        *legacy_feature_ratings(),
        *closed_core_feature_ratings(),
    )


def _summarize_tool(tool: FeatureTool, entries: tuple[FeatureRating, ...]) -> dict[str, Any]:
    weighted_score = 0
    max_score = 0
    native_count = 0
    for dimension in DIMENSIONS:
        feature_rating = _rating_for(entries, tool.slug, dimension.slug)
        points = LEVEL_POINTS.get(feature_rating.level, 0)
        weighted_score += points * dimension.weight
        max_score += 3 * dimension.weight
        if feature_rating.level in {"native", "managed", "strong"}:
            native_count += 1

    coverage = round((weighted_score / max_score) * 100) if max_score else 0
    return {
        "name": tool.name,
        "score": coverage,
        "band": _score_band(coverage),
        "native_or_managed_capabilities": native_count,
        "code_comparable": tool.code_comparable,
        "comparator_note": tool.comparator_note,
    }


def _rating_for(entries: tuple[FeatureRating, ...], tool: str, dimension: str) -> FeatureRating:
    for entry in entries:
        if entry.tool == tool and entry.dimension == dimension:
            return entry
    raise KeyError(f"Missing feature rating for {tool}/{dimension}")


def _score_band(coverage: int) -> str:
    if coverage >= 85:
        return "leader"
    if coverage >= 70:
        return "strong"
    if coverage >= 50:
        return "focused"
    return "limited"
