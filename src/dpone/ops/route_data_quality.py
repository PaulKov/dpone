"""Route-level data quality and exception management service."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from dpone.ops.routes import (
    DEFAULT_ROUTE_DATA_QUALITY_EVIDENCE,
    RouteDataQualityEvidenceReader,
    RouteDataQualityExceptionSummary,
    RouteDataQualityPolicy,
    RouteDataQualityReport,
    RouteDataQualityThresholds,
    RouteKey,
    RouteProfileCatalog,
)


class RouteDataQualityService:
    """Build a route data quality scorecard without executing the route."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        reader: RouteDataQualityEvidenceReader | None = None,
        policy: RouteDataQualityPolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._reader = reader or RouteDataQualityEvidenceReader()
        self._policy = policy or RouteDataQualityPolicy()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        artifacts: Mapping[str, str | Path] | None = None,
        required_evidence: Sequence[str] = (),
        min_score: float = 95.0,
        warning_score: float = 98.0,
        max_exception_ratio: float = 0.0,
        max_quarantine_rows: int = 0,
        max_exception_age_hours: float = 24.0,
    ) -> RouteDataQualityReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        thresholds = RouteDataQualityThresholds(
            min_score=min_score,
            warning_score=warning_score,
            max_exception_ratio=max_exception_ratio,
            max_quarantine_rows=max_quarantine_rows,
            max_exception_age_hours=max_exception_age_hours,
        )
        required = _required_evidence(extra=tuple(required_evidence))
        artifact_map = dict(artifacts or {})
        names = tuple(dict.fromkeys((*required, *sorted(artifact_map))))
        evidence = tuple(
            self._reader.read(
                name=name,
                path_value=artifact_map.get(name),
                required=name in required,
                route=route,
            )
            for name in names
        )
        decision = self._policy.evaluate(
            route_colon_id=route.colon_id,
            profile_exists=profile is not None,
            required_evidence=required,
            evidence=evidence,
            thresholds=thresholds,
        )
        report = RouteDataQualityReport(
            route=route,
            profile=profile,
            passed=decision.passed,
            status=decision.status,
            score=decision.score,
            thresholds=thresholds,
            dimensions=tuple(dimension for item in evidence for dimension in item.dimensions),
            exceptions=_exceptions(evidence),
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            required_evidence=required,
            evidence=evidence,
            output_dir=str(directory),
            json_path=str(directory / "route_data_quality.json"),
            markdown_path=str(directory / "route_data_quality.md"),
        )
        report.write()
        return report


def _required_evidence(*, extra: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*DEFAULT_ROUTE_DATA_QUALITY_EVIDENCE, *extra)))


def _exceptions(evidence: Sequence[object]) -> RouteDataQualityExceptionSummary:
    by_evidence: dict[str, dict[str, float | int]] = {}
    total_count = 0
    max_ratio = 0.0
    max_age = 0.0
    for item in evidence:
        name = getattr(item, "name")
        count = int(getattr(item, "exception_count"))
        ratio = float(getattr(item, "exception_ratio"))
        age = float(getattr(item, "max_exception_age_hours"))
        total_count += count
        max_ratio = max(max_ratio, ratio)
        max_age = max(max_age, age)
        by_evidence[str(name)] = {"count": count, "ratio": ratio, "max_age_hours": age}
    return RouteDataQualityExceptionSummary(
        total_count=total_count,
        max_ratio=round(max_ratio, 6),
        max_age_hours=round(max_age, 2),
        by_evidence=by_evidence,
    )


__all__ = ["RouteDataQualityService"]
