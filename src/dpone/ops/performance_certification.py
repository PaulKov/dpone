"""Release-grade performance certification evidence."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.ops.ids import utc_now_iso


@dataclass(frozen=True, slots=True)
class PerformanceMetricDecision:
    metric: str
    value: float | None
    rule: str
    threshold: float
    passed: bool
    blocker: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PerformanceCertificationReport:
    profile: str
    row_count: int
    passed: bool
    generated_at: str
    blockers: tuple[str, ...]
    results: tuple[PerformanceMetricDecision, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "row_count": self.row_count,
            "passed": self.passed,
            "evidence_status": self.evidence_status,
            "generated_at": self.generated_at,
            "blockers": list(self.blockers),
            "results": [item.to_dict() for item in self.results],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone performance certification",
            "",
            f"- Profile: `{self.profile}`",
            f"- Row count: `{self.row_count}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            f"- Generated at: `{self.generated_at}`",
            "",
            "| metric | value | rule | threshold | status |",
            "|---|---:|---|---:|---|",
        ]
        for item in self.results:
            status = "pass" if item.passed else "fail"
            value = "-" if item.value is None else f"{item.value:g}"
            lines.append(f"| `{item.metric}` | {value} | `{item.rule}` | {item.threshold:g} | {status} |")
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        lines.extend(
            [
                "",
                "## Blockers",
                "",
                blockers,
                "",
                "## Runbook",
                "",
                "1. If throughput regressed, inspect native fast path, partitioning, batch size, and target capacity.",
                "2. If duration or memory exceeded the maximum, reduce chunk size or switch to staging/object-storage paths.",
                "3. Update thresholds only after a reviewed capacity or benchmark-baseline change.",
                "4. Attach this artifact to live certification and release evidence packs.",
                "",
            ]
        )
        return "\n".join(lines)

    @property
    def evidence_status(self) -> str:
        """Expose the observed threshold result without production promotion."""

        return "PASS" if self.passed else "FAIL"


class PerformanceCertificationService:
    """Evaluates release-grade performance metrics against explicit thresholds."""

    def certify(
        self,
        *,
        output_dir: str | Path,
        profile: str,
        row_count: int,
        metrics: Mapping[str, object],
        minimums: Mapping[str, object] | None = None,
        maximums: Mapping[str, object] | None = None,
    ) -> PerformanceCertificationReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        decisions = (
            *(_minimum_decision(metric, metrics, threshold) for metric, threshold in (minimums or {}).items()),
            *(_maximum_decision(metric, metrics, threshold) for metric, threshold in (maximums or {}).items()),
        )
        blockers = tuple(item.blocker for item in decisions if item.blocker)
        json_path = directory / "performance_certification.json"
        markdown_path = directory / "performance_certification.md"
        report = PerformanceCertificationReport(
            profile=profile,
            row_count=max(1, min(int(row_count), 100000)),
            passed=not blockers,
            generated_at=utc_now_iso(),
            blockers=blockers,
            results=decisions,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def _minimum_decision(
    metric: str,
    metrics: Mapping[str, object],
    threshold: object,
) -> PerformanceMetricDecision:
    value = _float_or_none(metrics.get(metric))
    target = _threshold(threshold)
    passed = value is not None and value >= target
    blocker = None if passed else f"{metric}.below_min"
    return PerformanceMetricDecision(metric, value, "min", target, passed, blocker)


def _maximum_decision(
    metric: str,
    metrics: Mapping[str, object],
    threshold: object,
) -> PerformanceMetricDecision:
    value = _float_or_none(metrics.get(metric))
    target = _threshold(threshold)
    passed = value is not None and value <= target
    blocker = None if passed else f"{metric}.above_max"
    return PerformanceMetricDecision(metric, value, "max", target, passed, blocker)


def _float_or_none(value: object) -> float | None:
    if not isinstance(value, str | int | float | bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _threshold(value: object) -> float:
    parsed = _float_or_none(value)
    if parsed is None:
        raise ValueError("Performance thresholds must be numeric")
    return parsed


__all__ = ["PerformanceCertificationReport", "PerformanceCertificationService", "PerformanceMetricDecision"]
