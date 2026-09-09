"""Benchmark regression gate for long-running ELT/ETL performance suites."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class BenchmarkBaselineItem:
    metric: str
    direction: str
    actual: float
    baseline: float
    allowed_regression_ratio: float
    allowed_threshold: float
    passed: bool
    action: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BenchmarkBaselineReport:
    passed: bool
    allowed_regression_ratio: float
    items: tuple[BenchmarkBaselineItem, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "allowed_regression_ratio": self.allowed_regression_ratio,
            "items": [item.to_dict() for item in self.items],
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone benchmark baseline gate",
            "",
            f"- Passed: `{self.passed}`",
            f"- Allowed regression ratio: `{self.allowed_regression_ratio}`",
            "",
            "| metric | direction | actual | baseline | threshold | status | action |",
            "|---|---|---:|---:|---:|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(
                f"| `{item.metric}` | `{item.direction}` | `{item.actual}` | `{item.baseline}` | "
                f"`{item.allowed_threshold}` | {status} | {item.action} |"
            )
        lines.extend(
            [
                "",
                "## Benchmark runbook",
                "",
                "1. Re-run the same benchmark profile before changing the committed baseline.",
                "2. Confirm row count, source/sink versions, hardware profile, and bulk path are unchanged.",
                "3. If regression is real, tune partitioning, native bulk mode, batch size, or target finalizer policy.",
                "4. Update the baseline only after a reviewed intentional performance change.",
                "",
            ]
        )
        return "\n".join(lines)


class BenchmarkBaselineService:
    """Compares current benchmark metrics with a committed baseline profile."""

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        metrics: Mapping[str, Any],
        baseline: Mapping[str, Any],
        allowed_regression_ratio: float = 0.10,
    ) -> BenchmarkBaselineReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        items = tuple(
            self._item(
                metric=str(metric),
                actual=self._actual(str(metric), metrics),
                spec=spec,
                allowed_regression_ratio=allowed_regression_ratio,
            )
            for metric, spec in sorted(baseline.items())
        )
        report = BenchmarkBaselineReport(
            passed=all(item.passed for item in items),
            allowed_regression_ratio=allowed_regression_ratio,
            items=items,
            output_dir=str(directory),
        )
        (directory / "benchmark_baseline.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "benchmark_baseline.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _item(
        self,
        *,
        metric: str,
        actual: float,
        spec: Any,
        allowed_regression_ratio: float,
    ) -> BenchmarkBaselineItem:
        baseline, direction = self._spec(spec)
        threshold = self._threshold(baseline, direction, allowed_regression_ratio)
        passed = actual >= threshold if direction == "higher" else actual <= threshold
        return BenchmarkBaselineItem(
            metric=metric,
            direction=direction,
            actual=actual,
            baseline=baseline,
            allowed_regression_ratio=allowed_regression_ratio,
            allowed_threshold=threshold,
            passed=passed,
            action="No action required." if passed else self._action(metric, direction),
        )

    @staticmethod
    def _actual(metric: str, metrics: Mapping[str, Any]) -> float:
        if metric not in metrics:
            return float("-inf")
        return float(metrics[metric])

    @staticmethod
    def _spec(spec: Any) -> tuple[float, str]:
        if isinstance(spec, Mapping):
            direction = str(spec.get("direction", "higher"))
            if direction not in {"higher", "lower"}:
                direction = "higher"
            return float(spec.get("value", 0.0)), direction
        return float(spec), "higher"

    @staticmethod
    def _threshold(baseline: float, direction: str, allowed_regression_ratio: float) -> float:
        ratio = max(0.0, allowed_regression_ratio)
        if direction == "higher":
            return baseline * (1.0 - ratio)
        return baseline * (1.0 + ratio)

    @staticmethod
    def _action(metric: str, direction: str) -> str:
        if direction == "higher":
            return f"Increase `{metric}` by tuning parallelism, native bulk path, batch size, or target capacity."
        return f"Reduce `{metric}` by inspecting slow stages, target locks, network overhead, or finalization policy."
