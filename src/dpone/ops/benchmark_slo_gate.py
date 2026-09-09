"""Unified benchmark and SLO gate for certification evidence."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.benchmark_baseline import BenchmarkBaselineReport, BenchmarkBaselineService
from dpone.ops.slo import OpsSloService, SloReport


@dataclass(frozen=True, slots=True)
class BenchmarkSloGateReport:
    passed: bool
    benchmark_passed: bool
    slo_passed: bool
    metric_count: int
    blockers: tuple[str, ...]
    benchmark: BenchmarkBaselineReport
    slo: SloReport
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "benchmark_passed": self.benchmark_passed,
            "slo_passed": self.slo_passed,
            "metric_count": self.metric_count,
            "blockers": list(self.blockers),
            "benchmark": self.benchmark.to_dict(),
            "slo": self.slo.to_dict(),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        lines = [
            "# dpone benchmark + SLO gate",
            "",
            f"- Passed: `{self.passed}`",
            f"- Benchmark passed: `{self.benchmark_passed}`",
            f"- SLO passed: `{self.slo_passed}`",
            f"- Metric count: `{self.metric_count}`",
            "",
            "## Blockers",
            "",
            blockers,
            "",
            "## Benchmark",
            "",
            self.benchmark.to_markdown(),
            "",
            "## SLO",
            "",
            self.slo.to_markdown(),
            "",
            "## Runbook",
            "",
            "1. Re-run the same benchmark profile before updating committed baselines.",
            "2. Fix SLO breaches before publishing connector certification or release evidence.",
            "3. Attach `benchmark_slo_gate.json` to `dpone ops certification-pack`.",
            "4. If throughput regresses, inspect native bulk path, partitioning, target locks, and finalizer policy.",
            "",
        ]
        return "\n".join(lines)


class BenchmarkSloGateService:
    """Evaluates performance regressions and operational objectives together."""

    def __init__(
        self,
        *,
        benchmark_service: BenchmarkBaselineService | None = None,
        slo_service: OpsSloService | None = None,
    ) -> None:
        self._benchmark_service = benchmark_service or BenchmarkBaselineService()
        self._slo_service = slo_service or OpsSloService()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        metrics: Mapping[str, Any],
        baseline: Mapping[str, Any],
        objectives: Mapping[str, Any],
        allowed_regression_ratio: float = 0.10,
    ) -> BenchmarkSloGateReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        benchmark_dir = directory / "benchmark"
        benchmark = self._benchmark_service.evaluate(
            output_dir=benchmark_dir,
            metrics=metrics,
            baseline=baseline,
            allowed_regression_ratio=allowed_regression_ratio,
        )
        slo = self._slo_service.evaluate(metrics=metrics, objectives=objectives)
        blockers = tuple(
            item
            for item, passed in (
                ("benchmark_baseline.not_passed", benchmark.passed),
                ("slo.not_passed", slo.passed),
            )
            if not passed
        )
        json_path = directory / "benchmark_slo_gate.json"
        markdown_path = directory / "benchmark_slo_gate.md"
        report = BenchmarkSloGateReport(
            passed=not blockers,
            benchmark_passed=benchmark.passed,
            slo_passed=slo.passed,
            metric_count=len(metrics),
            blockers=blockers,
            benchmark=benchmark,
            slo=slo,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report
