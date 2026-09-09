"""SLO and error-budget evaluation for dpone ops."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SloCheckResult:
    code: str
    metric: str
    operator: str
    actual: float
    objective: float
    passed: bool
    severity: str
    action: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SloReport:
    results: tuple[SloCheckResult, ...]

    @property
    def passed(self) -> bool:
        return not any(not item.passed and item.severity == "fail" for item in self.results)

    def to_dict(self) -> dict[str, object]:
        return {"passed": self.passed, "results": [item.to_dict() for item in self.results]}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops SLO report",
            "",
            f"- Passed: `{self.passed}`",
            "",
            "| metric | rule | actual | objective | status | action |",
            "|---|---|---:|---:|---|---|",
        ]
        for item in self.results:
            status = "pass" if item.passed else item.severity
            lines.append(
                f"| `{item.metric}` | `{item.operator}` | `{item.actual}` | `{item.objective}` | "
                f"{status} | {item.action} |"
            )
        return "\n".join(lines) + "\n"


class OpsSloService:
    """Evaluates runtime metrics against release/operations objectives."""

    def evaluate(self, *, metrics: Mapping[str, Any], objectives: Mapping[str, Any]) -> SloReport:
        results: list[SloCheckResult] = []
        for metric, objective in objectives.items():
            metric_name = str(metric)
            if metric_name not in metrics:
                results.append(self._missing_metric(metric_name))
                continue
            metric_value = float(metrics[metric_name])
            if isinstance(objective, Mapping):
                if "max" in objective:
                    results.append(self._check(metric_name, metric_value, "max", float(objective["max"])))
                if "min" in objective:
                    results.append(self._check(metric_name, metric_value, "min", float(objective["min"])))
            else:
                results.append(self._check(metric_name, metric_value, "max", float(objective)))
        if not results:
            results.append(
                SloCheckResult(
                    code="slo.no_objectives",
                    metric="*",
                    operator="pass",
                    actual=0.0,
                    objective=0.0,
                    passed=True,
                    severity="pass",
                    action="Add freshness, throughput, latency, failure-rate, or retry-budget objectives.",
                )
            )
        return SloReport(results=tuple(results))

    def _check(self, metric: str, actual: float, operator: str, objective: float) -> SloCheckResult:
        passed = actual <= objective if operator == "max" else actual >= objective
        return SloCheckResult(
            code=f"slo.{metric}.{operator}",
            metric=metric,
            operator=operator,
            actual=actual,
            objective=objective,
            passed=passed,
            severity="pass" if passed else "fail",
            action=self._action(metric, operator, passed),
        )

    @staticmethod
    def _missing_metric(metric: str) -> SloCheckResult:
        return SloCheckResult(
            code=f"slo.{metric}.missing",
            metric=metric,
            operator="present",
            actual=0.0,
            objective=1.0,
            passed=False,
            severity="fail",
            action=f"Add `{metric}` to the run report metrics before evaluating this SLO.",
        )

    @staticmethod
    def _action(metric: str, operator: str, passed: bool) -> str:
        if passed:
            return "No action required."
        if "freshness" in metric:
            return "Reduce schedule interval, fix upstream delays, or investigate state lag."
        if "throughput" in metric:
            return "Tune partitioning, native bulk path, batch size, or retry policy."
        if "latency" in metric:
            return "Inspect slow stages, target locks, API backoff, and staging finalization."
        if "failure" in metric or "retry" in metric:
            return "Investigate connector errors, backoff settings, target throttling, and retry budget burn."
        if operator == "min":
            return "Increase capacity, parallelism, or source/sink batch efficiency."
        return "Reduce runtime cost, lag, or error rate before release."
