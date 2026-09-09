"""Runtime metric models and extraction.

This module owns metric naming and conversion from dpone run reports to a small
canonical metric model. Export-specific concerns live in sibling renderers.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MetricPoint:
    """One numeric runtime metric with stable labels."""

    name: str
    value: float
    labels: Mapping[str, str]
    description: str
    unit: str = "1"

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "value": self.value,
            "labels": dict(self.labels),
            "description": self.description,
            "unit": self.unit,
        }


class RuntimeMetricsExtractor:
    """Extract dependency-free runtime metrics from run reports or CLI inputs."""

    _RUN_RESULT_METRICS: Mapping[str, tuple[str, str, str]] = {
        "extracted_rows": ("dpone_extracted_rows", "Rows extracted by the dpone run.", "rows"),
        "inserted_rows": ("dpone_inserted_rows", "Rows inserted by the dpone run.", "rows"),
        "updated_rows": ("dpone_updated_rows", "Rows updated by the dpone run.", "rows"),
        "final_rows": ("dpone_final_rows", "Final target rows reported by the dpone run.", "rows"),
        "duration_seconds": ("dpone_duration_seconds", "dpone run duration in seconds.", "seconds"),
        "throughput_rows_per_second": (
            "dpone_throughput_rows_per_second",
            "Rows processed per second reported by the dpone run.",
            "rows/s",
        ),
    }

    def extract(
        self,
        *,
        run_report: Mapping[str, Any] | None = None,
        metrics: Mapping[str, float] | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> tuple[MetricPoint, ...]:
        effective_labels = dict(labels or {})
        points: list[MetricPoint] = []
        if run_report is not None:
            points.extend(self._from_run_report(run_report, effective_labels))
        for name, value in sorted((metrics or {}).items()):
            points.append(
                MetricPoint(
                    name=self._metric_name(name),
                    value=float(value),
                    labels=effective_labels,
                    description=f"Custom dpone runtime metric `{name}`.",
                )
            )
        return tuple(points)

    def _from_run_report(self, report: Mapping[str, Any], labels: Mapping[str, str]) -> list[MetricPoint]:
        result = self._mapping(report.get("result"))
        run_labels = dict(labels)
        for key in ("run_id", "process", "selector"):
            value = report.get(key)
            if value is not None and str(value):
                run_labels.setdefault(key, str(value))
        status = result.get("status")
        if status is not None and str(status):
            run_labels.setdefault("status", str(status))

        points: list[MetricPoint] = []
        for source_key, (metric_name, description, unit) in self._RUN_RESULT_METRICS.items():
            value = self._float_or_none(result.get(source_key))
            if source_key == "throughput_rows_per_second" and value is None:
                value = self._nested_run_throughput(result)
            if value is None:
                continue
            points.append(
                MetricPoint(
                    name=metric_name,
                    value=value,
                    labels=run_labels,
                    description=description,
                    unit=unit,
                )
            )
        attempts = self._float_or_none(report.get("attempts"))
        if attempts is not None:
            points.append(
                MetricPoint(
                    name="dpone_attempts",
                    value=attempts,
                    labels=run_labels,
                    description="Attempts used by the dpone run.",
                )
            )
        max_attempts = self._float_or_none(report.get("max_attempts"))
        if max_attempts is not None:
            points.append(
                MetricPoint(
                    name="dpone_max_attempts",
                    value=max_attempts,
                    labels=run_labels,
                    description="Maximum attempts configured for the dpone run.",
                )
            )
        retry_backoff = self._first_float(report, result, key="retry_backoff_seconds")
        if retry_backoff is not None:
            points.append(
                MetricPoint(
                    name="dpone_retry_backoff_seconds",
                    value=retry_backoff,
                    labels=run_labels,
                    description="Retry backoff configured or observed for the dpone run.",
                    unit="seconds",
                )
            )
        error_count = self._count_metric(report, result, count_key="error_count", sequence_keys=("errors", "blockers"))
        if error_count is not None:
            points.append(
                MetricPoint(
                    name="dpone_error_count",
                    value=error_count,
                    labels=run_labels,
                    description="Errors reported by the dpone run.",
                )
            )
        warning_count = self._count_metric(report, result, count_key="warning_count", sequence_keys=("warnings",))
        if warning_count is not None:
            points.append(
                MetricPoint(
                    name="dpone_warning_count",
                    value=warning_count,
                    labels=run_labels,
                    description="Warnings reported by the dpone run.",
                )
            )
        passed = report.get("passed")
        if passed is not None:
            points.append(
                MetricPoint(
                    name="dpone_run_passed",
                    value=1.0 if bool(passed) else 0.0,
                    labels=run_labels,
                    description="dpone run success marker, 1 for passed and 0 for failed.",
                )
            )
        return points

    @staticmethod
    def _mapping(value: object) -> Mapping[str, Any]:
        return value if isinstance(value, Mapping) else {}

    @classmethod
    def _nested_run_throughput(cls, result: Mapping[str, Any]) -> float | None:
        throughput = cls._mapping(result.get("run_throughput"))
        value = cls._float_or_none(throughput.get("rows_per_second"))
        if value is not None:
            return value
        details = cls._mapping(result.get("details"))
        throughput = cls._mapping(details.get("run_throughput"))
        return cls._float_or_none(throughput.get("rows_per_second"))

    @staticmethod
    def _float_or_none(value: Any) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _first_float(cls, *payloads: Mapping[str, Any], key: str) -> float | None:
        for payload in payloads:
            value = cls._float_or_none(payload.get(key))
            if value is not None:
                return value
        return None

    @classmethod
    def _count_metric(
        cls,
        *payloads: Mapping[str, Any],
        count_key: str,
        sequence_keys: tuple[str, ...],
    ) -> float | None:
        explicit = cls._first_float(*payloads, key=count_key)
        if explicit is not None:
            return explicit
        count = 0
        for payload in payloads:
            for key in sequence_keys:
                count += cls._sequence_count(payload.get(key))
        return float(count) if count else None

    @staticmethod
    def _sequence_count(value: object) -> int:
        if isinstance(value, str) or value is None:
            return 0
        if isinstance(value, Mapping):
            return len(value)
        try:
            return len(value)  # type: ignore[arg-type]
        except TypeError:
            return 0

    @staticmethod
    def _metric_name(name: str) -> str:
        raw = name.strip()
        if raw.startswith("dpone_"):
            return raw
        safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in raw.lower())
        return f"dpone_{safe}"
