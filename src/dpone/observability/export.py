"""Runtime metrics artifact export service."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


@dataclass(frozen=True, slots=True)
class RuntimeMetricsExportReport:
    passed: bool
    output_dir: str
    metric_count: int
    service_name: str
    namespace: str
    prometheus_path: str
    opentelemetry_path: str
    metrics_index_path: str
    json_report_path: str
    markdown_report_path: str
    blockers: tuple[str, ...]
    resource_attributes: Mapping[str, str]
    correlation_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "output_dir": self.output_dir,
            "metric_count": self.metric_count,
            "service_name": self.service_name,
            "namespace": self.namespace,
            "prometheus_path": self.prometheus_path,
            "opentelemetry_path": self.opentelemetry_path,
            "metrics_index_path": self.metrics_index_path,
            "json_report_path": self.json_report_path,
            "markdown_report_path": self.markdown_report_path,
            "blockers": list(self.blockers),
            "resource_attributes": dict(self.resource_attributes),
            "correlation_id": self.correlation_id,
        }

    def to_markdown(self) -> str:
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        return "\n".join(
            [
                "# dpone runtime metrics export",
                "",
                f"- Passed: `{self.passed}`",
                f"- Metric count: `{self.metric_count}`",
                f"- Service name: `{self.service_name}`",
                f"- Namespace: `{self.namespace}`",
                f"- Output dir: `{self.output_dir}`",
                "",
                "| artifact | path |",
                "|---|---|",
                f"| Prometheus | `{self.prometheus_path}` |",
                f"| OpenTelemetry JSON | `{self.opentelemetry_path}` |",
                f"| Metrics index | `{self.metrics_index_path}` |",
                f"| JSON report | `{self.json_report_path}` |",
                f"| Markdown report | `{self.markdown_report_path}` |",
                "",
                "## Blockers",
                "",
                blockers,
                "",
            ]
        )


class RuntimeMetricsExportService:
    """Build Prometheus, OpenTelemetry and human runtime metrics artifacts."""

    def __init__(
        self,
        *,
        extractor: Any | None = None,
        prometheus_renderer: Any | None = None,
        opentelemetry_renderer: Any | None = None,
        correlation_loader: Any | None = None,
    ) -> None:
        self._extractor = extractor or _symbol("dpone.observability.metrics:RuntimeMetricsExtractor")()
        self._prometheus_renderer = (
            prometheus_renderer or _symbol("dpone.observability.prometheus:PrometheusTextRenderer")()
        )
        self._opentelemetry_renderer = (
            opentelemetry_renderer or _symbol("dpone.observability.opentelemetry:OpenTelemetryJsonRenderer")()
        )
        self._correlation_loader = correlation_loader or _symbol(
            "dpone.observability.correlation:load_airflow_correlation"
        )

    def export(
        self,
        *,
        output_dir: str | Path,
        run_report_path: str | Path | None = None,
        metrics: Mapping[str, float] | None = None,
        labels: Mapping[str, str] | None = None,
        resource_attributes: Mapping[str, str] | None = None,
        service_name: str = "dpone",
        namespace: str = "dpone.local",
        airflow_evidence_bundle_path: str | Path | None = None,
    ) -> RuntimeMetricsExportReport:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        blockers: list[str] = []
        run_report = self._load_run_report(run_report_path, blockers)
        correlation_result = self._correlation_loader(airflow_evidence_bundle_path)
        blockers.extend(correlation_result.blockers)
        correlation = correlation_result.correlation
        points = self._extractor.extract(run_report=run_report, metrics=metrics, labels=labels)
        if not points:
            blockers.append("metrics.empty")

        prometheus_path = out / "prometheus_metrics.prom"
        opentelemetry_path = out / "opentelemetry_metrics.json"
        metrics_index_path = out / "metrics_index.json"
        json_report_path = out / "runtime_metrics.json"
        markdown_report_path = out / "runtime_metrics.md"

        prometheus_path.write_text(self._prometheus_renderer.render(points), encoding="utf-8")
        effective_resource_attributes = dict(resource_attributes or {})
        point_attributes: Mapping[str, str] = {}
        if correlation is not None:
            effective_resource_attributes.update(
                _symbol("dpone.observability.correlation:otel_resource_attributes")(correlation)
            )
            point_attributes = _symbol("dpone.observability.correlation:otel_point_attributes")(correlation)
        opentelemetry_payload = self._opentelemetry_renderer.render(
            points,
            service_name=service_name,
            namespace=namespace,
            resource_attributes=effective_resource_attributes,
            data_point_attributes=point_attributes,
        )
        opentelemetry_path.write_text(
            json.dumps(opentelemetry_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        report = RuntimeMetricsExportReport(
            passed=not blockers,
            output_dir=str(out),
            metric_count=len(points),
            service_name=service_name,
            namespace=namespace,
            prometheus_path=str(prometheus_path),
            opentelemetry_path=str(opentelemetry_path),
            metrics_index_path=str(metrics_index_path),
            json_report_path=str(json_report_path),
            markdown_report_path=str(markdown_report_path),
            blockers=tuple(blockers),
            resource_attributes=effective_resource_attributes,
            correlation_id=correlation.correlation_id if correlation is not None else None,
        )
        json_report_path.write_text(
            json.dumps(self._report_payload(report, points), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        markdown_report_path.write_text(report.to_markdown(), encoding="utf-8")
        _symbol("dpone.observability.artifacts:MetricsArtifactIndexService")().build(
            output_dir=out,
            artifacts={
                "opentelemetry_metrics.json": opentelemetry_path,
                "prometheus_metrics.prom": prometheus_path,
                "runtime_metrics.json": json_report_path,
                "runtime_metrics.md": markdown_report_path,
            },
        )
        return report

    @staticmethod
    def _load_run_report(path: str | Path | None, blockers: list[str]) -> Mapping[str, Any] | None:
        if path is None:
            return None
        report_path = Path(path)
        if not report_path.is_file():
            blockers.append("run_report.missing")
            return None
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            blockers.append("run_report.invalid_json")
            return None
        if not isinstance(payload, Mapping):
            blockers.append("run_report.invalid_shape")
            return None
        return payload

    @staticmethod
    def _report_payload(report: RuntimeMetricsExportReport, points: tuple[Any, ...]) -> dict[str, object]:
        payload = report.to_dict()
        payload["metrics"] = [point.to_dict() for point in points]
        return payload
