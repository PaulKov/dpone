"""Grafana and Prometheus alert pack generation."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ObservabilityPackReport:
    passed: bool
    output_dir: str
    service_name: str
    grafana_dashboard_path: str
    prometheus_alerts_path: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        return "\n".join(
            [
                "# dpone observability pack",
                "",
                f"- Passed: `{self.passed}`",
                f"- Service: `{self.service_name}`",
                f"- Grafana dashboard: `{self.grafana_dashboard_path}`",
                f"- Prometheus alerts: `{self.prometheus_alerts_path}`",
                "",
                "## Runbook",
                "",
                "1. Import `grafana_dashboard.json` into Grafana or provision it as a dashboard file.",
                "2. Load `prometheus_alerts.yml` into Prometheus/Alertmanager rules.",
                "3. Keep labels stable: `env`, `pipeline`, `source`, `sink`, `strategy`, `run_id`.",
                "4. Alert on failures and lag, then debug with run registry and reconciliation artifacts.",
                "",
            ]
        )


class ObservabilityPackService:
    """Builds local-first observability templates for dpone runtime metrics."""

    def build(
        self,
        *,
        output_dir: str | Path,
        service_name: str = "dpone",
        dashboard_title: str = "dpone runtime",
        alert_thresholds: Mapping[str, Mapping[str, float]] | None = None,
    ) -> ObservabilityPackReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        dashboard_path = directory / "grafana_dashboard.json"
        alerts_path = directory / "prometheus_alerts.yml"
        json_path = directory / "observability_pack.json"
        markdown_path = directory / "observability_pack.md"
        dashboard_path.write_text(
            json.dumps(_dashboard(dashboard_title), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        alerts_path.write_text(_alerts(service_name, alert_thresholds or {}), encoding="utf-8")
        report = ObservabilityPackReport(
            passed=True,
            output_dir=str(directory),
            service_name=service_name,
            grafana_dashboard_path=str(dashboard_path),
            prometheus_alerts_path=str(alerts_path),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def _dashboard(title: str) -> dict[str, object]:
    return {
        "title": title,
        "schemaVersion": 39,
        "timezone": "utc",
        "refresh": "30s",
        "tags": ["dpone", "elt", "runtime"],
        "panels": [
            _panel(1, "Rows processed", "sum(rate(dpone_extracted_rows[5m]))"),
            _panel(2, "Runtime seconds", "avg(dpone_duration_seconds)"),
            _panel(3, "Errors and warnings", "sum(dpone_error_count) + sum(dpone_warning_count)"),
            _panel(4, "State and CDC lag", "max(dpone_freshness_lag_seconds)"),
        ],
    }


def _panel(panel_id: int, title: str, expr: str) -> dict[str, object]:
    return {
        "id": panel_id,
        "title": title,
        "type": "timeseries",
        "targets": [{"expr": expr, "legendFormat": title}],
        "gridPos": {"h": 8, "w": 12, "x": 0 if panel_id % 2 else 12, "y": ((panel_id - 1) // 2) * 8},
    }


def _alerts(service_name: str, thresholds: Mapping[str, Mapping[str, float]]) -> str:
    rules: list[str] = [
        "groups:",
        f"  - name: {service_name}-dpone-runtime",
        "    rules:",
        "      - alert: DponeRunFailed",
        "        expr: dpone_run_passed == 0",
        "        for: 1m",
        "        labels:",
        "          severity: critical",
        "        annotations:",
        "          summary: dpone run failed",
    ]
    for metric, rule in sorted(thresholds.items()):
        if "max" in rule:
            expr = f"{metric} > {float(rule['max'])}"
        elif "min" in rule:
            expr = f"{metric} < {float(rule['min'])}"
        else:
            continue
        alert_name = "".join(part.title() for part in metric.split("_"))
        rules.extend(
            [
                f"      - alert: {alert_name}Threshold",
                f"        expr: {expr}",
                "        for: 5m",
                "        labels:",
                "          severity: warning",
                "        annotations:",
                f"          summary: {metric} breached configured threshold",
            ]
        )
    return "\n".join(rules) + "\n"
