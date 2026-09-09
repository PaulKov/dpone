from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands.registry import get_commands
from dpone.observability.export import RuntimeMetricsExportService
from dpone.observability.metrics import MetricPoint, RuntimeMetricsExtractor
from dpone.observability.prometheus import PrometheusTextRenderer


def _write_airflow_correlation_bundle(path: Path) -> str:
    from dpone.contracts.airflow_correlation import build_airflow_correlation
    from dpone.contracts.airflow_run_identity import AirflowRunIdentity

    def digest(char: str) -> str:
        return "sha256:" + char * 64

    identity = AirflowRunIdentity.from_mapping(
        {
            "schema": "dpone.airflow-run-identity.v1",
            "release_id": digest("a"),
            "deployment_id": digest("b"),
            "dag_spec": {"id": "orders_daily", "sha256": digest("c")},
            "workload_pack": {"id": "load_orders", "sha256": digest("d")},
            "runtime_image_digest": digest("e"),
            "binding_set_ref": None,
            "connection_registry_ref": None,
            "credential_runtime_ref": None,
            "airflow_bundle": None,
        }
    )
    correlation = build_airflow_correlation(
        run_identity=identity,
        attempt={
            "dag_id": "orders_daily",
            "task_id": "load_orders",
            "run_id": "scheduled__2026-07-17",
            "try_number": 1,
            "map_index": -1,
        },
        dpone_run_id="run-1",
        dpone_process="orders",
        runtime_evidence_sha256=digest("f"),
        pod={
            "name": "load-orders-x7f9",
            "uid": "pod-uid-123",
            "namespace": "airflow-example",
            "image_digest": digest("e"),
        },
    )
    path.write_text(
        json.dumps({"kind": "gitops.airflow_evidence_bundle", "correlation": correlation.to_dict()}),
        encoding="utf-8",
    )
    return correlation.correlation_id


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_run_report(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "manifest": "examples/postgres-to-mssql.yml",
                "process": "orders_to_mssql",
                "selector": None,
                "run_id": "01JZDPONERUN00000000000000",
                "passed": True,
                "attempts": 2,
                "max_attempts": 3,
                "result": {
                    "status": "success",
                    "extracted_rows": 10_000,
                    "inserted_rows": 2_000,
                    "updated_rows": 500,
                    "final_rows": 9_500,
                    "duration_seconds": 12.5,
                    "run_throughput": {
                        "schema_version": "dpone.runtime.throughput.v1",
                        "duration_seconds": 12.5,
                        "row_count": 10_000,
                        "row_count_source": "loaded_rows",
                        "rows_per_second": 800.0,
                        "confidence": "measured",
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def _write_failed_run_report(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "manifest": "examples/postgres-to-clickhouse.yml",
                "process": "orders_to_clickhouse",
                "selector": "daily",
                "run_id": "01JZDPONEFAILED0000000000",
                "passed": False,
                "attempts": 3,
                "max_attempts": 4,
                "retry_backoff_seconds": 7.5,
                "warnings": ["slow.finalization", "retry.backoff"],
                "result": {
                    "status": "failed",
                    "extracted_rows": 10_000,
                    "inserted_rows": 8_000,
                    "updated_rows": 250,
                    "final_rows": 8_250,
                    "duration_seconds": 40.0,
                    "throughput_rows_per_second": 250.0,
                    "errors": ["target.timeout", "target.retry_exhausted"],
                },
            }
        ),
        encoding="utf-8",
    )


def test_observability_command_group_is_registered() -> None:
    assert "observability" in {command.name for command in get_commands()}


def test_runtime_metrics_export_service_writes_prometheus_and_otel_artifacts(tmp_path: Path) -> None:
    run_report = tmp_path / "run_report.json"
    _write_run_report(run_report)

    report = RuntimeMetricsExportService().export(
        output_dir=tmp_path / "observability",
        run_report_path=run_report,
        labels={"env": "local"},
        service_name="dpone-ci",
        namespace="dpone.tests",
    )

    assert report.passed is True
    assert report.metric_count == 9
    assert Path(report.metrics_index_path).is_file()
    prometheus = Path(report.prometheus_path).read_text(encoding="utf-8")
    assert "# HELP dpone_extracted_rows Rows extracted by the dpone run." in prometheus
    assert 'dpone_extracted_rows{env="local",process="orders_to_mssql"' in prometheus
    assert "dpone_throughput_rows_per_second" in prometheus

    otel = json.loads(Path(report.opentelemetry_path).read_text(encoding="utf-8"))
    metrics = otel["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
    assert {item["name"] for item in metrics} >= {
        "dpone_extracted_rows",
        "dpone_run_passed",
        "dpone_throughput_rows_per_second",
    }
    attributes = otel["resourceMetrics"][0]["resource"]["attributes"]
    assert {"key": "service.name", "value": {"stringValue": "dpone-ci"}} in attributes

    index = json.loads(Path(report.metrics_index_path).read_text(encoding="utf-8"))
    assert {item["name"] for item in index["artifacts"]} == {
        "prometheus_metrics.prom",
        "opentelemetry_metrics.json",
        "runtime_metrics.json",
        "runtime_metrics.md",
    }
    assert all(len(item["sha256"]) == 64 for item in index["artifacts"])


def test_runtime_metrics_export_projects_airflow_correlation_only_to_otel(tmp_path: Path) -> None:
    run_report = tmp_path / "run_report.json"
    evidence_bundle = tmp_path / "airflow-evidence-bundle.json"
    _write_run_report(run_report)
    correlation_id = _write_airflow_correlation_bundle(evidence_bundle)

    report = RuntimeMetricsExportService().export(
        output_dir=tmp_path / "observability",
        run_report_path=run_report,
        airflow_evidence_bundle_path=evidence_bundle,
    )

    assert report.passed
    assert report.correlation_id == correlation_id
    prometheus = Path(report.prometheus_path).read_text(encoding="utf-8")
    assert correlation_id not in prometheus
    otel = json.loads(Path(report.opentelemetry_path).read_text(encoding="utf-8"))
    resource_attributes = {
        item["key"]: item["value"]["stringValue"] for item in otel["resourceMetrics"][0]["resource"]["attributes"]
    }
    assert resource_attributes["dpone.release.id"] == "sha256:" + "a" * 64
    assert resource_attributes["dpone.deployment.id"] == "sha256:" + "b" * 64
    assert resource_attributes["k8s.pod.uid"] == "pod-uid-123"
    point_attributes = {
        item["key"]: item["value"]["stringValue"]
        for item in otel["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["gauge"]["dataPoints"][0]["attributes"]
    }
    assert point_attributes["dpone.correlation.id"] == correlation_id
    assert point_attributes["airflow.run.id"] == "scheduled__2026-07-17"
    assert point_attributes["dpone.run.id"] == "run-1"
    assert "trace_id" not in json.dumps(otel)


def test_runtime_metrics_export_blocks_incomplete_airflow_correlation(tmp_path: Path) -> None:
    run_report = tmp_path / "run_report.json"
    bundle = tmp_path / "airflow-evidence-bundle.json"
    _write_run_report(run_report)
    bundle.write_text(json.dumps({"kind": "gitops.airflow_evidence_bundle", "correlation": {}}), encoding="utf-8")

    report = RuntimeMetricsExportService().export(
        output_dir=tmp_path / "observability",
        run_report_path=run_report,
        airflow_evidence_bundle_path=bundle,
    )

    assert not report.passed
    assert "airflow_correlation.invalid" in report.blockers


def test_runtime_metrics_export_rejects_symlinked_and_oversized_airflow_evidence(tmp_path: Path) -> None:
    run_report = tmp_path / "run_report.json"
    real_bundle = tmp_path / "real-bundle.json"
    linked_bundle = tmp_path / "linked-bundle.json"
    oversized_bundle = tmp_path / "oversized-bundle.json"
    _write_run_report(run_report)
    _write_airflow_correlation_bundle(real_bundle)
    linked_bundle.symlink_to(real_bundle)
    oversized_bundle.write_text("x" * (2 * 1024 * 1024 + 1), encoding="utf-8")

    linked = RuntimeMetricsExportService().export(
        output_dir=tmp_path / "linked-observability",
        run_report_path=run_report,
        airflow_evidence_bundle_path=linked_bundle,
    )
    oversized = RuntimeMetricsExportService().export(
        output_dir=tmp_path / "oversized-observability",
        run_report_path=run_report,
        airflow_evidence_bundle_path=oversized_bundle,
    )

    assert "airflow_correlation.symlink_forbidden" in linked.blockers
    assert "airflow_correlation.too_large" in oversized.blockers


def test_runtime_metrics_export_service_covers_failure_retry_and_warning_signals(tmp_path: Path) -> None:
    run_report = tmp_path / "failed_run_report.json"
    _write_failed_run_report(run_report)

    report = RuntimeMetricsExportService().export(
        output_dir=tmp_path / "observability",
        run_report_path=run_report,
        labels={"env": "ci", "source.system": "postgres"},
        resource_attributes={"deployment.environment": "ci", "service.version": "0.2.8"},
    )

    assert report.passed is True
    prometheus = Path(report.prometheus_path).read_text(encoding="utf-8")
    assert "dpone_error_count" in prometheus
    assert "dpone_warning_count" in prometheus
    assert "dpone_retry_backoff_seconds" in prometheus
    assert "dpone_throughput_rows_per_second" in prometheus
    assert 'source_system="postgres"' in prometheus

    runtime_report = json.loads(Path(report.json_report_path).read_text(encoding="utf-8"))
    metrics = {item["name"]: item["value"] for item in runtime_report["metrics"]}
    assert metrics["dpone_error_count"] == 2.0
    assert metrics["dpone_warning_count"] == 2.0
    assert metrics["dpone_retry_backoff_seconds"] == 7.5
    assert metrics["dpone_run_passed"] == 0.0

    otel = json.loads(Path(report.opentelemetry_path).read_text(encoding="utf-8"))
    attributes = otel["resourceMetrics"][0]["resource"]["attributes"]
    assert {"key": "deployment.environment", "value": {"stringValue": "ci"}} in attributes
    assert {"key": "service.version", "value": {"stringValue": "0.2.8"}} in attributes


def test_runtime_metrics_extractor_reads_nested_run_throughput() -> None:
    points = RuntimeMetricsExtractor().extract(
        run_report={
            "process": "orders",
            "result": {
                "status": "success",
                "duration_seconds": 2,
                "run_throughput": {
                    "schema_version": "dpone.runtime.throughput.v1",
                    "rows_per_second": 40,
                },
            },
        }
    )

    values = {point.name: point.value for point in points}
    assert values["dpone_throughput_rows_per_second"] == 40.0


def test_runtime_metrics_extractor_reads_process_result_details_throughput() -> None:
    report = {
        "passed": True,
        "result": {
            "status": "success",
            "details": {
                "run_throughput": {
                    "schema_version": "dpone.runtime.throughput.v1",
                    "rows_per_second": 55.5,
                }
            },
        },
    }

    metrics = RuntimeMetricsExtractor().extract(run_report=report)
    values = {metric.name: metric.value for metric in metrics}

    assert values["dpone_throughput_rows_per_second"] == 55.5


def test_runtime_metrics_export_cli_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch)
    run_report = tmp_path / "run_report.json"
    _write_run_report(run_report)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "observability",
                "metrics-export",
                "--output-dir",
                str(tmp_path / "obs"),
                "--run-report",
                str(run_report),
                "--metric",
                "lag_seconds=42",
                "--label",
                "env=ci",
                "--resource-attr",
                "deployment.environment=ci",
                "--service-name",
                "dpone-tests",
                "--namespace",
                "dpone.local-tests",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["metric_count"] == 10
    assert Path(payload["prometheus_path"]).is_file()
    assert Path(payload["opentelemetry_path"]).is_file()
    assert Path(payload["metrics_index_path"]).is_file()
    otel = json.loads(Path(payload["opentelemetry_path"]).read_text(encoding="utf-8"))
    assert {"key": "deployment.environment", "value": {"stringValue": "ci"}} in otel["resourceMetrics"][0]["resource"][
        "attributes"
    ]


def test_prometheus_renderer_escapes_label_values() -> None:
    rendered = PrometheusTextRenderer().render(
        [
            MetricPoint(
                name="dpone_test_metric",
                value=1.0,
                labels={"quoted": 'a"b', "multi": "line\nvalue"},
                description="test metric",
            )
        ]
    )

    assert 'multi="line\\nvalue"' in rendered
    assert 'quoted="a\\"b"' in rendered


def test_prometheus_renderer_sanitizes_label_names() -> None:
    rendered = PrometheusTextRenderer().render(
        [
            MetricPoint(
                name="dpone_test_metric",
                value=1.0,
                labels={"ci.run/id": "123", "9bad": "ok"},
                description="test metric",
            )
        ]
    )

    assert 'ci_run_id="123"' in rendered
    assert '_9bad="ok"' in rendered
