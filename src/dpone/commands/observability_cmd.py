from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.observability.export import RuntimeMetricsExportService


def cmd_metrics_export(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = RuntimeMetricsExportService().export(
        output_dir=args.output_dir,
        run_report_path=args.run_report,
        metrics=dict(args.metric),
        labels=dict(args.label),
        resource_attributes=dict(args.resource_attr),
        service_name=args.service_name,
        namespace=args.namespace,
        airflow_evidence_bundle_path=getattr(args, "airflow_evidence_bundle", None),
    )
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return 0 if report.passed else 1


def register_metrics_export_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "metrics-export",
        help="Export dpone run metrics as Prometheus text and OpenTelemetry-compatible JSON",
    )
    parser.add_argument("--output-dir", default=".dpone/observability/latest")
    parser.add_argument("--run-report", help="Path to JSON output from `dpone run --format json`")
    parser.add_argument(
        "--metric",
        action="append",
        default=[],
        type=_metric_pair,
        help="Custom numeric metric as name=value; can be repeated",
    )
    parser.add_argument(
        "--label",
        action="append",
        default=[],
        type=_label_pair,
        help="Metric label as key=value; can be repeated",
    )
    parser.add_argument(
        "--resource-attr",
        action="append",
        default=[],
        type=_label_pair,
        help="OpenTelemetry resource attribute as key=value; can be repeated",
    )
    parser.add_argument("--service-name", default="dpone")
    parser.add_argument("--namespace", default="dpone.local")
    parser.add_argument(
        "--airflow-evidence-bundle",
        help="Optional final Airflow evidence bundle used for OpenTelemetry correlation",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def _metric_pair(value: str) -> tuple[str, float]:
    key, raw = _split_pair(value)
    try:
        return key, float(raw)
    except ValueError as exc:
        msg = f"metric `{key}` must be numeric"
        raise argparse.ArgumentTypeError(msg) from exc


def _label_pair(value: str) -> tuple[str, str]:
    return _split_pair(value)


def _split_pair(value: str) -> tuple[str, str]:
    if "=" not in value:
        msg = f"expected key=value, got `{value}`"
        raise argparse.ArgumentTypeError(msg)
    key, raw = value.split("=", 1)
    key = key.strip()
    if not key:
        msg = f"expected non-empty key in `{value}`"
        raise argparse.ArgumentTypeError(msg)
    return key, raw.strip()
