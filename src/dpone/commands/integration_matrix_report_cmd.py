from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.ops.facades import IntegrationMatrixReportService


def cmd_integration_matrix_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = IntegrationMatrixReportService().build(artifact_dir=args.artifact_dir, output_dir=args.output_dir)
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return 0 if report.passed else 1


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "integration-matrix-report",
        help="Aggregate source/sink matrix case artifacts into certification_report.json",
    )
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
