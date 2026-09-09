from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.ops.facades import CertificationSuiteService


def cmd_certification_suite(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = CertificationSuiteService().evaluate(
        output_dir=args.output_dir,
        suite_id=args.suite_id,
        release_id=args.release_id,
        certification_report_path=args.certification_report,
        benchmark_baseline_path=args.benchmark_baseline,
        lineage_report_path=args.lineage_report,
        dbt_lineage_report_path=args.dbt_lineage_report,
        evidence_bundle_path=args.evidence_bundle,
        strategy_certification_bundle_path=args.strategy_certification_bundle,
        require_benchmark=args.require_benchmark,
        require_lineage=args.require_lineage,
        require_dbt_lineage=args.require_dbt_lineage,
        require_evidence=args.require_evidence,
        require_strategy_certification=args.require_strategy_certification,
    )
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return 0 if report.passed else 1


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "certification-suite",
        help="Aggregate matrix, benchmark, lineage, dbt, strategy, and evidence artifacts into one certification gate",
    )
    parser.add_argument("--output-dir", default=".dpone/certification-suite/latest")
    parser.add_argument("--suite-id", required=True)
    parser.add_argument("--release-id")
    parser.add_argument("--certification-report", required=True)
    parser.add_argument("--benchmark-baseline")
    parser.add_argument("--lineage-report")
    parser.add_argument("--dbt-lineage-report")
    parser.add_argument("--evidence-bundle")
    parser.add_argument("--strategy-certification-bundle")
    parser.add_argument("--require-benchmark", action="store_true")
    parser.add_argument("--require-lineage", action="store_true")
    parser.add_argument("--require-dbt-lineage", action="store_true")
    parser.add_argument("--require-evidence", action="store_true")
    parser.add_argument("--require-strategy-certification", action="store_true")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
