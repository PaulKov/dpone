from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.ops.facades import CertificationAutomationPlanService


def cmd_certification_automation_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = CertificationAutomationPlanService().build(
        output_dir=args.output_dir,
        profile=args.profile,
        row_count=args.row_count,
    )
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return 0 if report.passed else 1


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "certification-automation-plan",
        help="Render the full source/sink certification automation plan and required artifacts",
    )
    parser.add_argument("--output-dir", default=".dpone/certification-automation-plan/latest")
    parser.add_argument("--profile", choices=["mock_contract", "mock_local", "vendor_live"], default="mock_contract")
    parser.add_argument("--row-count", type=int, default=10000)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
