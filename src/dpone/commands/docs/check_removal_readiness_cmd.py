from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

from ...services.docs.check_removal_readiness_service import CheckRemovalReadinessService
from ..context import DocsCommandContext


def cmd_docs_check_removal_readiness(
    args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger
) -> int:
    svc = CheckRemovalReadinessService(ctx=ctx)
    exit_code, payload = svc.run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "check-removal-readiness",
        help="Validate that a planned shim-removal batch is internally ready and docs are in sync",
    )
    p.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    p.add_argument("--registry", default="docs/compatibility_registry.yaml", help="Compatibility registry path")
    p.add_argument("--doc", default="docs/shim-removal-plan.md", help="Shim removal plan markdown path")
    p.add_argument("--package", default="src/dpone", help="Package directory (default: src/dpone)")
    p.add_argument(
        "--batch",
        default="cleanup-release-1",
        help="Removal batch to validate (default: cleanup-release-1)",
    )
    p.add_argument("--write-doc", action="store_true", help="Update docs/shim-removal-plan.md before validation")
    return p
