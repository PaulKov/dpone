from __future__ import annotations

import argparse
import logging

from ...services.docs.update_dev_metrics_service import UpdateDevMetricsService
from ..context import DocsCommandContext


def cmd_docs_update_dev_metrics(args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger) -> int:
    svc = UpdateDevMetricsService(ctx=ctx)
    return int(svc.run(args))


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "update-dev-metrics",
        help=(
            "Update the auto-generated metrics section in docs/quality-metrics.md (LOC, coupling/cohesion) "
            "or check it for CI"
        ),
    )
    p.add_argument(
        "--check",
        action="store_true",
        help="Only check that docs are up-to-date (do not rewrite). Exit!=0 if differs.",
    )
    p.add_argument(
        "--target",
        choices=["all", "md", "quality"],
        default="all",
        help="Which docs to update/check: all (default) | md | quality",
    )
    p.add_argument(
        "--docs-dir",
        default="docs",
        help="Docs dir in the repository (default: docs)",
    )
    p.add_argument(
        "--top",
        type=int,
        default=15,
        help="How many largest modules to show (default: 15)",
    )
    return p
