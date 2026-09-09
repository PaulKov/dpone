from __future__ import annotations

import argparse
import logging

from ...services.docs.update_shim_removal_plan_service import UpdateShimRemovalPlanService
from ..context import DocsCommandContext


def cmd_docs_update_shim_removal_plan(
    args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger
) -> int:
    svc = UpdateShimRemovalPlanService(ctx=ctx)
    return int(svc.run(args))


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "update-shim-removal-plan",
        help="Update docs/shim-removal-plan.md from compatibility registry and internal import scan or check it in CI",
    )
    p.add_argument("--check", action="store_true", help="Only check that the shim removal plan is up-to-date.")
    p.add_argument("--registry", default="docs/compatibility_registry.yaml", help="Compatibility registry path")
    p.add_argument("--doc", default="docs/shim-removal-plan.md", help="Target markdown file")
    p.add_argument("--package", default="src/dpone", help="Package directory (default: src/dpone)")
    p.add_argument(
        "--batch",
        default="cleanup-release-1",
        help="Removal batch to render (default: cleanup-release-1)",
    )
    return p
