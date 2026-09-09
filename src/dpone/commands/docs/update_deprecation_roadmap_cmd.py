from __future__ import annotations

import argparse
import logging

from ...services.docs.update_deprecation_roadmap_service import UpdateDeprecationRoadmapService
from ..context import DocsCommandContext


def cmd_docs_update_deprecation_roadmap(
    args: argparse.Namespace,
    *,
    ctx: DocsCommandContext,
    logger: logging.Logger,
) -> int:
    svc = UpdateDeprecationRoadmapService(ctx=ctx)
    return int(svc.run(args))


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "update-deprecation-roadmap",
        help="Update docs/deprecation-roadmap.md from the compatibility registry or check it in CI",
    )
    p.add_argument("--check", action="store_true", help="Only check that the roadmap doc is up-to-date.")
    p.add_argument("--registry", default="docs/compatibility_registry.yaml", help="Compatibility registry yaml file")
    p.add_argument("--doc", default="docs/deprecation-roadmap.md", help="Target markdown file")
    p.add_argument("--package", default="src/dpone", help="Package root to scan for internal imports")
    p.add_argument(
        "--top-sources",
        type=int,
        default=3,
        help="How many canonical source modules to show per blocked entry",
    )
    return p
