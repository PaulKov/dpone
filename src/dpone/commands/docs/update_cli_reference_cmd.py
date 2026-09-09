from __future__ import annotations

import argparse
import logging

from ...services.docs.update_cli_reference_service import UpdateCliReferenceService
from ..context import DocsCommandContext


def cmd_docs_update_cli_reference(args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger) -> int:
    svc = UpdateCliReferenceService(ctx=ctx)
    return int(svc.run(args))


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "update-cli-reference",
        help="Update docs/cli-reference.md from the current argparse command tree or check it in CI",
    )
    p.add_argument("--check", action="store_true", help="Only check that CLI reference is up-to-date.")
    p.add_argument(
        "--doc",
        default="docs/cli-reference.md",
        help="Target markdown file (default: docs/cli-reference.md)",
    )
    return p
