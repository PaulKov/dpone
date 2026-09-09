from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

from ...services.docs.check_docs_service import CheckDocsService
from ..context import DocsCommandContext


def cmd_docs_check_docs(args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger) -> int:
    svc = CheckDocsService(ctx=ctx)
    exit_code, payload = svc.run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "check-docs",
        help="Check markdown links/anchors in README.md and docs/ (CI-friendly)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format: text (default) or json",
    )
    p.add_argument(
        "--docs-dir",
        default="docs",
        help="Docs directory relative to repo root (default: docs)",
    )
    p.add_argument(
        "--no-root-readme",
        dest="include_root_readme",
        action="store_false",
        help="Do not include top-level README.md in docs checks",
    )
    p.set_defaults(include_root_readme=True)
    return p
