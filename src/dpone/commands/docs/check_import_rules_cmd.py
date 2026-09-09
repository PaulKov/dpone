from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

from ...services.docs.check_import_rules_service import CheckImportRulesService
from ..context import DocsCommandContext


def cmd_docs_check_import_rules(args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger) -> int:
    svc = CheckImportRulesService(ctx=ctx)
    exit_code, payload = svc.run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "check-import-rules",
        help="Check architectural import rules for dpone.* layers (CI-friendly)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format: text (default) or json",
    )
    p.add_argument(
        "--package",
        default="src/dpone",
        help="Package directory relative to repo root (default: src/dpone)",
    )
    return p
