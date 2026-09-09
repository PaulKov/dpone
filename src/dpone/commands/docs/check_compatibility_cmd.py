from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

from ...services.docs.check_compatibility_service import CheckCompatibilityService
from ..context import DocsCommandContext


def cmd_docs_check_compatibility(args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger) -> int:
    svc = CheckCompatibilityService(ctx=ctx)
    exit_code, payload = svc.run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "check-compatibility",
        help="Validate compatibility/deprecation registry and sync docs block (CI-friendly)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format: text (default) or json",
    )
    p.add_argument(
        "--registry",
        default="docs/compatibility_registry.yaml",
        help="Compatibility registry path relative to repo root",
    )
    p.add_argument(
        "--doc",
        default="docs/compatibility.md",
        help="Compatibility markdown page to validate/update",
    )
    p.add_argument(
        "--package",
        default="src/dpone",
        help="Package directory relative to repo root (default: src/dpone)",
    )
    p.add_argument(
        "--write-doc",
        action="store_true",
        help="Update generated compatibility matrix block inside docs/compatibility.md",
    )
    return p
