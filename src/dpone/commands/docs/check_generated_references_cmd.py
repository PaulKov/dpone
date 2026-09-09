from __future__ import annotations

import argparse
import logging
from importlib import import_module

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

from ..context import DocsCommandContext


def cmd_docs_check_generated_references(
    args: argparse.Namespace,
    *,
    ctx: DocsCommandContext,
    logger: logging.Logger,
) -> int:
    del logger
    service = _build_service(ctx)
    exit_code, payload = service.run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "check-generated-references",
        help="Check generated documentation references without rewriting files",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument(
        "--cli-doc",
        default="docs/cli-reference.md",
        help="CLI reference markdown file (default: docs/cli-reference.md)",
    )
    parser.add_argument(
        "--gitops-schema-doc",
        default="docs/reference/gitops-schema-catalog.md",
        help="GitOps schema catalog markdown file (default: docs/reference/gitops-schema-catalog.md)",
    )
    parser.add_argument(
        "--manifest-schema-doc",
        default="docs/reference/manifest-schemas.md",
        help="Manifest schema reference markdown file (default: docs/reference/manifest-schemas.md)",
    )
    parser.add_argument(
        "--manifest-schema-root",
        default="src/dpone/schema",
        help="Canonical manifest schema directory (default: src/dpone/schema)",
    )
    return parser


def _build_service(ctx: DocsCommandContext):
    module = import_module("dpone.services.docs.check_generated_references_service")
    return module.CheckGeneratedReferencesService(ctx=ctx)


__all__ = [
    "cmd_docs_check_generated_references",
    "register_parser",
]
