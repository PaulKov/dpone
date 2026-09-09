from __future__ import annotations

import argparse
import logging

from ...services.docs.update_gitops_schema_reference_service import UpdateGitOpsSchemaReferenceService
from ..context import DocsCommandContext


def cmd_docs_update_gitops_schema_reference(
    args: argparse.Namespace,
    *,
    ctx: DocsCommandContext,
    logger: logging.Logger,
) -> int:
    del logger
    svc = UpdateGitOpsSchemaReferenceService(ctx=ctx)
    return int(svc.run(args))


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "update-gitops-schema-reference",
        help="Update docs/reference/gitops-schema-catalog.md from the registered GitOps schemas or check it in CI",
    )
    parser.add_argument("--check", action="store_true", help="Only check that the schema reference is up-to-date.")
    parser.add_argument(
        "--doc",
        default="docs/reference/gitops-schema-catalog.md",
        help="Target markdown file (default: docs/reference/gitops-schema-catalog.md)",
    )
    return parser


__all__ = [
    "cmd_docs_update_gitops_schema_reference",
    "register_parser",
]
