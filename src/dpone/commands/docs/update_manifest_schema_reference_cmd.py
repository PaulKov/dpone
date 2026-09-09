from __future__ import annotations

import argparse
import logging

from ...services.docs.update_manifest_schema_reference_service import UpdateManifestSchemaReferenceService
from ..context import DocsCommandContext


def cmd_docs_update_manifest_schema_reference(
    args: argparse.Namespace,
    *,
    ctx: DocsCommandContext,
    logger: logging.Logger,
) -> int:
    del logger
    return int(UpdateManifestSchemaReferenceService(ctx=ctx).run(args))


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "update-manifest-schema-reference",
        help="Update the public manifest schema reference from canonical JSON Schemas or check it in CI",
    )
    parser.add_argument("--check", action="store_true", help="Only check that the reference is up-to-date.")
    parser.add_argument(
        "--doc",
        default="docs/reference/manifest-schemas.md",
        help="Target markdown file (default: docs/reference/manifest-schemas.md)",
    )
    parser.add_argument(
        "--schema-root",
        default="src/dpone/schema",
        help="Canonical schema directory (default: src/dpone/schema)",
    )
    return parser


__all__ = ["cmd_docs_update_manifest_schema_reference", "register_parser"]
