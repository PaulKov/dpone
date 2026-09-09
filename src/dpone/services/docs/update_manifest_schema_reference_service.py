from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from .context import DocsServiceContext
from .manifest_schema_reference import (
    is_manifest_schema_reference_doc_in_sync,
    sync_manifest_schema_reference_doc,
)


class UpdateManifestSchemaReferenceService:
    """Update or verify the generated public manifest schema reference."""

    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> int:
        doc_path = self._resolve_path(getattr(args, "doc", "docs/reference/manifest-schemas.md"))
        schema_root = self._resolve_path(getattr(args, "schema_root", "src/dpone/schema"))
        if bool(getattr(args, "check", False)):
            if is_manifest_schema_reference_doc_in_sync(doc_path, schema_root=schema_root):
                self.log.info("Manifest schema reference is up-to-date: %s", doc_path)
                return 0
            self.log.error(
                "Manifest schema reference is outdated (%s). Run: dpone docs update-manifest-schema-reference",
                doc_path,
            )
            return 2
        changed, _ = sync_manifest_schema_reference_doc(doc_path, schema_root=schema_root)
        message = "Updated" if changed else "Already in sync"
        self.log.info("%s manifest schema reference: %s", message, doc_path)
        return 0

    def _resolve_path(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Doc and schema paths must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        return path


__all__ = ["UpdateManifestSchemaReferenceService"]
