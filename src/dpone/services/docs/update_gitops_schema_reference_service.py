from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from .context import DocsServiceContext
from .gitops_schema_reference import (
    is_gitops_schema_reference_doc_in_sync,
    sync_gitops_schema_reference_doc,
)


class UpdateGitOpsSchemaReferenceService:
    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> int:
        doc_path = self._resolve_path(getattr(args, "doc", "docs/reference/gitops-schema-catalog.md"))
        if bool(getattr(args, "check", False)):
            if is_gitops_schema_reference_doc_in_sync(doc_path):
                self.log.info("GitOps schema reference is up-to-date: %s", doc_path)
                return 0
            self.log.error(
                "GitOps schema reference is outdated (%s). Run: dpone docs update-gitops-schema-reference",
                doc_path,
            )
            return 2
        changed, _ = sync_gitops_schema_reference_doc(doc_path)
        if changed:
            self.log.info("Updated GitOps schema reference: %s", doc_path)
        else:
            self.log.info("GitOps schema reference already in sync: %s", doc_path)
        return 0

    def _resolve_path(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Doc path must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        return path


__all__ = ["UpdateGitOpsSchemaReferenceService"]
