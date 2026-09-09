from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from .cli_reference import is_cli_reference_doc_in_sync, sync_cli_reference_doc
from .context import DocsServiceContext


class UpdateCliReferenceService:
    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> int:
        doc_path = self._resolve_path(getattr(args, "doc", "docs/cli-reference.md"))
        parser = self._build_parser()
        if bool(getattr(args, "check", False)):
            if is_cli_reference_doc_in_sync(doc_path, parser=parser):
                self.log.info("CLI reference is up-to-date: %s", doc_path)
                return 0
            self.log.error("CLI reference is outdated (%s). Run: dpone docs update-cli-reference", doc_path)
            return 2
        changed, _ = sync_cli_reference_doc(doc_path, parser=parser)
        if changed:
            self.log.info("Updated CLI reference: %s", doc_path)
        else:
            self.log.info("CLI reference already in sync: %s", doc_path)
        return 0

    def _build_parser(self):
        from ...app.cli_reference_source import build_root_parser

        return build_root_parser()

    def _resolve_path(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Doc path must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        return path
