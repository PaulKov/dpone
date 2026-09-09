from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from .compatibility_registry import load_registry
from .context import DocsServiceContext
from .deprecation_roadmap import (
    analyze_deprecation_roadmap,
    is_deprecation_roadmap_doc_in_sync,
    render_deprecation_roadmap_block,
    sync_deprecation_roadmap_doc,
)


class UpdateDeprecationRoadmapService:
    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> int:
        registry_path = self._resolve_path(getattr(args, "registry", "docs/compatibility_registry.yaml"))
        doc_path = self._resolve_path(getattr(args, "doc", "docs/deprecation-roadmap.md"))
        package_dir = self._resolve_path(getattr(args, "package", "src/dpone"))
        top_sources = int(getattr(args, "top_sources", 3) or 3)
        entries = load_registry(registry_path, yaml_codec=self.ctx.yaml)
        rows = analyze_deprecation_roadmap(entries, package_dir=package_dir, top_sources=top_sources)
        rendered = render_deprecation_roadmap_block(rows)
        if bool(getattr(args, "check", False)):
            if is_deprecation_roadmap_doc_in_sync(doc_path, rendered_block=rendered):
                self.log.info("Deprecation roadmap is up-to-date: %s", doc_path)
                return 0
            self.log.error(
                "Deprecation roadmap is outdated (%s). Run: dpone docs update-deprecation-roadmap",
                doc_path,
            )
            return 2
        changed, _ = sync_deprecation_roadmap_doc(doc_path, rendered_block=rendered)
        if changed:
            self.log.info("Updated deprecation roadmap: %s", doc_path)
        else:
            self.log.info("Deprecation roadmap already in sync: %s", doc_path)
        return 0

    def _resolve_path(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Path argument must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        return path
