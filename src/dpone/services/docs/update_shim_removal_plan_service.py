from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from .compatibility_registry import load_registry
from .context import DocsServiceContext
from .shim_removal_plan import (
    DEFAULT_BATCH,
    analyze_shim_removal_plan,
    is_shim_removal_plan_doc_in_sync,
    render_shim_removal_plan_block,
    sync_shim_removal_plan_doc,
)


class UpdateShimRemovalPlanService:
    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> int:
        registry_path = self._resolve_path(getattr(args, "registry", "docs/compatibility_registry.yaml"))
        doc_path = self._resolve_path(getattr(args, "doc", "docs/shim-removal-plan.md"))
        package_dir = self._resolve_path(getattr(args, "package", "src/dpone"))
        batch = str(getattr(args, "batch", DEFAULT_BATCH) or DEFAULT_BATCH).strip()
        if not batch:
            raise DocsConfigurationError("Removal batch must not be empty")
        entries = load_registry(registry_path, yaml_codec=self.ctx.yaml)
        rows = analyze_shim_removal_plan(entries, package_dir=package_dir, batch=batch)
        if not rows:
            raise DocsConfigurationError(f"No compatibility entries matched removal batch: {batch}")
        rendered = render_shim_removal_plan_block(rows, batch=batch)
        if bool(getattr(args, "check", False)):
            if is_shim_removal_plan_doc_in_sync(doc_path, rendered_block=rendered):
                self.log.info("Shim removal plan is up-to-date: %s", doc_path)
                return 0
            self.log.error(
                "Shim removal plan is outdated (%s). Run: dpone docs update-shim-removal-plan --batch %s",
                doc_path,
                batch,
            )
            return 2
        changed, _ = sync_shim_removal_plan_doc(doc_path, rendered_block=rendered)
        if changed:
            self.log.info("Updated shim removal plan: %s", doc_path)
        else:
            self.log.info("Shim removal plan already in sync: %s", doc_path)
        return 0

    def _resolve_path(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Path argument must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        return path
