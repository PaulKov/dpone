from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from .compatibility_registry import load_registry
from .context import DocsServiceContext
from .shim_removal_plan import (
    DEFAULT_BATCH,
    ShimRemovalReadinessReport,
    analyze_shim_removal_plan,
    format_shim_removal_readiness_jsonable,
    format_shim_removal_readiness_text,
    is_shim_removal_plan_doc_in_sync,
    render_shim_removal_plan_block,
    sync_shim_removal_plan_doc,
)


class CheckRemovalReadinessService:
    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> tuple[int, str | dict]:
        fmt = str(getattr(args, "format", "text") or "text").strip().lower()
        registry_path = self._resolve_path(getattr(args, "registry", "docs/compatibility_registry.yaml"))
        doc_path = self._resolve_path(getattr(args, "doc", "docs/shim-removal-plan.md"))
        package_dir = self._resolve_path(getattr(args, "package", "src/dpone"))
        batch = str(getattr(args, "batch", DEFAULT_BATCH) or DEFAULT_BATCH).strip()
        if not batch:
            raise DocsConfigurationError("Removal batch must not be empty")

        entries = load_registry(registry_path, yaml_codec=self.ctx.yaml)
        rows = analyze_shim_removal_plan(entries, package_dir=package_dir, batch=batch)
        rendered = render_shim_removal_plan_block(rows, batch=batch)

        if bool(getattr(args, "write_doc", False)):
            changed, _ = sync_shim_removal_plan_doc(doc_path, rendered_block=rendered)
            if changed:
                self.log.info("Updated shim removal plan: %s", doc_path)
            else:
                self.log.info("Shim removal plan already in sync: %s", doc_path)

        doc_synced = is_shim_removal_plan_doc_in_sync(doc_path, rendered_block=rendered)
        report = ShimRemovalReadinessReport(
            batch=batch,
            registry_path=registry_path.as_posix(),
            doc_path=doc_path.as_posix(),
            selected_count=len(rows),
            ready_count=sum(1 for row in rows if row.internal_ready),
            blocked_count=sum(1 for row in rows if not row.internal_ready),
            doc_synced=doc_synced,
            rows=rows,
        )

        if fmt == "json":
            payload: str | dict = format_shim_removal_readiness_jsonable(report)
        elif fmt == "text":
            payload = format_shim_removal_readiness_text(report)
        else:
            raise DocsConfigurationError("--format must be text or json")

        exit_code = 0 if report.ok else 2
        if exit_code:
            self.log.error("Shim removal readiness check failed")
        else:
            self.log.info("Shim removal readiness OK")
        return exit_code, payload

    def _resolve_path(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Path argument must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        return path
