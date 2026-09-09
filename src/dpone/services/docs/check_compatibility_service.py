from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from .compatibility_registry import (
    CompatibilityReport,
    format_compatibility_report_jsonable,
    format_compatibility_report_text,
    is_compatibility_doc_in_sync,
    load_registry,
    render_compatibility_matrix,
    sync_compatibility_doc,
    validate_registry,
)
from .context import DocsServiceContext


class CheckCompatibilityService:
    """Validate compatibility/deprecation policy registry and generated docs block."""

    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> tuple[int, str | dict]:
        fmt = str(getattr(args, "format", "text") or "text").strip().lower()
        registry_path = self._resolve_path(getattr(args, "registry", "docs/compatibility_registry.yaml"))
        doc_path = self._resolve_path(getattr(args, "doc", "docs/compatibility.md"))
        package_dir = self._resolve_path(getattr(args, "package", "src/dpone"))

        if not registry_path.exists():
            raise DocsConfigurationError(f"Compatibility registry not found: {registry_path}")
        if not doc_path.exists():
            raise DocsConfigurationError(f"Compatibility doc not found: {doc_path}")
        if not package_dir.exists() or package_dir.name != "dpone":
            raise DocsConfigurationError(f"Package dir not found or invalid: {package_dir}")

        entries = load_registry(registry_path, yaml_codec=self.ctx.yaml)
        issues = validate_registry(entries, package_dir=package_dir)
        rendered = render_compatibility_matrix(entries)

        if bool(getattr(args, "write_doc", False)):
            changed, _ = sync_compatibility_doc(doc_path, rendered_matrix=rendered)
            if changed:
                self.log.info("Updated compatibility doc: %s", doc_path)
            else:
                self.log.info("Compatibility doc already in sync: %s", doc_path)

        doc_synced = is_compatibility_doc_in_sync(doc_path, rendered_matrix=rendered)
        report = CompatibilityReport(
            registry_path=registry_path.as_posix(),
            doc_path=doc_path.as_posix(),
            entry_count=len(entries),
            issues=issues,
            doc_synced=doc_synced,
        )

        if fmt == "json":
            payload: str | dict = format_compatibility_report_jsonable(report)
        elif fmt == "text":
            payload = format_compatibility_report_text(report)
        else:
            raise DocsConfigurationError("--format must be text or json")

        exit_code = 0 if report.ok else 2
        if exit_code:
            self.log.error("Compatibility policy check failed")
        else:
            self.log.info("Compatibility policy OK")
        return exit_code, payload

    def _resolve_path(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Path argument must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        return path
