from __future__ import annotations

import argparse

from dpone.services.docs.errors import DocsConfigurationError

from ...metrics.import_rules import (
    evaluate_import_rules,
    format_import_rule_report_jsonable,
    format_import_rule_report_text,
)
from .context import DocsServiceContext


class CheckImportRulesService:
    """Run architectural import-rule checks for dpone.* modules."""

    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> tuple[int, str | dict]:
        fmt = str(getattr(args, "format", "text") or "text").strip().lower()
        package = str(getattr(args, "package", "src/dpone") or "src/dpone")
        package_dir = (self.ctx.settings.repo_root / package).resolve()
        if not package_dir.exists():
            raise DocsConfigurationError(f"Package dir not found: {package_dir}")
        if package_dir.name != "dpone":
            raise DocsConfigurationError("Only src/dpone package is supported for now")

        report = evaluate_import_rules(package_dir, package_name="dpone")
        payload: str | dict
        if fmt == "json":
            payload = format_import_rule_report_jsonable(report)
        elif fmt == "text":
            payload = format_import_rule_report_text(report, package_dir=package_dir)
        else:
            raise DocsConfigurationError("--format must be text or json")

        exit_code = 0 if report.ok else 2
        if exit_code:
            self.log.error("Import rules violated: %s", report.violation_count)
        else:
            self.log.info("Import rules OK")
        return exit_code, payload
