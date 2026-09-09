from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.services.docs.errors import DocsConfigurationError

from .cli_reference import is_cli_reference_doc_in_sync
from .context import DocsServiceContext
from .gitops_schema_reference import is_gitops_schema_reference_doc_in_sync
from .manifest_schema_reference import is_manifest_schema_reference_doc_in_sync


@dataclass(frozen=True, slots=True)
class GeneratedReferenceCheck:
    name: str
    path: Path
    passed: bool
    fix_command: str

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "path": self.path.as_posix(),
            "passed": self.passed,
            "fix_command": self.fix_command,
        }


class CheckGeneratedReferencesService:
    """Validate generated documentation blocks without rewriting files."""

    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace | Mapping[str, Any]) -> tuple[int, dict[str, object] | str]:
        fmt = str(_arg(args, "format", "text") or "text").strip().lower()
        cli_doc = self._resolve_path(_arg(args, "cli_doc", "docs/cli-reference.md"))
        gitops_doc = self._resolve_path(_arg(args, "gitops_schema_doc", "docs/reference/gitops-schema-catalog.md"))
        manifest_doc = self._resolve_path(_arg(args, "manifest_schema_doc", "docs/reference/manifest-schemas.md"))
        manifest_schema_root = self._resolve_path(_arg(args, "manifest_schema_root", "src/dpone/schema"))
        checks = (
            GeneratedReferenceCheck(
                name="cli-reference",
                path=cli_doc,
                passed=is_cli_reference_doc_in_sync(cli_doc, parser=self._build_parser()),
                fix_command="dpone docs update-cli-reference",
            ),
            GeneratedReferenceCheck(
                name="gitops-schema-catalog",
                path=gitops_doc,
                passed=is_gitops_schema_reference_doc_in_sync(gitops_doc),
                fix_command="dpone docs update-gitops-schema-reference",
            ),
            GeneratedReferenceCheck(
                name="manifest-schema-reference",
                path=manifest_doc,
                passed=is_manifest_schema_reference_doc_in_sync(
                    manifest_doc,
                    schema_root=manifest_schema_root,
                ),
                fix_command="dpone docs update-manifest-schema-reference",
            ),
        )
        passed = all(check.passed for check in checks)
        payload = {
            "kind": "docs.generated_references",
            "passed": passed,
            "checks": [check.to_dict() for check in checks],
        }
        if fmt == "json":
            result: dict[str, object] | str = payload
        elif fmt == "text":
            result = _format_text(checks)
        else:
            raise DocsConfigurationError("--format must be text or json")
        if passed:
            self.log.info("Generated references are up-to-date")
            return 0, result
        self.log.error("Generated references are outdated")
        return 2, result

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


def _arg(args: argparse.Namespace | Mapping[str, Any], key: str, default: object) -> object:
    if isinstance(args, Mapping):
        return args.get(key, default)
    return getattr(args, key, default)


def _format_text(checks: tuple[GeneratedReferenceCheck, ...]) -> str:
    passed = tuple(check for check in checks if check.passed)
    failed = tuple(check for check in checks if not check.passed)
    if not failed:
        return f"✅ Generated references OK: {len(passed)}/{len(checks)} in sync."
    lines = [f"Generated references check failed: {len(failed)}/{len(checks)} outdated."]
    for check in failed:
        lines.append(f"- {check.name}: {check.path.as_posix()}")
        lines.append(f"  fix: {check.fix_command}")
    return "\n".join(lines)


__all__ = ["CheckGeneratedReferencesService", "GeneratedReferenceCheck"]
