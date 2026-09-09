from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from .context import DocsServiceContext
from .markdown_links import (
    check_markdown_links,
    format_markdown_link_report_jsonable,
    format_markdown_link_report_text,
)


class CheckDocsService:
    """CI-friendly markdown/docs validation.

    Current scope:
    - README + docs/**/*.md local link resolution
    - local anchor validation for markdown targets
    """

    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> tuple[int, str | dict]:
        fmt = str(getattr(args, "format", "text") or "text").strip().lower()
        repo_root = self.ctx.settings.repo_root
        docs_dir = self._resolve_path(getattr(args, "docs_dir", "docs"), repo_root=repo_root)
        if not docs_dir.exists() or not docs_dir.is_dir():
            raise DocsConfigurationError(f"Docs dir not found: {docs_dir}")

        files = list(
            self._collect_markdown_files(
                docs_dir=docs_dir,
                repo_root=repo_root,
                include_root_readme=bool(getattr(args, "include_root_readme", True)),
            )
        )
        if not files:
            raise DocsConfigurationError("No markdown files selected for docs check")

        report = check_markdown_links(files)
        if fmt == "json":
            payload: str | dict = format_markdown_link_report_jsonable(report)
        elif fmt == "text":
            payload = format_markdown_link_report_text(report, root=repo_root)
        else:
            raise DocsConfigurationError("--format must be text or json")

        exit_code = 0 if report.ok else 2
        if exit_code:
            self.log.error("Docs check failed: %s issue(s)", report.issue_count)
        else:
            self.log.info("Docs check OK")
        return exit_code, payload

    def _collect_markdown_files(self, *, docs_dir: Path, repo_root: Path, include_root_readme: bool) -> Iterable[Path]:
        seen: set[Path] = set()
        if include_root_readme:
            readme = repo_root / "README.md"
            if readme.exists():
                seen.add(readme.resolve())
                yield readme.resolve()
        for path in sorted(docs_dir.rglob("*.md")):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            yield resolved

    @staticmethod
    def _resolve_path(raw: object, *, repo_root: Path) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Path argument must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (repo_root / path).resolve()
        return path
