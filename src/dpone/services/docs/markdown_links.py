from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
_EXTERNAL_PREFIXES = (
    "http:",
    "https:",
    "mailto:",
    "tel:",
    "ftp:",
)


@dataclass(frozen=True)
class MarkdownLinkIssue:
    kind: str
    source_path: str
    lineno: int
    target: str
    detail: str


@dataclass(frozen=True)
class MarkdownLinkReport:
    checked_file_count: int
    checked_link_count: int
    issues: tuple[MarkdownLinkIssue, ...]

    @property
    def ok(self) -> bool:
        return not self.issues

    @property
    def issue_count(self) -> int:
        return len(self.issues)


def check_markdown_links(files: Sequence[Path]) -> MarkdownLinkReport:
    anchors_by_file = {path.resolve(): _collect_anchors(path) for path in files}
    issues: list[MarkdownLinkIssue] = []
    checked_links = 0

    for path in files:
        in_code_fence = False
        for lineno, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.rstrip("\n")
            if _is_fence(line):
                in_code_fence = not in_code_fence
                continue
            if in_code_fence:
                continue
            for match in _LINK_RE.finditer(line):
                raw_target = _normalize_link_target(match.group(1))
                if not raw_target or _is_external(raw_target):
                    continue
                checked_links += 1
                issues.extend(_check_single_link(path, lineno, raw_target, anchors_by_file))

    return MarkdownLinkReport(
        checked_file_count=len(files),
        checked_link_count=checked_links,
        issues=tuple(sorted(issues, key=lambda x: (x.source_path, x.lineno, x.kind, x.target))),
    )


def format_markdown_link_report_text(report: MarkdownLinkReport, *, root: Path | None = None) -> str:
    if report.ok:
        return (
            "✅ Docs check OK: "
            f"{report.checked_file_count} markdown files, {report.checked_link_count} local links checked."
        )

    lines: list[str] = []
    lines.append(
        "Docs check failed: "
        f"{report.issue_count} issue(s), {report.checked_file_count} files, {report.checked_link_count} links checked"
    )
    current_file = ""
    for issue in report.issues:
        source = issue.source_path
        if root is not None:
            try:
                source = Path(source).resolve().relative_to(root.resolve()).as_posix()
            except Exception:
                pass
        if source != current_file:
            current_file = source
            lines.append("")
            lines.append(f"[{source}]")
        lines.append(f"- L{issue.lineno} {issue.kind}: {issue.target}")
        lines.append(f"    {issue.detail}")
    return "\n".join(lines)


def format_markdown_link_report_jsonable(report: MarkdownLinkReport) -> dict:
    return {
        "ok": report.ok,
        "checked_file_count": report.checked_file_count,
        "checked_link_count": report.checked_link_count,
        "issue_count": report.issue_count,
        "issues": [
            {
                "kind": issue.kind,
                "source_path": issue.source_path,
                "lineno": issue.lineno,
                "target": issue.target,
                "detail": issue.detail,
            }
            for issue in report.issues
        ],
    }


def _check_single_link(
    source_path: Path,
    lineno: int,
    raw_target: str,
    anchors_by_file: dict[Path, set[str]],
) -> list[MarkdownLinkIssue]:
    issues: list[MarkdownLinkIssue] = []
    target_part, anchor = _split_anchor(raw_target)

    if not target_part:
        target_path = source_path.resolve()
    else:
        target_path = (source_path.parent / unquote(target_part)).resolve()

    if target_part and not target_path.exists():
        issues.append(
            MarkdownLinkIssue(
                kind="missing-file",
                source_path=source_path.resolve().as_posix(),
                lineno=lineno,
                target=raw_target,
                detail=f"Target file does not exist: {target_path}",
            )
        )
        return issues

    if anchor:
        if target_path.suffix.lower() != ".md":
            issues.append(
                MarkdownLinkIssue(
                    kind="anchor-on-non-markdown",
                    source_path=source_path.resolve().as_posix(),
                    lineno=lineno,
                    target=raw_target,
                    detail="Anchor checks are only supported for Markdown files.",
                )
            )
            return issues
        known_anchors = anchors_by_file.get(target_path.resolve())
        if known_anchors is None:
            issues.append(
                MarkdownLinkIssue(
                    kind="missing-markdown-target",
                    source_path=source_path.resolve().as_posix(),
                    lineno=lineno,
                    target=raw_target,
                    detail=f"Markdown target was not part of checked set: {target_path}",
                )
            )
            return issues
        if anchor not in known_anchors:
            issues.append(
                MarkdownLinkIssue(
                    kind="missing-anchor",
                    source_path=source_path.resolve().as_posix(),
                    lineno=lineno,
                    target=raw_target,
                    detail=f"Anchor '#{anchor}' not found in {target_path}",
                )
            )
    return issues


def _collect_anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    seen: dict[str, int] = {}
    in_code_fence = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip("\n")
        if _is_fence(line):
            in_code_fence = not in_code_fence
            continue
        if in_code_fence:
            continue
        if not line.lstrip().startswith("#"):
            continue
        heading = line.lstrip("#").strip()
        if not heading:
            continue
        base = _slugify_heading(heading)
        if not base:
            continue
        idx = seen.get(base, 0)
        seen[base] = idx + 1
        anchor = base if idx == 0 else f"{base}-{idx}"
        anchors.add(anchor)
    return anchors


def _slugify_heading(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[`*_~\[\]()<>]", "", text)
    text = re.sub(r"[^\w\s\-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "-", text, flags=re.UNICODE)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def _normalize_link_target(raw: str) -> str:
    value = raw.strip()
    if value.startswith("<") and ">" in value:
        return value[1 : value.index(">")].strip()
    if ' "' in value:
        value = value.split(' "', 1)[0].strip()
    if " '" in value:
        value = value.split(" '", 1)[0].strip()
    return value


def _split_anchor(target: str) -> tuple[str, str | None]:
    if "#" not in target:
        return target, None
    path_part, anchor = target.split("#", 1)
    return path_part, _slugify_heading(anchor) if anchor else None


def _is_external(target: str) -> bool:
    t = target.lower()
    return t.startswith(_EXTERNAL_PREFIXES)


def _is_fence(line: str) -> bool:
    stripped = line.strip()
    return stripped.startswith("```") or stripped.startswith("~~~")
