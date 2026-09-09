from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import module_size_legacy as _legacy
from .loc import count_lines, count_sloc, iter_py_files
from .module_size_baseline import ModuleSizeDebtEntry, ModuleSizePolicyIssue
from .module_size_thresholds import ModuleSizeThresholds

ModuleSizeBaselineEntry = _legacy.ModuleSizeBaselineEntry
load_module_size_baseline = _legacy.load_module_size_baseline
write_module_size_baseline = _legacy.write_module_size_baseline


ModuleSizeTrackedEntry = ModuleSizeBaselineEntry | ModuleSizeDebtEntry


@dataclass(frozen=True)
class ModuleSizeItem:
    path: str
    lines: int
    sloc: int


@dataclass(frozen=True)
class ModuleSizeIssue:
    path: str
    lines: int
    sloc: int
    severity: str
    message: str


@dataclass(frozen=True)
class ModuleSizeReport:
    package: str
    thresholds: ModuleSizeThresholds
    items: tuple[ModuleSizeItem, ...]
    allowlisted: tuple[ModuleSizeTrackedEntry, ...]
    issues: tuple[ModuleSizeIssue, ...]
    base_sha: str | None = None
    head_sha: str | None = None
    debt_model: str = "none"

    @property
    def ok(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)

    @property
    def issue_count(self) -> int:
        return len(self.issues)


def analyze_module_sizes(
    package_dir: Path,
    *,
    repo_root: Path,
    thresholds: ModuleSizeThresholds,
    baseline: tuple[ModuleSizeTrackedEntry, ...] = (),
    policy_issues: tuple[ModuleSizePolicyIssue, ...] = (),
    base_sha: str | None = None,
    head_sha: str | None = None,
    warning_debt_requires_baseline: bool = False,
    module_paths: tuple[Path, ...] | None = None,
    source_bytes_by_path: Mapping[str, bytes] | None = None,
) -> ModuleSizeReport:
    package_dir = package_dir.absolute()
    repo_root = repo_root.absolute()
    debt_model = _debt_model(baseline, warning_debt_requires_baseline=warning_debt_requires_baseline)
    baseline_by_path = {entry.path: entry for entry in baseline}
    items: list[ModuleSizeItem] = []
    allowlisted: list[ModuleSizeTrackedEntry] = []
    issues: list[ModuleSizeIssue] = []

    sources = _module_sources(
        package_dir=package_dir,
        repo_root=repo_root,
        module_paths=module_paths,
        source_bytes_by_path=source_bytes_by_path,
    )
    for rel, raw, source_error in sources:
        if source_error is not None:
            issues.append(
                ModuleSizeIssue(
                    path=rel,
                    lines=0,
                    sloc=0,
                    severity="error",
                    message=source_error,
                )
            )
            continue
        assert raw is not None
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            issues.append(
                ModuleSizeIssue(
                    path=rel,
                    lines=0,
                    sloc=0,
                    severity="error",
                    message="Module cannot be read as trusted UTF-8 source: UnicodeDecodeError",
                )
            )
            continue
        lines = count_lines(text)
        sloc = count_sloc(text)
        item = ModuleSizeItem(path=rel, lines=lines, sloc=sloc)
        items.append(item)
        line_error = lines > thresholds.max_lines
        sloc_error = thresholds.max_sloc is not None and sloc > thresholds.max_sloc
        line_warn = lines > thresholds.warn_lines
        sloc_warn = thresholds.warn_sloc is not None and sloc > thresholds.warn_sloc
        if line_error or sloc_error:
            limits = [f"{lines} LOC exceeds max_lines={thresholds.max_lines}"] if line_error else []
            if sloc_error:
                limits.append(f"{sloc} SLOC exceeds max_sloc={thresholds.max_sloc}")
            issues.append(
                ModuleSizeIssue(
                    path=rel,
                    lines=lines,
                    sloc=sloc,
                    severity="error",
                    message="Module violates hard max: " + " and ".join(limits),
                )
            )
            continue
        baseline_entry = baseline_by_path.get(rel)
        if baseline_entry is not None:
            allowlisted.append(baseline_entry)
            if isinstance(baseline_entry, ModuleSizeBaselineEntry):
                continue
            if lines > baseline_entry.max_lines or sloc > baseline_entry.max_sloc:
                issues.append(
                    ModuleSizeIssue(
                        path=rel,
                        lines=lines,
                        sloc=sloc,
                        severity="error",
                        message=(
                            "Module grew beyond exact baseline cap: "
                            f"current={lines} LOC/{sloc} SLOC, "
                            f"cap={baseline_entry.max_lines} LOC/{baseline_entry.max_sloc} SLOC"
                        ),
                    )
                )
            elif lines < baseline_entry.max_lines or sloc < baseline_entry.max_sloc:
                issues.append(
                    ModuleSizeIssue(
                        path=rel,
                        lines=lines,
                        sloc=sloc,
                        severity="error",
                        message=(
                            "Module shrank; deterministic ratchet update required: "
                            f"current={lines} LOC/{sloc} SLOC, "
                            f"cap={baseline_entry.max_lines} LOC/{baseline_entry.max_sloc} SLOC"
                        ),
                    )
                )
            elif not (line_warn or sloc_warn):
                issues.append(
                    ModuleSizeIssue(
                        path=rel,
                        lines=lines,
                        sloc=sloc,
                        severity="error",
                        message="Baseline entry is stale because the module is below warning thresholds",
                    )
                )
            continue
        if line_warn or sloc_warn:
            limits = [f"{lines} LOC exceeds warn_lines={thresholds.warn_lines}"] if line_warn else []
            if sloc_warn:
                limits.append(f"{sloc} SLOC exceeds warn_sloc={thresholds.warn_sloc}")
            issues.append(
                ModuleSizeIssue(
                    path=rel,
                    lines=lines,
                    sloc=sloc,
                    severity="error" if warning_debt_requires_baseline else "warn",
                    message=(
                        "Module has unbaselined warning debt: " if warning_debt_requires_baseline else "Module has "
                    )
                    + " and ".join(limits),
                )
            )

    item_by_path = {item.path: item for item in items}
    for entry in baseline:
        if isinstance(entry, ModuleSizeDebtEntry) and entry.path not in item_by_path:
            issues.append(
                ModuleSizeIssue(
                    path=entry.path,
                    lines=0,
                    sloc=0,
                    severity="error",
                    message="Baseline entry is stale or its module is missing",
                )
            )
    for issue in policy_issues:
        current_item = item_by_path.get(issue.path)
        issues.append(
            ModuleSizeIssue(
                path=issue.path,
                lines=current_item.lines if current_item else 0,
                sloc=current_item.sloc if current_item else 0,
                severity="error",
                message=issue.message,
            )
        )

    return ModuleSizeReport(
        package=package_dir.relative_to(repo_root).as_posix(),
        thresholds=thresholds,
        items=tuple(sorted(items, key=lambda item: (-item.lines, item.path))),
        allowlisted=tuple(sorted(allowlisted, key=lambda entry: entry.path)),
        issues=tuple(sorted(issues, key=lambda issue: (issue.severity != "error", -issue.lines, issue.path))),
        base_sha=base_sha,
        head_sha=head_sha,
        debt_model=debt_model,
    )


def collect_module_size_paths(package_dir: Path) -> tuple[Path, ...]:
    """Return the deterministic lexical file set consumed by the analyzer."""

    return tuple(sorted(iter_py_files(package_dir)))


def _module_sources(
    *,
    package_dir: Path,
    repo_root: Path,
    module_paths: tuple[Path, ...] | None,
    source_bytes_by_path: Mapping[str, bytes] | None,
) -> tuple[tuple[str, bytes | None, str | None], ...]:
    if source_bytes_by_path is not None:
        return tuple((path, raw, None) for path, raw in sorted(source_bytes_by_path.items()))
    sources: list[tuple[str, bytes | None, str | None]] = []
    for path in module_paths if module_paths is not None else collect_module_size_paths(package_dir):
        rel = path.relative_to(repo_root).as_posix()
        error = _unsafe_module_reason(path, package_dir=package_dir, repo_root=repo_root)
        if error is not None:
            sources.append((rel, None, error))
            continue
        try:
            sources.append((rel, path.read_bytes(), None))
        except OSError as exc:
            sources.append((rel, None, f"Module cannot be read as trusted source: {type(exc).__name__}"))
    return tuple(sources)


def _unsafe_module_reason(path: Path, *, package_dir: Path, repo_root: Path) -> str | None:
    try:
        path.relative_to(package_dir)
        relative = path.relative_to(repo_root)
    except ValueError:
        return "Module path escapes the configured package or repository"
    current = repo_root
    try:
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                return "Module path contains a symlink and is not trusted commit content"
        resolved = path.resolve(strict=True)
    except OSError as exc:
        return f"Module path cannot be resolved safely: {type(exc).__name__}"
    if not resolved.is_relative_to(package_dir) or not resolved.is_relative_to(repo_root):
        return "Module path resolves outside the configured package or repository"
    if not resolved.is_file():
        return "Module path is not a regular file"
    return None


def format_module_size_report_jsonable(report: ModuleSizeReport) -> dict[str, Any]:
    return {
        "schema_version": "dpone.module-size-report.v2",
        "debt_model": report.debt_model,
        "ok": report.ok,
        "package": report.package,
        "base_sha": report.base_sha,
        "head_sha": report.head_sha,
        "thresholds": {
            "warn_lines": report.thresholds.warn_lines,
            "max_lines": report.thresholds.max_lines,
            "warn_sloc": report.thresholds.warn_sloc,
            "max_sloc": report.thresholds.max_sloc,
        },
        "issue_count": report.issue_count,
        "issues": [
            {
                "path": issue.path,
                "lines": issue.lines,
                "sloc": issue.sloc,
                "severity": issue.severity,
                "message": issue.message,
            }
            for issue in report.issues
        ],
        "allowlisted": [_format_allowlisted_entry(entry) for entry in report.allowlisted],
        "largest": [
            {
                "path": item.path,
                "lines": item.lines,
                "sloc": item.sloc,
            }
            for item in report.items[:20]
        ],
    }


def format_module_size_report_text(report: ModuleSizeReport) -> str:
    lines = [
        f"Module size check for {report.package}",
        f"Status: {'OK' if report.ok else 'FAIL'}",
        (
            f"Thresholds: warn>{report.thresholds.warn_lines} LOC, fail>{report.thresholds.max_lines} LOC"
            + _format_sloc_thresholds(report.thresholds)
        ),
        f"Tracked debt entries: {len(report.allowlisted)}",
        f"Debt policy: {report.debt_model}",
    ]
    if report.base_sha is not None and report.head_sha is not None:
        lines.append(f"Git identity: base={report.base_sha}, head={report.head_sha}")
    if report.issues:
        lines.append(f"Issues ({len(report.issues)}):")
        for issue in report.issues:
            lines.append(f"  - [{issue.severity}] {issue.path}: {issue.lines} LOC / {issue.sloc} SLOC; {issue.message}")
    else:
        lines.append("No module-size issues.")
    return "\n".join(lines) + "\n"


def _format_sloc_thresholds(thresholds: ModuleSizeThresholds) -> str:
    parts: list[str] = []
    if thresholds.warn_sloc is not None:
        parts.append(f"warn>{thresholds.warn_sloc} SLOC")
    if thresholds.max_sloc is not None:
        parts.append(f"fail>{thresholds.max_sloc} SLOC")
    return "; " + ", ".join(parts) if parts else ""


def _format_allowlisted_entry(entry: ModuleSizeTrackedEntry) -> dict[str, Any]:
    if isinstance(entry, ModuleSizeBaselineEntry):
        return {
            "path": entry.path,
            "lines": entry.lines,
            "owner": entry.owner,
            "reason": entry.reason,
            "target": entry.target,
        }
    return {
        "path": entry.path,
        "max_lines": entry.max_lines,
        "max_sloc": entry.max_sloc,
        "owner": entry.owner,
        "reason": entry.reason,
        "target_sloc": entry.target_sloc,
        "target_date": entry.target_date.isoformat(),
        "accepted_adr": entry.accepted_adr,
        "baseline_commit": entry.baseline_commit,
    }


def _debt_model(baseline: tuple[ModuleSizeTrackedEntry, ...], *, warning_debt_requires_baseline: bool) -> str:
    has_ratchet = any(isinstance(entry, ModuleSizeDebtEntry) for entry in baseline)
    has_legacy = any(isinstance(entry, ModuleSizeBaselineEntry) for entry in baseline)
    if has_ratchet and has_legacy:
        raise ValueError("module-size baseline entries must use exactly one debt model")
    if has_ratchet:
        return "ratchet-v2"
    if has_legacy:
        return "legacy-v1"
    return "ratchet-v2" if warning_debt_requires_baseline else "none"
