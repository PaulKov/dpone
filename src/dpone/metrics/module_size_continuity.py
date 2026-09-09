"""Exact-head retirement rules for module-size debt entries."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from .module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    ModuleSizePolicyIssue,
)


def grandfathered_debt_allowed(
    entry: ModuleSizeDebtEntry,
    *,
    prior: ModuleSizeDebtEntry | None,
    bootstrap_allowed: bool,
    bootstrap_commit: str,
    bootstrap_base_sha: str,
    source_at: Callable[[str, str], str],
) -> bool:
    """Allow null-ADR debt only through continuous or doubly bounded provenance."""

    if (
        prior is not None
        and prior.accepted_adr is None
        and entry.baseline_commit == prior.baseline_commit
        and entry.max_lines <= prior.max_lines
        and entry.max_sloc <= prior.max_sloc
    ):
        return True
    if not bootstrap_allowed or entry.baseline_commit != bootstrap_commit:
        return False
    try:
        audited_size = _source_size(source_at(bootstrap_commit, entry.path))
        base_size = _source_size(source_at(bootstrap_base_sha, entry.path))
    except (ModuleSizeBaselineError, UnicodeDecodeError):
        return False
    return entry.max_lines <= min(audited_size[0], base_size[0]) and entry.max_sloc <= min(
        audited_size[1], base_size[1]
    )


def _source_size(source: str) -> tuple[int, int]:
    lines = source.splitlines()
    return len(lines), sum(1 for line in lines if line.strip() and not line.lstrip().startswith("#"))


def baseline_budget_issues(
    baseline: ModuleSizeBaseline,
    budget_limits: tuple[int, int, int, int] | None,
) -> tuple[ModuleSizePolicyIssue, ...]:
    """Bind every debt entry to the authoritative exact-head budgets."""

    if budget_limits is None:
        return ()
    _warn_lines, max_lines, warn_sloc, max_sloc = budget_limits
    issues: list[ModuleSizePolicyIssue] = []
    for entry in baseline.entries:
        if entry.max_lines > max_lines:
            issues.append(ModuleSizePolicyIssue(entry.path, f"max_lines exceeds authoritative max_loc={max_lines}"))
        if entry.max_sloc > max_sloc:
            issues.append(ModuleSizePolicyIssue(entry.path, f"max_sloc exceeds authoritative max_sloc={max_sloc}"))
        if entry.target_sloc > warn_sloc:
            issues.append(ModuleSizePolicyIssue(entry.path, f"target_sloc exceeds authoritative warn_sloc={warn_sloc}"))
    return tuple(issues)


def retired_debt_issues(
    baseline: ModuleSizeBaseline,
    *,
    previous: Mapping[str, ModuleSizeDebtEntry],
    exact_renames: Sequence[tuple[str, str]],
    deleted_paths: Sequence[str],
    rename_only: bool,
    current_module_sizes: Mapping[str, tuple[int, int]],
    warning_thresholds: tuple[int, int],
) -> tuple[ModuleSizePolicyIssue, ...]:
    """Reject unexplained debt disappearance while allowing verified remediation."""

    current_entries = {entry.path for entry in baseline.entries}
    renamed = dict(exact_renames)
    warn_lines, warn_sloc = warning_thresholds
    issues: list[ModuleSizePolicyIssue] = []
    for path in sorted(set(previous) - current_entries):
        if path in deleted_paths:
            continue
        renamed_path = renamed.get(path)
        if renamed_path is not None:
            if rename_only and renamed_path in current_entries:
                continue
            issues.append(ModuleSizePolicyIssue(path, "renamed debt entry must carry the same or lower exact caps"))
            continue
        sizes = current_module_sizes.get(path)
        if sizes is None or sizes[0] > warn_lines or sizes[1] > warn_sloc:
            issues.append(ModuleSizePolicyIssue(path, "prior warning debt disappeared without verified remediation"))
    return tuple(issues)


__all__ = ["baseline_budget_issues", "grandfathered_debt_allowed", "retired_debt_issues"]
