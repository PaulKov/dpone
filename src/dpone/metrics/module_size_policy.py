"""Git-bound continuity policy for module-size technical-debt baselines."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    ModuleSizePolicyIssue,
    confined_repo_path,
    decode_json_object,
    decode_module_size_baseline,
    exact_sha,
    is_legacy_empty_baseline,
    load_module_size_baseline,
    write_module_size_baseline,
)
from .module_size_continuity import baseline_budget_issues, grandfathered_debt_allowed, retired_debt_issues

AUDITED_BOOTSTRAP_COMMIT = "e1d93822b47234e940829319cac0dc9678f6906c"
_ADR_EXCEPTION_SCHEMA = "dpone.module-size-debt-exception.v2"
_ADR_EXCEPTION_FIELDS = frozenset(
    {
        "schema_version",
        "path",
        "max_lines",
        "max_sloc",
        "owner",
        "reason",
        "target_sloc",
        "target_date",
        "baseline_commit",
    }
)
_ADR_EXCEPTION = re.compile(
    r"<!--\s*dpone-module-size-debt-exception-v2\s*\n(?P<payload>\{.*?\})\s*\n-->",
    re.DOTALL,
)
_ADR_STATUS_SECTION = re.compile(
    r"^## Status[ \t]*\r?\n(?P<body>.*?)(?=^##(?:[ \t].*)?\r?$|\Z)",
    re.DOTALL | re.MULTILINE,
)
_BaselineState = tuple[ModuleSizeBaseline | None, bool]


@dataclass(frozen=True)
class ModuleSizeGitContext:
    base_sha: str
    head_sha: str
    previous_baseline: ModuleSizeBaseline | None
    exact_renames: tuple[tuple[str, str], ...]
    deleted_paths: tuple[str, ...]
    rename_only: bool
    base_has_legacy_baseline: bool = False


def resolve_module_size_git_context(
    *,
    repo_root: Path,
    baseline_path: Path,
    base_ref: str,
    head_ref: str,
) -> ModuleSizeGitContext:
    """Resolve exact comparison commits and the prior baseline fail-closed."""

    base_sha = exact_sha(base_ref, label="--base-ref")
    head_sha = exact_sha(head_ref, label="--head-ref")
    current_head = _git(repo_root, "rev-parse", "HEAD^{commit}")
    if current_head != head_sha:
        raise ModuleSizeBaselineError(f"--head-ref {head_sha} does not match checked-out HEAD {current_head}")
    if base_sha == head_sha:
        raise ModuleSizeBaselineError("--base-ref and --head-ref must identify distinct commits")
    for label, sha in (("base", base_sha), ("head", head_sha)):
        _git(repo_root, "cat-file", "-e", f"{sha}^{{commit}}", error=f"{label} commit is unavailable: {sha}")
    _git(repo_root, "merge-base", "--is-ancestor", base_sha, head_sha, error="base commit is not an ancestor of head")
    relative = _relative_to_repo(baseline_path, repo_root)
    previous, base_has_legacy_baseline = _load_previous_baseline(repo_root, base_sha, relative)
    statuses, renames, deleted = _diff_status(repo_root, base_sha, head_sha, ignored_path=relative)
    return ModuleSizeGitContext(
        base_sha=base_sha,
        head_sha=head_sha,
        previous_baseline=previous,
        exact_renames=renames,
        deleted_paths=deleted,
        rename_only=len(statuses) == 1 and statuses[0] == "R100" and len(renames) == 1,
        base_has_legacy_baseline=base_has_legacy_baseline,
    )


def validate_module_size_baseline(
    baseline: ModuleSizeBaseline,
    *,
    repo_root: Path,
    git_context: ModuleSizeGitContext | None,
    as_of: date,
    accepted_adr_text_by_path: Mapping[str, str] | None = None,
    trusted_module_paths: frozenset[str] | None = None,
    current_module_sizes: Mapping[str, tuple[int, int]] | None = None,
    warning_thresholds: tuple[int, int] | None = None,
    budget_limits: tuple[int, int, int, int] | None = None,
) -> tuple[ModuleSizePolicyIssue, ...]:
    """Validate deadlines, ADRs, safe paths and grandfathered provenance."""

    issues = list(baseline_budget_issues(baseline, budget_limits))
    previous = {
        entry.path: entry
        for entry in (git_context.previous_baseline.entries if git_context and git_context.previous_baseline else ())
    }
    renamed_from = {new: old for old, new in (git_context.exact_renames if git_context else ())}
    for entry in baseline.entries:
        if trusted_module_paths is not None:
            if entry.path not in trusted_module_paths:
                issues.append(ModuleSizePolicyIssue(entry.path, "module is absent from the exact-HEAD snapshot"))
        else:
            module_path = repo_root / entry.path
            if _has_symlink_component(module_path, repo_root) or (
                module_path.exists() and not module_path.resolve().is_relative_to(repo_root.resolve())
            ):
                issues.append(ModuleSizePolicyIssue(entry.path, "module path is symlinked or escapes the repository"))
        if entry.target_date < as_of:
            issues.append(ModuleSizePolicyIssue(entry.path, f"target_date expired on {entry.target_date.isoformat()}"))
        if entry.accepted_adr is not None:
            error = _validate_accepted_adr(repo_root, entry, accepted_adr_text_by_path=accepted_adr_text_by_path)
            if error:
                issues.append(ModuleSizePolicyIssue(entry.path, error))
        if git_context is None:
            continue
        if not _is_ancestor(repo_root, entry.baseline_commit, git_context.base_sha):
            issues.append(
                ModuleSizePolicyIssue(entry.path, "baseline_commit is unavailable or not an ancestor of base")
            )
            continue
        prior = _prior_entry(
            entry,
            previous=previous,
            renamed_from=renamed_from,
            rename_only=git_context.rename_only,
        )
        if prior is not None:
            if entry.max_lines > prior.max_lines or entry.max_sloc > prior.max_sloc:
                issues.append(ModuleSizePolicyIssue(entry.path, "exact baseline caps cannot increase"))
            if _metadata_changed(entry, prior) and (
                entry.accepted_adr is None or entry.accepted_adr == prior.accepted_adr
            ):
                issues.append(
                    ModuleSizePolicyIssue(entry.path, "debt metadata changed without a newly bound Accepted ADR")
                )
        if entry.accepted_adr is None and not grandfathered_debt_allowed(
            entry,
            prior=prior,
            bootstrap_allowed=(git_context.base_has_legacy_baseline and git_context.previous_baseline is None),
            bootstrap_commit=AUDITED_BOOTSTRAP_COMMIT,
            bootstrap_base_sha=git_context.base_sha,
            source_at=lambda commit, path: _git_bytes(repo_root, "show", f"{commit}:{path}").decode("utf-8"),
        ):
            issues.append(
                ModuleSizePolicyIssue(
                    entry.path, "accepted_adr:null lacks audited bootstrap, continuity, or exact rename provenance"
                )
            )
    if git_context is not None and current_module_sizes is not None and warning_thresholds is not None:
        issues.extend(
            retired_debt_issues(
                baseline,
                previous=previous,
                exact_renames=git_context.exact_renames,
                deleted_paths=git_context.deleted_paths,
                rename_only=git_context.rename_only,
                current_module_sizes=current_module_sizes,
                warning_thresholds=warning_thresholds,
            )
        )
    return tuple(issues)


def _prior_entry(
    entry: ModuleSizeDebtEntry,
    *,
    previous: dict[str, ModuleSizeDebtEntry],
    renamed_from: dict[str, str],
    rename_only: bool,
) -> ModuleSizeDebtEntry | None:
    prior = previous.get(entry.path)
    if prior is not None:
        return prior
    old_path = renamed_from.get(entry.path)
    return previous.get(old_path) if rename_only and old_path else None


def _metadata_changed(entry: ModuleSizeDebtEntry, prior: ModuleSizeDebtEntry) -> bool:
    return (
        entry.owner,
        entry.reason,
        entry.target_sloc,
        entry.target_date,
        entry.accepted_adr,
        entry.baseline_commit,
    ) != (
        prior.owner,
        prior.reason,
        prior.target_sloc,
        prior.target_date,
        prior.accepted_adr,
        prior.baseline_commit,
    )


def _validate_accepted_adr(
    repo_root: Path,
    entry: ModuleSizeDebtEntry,
    *,
    accepted_adr_text_by_path: Mapping[str, str] | None,
) -> str | None:
    relative = entry.accepted_adr
    if relative is None:
        return "accepted_adr is missing"
    if accepted_adr_text_by_path is not None:
        text = accepted_adr_text_by_path.get(relative)
        if text is None:
            return f"accepted_adr is absent from the exact-HEAD snapshot: {relative}"
    else:
        target = repo_root / relative
        if _has_symlink_component(target, repo_root):
            return f"accepted_adr is symlinked: {relative}"
        resolved = target.resolve()
        if not resolved.is_relative_to((repo_root / "docs/adr").resolve()) or not resolved.is_file():
            return f"accepted_adr does not exist: {relative}"
        text = resolved.read_text(encoding="utf-8")
    status_sections = tuple(_ADR_STATUS_SECTION.finditer(text))
    if len(status_sections) != 1 or status_sections[0].group("body").strip() not in {"Accepted", "Accepted."}:
        return f"accepted_adr is not Accepted: {relative}"
    blocks = tuple(_ADR_EXCEPTION.finditer(text))
    if len(blocks) != 1:
        return f"accepted_adr must contain exactly one bound {_ADR_EXCEPTION_SCHEMA} payload: {relative}"
    try:
        payload = decode_json_object(blocks[0].group("payload").encode("utf-8"), source=relative)
    except ModuleSizeBaselineError as exc:
        return f"accepted_adr exception payload is invalid: {exc}"
    if set(payload) != _ADR_EXCEPTION_FIELDS:
        return f"accepted_adr exception payload fields are not closed: {relative}"
    expected = {
        "schema_version": _ADR_EXCEPTION_SCHEMA,
        "path": entry.path,
        "max_lines": entry.max_lines,
        "max_sloc": entry.max_sloc,
        "owner": entry.owner,
        "reason": entry.reason,
        "target_sloc": entry.target_sloc,
        "target_date": entry.target_date.isoformat(),
        "baseline_commit": entry.baseline_commit,
    }
    if payload != expected:
        return f"accepted_adr exception payload does not bind the exact debt entry: {relative}"
    return None


def _relative_to_repo(path: Path, repo_root: Path) -> str:
    return (
        confined_repo_path(repo_root, path, label="Module-size input")
        .relative_to(Path(repo_root).absolute())
        .as_posix()
    )


def _load_previous_baseline(repo_root: Path, base_sha: str, relative: str) -> _BaselineState:
    tracked = _git(repo_root, "ls-tree", "-r", "--name-only", base_sha, "--", relative)
    if tracked != relative:
        return None, False
    raw = _git_bytes(repo_root, "show", f"{base_sha}:{relative}")
    if is_legacy_empty_baseline(raw, source=f"{base_sha}:{relative}"):
        return None, True
    return decode_module_size_baseline(raw, source=f"{base_sha}:{relative}"), False


def _diff_status(
    repo_root: Path, base_sha: str, head_sha: str, *, ignored_path: str
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...], tuple[str, ...]]:
    raw = _git_bytes(repo_root, "diff", "--name-status", "-z", "--find-renames=100%", base_sha, head_sha)
    parts = raw.decode("utf-8").split("\0")
    statuses: list[str] = []
    renames: list[tuple[str, str]] = []
    deleted: list[str] = []
    index = 0
    while index < len(parts) and parts[index]:
        status = parts[index]
        index += 1
        if status.startswith("R"):
            old_path, new_path = parts[index], parts[index + 1]
            index += 2
            if old_path == ignored_path or new_path == ignored_path:
                continue
            statuses.append(status)
            renames.append((old_path, new_path))
        else:
            changed_path = parts[index]
            index += 1
            if changed_path == ignored_path:
                continue
            statuses.append(status)
            if status == "D":
                deleted.append(changed_path)
    return tuple(statuses), tuple(renames), tuple(deleted)


def _is_ancestor(repo_root: Path, ancestor: str, descendant: str) -> bool:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "merge-base", "--is-ancestor", ancestor, descendant],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _has_symlink_component(path: Path, repo_root: Path) -> bool:
    try:
        relative = path.relative_to(repo_root)
    except ValueError:
        return True
    current = repo_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            return True
    return False


def _git(repo_root: Path, *args: str, error: str | None = None) -> str:
    return _git_bytes(repo_root, *args, error=error).decode("utf-8").strip()


def _git_bytes(repo_root: Path, *args: str, error: str | None = None) -> bytes:
    try:
        result = subprocess.run(["git", "-C", str(repo_root), *args], check=True, capture_output=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ModuleSizeBaselineError(error or f"Git history lookup failed: {' '.join(args)}") from exc
    return result.stdout


__all__ = [
    "AUDITED_BOOTSTRAP_COMMIT",
    "ModuleSizeBaseline",
    "ModuleSizeBaselineError",
    "ModuleSizeDebtEntry",
    "ModuleSizeGitContext",
    "ModuleSizePolicyIssue",
    "confined_repo_path",
    "decode_module_size_baseline",
    "is_legacy_empty_baseline",
    "load_module_size_baseline",
    "resolve_module_size_git_context",
    "validate_module_size_baseline",
    "write_module_size_baseline",
]
