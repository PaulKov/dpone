"""Machine-readable outcome for failure-safe module-size baseline writes."""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from dpone.manifest.project_root import ProjectRootIdentity, inspect_project_root, verify_project_root

from .module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    confined_repo_path,
    encode_module_size_baseline,
    exact_sha,
    write_confined_module_size_baseline_bytes,
)

if TYPE_CHECKING:
    from .module_size import ModuleSizeReport, ModuleSizeThresholds
    from .module_size_baseline import ModuleSizePolicyIssue
    from .module_size_policy import ModuleSizeGitContext


class ModuleSizeCandidateSnapshot(Protocol):
    @property
    def root_identity(self) -> ProjectRootIdentity: ...

    @property
    def head_sha(self) -> str: ...

    @property
    def baseline(self) -> bytes: ...


class ModuleSizeSnapshotLoader(Protocol):
    def __call__(
        self,
        *,
        repo_root: Path,
        package_dir: Path,
        baseline_path: Path,
        head_sha: str,
    ) -> ModuleSizeCandidateSnapshot: ...


@dataclass(frozen=True)
class ModuleSizeBaselineWriteOutcome:
    changed: bool
    baseline_path: str
    base_sha: str
    head_sha: str

    @property
    def status(self) -> str:
        return "CANDIDATE_WRITTEN" if self.changed else "NO_CHANGE"


def format_module_size_write_outcome(outcome: ModuleSizeBaselineWriteOutcome, *, fmt: str) -> str | dict[str, Any]:
    next_action = (
        "Commit the candidate and rerun against the new exact head without --write-baseline."
        if outcome.changed
        else "No file was written; rerun the exact head without --write-baseline."
    )
    if fmt == "text":
        return (
            f"Module-size baseline write status: {outcome.status}\n"
            f"Changed: {str(outcome.changed).lower()}\n"
            f"Baseline: {outcome.baseline_path}\n"
            f"Next action: {next_action}\n"
        )
    return {
        "schema_version": "dpone.module-size-baseline-write.v2",
        "ok": False,
        "status": outcome.status,
        "changed": outcome.changed,
        "baseline_path": outcome.baseline_path,
        "base_sha": outcome.base_sha,
        "head_sha": outcome.head_sha,
        "next_action": next_action,
    }


def ratchet_module_size_baseline(
    baseline: ModuleSizeBaseline,
    *,
    report: ModuleSizeReport,
    thresholds: ModuleSizeThresholds,
    bootstrap: bool,
    bootstrap_commit: str,
    git_context: ModuleSizeGitContext,
    policy_issues: tuple[ModuleSizePolicyIssue, ...],
) -> ModuleSizeBaseline:
    """Build one lower-or-equal exact-cap candidate without writing it."""

    if policy_issues:
        raise ModuleSizeBaselineError("Cannot rewrite a baseline with policy violations")
    hard = [issue for issue in report.issues if "hard max" in issue.message]
    if hard:
        raise ModuleSizeBaselineError("Hard module-size violations must be fixed before baseline rewrite")
    existing = {entry.path: entry for entry in baseline.entries}
    current_paths = {item.path for item in report.items}
    renamed_from = {new: old for old, new in git_context.exact_renames}
    _reject_unexplained_missing(existing, current_paths=current_paths, git_context=git_context)

    entries: list[ModuleSizeDebtEntry] = []
    for item in sorted(report.items, key=lambda value: value.path):
        warning = item.lines > thresholds.warn_lines or (
            thresholds.warn_sloc is not None and item.sloc > thresholds.warn_sloc
        )
        if not warning:
            continue
        prior = existing.get(item.path)
        if prior is None and git_context.rename_only:
            old_path = renamed_from.get(item.path)
            prior = existing.get(old_path) if old_path else None
        if prior is None and not bootstrap:
            raise ModuleSizeBaselineError(
                f"Cannot create new debt entry automatically: {item.path}; an Accepted ADR is required"
            )
        entries.append(
            _bootstrap_entry(
                item.path,
                lines=item.lines,
                sloc=item.sloc,
                thresholds=thresholds,
                bootstrap_commit=bootstrap_commit,
            )
            if prior is None
            else _tightened_entry(prior, path=item.path, lines=item.lines, sloc=item.sloc)
        )
    return ModuleSizeBaseline(entries=tuple(entries))


def write_baseline_if_unchanged(
    *,
    repo_root: Path,
    baseline_path: Path,
    baseline: ModuleSizeBaseline,
    expected_bytes: bytes,
    expected_head_sha: str,
    root_identity: ProjectRootIdentity | None = None,
) -> None:
    """Write one candidate only while the evaluated Git and file identity holds."""

    expected_head = exact_sha(expected_head_sha, label="module-size transaction head")
    if root_identity is None:
        try:
            root_identity = inspect_project_root(repo_root)
        except OSError as exc:
            raise ModuleSizeBaselineError("Module-size repository root could not be identified safely") from exc
        assert root_identity is not None
    target = confined_repo_path(repo_root, baseline_path, label="Module-size baseline")
    try:
        verify_project_root(root_identity)
        current_head = _git(repo_root, "rev-parse", "HEAD^{commit}")
        if current_head != expected_head:
            raise ModuleSizeBaselineError("Module-size inputs changed during evaluation: checked-out HEAD changed")
        try:
            current_bytes = target.read_bytes()
        except OSError as exc:
            raise ModuleSizeBaselineError(
                "Module-size inputs changed during evaluation: baseline is unreadable"
            ) from exc
        if current_bytes != expected_bytes:
            raise ModuleSizeBaselineError("Module-size inputs changed during evaluation: baseline bytes changed")
        candidate_bytes = encode_module_size_baseline(baseline)
        try:
            _write_transaction_bytes(
                repo_root,
                target,
                content=candidate_bytes,
                expected_bytes=expected_bytes,
                root_identity=root_identity,
            )
        except ModuleSizeBaselineError as exc:
            raise ModuleSizeBaselineError(
                "Module-size baseline write failed without a certifiable candidate; inspect transaction recovery state"
            ) from exc
        try:
            post_write_head = _git(repo_root, "rev-parse", "HEAD^{commit}")
        except ModuleSizeBaselineError as exc:
            _restore_previous_baseline(
                repo_root,
                target,
                expected_bytes=expected_bytes,
                candidate_bytes=candidate_bytes,
                root_identity=root_identity,
            )
            raise ModuleSizeBaselineError(
                "Module-size post-write Git identity is unavailable; the prior baseline was restored"
            ) from exc
        if post_write_head != expected_head:
            _restore_previous_baseline(
                repo_root,
                target,
                expected_bytes=expected_bytes,
                candidate_bytes=candidate_bytes,
                root_identity=root_identity,
            )
            raise ModuleSizeBaselineError(
                "Module-size inputs changed during evaluation: checked-out HEAD changed during write"
            )
        verify_project_root(root_identity)
    except OSError as exc:
        raise ModuleSizeBaselineError("Module-size repository root changed during the baseline transaction") from exc


def persist_baseline_candidate(
    *,
    repo_root: Path,
    package_dir: Path,
    baseline_path: Path,
    baseline: ModuleSizeBaseline,
    snapshot: ModuleSizeCandidateSnapshot,
    snapshot_loader: ModuleSizeSnapshotLoader,
    authoring_lock: Callable[[Path], AbstractContextManager[None]],
) -> None:
    """Serialize snapshot revalidation and one exact-identity candidate write."""

    with authoring_lock(repo_root):
        current = snapshot_loader(
            repo_root=repo_root,
            package_dir=package_dir,
            baseline_path=baseline_path,
            head_sha=snapshot.head_sha,
        )
        if current != snapshot:
            raise ModuleSizeBaselineError("Module-size inputs changed during evaluation")
        write_baseline_if_unchanged(
            repo_root=repo_root,
            baseline_path=baseline_path,
            baseline=baseline,
            expected_bytes=snapshot.baseline,
            expected_head_sha=snapshot.head_sha,
            root_identity=snapshot.root_identity,
        )


def _reject_unexplained_missing(
    existing: dict[str, ModuleSizeDebtEntry],
    *,
    current_paths: set[str],
    git_context: ModuleSizeGitContext,
) -> None:
    renamed = {old for old, _new in git_context.exact_renames}
    for missing in sorted(set(existing) - current_paths):
        if missing not in git_context.deleted_paths and missing not in renamed:
            raise ModuleSizeBaselineError(
                f"Cannot retire stale baseline entry without an exact Git deletion: {missing}"
            )


def _restore_previous_baseline(
    repo_root: Path,
    target: Path,
    *,
    expected_bytes: bytes,
    candidate_bytes: bytes,
    root_identity: ProjectRootIdentity,
) -> None:
    """Restore only bytes written by this transaction; never clobber another editor."""

    try:
        _write_transaction_bytes(
            repo_root,
            target,
            content=expected_bytes,
            expected_bytes=candidate_bytes,
            root_identity=root_identity,
        )
    except ModuleSizeBaselineError as exc:
        raise ModuleSizeBaselineError(
            "Module-size HEAD changed during write; concurrent bytes were preserved or rollback requires recovery"
        ) from exc


def _write_transaction_bytes(
    repo_root: Path,
    target: Path,
    *,
    content: bytes,
    expected_bytes: bytes,
    root_identity: ProjectRootIdentity,
) -> None:
    relative = target.relative_to(Path(repo_root).absolute())
    write_confined_module_size_baseline_bytes(
        anchor=root_identity.path,
        parts=relative.parts,
        content=content,
        expected_bytes=expected_bytes,
        root_identity=root_identity,
    )


def _bootstrap_entry(
    path: str,
    *,
    lines: int,
    sloc: int,
    thresholds: ModuleSizeThresholds,
    bootstrap_commit: str,
) -> ModuleSizeDebtEntry:
    return ModuleSizeDebtEntry(
        path=path,
        max_lines=lines,
        max_sloc=sloc,
        owner="architecture",
        reason="audited pre-existing debt at DPONE-CI-SHADOW-CLOSURE bootstrap",
        target_sloc=max(1, min(thresholds.warn_sloc or sloc, sloc - 1)),
        target_date=date(2026, 12, 31),
        accepted_adr=None,
        baseline_commit=bootstrap_commit,
    )


def _tightened_entry(prior: ModuleSizeDebtEntry, *, path: str, lines: int, sloc: int) -> ModuleSizeDebtEntry:
    if lines > prior.max_lines or sloc > prior.max_sloc:
        raise ModuleSizeBaselineError(f"Cannot increase exact baseline cap for {path}")
    return replace(prior, path=path, max_lines=lines, max_sloc=sloc)


def _git(repo_root: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ModuleSizeBaselineError(f"Git module-size transaction lookup failed: {' '.join(args)}") from exc
    return result.stdout.strip()


__all__ = [
    "ModuleSizeBaselineWriteOutcome",
    "format_module_size_write_outcome",
    "persist_baseline_candidate",
    "ratchet_module_size_baseline",
    "write_baseline_if_unchanged",
]
