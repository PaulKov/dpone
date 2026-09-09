"""One audited v2-ledger reanchor after the approved source-history replacement.

This is not a missing-history fallback. Only the pinned, parentless clean root
can supply the prior cohort; its ledger, budgets, and exact source measurements
are independently checked. Normal ancestry and no-growth policy still apply.
"""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass, replace
from pathlib import Path

from .loc import count_lines, count_sloc
from .module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    decode_module_size_baseline,
)

BASELINE_PATH = "docs/module_size_baseline.json"
BUDGET_PATH = "docs/benchmarks/quality_budgets.yml"


@dataclass(frozen=True)
class RootMigrationPolicy:
    root_commit: str
    legacy_commit: str
    baseline_sha256: str
    budgets_sha256: str
    expected_entries: int


ROOT_POLICY = RootMigrationPolicy(
    root_commit="f8c6a4a5e75d167829c05f65d5d3033acb193878",
    legacy_commit="e1d93822b47234e940829319cac0dc9678f6906c",
    baseline_sha256="0858ac904defebd34c5b8fc94d886da802c78d81565aa286747b660dfd7b821f",
    budgets_sha256="19bd5266ca63bdb3c01f0c9a5396ea72a78e2d9309a4ce53ac86fb26e55114fe",
    expected_entries=51,
)


@dataclass(frozen=True)
class VerifiedRootMigration:
    root_commit: str
    baseline: ModuleSizeBaseline


def load_root_migration(
    *,
    repo_root: Path,
    base_sha: str,
    head_sha: str,
    previous: ModuleSizeBaseline | None,
) -> VerifiedRootMigration | None:
    """Prove the one fixed root cohort or return no exception for any other base."""

    policy = ROOT_POLICY
    if base_sha != policy.root_commit:
        return None
    if base_sha == head_sha:
        raise ModuleSizeBaselineError("Root migration requires distinct base and head commits")
    if _git(repo_root, "show", "-s", "--format=%P", policy.root_commit).strip():
        raise ModuleSizeBaselineError("Root migration anchor must be a parentless root commit")
    _git(repo_root, "merge-base", "--is-ancestor", policy.root_commit, head_sha)
    baseline_raw = _blob(repo_root, policy.root_commit, BASELINE_PATH)
    _check_digest(baseline_raw, policy.baseline_sha256, "root baseline")
    budgets_raw = _blob(repo_root, policy.root_commit, BUDGET_PATH)
    _check_digest(budgets_raw, policy.budgets_sha256, "root budgets")
    _check_digest(_blob(repo_root, head_sha, BUDGET_PATH), policy.budgets_sha256, "head budgets")
    baseline = decode_module_size_baseline(baseline_raw, source="audited clean-root baseline")
    if baseline != previous or len(baseline.entries) != policy.expected_entries:
        raise ModuleSizeBaselineError("Root migration prior ledger does not match the audited cohort")
    for entry in baseline.entries:
        if entry.baseline_commit != policy.legacy_commit or entry.accepted_adr is not None:
            raise ModuleSizeBaselineError("Root migration cannot adopt another provenance or ADR cohort")
        try:
            source = _blob(repo_root, policy.root_commit, entry.path).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ModuleSizeBaselineError("Root migration source must be UTF-8") from exc
        if (count_lines(source), count_sloc(source)) != (entry.max_lines, entry.max_sloc):
            raise ModuleSizeBaselineError(f"Root migration cap is not the exact audited source size: {entry.path}")
    return VerifiedRootMigration(policy.root_commit, baseline)


def normalized_root_prior(
    entry: ModuleSizeDebtEntry,
    prior: ModuleSizeDebtEntry | None,
    migration: VerifiedRootMigration | None,
) -> ModuleSizeDebtEntry | None:
    """Normalize only the authorized provenance field; never metadata or caps."""

    if migration is None or prior is None or entry.baseline_commit != migration.root_commit:
        return prior
    if prior not in migration.baseline.entries:
        return prior
    same_metadata = (
        replace(
            entry,
            max_lines=prior.max_lines,
            max_sloc=prior.max_sloc,
            baseline_commit=prior.baseline_commit,
        )
        == prior
    )
    if not same_metadata or entry.max_lines > prior.max_lines or entry.max_sloc > prior.max_sloc:
        return prior
    return replace(prior, baseline_commit=migration.root_commit)


def reanchor_candidate_entries(
    baseline: ModuleSizeBaseline,
    migration: VerifiedRootMigration,
) -> ModuleSizeBaseline:
    """Construct only recognized root entries; this does not certify the candidate."""

    old_by_path = {entry.path: entry for entry in migration.baseline.entries}
    candidates: list[ModuleSizeDebtEntry] = []
    for entry in baseline.entries:
        prior = old_by_path.get(entry.path)
        if prior is None or entry.baseline_commit not in {prior.baseline_commit, migration.root_commit}:
            raise ModuleSizeBaselineError("Root migration cannot add debt or adopt unrelated provenance")
        candidate = replace(entry, baseline_commit=migration.root_commit)
        if normalized_root_prior(candidate, prior, migration) == prior:
            raise ModuleSizeBaselineError("Root migration cannot relax caps, ownership, deadline or other metadata")
        candidates.append(candidate)
    return ModuleSizeBaseline(tuple(candidates))


def _check_digest(raw: bytes, expected: str, label: str) -> None:
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ModuleSizeBaselineError(f"Root migration {label} digest does not match the audited anchor")


def _blob(repo_root: Path, commit: str, path: str) -> bytes:
    record = _git(repo_root, "ls-tree", "-z", commit, "--", path)
    parts = record.rstrip(b"\0").split(b"\t", 1)
    if len(parts) != 2 or parts[1].decode("utf-8") != path:
        raise ModuleSizeBaselineError("Root migration input is not one exact tracked file")
    fields = parts[0].split()
    if len(fields) != 3 or fields[0] not in {b"100644", b"100755"} or fields[1] != b"blob":
        raise ModuleSizeBaselineError("Root migration input must be a regular Git blob")
    return _git(repo_root, "cat-file", "blob", fields[2].decode("ascii"))


def _git(repo_root: Path, *args: str) -> bytes:
    try:
        return subprocess.check_output(
            ["git", "--no-replace-objects", "-C", str(repo_root), *args],
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ModuleSizeBaselineError("Root migration Git identity or immutable input is unavailable") from exc
