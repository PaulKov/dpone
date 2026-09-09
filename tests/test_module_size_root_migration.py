from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from tools.migrate_module_size_root import build_candidate, main

import dpone.metrics.module_size_root_migration as migration
from dpone.metrics.module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    encode_module_size_baseline,
)
from dpone.metrics.module_size_policy import resolve_module_size_git_context, validate_module_size_baseline


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", message)
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def audited_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, ModuleSizeDebtEntry]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Fixture")
    _git(repo, "config", "user.email", "fixture@example.invalid")
    source = repo / "src/dpone/legacy.py"
    source.parent.mkdir(parents=True)
    source.write_text("value = 1\n" * 360)
    entry = ModuleSizeDebtEntry(
        path="src/dpone/legacy.py",
        max_lines=360,
        max_sloc=360,
        owner="architecture",
        reason="audited fixture",
        target_sloc=350,
        target_date=date(2099, 12, 31),
        accepted_adr=None,
        baseline_commit="e" * 40,
    )
    baseline = repo / migration.BASELINE_PATH
    baseline.parent.mkdir(parents=True)
    baseline.write_bytes(encode_module_size_baseline(ModuleSizeBaseline((entry,))))
    budgets = repo / migration.BUDGET_PATH
    budgets.parent.mkdir(parents=True)
    budgets.write_text("global:\n  warn_loc: 450\n  max_loc: 600\n  warn_sloc: 350\n  max_sloc: 400\n")
    root = _commit(repo, "root fixture")
    policy = migration.RootMigrationPolicy(
        root_commit=root,
        legacy_commit=entry.baseline_commit,
        baseline_sha256=hashlib.sha256(baseline.read_bytes()).hexdigest(),
        budgets_sha256=hashlib.sha256(budgets.read_bytes()).hexdigest(),
        expected_entries=1,
    )
    monkeypatch.setattr(migration, "ROOT_POLICY", policy)
    (repo / "README.md").write_text("successor\n")
    _commit(repo, "successor fixture")
    return repo, entry


def _context(repo: Path):
    return resolve_module_size_git_context(
        repo_root=repo,
        baseline_path=repo / migration.BASELINE_PATH,
        base_ref=migration.ROOT_POLICY.root_commit,
        head_ref=_git(repo, "rev-parse", "HEAD"),
    )


def _issues(repo: Path, entry: ModuleSizeDebtEntry):
    return validate_module_size_baseline(
        ModuleSizeBaseline((entry,)),
        repo_root=repo,
        git_context=_context(repo),
        as_of=date(2026, 9, 9),
        budget_limits=(450, 600, 350, 400),
    )


def test_exact_root_reanchor_preserves_ordinary_validation(audited_root) -> None:
    repo, original = audited_root
    reanchored = replace(original, baseline_commit=migration.ROOT_POLICY.root_commit)
    assert _issues(repo, reanchored) == ()
    assert any("unavailable" in item.message for item in _issues(repo, original))


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner", "other"),
        ("reason", "changed"),
        ("target_date", date(2100, 1, 1)),
        ("target_sloc", 351),
        ("accepted_adr", "docs/adr/missing.md"),
        ("max_lines", 361),
        ("max_sloc", 361),
        ("baseline_commit", "a" * 40),
    ],
)
def test_reanchor_cannot_relax_metadata_or_caps(audited_root, field, value) -> None:
    repo, original = audited_root
    candidate = (
        replace(original, baseline_commit=migration.ROOT_POLICY.root_commit, **{field: value})
        if field != "baseline_commit"
        else replace(original, baseline_commit=value)
    )
    assert _issues(repo, candidate)


@pytest.mark.parametrize("field", ["baseline_sha256", "budgets_sha256"])
def test_corrupt_anchor_digest_fails_closed(audited_root, monkeypatch, field) -> None:
    repo, original = audited_root
    monkeypatch.setattr(migration, "ROOT_POLICY", replace(migration.ROOT_POLICY, **{field: "0" * 64}))
    with pytest.raises(ModuleSizeBaselineError, match="digest"):
        _issues(repo, replace(original, baseline_commit=migration.ROOT_POLICY.root_commit))


def test_head_budget_change_is_not_absorbed(audited_root) -> None:
    repo, original = audited_root
    path = repo / migration.BUDGET_PATH
    path.write_text(path.read_text().replace("max_sloc: 400", "max_sloc: 500"))
    _commit(repo, "attempt budget relaxation")
    with pytest.raises(ModuleSizeBaselineError, match="budgets"):
        _issues(repo, replace(original, baseline_commit=migration.ROOT_POLICY.root_commit))


def test_unrelated_base_has_no_root_exception(audited_root) -> None:
    repo, original = audited_root
    context = replace(_context(repo), base_sha="b" * 40)
    candidate = replace(original, baseline_commit=migration.ROOT_POLICY.root_commit)
    assert validate_module_size_baseline(
        ModuleSizeBaseline((candidate,)),
        repo_root=repo,
        git_context=context,
        as_of=date(2026, 9, 9),
    )


def test_non_root_commit_cannot_be_approved_as_root(audited_root, monkeypatch) -> None:
    repo, original = audited_root
    child = _git(repo, "rev-parse", "HEAD")
    monkeypatch.setattr(migration, "ROOT_POLICY", replace(migration.ROOT_POLICY, root_commit=child))
    (repo / "README.md").write_text("third\n")
    _commit(repo, "third")
    with pytest.raises(ModuleSizeBaselineError, match="root commit"):
        _issues(repo, replace(original, baseline_commit=child))


def test_producer_only_writes_candidate_and_final_head_can_pass(audited_root, capsys) -> None:
    repo, original = audited_root
    head = _git(repo, "rev-parse", "HEAD")
    assert main(["--repo-root", str(repo), "--head-ref", head, "--write-baseline"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "CANDIDATE_WRITTEN"
    assert payload["ok"] is False
    written = json.loads((repo / migration.BASELINE_PATH).read_text())["debt"][original.path]
    assert written["baseline_commit"] == migration.ROOT_POLICY.root_commit
    assert written["max_sloc"] == original.max_sloc
    _commit(repo, "reviewed candidate")
    assert _issues(repo, replace(original, baseline_commit=migration.ROOT_POLICY.root_commit)) == ()
    assert main(["--repo-root", str(repo), "--head-ref", _git(repo, "rev-parse", "HEAD"), "--write-baseline"]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "NO_CHANGE"


def test_producer_dry_run_does_not_write(audited_root, capsys) -> None:
    repo, _ = audited_root
    before = (repo / migration.BASELINE_PATH).read_bytes()
    assert main(["--repo-root", str(repo), "--head-ref", _git(repo, "rev-parse", "HEAD")]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "CANDIDATE_READY"
    assert (repo / migration.BASELINE_PATH).read_bytes() == before


@pytest.mark.parametrize("operation", ["growth", "hard_growth", "new_debt", "dirty", "expired"])
def test_producer_rejects_nonpassing_candidates(audited_root, operation) -> None:
    repo, _ = audited_root
    path = repo / "src/dpone/legacy.py"
    if operation == "growth":
        path.write_text(path.read_text() + "grown = 1\n")
    elif operation == "hard_growth":
        path.write_text("grown = 1\n" * 401)
    elif operation == "new_debt":
        (repo / "src/dpone/new.py").write_text("new = 1\n" * 360)
    elif operation == "dirty":
        path.write_text(path.read_text() + "dirty = 1\n")
    if operation in {"growth", "hard_growth", "new_debt"}:
        _commit(repo, "candidate condition")
    as_of = date(2100, 1, 1) if operation == "expired" else date(2026, 9, 9)
    with pytest.raises(ModuleSizeBaselineError):
        build_candidate(repo, _git(repo, "rev-parse", "HEAD"), as_of=as_of)


def test_producer_tightens_but_does_not_regrant_headroom(audited_root) -> None:
    repo, _ = audited_root
    (repo / "src/dpone/legacy.py").write_text("value = 1\n" * 359)
    _commit(repo, "actual improvement")
    candidate, _, _ = build_candidate(repo, _git(repo, "rev-parse", "HEAD"), as_of=date(2026, 9, 9))
    assert candidate.entries[0].max_sloc == candidate.entries[0].max_lines == 359


def test_root_mode_still_requires_distinct_commits(audited_root) -> None:
    repo, _ = audited_root
    with pytest.raises(ModuleSizeBaselineError):
        build_candidate(repo, migration.ROOT_POLICY.root_commit, as_of=date(2026, 9, 9))


def test_previous_ledger_substitution_is_rejected(audited_root) -> None:
    repo, original = audited_root
    context = _context(repo)
    with pytest.raises(ModuleSizeBaselineError, match="prior ledger"):
        migration.load_root_migration(
            repo_root=repo,
            base_sha=context.base_sha,
            head_sha=context.head_sha,
            previous=ModuleSizeBaseline((replace(original, max_sloc=359),)),
        )


def test_unexpected_cohort_size_is_rejected(audited_root, monkeypatch) -> None:
    repo, original = audited_root
    monkeypatch.setattr(migration, "ROOT_POLICY", replace(migration.ROOT_POLICY, expected_entries=2))
    with pytest.raises(ModuleSizeBaselineError, match="cohort"):
        _issues(repo, replace(original, baseline_commit=migration.ROOT_POLICY.root_commit))


def test_reanchored_descendants_use_normal_continuity(audited_root) -> None:
    repo, original = audited_root
    reanchored = replace(original, baseline_commit=migration.ROOT_POLICY.root_commit)
    (repo / migration.BASELINE_PATH).write_bytes(encode_module_size_baseline(ModuleSizeBaseline((reanchored,))))
    base = _commit(repo, "migration")
    (repo / "README.md").write_text("ordinary successor\n")
    head = _commit(repo, "normal continuity")
    context = resolve_module_size_git_context(
        repo_root=repo,
        baseline_path=repo / migration.BASELINE_PATH,
        base_ref=base,
        head_ref=head,
    )
    assert (
        migration.load_root_migration(
            repo_root=repo,
            base_sha=base,
            head_sha=head,
            previous=context.previous_baseline,
        )
        is None
    )
    assert (
        validate_module_size_baseline(
            ModuleSizeBaseline((reanchored,)),
            repo_root=repo,
            git_context=context,
            as_of=date(2026, 9, 9),
        )
        == ()
    )
    assert validate_module_size_baseline(
        ModuleSizeBaseline((replace(reanchored, max_sloc=361),)),
        repo_root=repo,
        git_context=context,
        as_of=date(2026, 9, 9),
    )


def test_root_source_caps_are_independently_measured(audited_root, monkeypatch) -> None:
    repo, original = audited_root
    read_blob = migration._blob

    def altered_source(repo_root, commit, path):
        if path == original.path and commit == migration.ROOT_POLICY.root_commit:
            return b"shorter = 1\n" * 359
        return read_blob(repo_root, commit, path)

    monkeypatch.setattr(migration, "_blob", altered_source)
    with pytest.raises(ModuleSizeBaselineError, match="exact audited source size"):
        _issues(repo, replace(original, baseline_commit=migration.ROOT_POLICY.root_commit))


def test_arbitrary_root_id_cannot_activate_migration(audited_root, monkeypatch) -> None:
    repo, original = audited_root
    pinned = migration.ROOT_POLICY.root_commit
    monkeypatch.setattr(migration, "ROOT_POLICY", replace(migration.ROOT_POLICY, root_commit="b" * 40))
    context = resolve_module_size_git_context(
        repo_root=repo,
        baseline_path=repo / migration.BASELINE_PATH,
        base_ref=pinned,
        head_ref=_git(repo, "rev-parse", "HEAD"),
    )
    assert validate_module_size_baseline(
        ModuleSizeBaseline((replace(original, baseline_commit=pinned),)),
        repo_root=repo,
        git_context=context,
        as_of=date(2026, 9, 9),
    )


def test_rejected_producer_does_not_write_or_claim_pass(audited_root, capsys) -> None:
    repo, _ = audited_root
    before = (repo / migration.BASELINE_PATH).read_bytes()
    (repo / "src/dpone/legacy.py").write_text("grown = 1\n" * 401)
    head = _commit(repo, "hard limit violation")
    assert main(["--repo-root", str(repo), "--head-ref", head, "--write-baseline"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "REJECTED"
    assert payload["ok"] is False
    assert (repo / migration.BASELINE_PATH).read_bytes() == before
