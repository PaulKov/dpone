"""Public-root adoption does not prove or relax historical ancestry."""

import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.metrics.module_size_baseline import ModuleSizeBaselineError
from dpone.metrics.module_size_public_snapshot import PUBLIC_ROOT, PublicSnapshotDebt

ROOT = Path(__file__).parents[1]


def read_source(path):
    return subprocess.check_output(["git", "-C", str(ROOT), "show", f"{PUBLIC_ROOT}:{path}"])


def test_public_root_caps_are_exact_and_only_provenance_changes():
    debt = PublicSnapshotDebt.load(read_source)
    assert len(debt.baseline.entries) == 51
    for prior in debt.baseline.entries:
        candidate = replace(prior, baseline_commit=PUBLIC_ROOT)
        assert debt.permits(candidate, prior)
        assert not debt.permits(candidate, None)
        assert not debt.permits(prior, prior)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_lines", 999),
        ("max_sloc", 999),
        ("owner", "other"),
        ("reason", "other"),
        ("target_sloc", 1),
        ("baseline_commit", "f" * 40),
        ("accepted_adr", "docs/adr/other.md"),
    ],
)
def test_adoption_rejects_modified_contract(field, value):
    debt = PublicSnapshotDebt.load(read_source)
    prior = debt.baseline.entries[0]
    candidate = replace(prior, baseline_commit=PUBLIC_ROOT)
    assert not debt.permits(replace(candidate, **{field: value}), prior)
    assert not debt.permits(candidate, replace(prior, baseline_commit="e" * 40))


def test_adoption_preserves_already_tightened_caps():
    debt = PublicSnapshotDebt.load(read_source)
    prior = replace(debt.baseline.entries[0], max_lines=390, max_sloc=351)
    assert debt.permits(replace(prior, baseline_commit=PUBLIC_ROOT), prior)


@pytest.mark.parametrize("mutation", ["ledger", "source", "missing"])
def test_unverified_public_source_cannot_authorize_transition(mutation):
    def read(path):
        if mutation == "missing":
            raise ModuleSizeBaselineError("missing root")
        payload = read_source(path)
        if (path.endswith(".json")) == (mutation == "ledger"):
            return payload + b"\n# changed\n"
        return payload

    with pytest.raises(ModuleSizeBaselineError):
        PublicSnapshotDebt.load(read)


def test_transition_passes_real_ancestry_and_keeps_ordinary_missing_history_failure():
    from datetime import date

    from dpone.metrics.module_size_baseline import ModuleSizeBaseline
    from dpone.metrics.module_size_policy import ModuleSizeGitContext, validate_module_size_baseline

    debt = PublicSnapshotDebt.load(read_source)
    head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    context = ModuleSizeGitContext(PUBLIC_ROOT, head, debt.baseline, (), (), False)
    migrated = ModuleSizeBaseline(tuple(replace(row, baseline_commit=PUBLIC_ROOT) for row in debt.baseline.entries))
    assert not validate_module_size_baseline(migrated, repo_root=ROOT, git_context=context, as_of=date(2026, 9, 9))
    rejected = validate_module_size_baseline(debt.baseline, repo_root=ROOT, git_context=context, as_of=date(2026, 9, 9))
    assert len([issue for issue in rejected if "ancestor" in issue.message]) == 51
