from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import pytest

import dpone.metrics.module_size_confined_write as confined_write
import dpone.metrics.module_size_policy as module_size_policy
import dpone.metrics.module_size_snapshot as snapshot_module
from dpone.metrics.module_size_policy import (
    AUDITED_BOOTSTRAP_COMMIT,
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    ModuleSizeGitContext,
    load_module_size_baseline,
    resolve_module_size_git_context,
    validate_module_size_baseline,
    write_module_size_baseline,
)
from dpone.metrics.module_size_snapshot import load_module_size_head_snapshot
from dpone.metrics.module_size_write import ratchet_module_size_baseline


def _write(path: Path, line_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(f"value_{i} = {i}" for i in range(line_count)) + "\n", encoding="utf-8")


def _write_sized_source(path: Path, *, lines: int, sloc: int) -> None:
    assert 0 < sloc <= lines
    source = [*(f"value_{i} = {i}" for i in range(sloc)), *(f"# padding_{i}" for i in range(lines - sloc))]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(source) + "\n", encoding="utf-8")


def _entry(path: str, *, lines: int, sloc: int, commit: str = AUDITED_BOOTSTRAP_COMMIT) -> ModuleSizeDebtEntry:
    return ModuleSizeDebtEntry(
        path=path,
        max_lines=lines,
        max_sloc=sloc,
        owner="architecture",
        reason="audited legacy orchestration debt",
        target_sloc=min(350, sloc - 1),
        target_date=date(2099, 12, 31),
        accepted_adr=None,
        baseline_commit=commit,
    )


def _write_budgets(repo: Path) -> None:
    budgets = repo / "docs/benchmarks/quality_budgets.yml"
    budgets.parent.mkdir(parents=True, exist_ok=True)
    budgets.write_text(
        "global:\n  warn_loc: 450\n  max_loc: 600\n  warn_sloc: 350\n  max_sloc: 400\n",
        encoding="utf-8",
    )


def _write_bound_adr(repo: Path, entry: ModuleSizeDebtEntry, *, status: str = "Accepted.") -> None:
    assert entry.accepted_adr is not None
    payload = {
        "schema_version": "dpone.module-size-debt-exception.v2",
        "path": entry.path,
        "max_lines": entry.max_lines,
        "max_sloc": entry.max_sloc,
        "owner": entry.owner,
        "reason": entry.reason,
        "target_sloc": entry.target_sloc,
        "target_date": entry.target_date.isoformat(),
        "baseline_commit": entry.baseline_commit,
    }
    target = repo / entry.accepted_adr
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        f"# Debt exception\n\n## Status\n\n{status}\n\n"
        "## Debt exception\n\n"
        "<!-- dpone-module-size-debt-exception-v2\n"
        f"{json.dumps(payload, sort_keys=True)}\n-->\n",
        encoding="utf-8",
    )


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def _init_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test")
    _write(repo / "src/dpone/legacy.py", 360)
    _write_budgets(repo)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "base")
    return repo, _git(repo, "rev-parse", "HEAD")


@pytest.mark.parametrize(
    "payload,match",
    [
        ('{"schema_version":2,"schema_version":2,"debt":{}}', "duplicate JSON key"),
        ('{"schema_version":2,"debt":{},"extra":true}', "unknown fields"),
        ('{"schema_version":2,"debt":{"../escape.py":{}}}', "canonical confined"),
        ('{"schema_version":1,"debt":{}}', "schema_version must equal 2"),
    ],
)
def test_v2_baseline_is_closed_duplicate_free_and_confined(tmp_path: Path, payload: str, match: str) -> None:
    path = tmp_path / "baseline.json"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(ModuleSizeBaselineError, match=match):
        load_module_size_baseline(path)


def test_baseline_round_trip_is_atomic_canonical_and_rejects_expired_debt(tmp_path: Path) -> None:
    path = tmp_path / "docs/module_size_baseline.json"
    baseline = ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360),))
    write_module_size_baseline(path, baseline)

    assert load_module_size_baseline(path) == baseline
    assert path.read_text(encoding="utf-8").endswith("\n")
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []
    expired = replace(baseline.entries[0], target_date=date(2026, 8, 7))
    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(expired,)), repo_root=tmp_path, git_context=None, as_of=date(2026, 8, 8)
    )
    assert any("target_date expired" in issue.message for issue in issues)


def test_atomic_replace_failure_preserves_previous_baseline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "baseline.json"
    original = ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360),))
    write_module_size_baseline(path, original)
    previous = path.read_bytes()
    monkeypatch.setattr(
        confined_write.os,
        "rename",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("injected")),
    )
    with pytest.raises(ModuleSizeBaselineError, match="Cannot atomically persist"):
        write_module_size_baseline(
            path,
            ModuleSizeBaseline(entries=(replace(original.entries[0], max_lines=359, max_sloc=359),)),
        )
    assert path.read_bytes() == previous


def test_codec_rejects_bool_caps_duplicate_paths_and_symlinked_modules(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    entry = _entry("src/dpone/legacy.py", lines=360, sloc=360)
    with pytest.raises(ModuleSizeBaselineError, match="lossless canonical"):
        write_module_size_baseline(path, ModuleSizeBaseline(entries=(entry, entry)))
    write_module_size_baseline(path, ModuleSizeBaseline(entries=(entry,)))
    path.write_text(path.read_text(encoding="utf-8").replace('"max_lines": 360', '"max_lines": true'))
    with pytest.raises(ModuleSizeBaselineError, match="positive integer"):
        load_module_size_baseline(path)

    target = tmp_path / "outside.py"
    _write(target, 360)
    link = tmp_path / "src/dpone/legacy.py"
    link.parent.mkdir(parents=True)
    link.symlink_to(target)
    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(entry,)), repo_root=tmp_path, git_context=None, as_of=date(2026, 8, 8)
    )
    assert any("symlinked" in issue.message for issue in issues)


def test_authoritative_budgets_reject_semantically_oversized_debt() -> None:
    entries = (
        _entry("src/dpone/lines.py", lines=601, sloc=360),
        _entry("src/dpone/sloc.py", lines=450, sloc=401),
        replace(_entry("src/dpone/target.py", lines=450, sloc=360), target_sloc=351),
    )

    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=entries),
        repo_root=Path("."),
        git_context=None,
        as_of=date(2026, 8, 8),
        budget_limits=(450, 600, 350, 400),
    )

    assert {issue.message for issue in issues} >= {
        "max_lines exceeds authoritative max_loc=600",
        "max_sloc exceeds authoritative max_sloc=400",
        "target_sloc exceeds authoritative warn_sloc=350",
    }


def test_only_exact_accepted_adr_status_is_authoritative(tmp_path: Path) -> None:
    adr = tmp_path / "docs/adr/0047-module-size-debt-ratchet.md"
    adr.parent.mkdir(parents=True)
    adr.write_text("# ADR\n\n## Status\n\nAccepted, subject to owner attestation.\n", encoding="utf-8")
    entry = replace(
        _entry("src/dpone/legacy.py", lines=360, sloc=360),
        accepted_adr="docs/adr/0047-module-size-debt-ratchet.md",
    )
    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(entry,)), repo_root=tmp_path, git_context=None, as_of=date(2026, 8, 8)
    )
    assert any("not Accepted" in issue.message for issue in issues)


@pytest.mark.parametrize(
    "status",
    [
        "Accepted.\n\n## Status\n\nRejected.",
        "Accepted.\n\nRejected.",
        "Superseded by ADR 9999.",
    ],
)
def test_accepted_adr_rejects_duplicate_or_contradictory_status_sections(tmp_path: Path, status: str) -> None:
    entry = replace(
        _entry("src/dpone/legacy.py", lines=360, sloc=360),
        accepted_adr="docs/adr/0047-module-size-debt-ratchet.md",
    )
    _write_bound_adr(tmp_path, entry, status=status)

    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(entry,)), repo_root=tmp_path, git_context=None, as_of=date(2026, 8, 8)
    )

    assert any("not Accepted" in issue.message for issue in issues)


def test_previous_debt_cannot_disappear_during_exact_rename_even_after_shrink(tmp_path: Path) -> None:
    prior = _entry("src/dpone/legacy.py", lines=360, sloc=360)
    context = ModuleSizeGitContext(
        base_sha=prior.baseline_commit,
        head_sha=prior.baseline_commit,
        previous_baseline=ModuleSizeBaseline(entries=(prior,)),
        exact_renames=((prior.path, "src/dpone/renamed.py"),),
        deleted_paths=(),
        rename_only=True,
    )

    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=()),
        repo_root=tmp_path,
        git_context=context,
        as_of=date(2026, 8, 8),
        trusted_module_paths=frozenset({"src/dpone/renamed.py"}),
        current_module_sizes={"src/dpone/renamed.py": (300, 300)},
        warning_thresholds=(450, 350),
    )

    assert any("renamed debt entry" in issue.message for issue in issues)


def test_accepted_adr_binds_entry_but_never_authorizes_cap_growth_or_deadline_drift(tmp_path: Path) -> None:
    repo, base = _init_repo(tmp_path)
    prior = _entry("src/dpone/legacy.py", lines=360, sloc=360, commit=base)
    adr_path = "docs/adr/0047-module-size-debt-ratchet.md"
    accepted = replace(prior, accepted_adr=adr_path)
    _write_bound_adr(repo, accepted)
    context = ModuleSizeGitContext(
        base_sha=base,
        head_sha=base,
        previous_baseline=ModuleSizeBaseline(entries=(prior,)),
        exact_renames=(),
        deleted_paths=(),
        rename_only=False,
    )
    assert (
        validate_module_size_baseline(
            ModuleSizeBaseline(entries=(accepted,)), repo_root=repo, git_context=context, as_of=date(2026, 8, 8)
        )
        == ()
    )

    grown = replace(accepted, max_lines=361, max_sloc=361)
    _write_bound_adr(repo, grown)
    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(grown,)), repo_root=repo, git_context=context, as_of=date(2026, 8, 8)
    )
    assert any("caps cannot increase" in issue.message for issue in issues)
    extended = replace(prior, target_date=date(2100, 1, 1))
    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(extended,)), repo_root=repo, git_context=context, as_of=date(2026, 8, 8)
    )
    assert any("metadata changed" in issue.message for issue in issues)


def test_git_context_requires_exact_history_and_accepts_continuity(tmp_path: Path) -> None:
    repo, base = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    bootstrap_entry = _entry("src/dpone/legacy.py", lines=360, sloc=360, commit=base)
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=(bootstrap_entry,)))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "baseline")
    head = _git(repo, "rev-parse", "HEAD")

    context = resolve_module_size_git_context(
        repo_root=repo,
        baseline_path=baseline_path,
        base_ref=base,
        head_ref=head,
    )
    assert context.base_sha == base
    assert context.head_sha == head
    with pytest.raises(ModuleSizeBaselineError, match="full lowercase 40-character SHA"):
        resolve_module_size_git_context(repo_root=repo, baseline_path=baseline_path, base_ref="HEAD~1", head_ref=head)
    with pytest.raises(ModuleSizeBaselineError, match="checked-out HEAD"):
        resolve_module_size_git_context(repo_root=repo, baseline_path=baseline_path, base_ref=base, head_ref=base)
    with pytest.raises(ModuleSizeBaselineError, match="must identify distinct commits"):
        resolve_module_size_git_context(repo_root=repo, baseline_path=baseline_path, base_ref=head, head_ref=head)


def test_grandfathered_bootstrap_survives_a_descendant_base_with_the_legacy_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, audited = _init_repo(tmp_path)
    monkeypatch.setattr(module_size_policy, "AUDITED_BOOTSTRAP_COMMIT", audited)
    (repo / "docs/module_size_baseline.json").write_text('{"version":1,"entries":[]}\n', encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "retain legacy ledger on descendant base")
    descendant = _git(repo, "rev-parse", "HEAD")
    entry = _entry("src/dpone/legacy.py", lines=360, sloc=360, commit=audited)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=(entry,)))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "migrate descendant legacy ledger")
    head = _git(repo, "rev-parse", "HEAD")
    context = resolve_module_size_git_context(
        repo_root=repo,
        baseline_path=baseline_path,
        base_ref=descendant,
        head_ref=head,
    )
    assert context.base_has_legacy_baseline is True

    assert (
        validate_module_size_baseline(
            ModuleSizeBaseline(entries=(entry,)),
            repo_root=repo,
            git_context=context,
            as_of=date(2026, 8, 8),
        )
        == ()
    )
    rejected = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(entry,)),
        repo_root=repo,
        git_context=replace(context, base_has_legacy_baseline=False),
        as_of=date(2026, 8, 8),
    )
    assert any("lacks audited bootstrap" in issue.message for issue in rejected)


@pytest.mark.parametrize(
    ("base_lines", "base_sloc", "head_lines", "head_sloc"),
    ((355, 355, 358, 355), (360, 355, 360, 358)),
)
def test_descendant_legacy_bootstrap_rejects_regrowth_over_each_exact_base_metric(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    base_lines: int,
    base_sloc: int,
    head_lines: int,
    head_sloc: int,
) -> None:
    repo, audited = _init_repo(tmp_path)
    monkeypatch.setattr(module_size_policy, "AUDITED_BOOTSTRAP_COMMIT", audited)
    _write_sized_source(repo / "src/dpone/legacy.py", lines=base_lines, sloc=base_sloc)
    (repo / "docs/module_size_baseline.json").write_text('{"version":1,"entries":[]}\n', encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "shrink while retaining the legacy ledger")
    descendant = _git(repo, "rev-parse", "HEAD")

    _write_sized_source(repo / "src/dpone/legacy.py", lines=head_lines, sloc=head_sloc)
    regrown = _entry("src/dpone/legacy.py", lines=head_lines, sloc=head_sloc, commit=audited)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=(regrown,)))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "attempt to absorb descendant regrowth")
    head = _git(repo, "rev-parse", "HEAD")
    context = resolve_module_size_git_context(
        repo_root=repo,
        baseline_path=baseline_path,
        base_ref=descendant,
        head_ref=head,
    )

    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(regrown,)),
        repo_root=repo,
        git_context=context,
        as_of=date(2026, 8, 8),
    )

    assert any("lacks audited bootstrap" in issue.message for issue in issues)


def test_null_adr_continuity_accepts_only_a_pure_exact_rename(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    old = _entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin)
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=(old,)))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "baseline")
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "src/dpone/legacy.py").rename(repo / "src/dpone/renamed.py")
    renamed = replace(old, path="src/dpone/renamed.py")
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=(renamed,)))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "exact rename")
    head = _git(repo, "rev-parse", "HEAD")

    context = resolve_module_size_git_context(repo_root=repo, baseline_path=baseline_path, base_ref=base, head_ref=head)
    assert context.rename_only is True
    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(renamed,)), repo_root=repo, git_context=context, as_of=date(2026, 8, 8)
    )
    assert issues == ()

    mixed = ModuleSizeGitContext(
        base_sha=context.base_sha,
        head_sha=context.head_sha,
        previous_baseline=context.previous_baseline,
        exact_renames=context.exact_renames,
        deleted_paths=context.deleted_paths,
        rename_only=False,
    )
    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=(renamed,)), repo_root=repo, git_context=mixed, as_of=date(2026, 8, 8)
    )
    assert any("lacks audited bootstrap" in issue.message for issue in issues)


def test_multiple_exact_renames_do_not_authorize_null_adr_continuity(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    _write(repo / "src/dpone/second.py", 360)
    baseline_path = repo / "docs/module_size_baseline.json"
    old = (
        _entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),
        _entry("src/dpone/second.py", lines=360, sloc=360, commit=origin),
    )
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=old))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "two debt entries")
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "src/dpone/legacy.py").rename(repo / "src/dpone/first_renamed.py")
    (repo / "src/dpone/second.py").rename(repo / "src/dpone/second_renamed.py")
    renamed = (
        replace(old[0], path="src/dpone/first_renamed.py"),
        replace(old[1], path="src/dpone/second_renamed.py"),
    )
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=renamed))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "two exact renames")
    head = _git(repo, "rev-parse", "HEAD")

    context = resolve_module_size_git_context(repo_root=repo, baseline_path=baseline_path, base_ref=base, head_ref=head)
    issues = validate_module_size_baseline(
        ModuleSizeBaseline(entries=renamed), repo_root=repo, git_context=context, as_of=date(2026, 8, 8)
    )

    assert context.rename_only is False
    assert sum("lacks audited bootstrap" in issue.message for issue in issues) == 2


def test_exact_worktree_rejects_ignored_referenced_adr_not_present_at_head(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    adr_path = "docs/adr/0047-module-size-debt-ratchet.md"
    entry = replace(
        _entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),
        accepted_adr=adr_path,
    )
    write_module_size_baseline(baseline_path, ModuleSizeBaseline(entries=(entry,)))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "baseline references future ADR")
    head = _git(repo, "rev-parse", "HEAD")
    exclude = repo / ".git/info/exclude"
    exclude.write_text(exclude.read_text(encoding="utf-8") + f"\n{adr_path}\n", encoding="utf-8")
    _write_bound_adr(repo, entry)
    assert _git(repo, "status", "--porcelain", "--untracked-files=all") == ""

    with pytest.raises(ModuleSizeBaselineError, match=f"not one tracked HEAD blob: {adr_path}"):
        load_module_size_head_snapshot(
            repo_root=repo,
            package_dir=repo / "src/dpone",
            baseline_path=baseline_path,
            head_sha=head,
        )


def test_exact_head_snapshot_batches_source_blobs_in_constant_process_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, origin = _init_repo(tmp_path)
    for index in range(24):
        _write(repo / f"src/dpone/components/module_{index}.py", index + 1)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "many modules")
    head = _git(repo, "rev-parse", "HEAD")
    run = snapshot_module.subprocess.run
    cat_file_commands: list[tuple[str, ...]] = []

    def record_run(command: list[str], *args: Any, **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        if "cat-file" in command:
            cat_file_commands.append(tuple(command))
        return run(command, *args, **kwargs)

    monkeypatch.setattr(snapshot_module.subprocess, "run", record_run)
    snapshot = load_module_size_head_snapshot(
        repo_root=repo,
        package_dir=repo / "src/dpone",
        baseline_path=baseline_path,
        head_sha=head,
    )

    assert len(snapshot.sources) == 25
    assert len(cat_file_commands) == 3
    assert sum("--batch" in command for command in cat_file_commands) == 1


@pytest.mark.parametrize(
    "response,match",
    [
        (b"", "truncated"),
        (f"{'a' * 40} blob 2\nx\n".encode(), "identity mismatch"),
        (f"{'b' * 40} blob 1\nx\n".encode(), "identity mismatch"),
        (f"{'a' * 40} blob 1\nx\nextra".encode(), "unexpected data"),
    ],
)
def test_source_batch_rejects_malformed_framing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    response: bytes,
    match: str,
) -> None:
    monkeypatch.setattr(
        snapshot_module.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, stdout=response, stderr=b""),
    )
    with pytest.raises(ModuleSizeBaselineError, match=match):
        snapshot_module._batch_blobs(tmp_path, (("a" * 40, 1),))


def test_exact_head_snapshot_rejects_oversized_control_blob(tmp_path: Path) -> None:
    repo, origin = _init_repo(tmp_path)
    baseline_path = repo / "docs/module_size_baseline.json"
    write_module_size_baseline(
        baseline_path,
        ModuleSizeBaseline(entries=(_entry("src/dpone/legacy.py", lines=360, sloc=360, commit=origin),)),
    )
    (repo / "docs/benchmarks/quality_budgets.yml").write_bytes(b"x" * ((1 << 20) + 1))
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "oversized control input")
    head = _git(repo, "rev-parse", "HEAD")

    with pytest.raises(ModuleSizeBaselineError, match="control input exceeds byte limit"):
        load_module_size_head_snapshot(
            repo_root=repo,
            package_dir=repo / "src/dpone",
            baseline_path=baseline_path,
            head_sha=head,
        )


def test_true_split_cannot_create_warning_debt_without_accepted_adr(tmp_path: Path) -> None:
    from dpone.metrics.module_size import ModuleSizeThresholds, analyze_module_sizes

    package = tmp_path / "src/dpone"
    _write(package / "split_child.py", 360)
    parent = _entry("src/dpone/parent.py", lines=360, sloc=360)
    thresholds = ModuleSizeThresholds(warn_lines=450, max_lines=600, warn_sloc=350, max_sloc=400)
    report = analyze_module_sizes(package, repo_root=tmp_path, thresholds=thresholds, baseline=(parent,))
    context = ModuleSizeGitContext(
        base_sha=parent.baseline_commit,
        head_sha=parent.baseline_commit,
        previous_baseline=ModuleSizeBaseline(entries=(parent,)),
        exact_renames=(),
        deleted_paths=(parent.path,),
        rename_only=False,
    )

    with pytest.raises(ModuleSizeBaselineError, match="Accepted ADR is required"):
        ratchet_module_size_baseline(
            ModuleSizeBaseline(entries=(parent,)),
            report=report,
            thresholds=thresholds,
            bootstrap=False,
            bootstrap_commit=AUDITED_BOOTSTRAP_COMMIT,
            git_context=context,
            policy_issues=(),
        )
