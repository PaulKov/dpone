from __future__ import annotations

import os
import shutil
import stat
import subprocess
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

import dpone.metrics.module_size_confined_write as confined_write
import dpone.metrics.module_size_write as write_module
from dpone.manifest.project_root import ProjectRootIdentity, inspect_project_root
from dpone.metrics.module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    write_module_size_baseline,
)
from dpone.metrics.module_size_write import write_baseline_if_unchanged


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def _repo(tmp_path: Path) -> tuple[Path, Path, ModuleSizeBaseline, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test")
    baseline_path = repo / "docs/module_size_baseline.json"
    baseline = ModuleSizeBaseline(
        entries=(
            ModuleSizeDebtEntry(
                path="src/dpone/legacy.py",
                max_lines=360,
                max_sloc=360,
                owner="architecture",
                reason="audited legacy debt",
                target_sloc=350,
                target_date=date(2099, 12, 31),
                accepted_adr=None,
                baseline_commit="0" * 40,
            ),
        )
    )
    write_module_size_baseline(baseline_path, baseline)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "baseline")
    return repo, baseline_path, baseline, _git(repo, "rev-parse", "HEAD")


def test_transaction_preserves_concurrent_baseline_bytes(tmp_path: Path) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    original = path.read_bytes()
    concurrent = ModuleSizeBaseline(entries=(replace(baseline.entries[0], owner="concurrent-owner"),))
    write_module_size_baseline(path, concurrent)
    concurrent_bytes = path.read_bytes()

    with pytest.raises(ModuleSizeBaselineError, match="baseline bytes changed"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=baseline,
            expected_bytes=original,
            expected_head_sha=head,
        )

    assert path.read_bytes() == concurrent_bytes


def test_transaction_rejects_changed_head_without_writing(tmp_path: Path) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    (repo / "README.md").write_text("new head\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-qm", "advance head")

    with pytest.raises(ModuleSizeBaselineError, match="checked-out HEAD changed"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=baseline,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() == before


def test_transaction_rejects_replaced_repository_root_without_writing(tmp_path: Path) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    identity = inspect_project_root(repo)
    assert identity is not None
    original = tmp_path / "original-repo"
    repo.rename(original)
    shutil.copytree(original, repo, symlinks=True)
    replacement_path = repo / "docs/module_size_baseline.json"

    with pytest.raises(ModuleSizeBaselineError, match="repository root changed"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=replacement_path,
            baseline=baseline,
            expected_bytes=before,
            expected_head_sha=head,
            root_identity=identity,
        )

    assert replacement_path.read_bytes() == before
    assert (original / "docs/module_size_baseline.json").read_bytes() == before


def test_transaction_restores_prior_bytes_when_head_changes_during_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    real_write = write_module._write_transaction_bytes
    advanced = False

    def write_then_advance(
        repo_root: Path,
        target: Path,
        *,
        content: bytes,
        expected_bytes: bytes,
        root_identity: ProjectRootIdentity,
    ) -> None:
        nonlocal advanced
        real_write(
            repo_root,
            target,
            content=content,
            expected_bytes=expected_bytes,
            root_identity=root_identity,
        )
        if advanced:
            return
        advanced = True
        (repo / "README.md").write_text("advanced during write\n", encoding="utf-8")
        _git(repo, "add", "README.md")
        _git(repo, "commit", "-qm", "advance during write")

    monkeypatch.setattr(write_module, "_write_transaction_bytes", write_then_advance)

    with pytest.raises(ModuleSizeBaselineError, match="HEAD changed during write"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() == before
    assert not Path(_git(repo, "rev-parse", "--git-path", "dpone-locks/module-size-baseline.lock")).exists()


def test_transaction_restores_prior_bytes_when_post_write_git_check_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    real_git = write_module._git
    head_lookups = 0

    def fail_post_write_head(repo_root: Path, *args: str) -> str:
        nonlocal head_lookups
        if args == ("rev-parse", "HEAD^{commit}"):
            head_lookups += 1
            if head_lookups == 2:
                raise ModuleSizeBaselineError("simulated Git lookup failure")
        return real_git(repo_root, *args)

    monkeypatch.setattr(write_module, "_git", fail_post_write_head)

    with pytest.raises(ModuleSizeBaselineError, match="post-write Git identity is unavailable"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() == before
    assert not Path(_git(repo, "rev-parse", "--git-path", "dpone-locks/module-size-baseline.lock")).exists()


def test_transaction_normalizes_replace_failure_and_preserves_prior_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))

    def fail_replace(*_args: object, **_kwargs: object) -> None:
        raise OSError("sensitive filesystem detail")

    monkeypatch.setattr(confined_write, "replace_file_if_digest", fail_replace)

    with pytest.raises(ModuleSizeBaselineError, match="without a certifiable candidate") as raised:
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert "sensitive filesystem detail" not in str(raised.value)
    assert path.read_bytes() == before


def test_transaction_recovers_candidate_after_parent_fsync_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    real_fsync = confined_write.os.fsync
    failed = False

    def fail_first_parent_fsync(descriptor: int) -> None:
        nonlocal failed
        if not failed and stat.S_ISDIR(os.fstat(descriptor).st_mode):
            failed = True
            raise OSError("sensitive durability detail")
        real_fsync(descriptor)

    monkeypatch.setattr(confined_write.os, "fsync", fail_first_parent_fsync)

    with pytest.raises(ModuleSizeBaselineError, match="without a certifiable candidate"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() == before
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []

    monkeypatch.setattr(confined_write.os, "fsync", real_fsync)
    write_baseline_if_unchanged(
        repo_root=repo,
        baseline_path=path,
        baseline=candidate,
        expected_bytes=before,
        expected_head_sha=head,
    )

    assert path.read_bytes() != before
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_transaction_cleans_staging_after_close_reports_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    real_close = confined_write.os.close
    failed = False

    def close_then_report_failure(descriptor: int) -> None:
        nonlocal failed
        is_regular = stat.S_ISREG(os.fstat(descriptor).st_mode)
        real_close(descriptor)
        if not failed and is_regular:
            failed = True
            raise OSError("sensitive close detail")

    monkeypatch.setattr(confined_write.os, "close", close_then_report_failure)

    with pytest.raises(ModuleSizeBaselineError, match="without a certifiable candidate"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() == before
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []

    monkeypatch.setattr(confined_write.os, "close", real_close)
    write_baseline_if_unchanged(
        repo_root=repo,
        baseline_path=path,
        baseline=candidate,
        expected_bytes=before,
        expected_head_sha=head,
    )

    assert path.read_bytes() != before
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_transaction_reports_uncertain_outcome_when_rollback_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    real_write = write_module._write_transaction_bytes
    writes = 0

    def install_then_fail(
        repo_root: Path,
        target: Path,
        *,
        content: bytes,
        expected_bytes: bytes,
        root_identity: ProjectRootIdentity,
    ) -> None:
        nonlocal writes
        writes += 1
        if writes == 1:
            real_write(
                repo_root,
                target,
                content=content,
                expected_bytes=expected_bytes,
                root_identity=root_identity,
            )
            raise ModuleSizeBaselineError("simulated post-replace durability failure")
        raise ModuleSizeBaselineError("simulated rollback failure")

    monkeypatch.setattr(write_module, "_write_transaction_bytes", install_then_fail)

    with pytest.raises(ModuleSizeBaselineError, match="without a certifiable candidate"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() != before
    assert not Path(_git(repo, "rev-parse", "--git-path", "dpone-locks/module-size-baseline.lock")).exists()


def test_transaction_never_writes_through_swapped_parent_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    safe_docs = repo / "docs.safe"
    outside = tmp_path / "outside"
    outside.mkdir()
    real_stage = confined_write._stage
    swapped = False

    def swap_parent_then_stage(parent_fd: int, leaf: str, content: bytes) -> str:
        nonlocal swapped
        if not swapped:
            swapped = True
            (repo / "docs").rename(safe_docs)
            (repo / "docs").symlink_to(outside, target_is_directory=True)
        return real_stage(parent_fd, leaf, content)

    monkeypatch.setattr(confined_write, "_stage", swap_parent_then_stage)

    with pytest.raises(ModuleSizeBaselineError, match="without a certifiable candidate"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert not (outside / path.name).exists()
    assert (safe_docs / path.name).read_bytes() == before


def test_transaction_never_reports_success_after_final_parent_namespace_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    canonical_winner = before.replace(b'"reason": "audited legacy debt"', b'"reason": "canonical replacement"')
    detached_docs = repo / "docs.detached"
    real_require = confined_write._require_current_parent
    checks = 0

    def swap_after_final_old_parent_check(
        anchor: Path,
        parent_parts: tuple[str, ...],
        parent_fd: int,
        *,
        root_identity: confined_write.ModuleSizeRootIdentity | None,
    ) -> None:
        nonlocal checks
        real_require(anchor, parent_parts, parent_fd, root_identity=root_identity)
        checks += 1
        if checks == 2:
            (repo / "docs").rename(detached_docs)
            (repo / "docs").mkdir()
            (repo / "docs/module_size_baseline.json").write_bytes(canonical_winner)

    monkeypatch.setattr(confined_write, "_require_current_parent", swap_after_final_old_parent_check)

    with pytest.raises(ModuleSizeBaselineError, match="without a certifiable candidate"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() == canonical_winner
    assert (detached_docs / path.name).read_bytes() == before


def test_transaction_preserves_editor_change_before_atomic_exchange(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    concurrent = before.replace(b'"reason": "audited legacy debt"', b'"reason": "concurrent edit"')
    assert concurrent != before
    real_replace = confined_write.replace_file_if_digest

    def replace_with_race(*args: object, **kwargs: object) -> object:
        def race(phase: str) -> None:
            if phase == "before_exchange":
                path.write_bytes(concurrent)

        return real_replace(*args, **kwargs, phase_hook=race)

    monkeypatch.setattr(confined_write, "replace_file_if_digest", replace_with_race)

    with pytest.raises(ModuleSizeBaselineError, match="without a certifiable candidate"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() == concurrent


def test_transaction_never_reports_success_when_candidate_changes_after_exchange(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, path, baseline, head = _repo(tmp_path)
    before = path.read_bytes()
    candidate = ModuleSizeBaseline(entries=(replace(baseline.entries[0], max_lines=359, max_sloc=359),))
    concurrent = before.replace(b'"reason": "audited legacy debt"', b'"reason": "post-replace edit"')
    real_replace = confined_write.replace_file_if_digest

    def replace_with_race(*args: object, **kwargs: object) -> object:
        def race(phase: str) -> None:
            if phase == "exchange_linearized":
                path.write_bytes(concurrent)

        return real_replace(*args, **kwargs, phase_hook=race)

    monkeypatch.setattr(confined_write, "replace_file_if_digest", replace_with_race)

    with pytest.raises(ModuleSizeBaselineError, match="without a certifiable candidate"):
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=path,
            baseline=candidate,
            expected_bytes=before,
            expected_head_sha=head,
        )

    assert path.read_bytes() in {before, concurrent}
