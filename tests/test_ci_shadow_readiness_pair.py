from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from dpone.manifest.ci_shadow_readiness_pair import ReadinessPairError, recover_pair, write_pair

_SUBJECT_A = "a" * 40
_SUBJECT_B = "b" * 40


def _write(directory: Path, *, subject: str = _SUBJECT_A, phase_hook=None) -> None:
    write_pair(
        directory,
        json_name="report.json",
        markdown_name="report.md",
        json_bytes=b'{"status":"UNVERIFIED"}',
        markdown_bytes=b"# Unverified\n",
        report_kind="readiness",
        subject_commit_sha=subject,
        phase_hook=phase_hook,
    )


def test_create_only_pair_is_idempotent_for_exact_subject(tmp_path: Path) -> None:
    _write(tmp_path)
    _write(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b'{"status":"UNVERIFIED"}'
    assert (tmp_path / "report.md").read_bytes() == b"# Unverified\n"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()
    assert (tmp_path / ".dpone-readiness-pair-identity.json").exists()


def test_same_bytes_for_a_different_subject_conflict(tmp_path: Path) -> None:
    _write(tmp_path)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_CONFLICT"):
        _write(tmp_path, subject=_SUBJECT_B)


def test_malformed_journal_is_unverified_and_preserved(tmp_path: Path) -> None:
    journal = tmp_path / ".dpone-readiness-pair.json"
    journal.write_text(json.dumps({"prior_json": "must-not-be-here"}), encoding="utf-8")

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        recover_pair(tmp_path)

    assert journal.exists()


def test_interruption_after_json_link_rolls_back_only_owned_bytes(tmp_path: Path) -> None:
    def interrupt(phase: str) -> None:
        if phase == "json_replaced":
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        _write(tmp_path, phase_hook=interrupt)

    recover_pair(tmp_path)

    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "report.md").exists()
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_external_winner_is_preserved_and_transaction_stays_unverified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_link = os.link

    def external_winner(source: str, destination: str, **kwargs: object) -> None:
        if destination == "report.json":
            (tmp_path / destination).write_bytes(b"foreign-winner")
            raise FileExistsError
        real_link(source, destination, **kwargs)

    monkeypatch.setattr("dpone.manifest.ci_shadow_readiness_pair.os.link", external_winner)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b"foreign-winner"
    assert (tmp_path / ".dpone-readiness-pair.json").exists()


def test_existing_pair_rolls_back_from_a_json_phase_interruption(tmp_path: Path) -> None:
    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")

    def interrupt(phase: str) -> None:
        if phase == "json_replaced":
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        _write(tmp_path, phase_hook=interrupt)

    recover_pair(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b"old-json"
    assert (tmp_path / "report.md").read_bytes() == b"old-markdown"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_json_replaced_rolls_forward_when_markdown_was_already_installed(tmp_path: Path) -> None:
    def interrupt(phase: str) -> None:
        if phase == "before_pair_state":
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        _write(tmp_path, phase_hook=interrupt)

    recover_pair(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b'{"status":"UNVERIFIED"}'
    assert (tmp_path / "report.md").read_bytes() == b"# Unverified\n"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_foreign_existing_json_winner_is_preserved_before_exchange(tmp_path: Path) -> None:
    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")

    def foreign_writer(phase: str) -> None:
        if phase == "before_json_exchange":
            (tmp_path / "report.json").write_bytes(b"foreign-winner")

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path, phase_hook=foreign_writer)

    assert (tmp_path / "report.json").read_bytes() == b"foreign-winner"
    assert (tmp_path / ".dpone-readiness-pair.json").exists()


def test_native_exchange_error_preserves_existing_pair_for_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair as pair_module

    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")

    def failing_exchange(*_: object) -> None:
        raise OSError("injected exchange failure")

    monkeypatch.setattr(pair_module, "get_native_atomic_exchange", lambda: failing_exchange)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b"old-json"
    assert (tmp_path / "report.md").read_bytes() == b"old-markdown"
    assert (tmp_path / ".dpone-readiness-pair.json").exists()


@pytest.mark.parametrize("failed_name", ["report.json", "report.md"])
def test_exchange_error_after_linearization_recovers_existing_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_name: str
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair as pair_module

    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")
    native_exchange = pair_module.get_native_atomic_exchange()

    def exchange_then_fail(parent_fd: int, left: str, right: str) -> None:
        native_exchange(parent_fd, left, right)
        if left == failed_name:
            raise OSError("injected post-linearization failure")

    monkeypatch.setattr(pair_module, "get_native_atomic_exchange", lambda: exchange_then_fail)
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    monkeypatch.setattr(pair_module, "get_native_atomic_exchange", lambda: native_exchange)
    recover_pair(tmp_path)

    expected = (
        (b"old-json", b"old-markdown")
        if failed_name == "report.json"
        else (b'{"status":"UNVERIFIED"}', b"# Unverified\n")
    )
    assert ((tmp_path / "report.json").read_bytes(), (tmp_path / "report.md").read_bytes()) == expected
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


@pytest.mark.parametrize("failed_transition", [1, 2, 3])
def test_journal_transition_error_after_linearization_recovers_exact_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_transition: int
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair_fs as pair_fs

    real_replace = pair_fs.replace_file_if_digest
    transitions = 0

    def replace_then_fail(*args: object, **kwargs: object):
        nonlocal transitions
        result = real_replace(*args, **kwargs)
        transitions += 1
        if transitions == failed_transition:
            raise OSError("injected post-linearization journal-transition failure")
        return result

    monkeypatch.setattr(pair_fs, "replace_file_if_digest", replace_then_fail)
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    monkeypatch.setattr(pair_fs, "replace_file_if_digest", real_replace)
    recover_pair(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b'{"status":"UNVERIFIED"}'
    assert (tmp_path / "report.md").read_bytes() == b"# Unverified\n"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_foreign_journal_before_first_exchange_blocks_public_mutation(tmp_path: Path) -> None:
    def replace_journal(phase: str) -> None:
        if phase == "before_json_exchange":
            (tmp_path / ".dpone-readiness-pair.json").write_bytes(b"foreign-journal")

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path, phase_hook=replace_journal)

    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "report.md").exists()


def test_foreign_journal_before_markdown_exchange_preserves_prior_markdown(tmp_path: Path) -> None:
    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")

    def replace_journal(phase: str) -> None:
        if phase == "before_markdown_exchange":
            (tmp_path / ".dpone-readiness-pair.json").write_bytes(b"foreign-journal")

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path, phase_hook=replace_journal)

    assert (tmp_path / "report.md").read_bytes() == b"old-markdown"


def test_missing_retained_backup_preserves_journal_as_unverified(tmp_path: Path) -> None:
    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")

    def interrupt(phase: str) -> None:
        if phase == "json_replaced":
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError):
        _write(tmp_path, phase_hook=interrupt)
    next(tmp_path.glob(".report.json.dpone-backup-*")).unlink()

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        recover_pair(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair.json").exists()


@pytest.mark.parametrize(
    ("artifact", "pattern"), [("stage", ".report.json.dpone-stage-*"), ("backup", ".report.json.dpone-backup-*")]
)
def test_symlinked_retained_artifact_is_preserved_as_unverified(tmp_path: Path, artifact: str, pattern: str) -> None:
    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")

    def interrupt(phase: str) -> None:
        if phase == "json_replaced":
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        _write(tmp_path, phase_hook=interrupt)
    retained = next(tmp_path.glob(pattern))
    outside = tmp_path / f"outside-{artifact}"
    outside.write_bytes(b"sensitive")
    retained.unlink()
    retained.symlink_to(outside.name)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        recover_pair(tmp_path)

    assert outside.read_bytes() == b"sensitive"
    assert retained.is_symlink()
    assert (tmp_path / ".dpone-readiness-pair.json").exists()


def test_same_content_foreign_json_inode_blocks_recovery_before_cleanup(tmp_path: Path) -> None:
    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")

    def interrupt(phase: str) -> None:
        if phase == "json_replaced":
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError):
        _write(tmp_path, phase_hook=interrupt)
    expected = (tmp_path / "report.json").read_bytes()
    replacement = tmp_path / "foreign-json"
    replacement.write_bytes(expected)
    os.replace(replacement, tmp_path / "report.json")

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        recover_pair(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair.json").exists()
    assert next(tmp_path.glob(".report.md.dpone-backup-* ".strip())).exists()


def test_identity_receipt_cannot_be_replayed_into_another_directory(tmp_path: Path) -> None:
    _write(tmp_path)
    clone = tmp_path / "clone"
    clone.mkdir()
    for name in ("report.json", "report.md", ".dpone-readiness-pair-identity.json"):
        (clone / name).write_bytes((tmp_path / name).read_bytes())

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_CONFLICT"):
        _write(clone)


def test_completed_pair_rejects_same_content_foreign_inode(tmp_path: Path) -> None:
    _write(tmp_path)
    replacement = tmp_path / "foreign-json"
    replacement.write_bytes((tmp_path / "report.json").read_bytes())
    os.replace(replacement, tmp_path / "report.json")

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)


def test_malformed_identity_receipt_is_unverified(tmp_path: Path) -> None:
    _write(tmp_path)
    (tmp_path / ".dpone-readiness-pair-identity.json").write_bytes(b"not-json")

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)


def test_committed_recovery_completes_after_one_cleanup_leaf_was_removed(tmp_path: Path) -> None:
    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")

    def interrupt(phase: str) -> None:
        if phase == "before_cleanup":
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError):
        _write(tmp_path, phase_hook=interrupt)
    next(tmp_path.glob(".report.json.dpone-stage-*")).unlink()

    recover_pair(tmp_path)

    assert not (tmp_path / ".dpone-readiness-pair.json").exists()
    assert (tmp_path / "report.json").read_bytes() == b'{"status":"UNVERIFIED"}'


@pytest.mark.parametrize(
    ("existing", "pattern"),
    [
        (False, ".report.json.dpone-stage-*"),
        (False, ".report.md.dpone-stage-*"),
        (True, ".report.json.dpone-stage-*"),
        (True, ".report.md.dpone-stage-*"),
        (True, ".report.json.dpone-backup-*"),
        (True, ".report.md.dpone-backup-*"),
    ],
)
def test_committed_recovery_is_idempotent_when_any_retained_leaf_was_already_removed(
    tmp_path: Path, existing: bool, pattern: str
) -> None:
    if existing:
        (tmp_path / "report.json").write_bytes(b"old-json")
        (tmp_path / "report.md").write_bytes(b"old-markdown")

    def interrupt(phase: str) -> None:
        if phase == "before_cleanup":
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        _write(tmp_path, phase_hook=interrupt)
    next(tmp_path.glob(pattern)).unlink()

    recover_pair(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b'{"status":"UNVERIFIED"}'
    assert (tmp_path / "report.md").read_bytes() == b"# Unverified\n"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_committed_cleanup_delete_failure_retries_from_retained_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair_fs as pair_fs

    original = pair_fs.remove_file_if_owned
    calls = 0

    def fail_first_delete(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected delete failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(pair_fs, "remove_file_if_owned", fail_first_delete)
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair.json").exists()
    monkeypatch.setattr(pair_fs, "remove_file_if_owned", original)
    recover_pair(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b'{"status":"UNVERIFIED"}'
    assert (tmp_path / "report.md").read_bytes() == b"# Unverified\n"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


@pytest.mark.parametrize(
    ("existing", "pattern"),
    [
        (False, ".report.json.dpone-stage-*"),
        (False, ".report.md.dpone-stage-*"),
        (True, ".report.json.dpone-backup-*"),
        (True, ".report.md.dpone-backup-*"),
    ],
)
def test_committed_cleanup_directory_sync_failure_retries_exactly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, existing: bool, pattern: str
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair_fs as pair_fs

    if existing:
        (tmp_path / "report.json").write_bytes(b"old-json")
        (tmp_path / "report.md").write_bytes(b"old-markdown")
    original = pair_fs.os.fsync
    directory = tmp_path.stat()

    def fail_after_selected_delete(fd: int) -> None:
        current = os.fstat(fd)
        if (
            (current.st_dev, current.st_ino) == (directory.st_dev, directory.st_ino)
            and not list(tmp_path.glob(pattern))
            and (tmp_path / ".dpone-readiness-pair.json").exists()
        ):
            raise OSError("injected cleanup directory-sync failure")
        original(fd)

    monkeypatch.setattr(pair_fs.os, "fsync", fail_after_selected_delete)
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair.json").exists()
    monkeypatch.setattr(pair_fs.os, "fsync", original)
    recover_pair(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b'{"status":"UNVERIFIED"}'
    assert (tmp_path / "report.md").read_bytes() == b"# Unverified\n"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_identity_link_failure_retries_from_pair_replaced_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = os.link

    def fail_identity_link(source: str, destination: str, **kwargs: object) -> None:
        if destination == ".dpone-readiness-pair-identity.json":
            raise OSError("injected identity-link failure")
        original(source, destination, **kwargs)

    monkeypatch.setattr(os, "link", fail_identity_link)
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair.json").exists()
    monkeypatch.setattr(os, "link", original)
    recover_pair(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair-identity.json").exists()
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize(
    ("phase", "committed"),
    [
        ("journal_durable", False),
        ("json_state_durable", True),
        ("pair_state_durable", True),
        ("committed_state_durable", True),
        ("before_cleanup", True),
    ],
)
def test_durable_boundary_interruption_recovers_deterministically(
    tmp_path: Path, existing: bool, phase: str, committed: bool
) -> None:
    if existing:
        (tmp_path / "report.json").write_bytes(b"old-json")
        (tmp_path / "report.md").write_bytes(b"old-markdown")

    def interrupt(observed: str) -> None:
        if observed == phase:
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        _write(tmp_path, phase_hook=interrupt)

    recover_pair(tmp_path)
    if not existing and not committed:
        assert not (tmp_path / "report.json").exists()
        assert not (tmp_path / "report.md").exists()
    else:
        observed = ((tmp_path / "report.json").read_bytes(), (tmp_path / "report.md").read_bytes())
        expected = (b'{"status":"UNVERIFIED"}', b"# Unverified\n") if committed else (b"old-json", b"old-markdown")
        assert observed == expected
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_cleanup_preserves_same_content_foreign_journal(tmp_path: Path) -> None:
    def replace_journal(phase: str) -> None:
        if phase == "before_cleanup":
            journal = tmp_path / ".dpone-readiness-pair.json"
            replacement = tmp_path / "foreign-journal"
            replacement.write_bytes(journal.read_bytes())
            os.replace(replacement, journal)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path, phase_hook=replace_journal)

    assert (tmp_path / ".dpone-readiness-pair.json").exists()
    assert next(tmp_path.glob(".report.json.dpone-stage-*")).exists()
    assert next(tmp_path.glob(".report.md.dpone-stage-*")).exists()


@pytest.mark.parametrize("json_name,report_kind", [("x" * 129, "readiness"), ("report.json", "x" * 129)])
def test_over_limit_inputs_fail_before_creating_artifacts(tmp_path: Path, json_name: str, report_kind: str) -> None:
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_INVALID"):
        write_pair(
            tmp_path,
            json_name=json_name,
            markdown_name="report.md",
            json_bytes=b"{}",
            markdown_bytes=b"# report\n",
            report_kind=report_kind,
            subject_commit_sha=_SUBJECT_A,
        )

    assert list(tmp_path.iterdir()) == []


def test_unsupported_lock_platform_fails_before_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.manifest import ci_shadow_readiness_pair_fs as pair_fs

    def unavailable(_: str) -> object:
        raise ImportError("unsupported")

    monkeypatch.setattr(pair_fs, "import_module", unavailable)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNSUPPORTED"):
        _write(tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_short_write_fails_closed_before_public_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.manifest import ci_shadow_readiness_pair_fs as pair_fs

    monkeypatch.setattr(pair_fs.os, "write", lambda *_: 0)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "report.md").exists()


def test_public_link_directory_sync_failure_recovers_from_prepared_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair as pair_module

    original = pair_module.os.fsync
    directory = tmp_path.stat()

    def fail_after_public_link(fd: int) -> None:
        current = os.fstat(fd)
        if (current.st_dev, current.st_ino) == (directory.st_dev, directory.st_ino) and (
            tmp_path / "report.json"
        ).exists():
            raise OSError("injected public-link directory-sync failure")
        original(fd)

    monkeypatch.setattr(pair_module.os, "fsync", fail_after_public_link)
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair.json").exists()
    monkeypatch.setattr(pair_module.os, "fsync", original)
    recover_pair(tmp_path)

    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "report.md").exists()
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_markdown_link_directory_sync_failure_recovers_from_json_replaced_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair as pair_module

    original = pair_module.os.fsync
    directory = tmp_path.stat()

    def fail_after_markdown_link(fd: int) -> None:
        current = os.fstat(fd)
        if (current.st_dev, current.st_ino) == (directory.st_dev, directory.st_ino) and (
            tmp_path / "report.md"
        ).exists():
            raise OSError("injected markdown-link directory-sync failure")
        original(fd)

    monkeypatch.setattr(pair_module.os, "fsync", fail_after_markdown_link)
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair.json").exists()
    monkeypatch.setattr(pair_module.os, "fsync", original)
    recover_pair(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b'{"status":"UNVERIFIED"}'
    assert (tmp_path / "report.md").read_bytes() == b"# Unverified\n"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


@pytest.mark.parametrize("name", ["report.json", "report.md"])
def test_backup_directory_sync_failure_preserves_prior_public_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair as pair_module

    (tmp_path / "report.json").write_bytes(b"old-json")
    (tmp_path / "report.md").write_bytes(b"old-markdown")
    original = pair_module.os.fsync
    directory = tmp_path.stat()

    def fail_after_backup_link(fd: int) -> None:
        current = os.fstat(fd)
        if (current.st_dev, current.st_ino) == (directory.st_dev, directory.st_ino) and list(
            tmp_path.glob(f".{name}.dpone-backup-*")
        ):
            raise OSError("injected backup directory-sync failure")
        original(fd)

    monkeypatch.setattr(pair_module.os, "fsync", fail_after_backup_link)
    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / "report.json").read_bytes() == b"old-json"
    assert (tmp_path / "report.md").read_bytes() == b"old-markdown"
    assert not (tmp_path / ".dpone-readiness-pair.json").exists()


def test_stage_fsync_failure_fails_closed_before_public_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair_fs as pair_fs

    def fail_fsync(_: int) -> None:
        raise OSError("injected fsync failure")

    monkeypatch.setattr(pair_fs.os, "fsync", fail_fsync)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "report.md").exists()


def test_symlink_journal_is_preserved_as_unverified(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.write_bytes(b"sensitive")
    (tmp_path / ".dpone-readiness-pair.json").symlink_to(outside.name)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        recover_pair(tmp_path)

    assert outside.read_bytes() == b"sensitive"
    assert (tmp_path / ".dpone-readiness-pair.json").is_symlink()


@pytest.mark.parametrize("name", ["report.json", "report.md"])
def test_symlinked_public_member_is_preserved_as_unverified(tmp_path: Path, name: str) -> None:
    outside = tmp_path / "outside"
    outside.write_bytes(b"sensitive")
    (tmp_path / name).symlink_to(outside.name)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert outside.read_bytes() == b"sensitive"
    assert (tmp_path / name).is_symlink()


def test_nonblocking_pair_lock_rejects_contention(tmp_path: Path) -> None:
    from dpone.manifest import ci_shadow_readiness_pair_fs as pair_fs

    with pair_fs.open_directory(tmp_path) as parent_fd, pair_fs.pair_lock(parent_fd, ".dpone-readiness-pair.lock"):
        with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_LOCKED"):
            with pair_fs.pair_lock(parent_fd, ".dpone-readiness-pair.lock"):
                pytest.fail("contended lock must not be acquired")


def test_foreign_journal_winner_is_not_clobbered_during_state_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dpone.manifest import ci_shadow_readiness_pair_fs as pair_fs

    real_replace = pair_fs.replace_file_if_digest
    calls = 0

    def foreign_before_transition(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        if calls == 1:
            (tmp_path / ".dpone-readiness-pair.json").write_bytes(b"foreign-journal")
        return real_replace(*args, **kwargs)

    monkeypatch.setattr(pair_fs, "replace_file_if_digest", foreign_before_transition)

    with pytest.raises(ReadinessPairError, match="READINESS_OUTPUT_UNVERIFIED"):
        _write(tmp_path)

    assert (tmp_path / ".dpone-readiness-pair.json").read_bytes() == b"foreign-journal"
