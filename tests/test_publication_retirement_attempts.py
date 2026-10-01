"""A restart/concurrent invocation cannot reclaim an attempted plan."""

import os
import stat
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module

import pytest


def adapter(directory):
    return import_module("dpone.adapters.publication_retirement_attempts").PrivateRetirementAttempts(directory)


def test_atomic_claim_has_one_winner_across_adapter_instances(tmp_path):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: adapter(tmp_path).claim("a" * 64), range(8)))
    assert sum(results) == 1
    assert adapter(tmp_path).claim("a" * 64) is False
    assert adapter(tmp_path).claim("b" * 64) is True
    files = tuple(tmp_path.iterdir())
    assert len(files) == 2
    assert all(path.stat().st_mode & 0o077 == 0 for path in files)


@pytest.mark.parametrize("digest", ["", "../outside", "G" * 64, "a" * 65, None])
def test_claim_rejects_non_digest_without_creating_files(tmp_path, digest):
    with pytest.raises(ValueError):
        adapter(tmp_path).claim(digest)
    assert tuple(tmp_path.iterdir()) == ()


def test_readiness_is_stable_across_restart_without_creating_files(tmp_path):
    journal = adapter(tmp_path)
    identity = journal.require_ready()
    assert len(identity) == 64 and int(identity, 16) >= 0
    assert journal.require_ready() == adapter(tmp_path).require_ready() == identity
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.parametrize("kind", ["missing", "symlink", "shared", "relative", "noncanonical"])
def test_unadmitted_directory_cannot_be_ready_or_claim(tmp_path, kind, monkeypatch):
    directory = tmp_path / "journal"
    if kind != "missing":
        directory.mkdir(mode=0o700)
    if kind == "symlink":
        link = tmp_path / "alias"
        link.symlink_to(directory, target_is_directory=True)
        directory = link
    elif kind == "shared":
        directory.chmod(0o755)
    elif kind == "relative":
        monkeypatch.chdir(tmp_path)
        directory = type(tmp_path)("journal")
    elif kind == "noncanonical":
        directory = directory / ".." / "journal"
    journal = adapter(directory)
    with pytest.raises((OSError, ValueError)):
        journal.require_ready()
    with pytest.raises((OSError, ValueError)):
        journal.claim("a" * 64)
    assert not tuple(tmp_path.rglob("*.attempt"))


def test_replaced_directory_is_not_silently_readmitted(tmp_path):
    directory = tmp_path / "journal"
    directory.mkdir(mode=0o700)
    journal = adapter(directory)
    journal.require_ready()
    directory.rename(tmp_path / "previous")
    directory.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="journal"):
        journal.claim("a" * 64)
    with pytest.raises(ValueError, match="journal"):
        journal.require_ready()
    assert not tuple(tmp_path.rglob("*.attempt"))


def test_wrong_owner_cannot_admit_or_claim(tmp_path, monkeypatch):
    owner = tmp_path.stat().st_uid
    monkeypatch.setattr(os, "getuid", lambda: owner + 1)
    journal = adapter(tmp_path)
    with pytest.raises(ValueError, match="owner-private"):
        journal.require_ready()
    with pytest.raises(ValueError, match="owner-private"):
        journal.claim("a" * 64)
    assert tuple(tmp_path.iterdir()) == ()


def test_claim_is_bound_to_admitted_directory_during_path_replacement(tmp_path, monkeypatch):
    directory = tmp_path / "journal"
    directory.mkdir(mode=0o700)
    previous = tmp_path / "previous"
    journal = adapter(directory)
    journal.require_ready()
    real_fsync = os.fsync
    replaced = False

    def replace_path_after_payload_flush(fd):
        nonlocal replaced
        real_fsync(fd)
        if stat.S_ISREG(os.fstat(fd).st_mode) and not replaced:
            directory.rename(previous)
            directory.mkdir(mode=0o700)
            replaced = True

    monkeypatch.setattr(os, "fsync", replace_path_after_payload_flush)
    with pytest.raises(ValueError, match="journal"):
        journal.claim("a" * 64)
    assert tuple(directory.iterdir()) == ()
    assert (previous / f"{'a' * 64}.attempt").is_file()
    assert len(tuple(previous.iterdir())) == 1


def test_lost_directory_fsync_ack_preserves_attempt_across_restart(tmp_path, monkeypatch):
    real_fsync = os.fsync

    def lost_ack(fd):
        real_fsync(fd)
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("synthetic lost acknowledgement")

    monkeypatch.setattr(os, "fsync", lost_ack)
    with pytest.raises(OSError):
        adapter(tmp_path).claim("a" * 64)
    monkeypatch.setattr(os, "fsync", real_fsync)
    assert adapter(tmp_path).claim("a" * 64) is False
    assert len(tuple(tmp_path.iterdir())) == 1
