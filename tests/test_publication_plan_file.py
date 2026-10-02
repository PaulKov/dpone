"""Operator plans appear atomically, stay private and never replace evidence."""

import os
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module

import pytest


def api():
    return import_module("dpone.adapters.publication_plan_file")


def test_roundtrip_is_private_and_has_no_temporary_leftovers(tmp_path):
    path = tmp_path / "plan.json"
    api().write_private_plan(path, b'{"schema":"example.v1"}\n')
    assert api().read_private_plan(path) == b'{"schema":"example.v1"}\n'
    assert path.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.iterdir()) == [path]


def test_existing_receipt_is_never_overwritten(tmp_path):
    path = tmp_path / "plan.json"
    path.write_bytes(b"original")
    with pytest.raises(FileExistsError):
        api().write_private_plan(path, b"replacement")
    assert path.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [path]


def test_one_concurrent_publisher_wins_without_partial_content(tmp_path):
    path = tmp_path / "plan.json"
    payloads = [bytes([i]) * 32768 for i in range(8)]

    def attempt(payload):
        try:
            api().write_private_plan(path, payload)
            return payload
        except FileExistsError:
            return None

    with ThreadPoolExecutor(max_workers=8) as workers:
        results = list(workers.map(attempt, payloads))
    winners = [value for value in results if value is not None]
    assert len(winners) == 1
    assert api().read_private_plan(path) == winners[0]
    assert list(tmp_path.iterdir()) == [path]


def test_plan_is_not_visible_before_payload_fsync(tmp_path, monkeypatch):
    path = tmp_path / "plan.json"
    real_fsync = os.fsync
    calls = []

    def fsync(fd):
        calls.append(path.exists())
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    api().write_private_plan(path, b"complete")
    assert calls == [False, True]


def test_payload_fsync_failure_leaves_no_plan(tmp_path, monkeypatch):
    def fail(_):
        raise OSError("synthetic fsync failure")

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError):
        api().write_private_plan(tmp_path / "plan.json", b"complete")
    assert list(tmp_path.iterdir()) == []


def test_directory_fsync_failure_preserves_published_plan_for_readback(tmp_path, monkeypatch):
    path = tmp_path / "plan.json"
    real_fsync = os.fsync
    calls = 0

    def fail_second(fd):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic directory fsync failure")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_second)
    with pytest.raises(OSError):
        api().write_private_plan(path, b"complete")
    assert api().read_private_plan(path) == b"complete"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("content", [b"", b"x" * (1024 * 1024 + 1), "not bytes"], ids=["empty", "oversize", "text"])
def test_invalid_payload_creates_nothing(tmp_path, content):
    with pytest.raises(ValueError):
        api().write_private_plan(tmp_path / "plan.json", content)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory", "public", "oversize", "empty", "hardlink"])
def test_read_rejects_unsafe_plan_files(tmp_path, kind):
    path = tmp_path / "plan.json"
    if kind == "symlink":
        source = tmp_path / "source"
        source.write_bytes(b"plan")
        source.chmod(0o600)
        path.symlink_to(source)
    elif kind == "fifo":
        os.mkfifo(path, 0o600)
    elif kind == "directory":
        path.mkdir(mode=0o700)
    else:
        path.write_bytes(b"" if kind == "empty" else b"x" * (1024 * 1024 + 1) if kind == "oversize" else b"plan")
        path.chmod(0o644 if kind == "public" else 0o600)
        if kind == "hardlink":
            os.link(path, tmp_path / "other")
    with pytest.raises((ValueError, OSError)):
        api().read_private_plan(path)


def test_write_refuses_a_symlink_destination(tmp_path):
    path = tmp_path / "plan.json"
    destination = tmp_path / "destination"
    destination.write_bytes(b"preserved")
    path.symlink_to(destination)
    with pytest.raises(FileExistsError):
        api().write_private_plan(path, b"replacement")
    assert destination.read_bytes() == b"preserved"
