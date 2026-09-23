"""Confined existing-file reads never repair missing evidence."""

from hashlib import sha256

import pytest

from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory


def test_exact_read_and_missing_root(tmp_path):
    payload = b"actual historical bytes"
    (tmp_path / "proof.json").write_bytes(payload)
    with PinnedEvidenceReadFactory(tmp_path)() as reader:
        assert reader.read("proof.json", len(payload), sha256(payload).hexdigest()) == payload
    with pytest.raises((ValueError, OSError)):
        with PinnedEvidenceReadFactory(tmp_path / "absent")():
            pass
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("fault", ["symlink", "fifo", "short", "extra", "hash"])
def test_unsafe_or_changed_file_rejected(tmp_path, fault):
    import os

    path = tmp_path / "proof.json"
    if fault == "symlink":
        (tmp_path / "target").write_bytes(b"abc")
        path.symlink_to(tmp_path / "target")
    elif fault == "fifo":
        os.mkfifo(path)
    else:
        path.write_bytes({"short": b"ab", "extra": b"abcd", "hash": b"xyz"}[fault])
    with PinnedEvidenceReadFactory(tmp_path)() as reader:
        with pytest.raises(ValueError):
            reader.read("proof.json", 3, sha256(b"abc").hexdigest())


def test_short_os_reads_are_accumulated(tmp_path, monkeypatch):
    import os

    payload = b"abcdef"
    (tmp_path / "proof.json").write_bytes(payload)
    original = os.read
    monkeypatch.setattr(os, "read", lambda fd, size: original(fd, min(size, 1)))
    with PinnedEvidenceReadFactory(tmp_path)() as reader:
        assert reader.read("proof.json", len(payload), sha256(payload).hexdigest()) == payload


def test_root_replacement_rejected_on_context_exit(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(ValueError):
        with PinnedEvidenceReadFactory(root)():
            root.rename(tmp_path / "old")
            root.mkdir()


def test_file_replacement_after_read_rejected(tmp_path, monkeypatch):
    import os

    path = tmp_path / "proof.json"
    path.write_bytes(b"abc")
    original = os.read
    replaced = False

    def read(fd, size):
        nonlocal replaced
        data = original(fd, size)
        if data and not replaced:
            replaced = True
            path.rename(tmp_path / "old")
            path.write_bytes(b"abc")
        return data

    monkeypatch.setattr(os, "read", read)
    with PinnedEvidenceReadFactory(tmp_path)() as reader:
        with pytest.raises(ValueError):
            reader.read("proof.json", 3, sha256(b"abc").hexdigest())


@pytest.mark.parametrize("flag", ["O_NOFOLLOW", "O_DIRECTORY", "O_NONBLOCK"])
def test_missing_required_os_flag_fails_before_open(tmp_path, monkeypatch, flag):
    import os

    monkeypatch.delattr(os, flag)
    monkeypatch.setattr(os, "open", lambda *a, **k: pytest.fail("unsupported admission must not open"))
    with pytest.raises(ValueError):
        with PinnedEvidenceReadFactory(tmp_path)():
            pass


def test_symlink_root_rejected_before_entry(tmp_path):
    (tmp_path / "actual").mkdir()
    (tmp_path / "alias").symlink_to(tmp_path / "actual")
    with pytest.raises(ValueError):
        with PinnedEvidenceReadFactory(tmp_path / "alias")():
            pass


@pytest.mark.parametrize("failed_close", [1, 2])
def test_file_or_root_close_failure_prevents_usable_result(tmp_path, monkeypatch, failed_close):
    import os

    payload = b"abc"
    (tmp_path / "proof.json").write_bytes(payload)
    original = os.close
    closes = []

    def close(fd):
        closes.append(fd)
        original(fd)
        if len(closes) == failed_close:
            raise OSError("synthetic close uncertainty")

    monkeypatch.setattr(os, "close", close)
    with pytest.raises(OSError):
        with PinnedEvidenceReadFactory(tmp_path)() as reader:
            reader.read("proof.json", len(payload), sha256(payload).hexdigest())
    assert len(closes) == 2
