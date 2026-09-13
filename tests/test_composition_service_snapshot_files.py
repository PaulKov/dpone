"""Real filesystem operations with simulated Linux identity/ancestor provisioning.

These tests are not a deployed nonroot custody or host-isolation certificate.
"""

import os
import stat
from dataclasses import replace

import pytest

from dpone.adapters import composition_service_snapshot_files as module
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot import SnapshotLimits
from dpone.contracts.composition_snapshot_capture import (
    SnapshotCaptureRecord,
    SnapshotCaptureSubject,
    attempt_snapshot_target,
    capture_digest,
)
from tests.composition_snapshot_helpers import digest, intent


@pytest.fixture
def files(tmp_path, monkeypatch):
    root = tmp_path / "captures"
    root.mkdir(mode=0o700)
    info = root.stat()
    events = []
    monkeypatch.setattr(module.sys, "platform", "linux")

    # The temporary parent is not administrator-provisioned. Simulate only that
    # traversal boundary; real root, attempt and file metadata are still checked.
    def opened(self):
        fd = os.open(self.root, module.PROTECTED_FLAGS)
        try:
            observed = os.fstat(fd)
            self._shape(observed, directory=True)
            module._require((observed.st_dev, observed.st_ino) == self._volume, "volume")
            return fd
        except BaseException:
            os.close(fd)
            raise

    monkeypatch.setattr(module.ServiceSnapshotFiles, "_open_root", opened)
    store = module.ServiceSnapshotFiles(
        root,
        dispatcher_uid=os.geteuid(),
        dispatcher_gid=os.getegid(),
        root_device=info.st_dev,
        root_inode=info.st_ino,
        require_custody=lambda: events.append("custody"),
    )
    value = intent()
    subject = SnapshotCaptureSubject(
        value.attempt,
        attempt_snapshot_target(value.target, value.attempt),
        digest("source"),
        ("db", "dbo", "data"),
        SnapshotLimits(100, 65536, 65536, 65536, 65536, 131072),
    )
    yield store, subject, events
    store.close()


def record(subject, source, payload):
    return SnapshotCaptureRecord(
        subject.subject_sha256,
        capture_digest(source),
        capture_digest(payload),
        digest("content"),
        digest("schema"),
        digest("physical"),
        digest("baseline"),
        intent().generation.old_target_uuid,
        (("id", "Int32"),),
        0,
        len(source),
        len(payload),
        0,
        len(source) + len(payload),
    )


@pytest.mark.parametrize("payload", [b"", b"native\x00payload"])
def test_exclusive_roundtrip_modes_and_closed_lifecycle(files, payload):
    store, subject, events = files
    source = b'{"source":"original"}'
    store.write_once(subject, source, payload)
    path = store.root / subject.attempt.attempt_sha256[7:]
    assert stat.S_IMODE(path.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 and p.stat().st_nlink == 1 for p in path.iterdir())
    before = len(events)
    assert store.read(subject, record(subject, source, payload)) == (source, payload)
    assert len(events) > before
    with pytest.raises(CompositionAdmissionError):
        store.write_once(subject, source, payload)
    store.close()
    with pytest.raises(CompositionAdmissionError):
        store.read(subject, record(subject, source, payload))


@pytest.mark.parametrize("damage", ["mode", "link", "symlink", "bytes", "oversize", "subject", "volume"])
def test_reader_rejects_changed_custody_or_original(files, damage):
    store, subject, _ = files
    store.write_once(subject, b"source", b"payload")
    original = record(subject, b"source", b"payload")
    path = store.root / subject.attempt.attempt_sha256[7:] / "payload.native"
    if damage == "mode":
        path.chmod(0o640)
    elif damage == "link":
        os.link(path, path.with_name("alias"))
    elif damage == "symlink":
        path.rename(path.with_name("old"))
        path.symlink_to("old")
    elif damage == "bytes":
        path.write_bytes(b"changed")
    elif damage == "oversize":
        path.write_bytes(b"x" * (subject.limits.max_wire_bytes + 1))
    elif damage == "subject":
        original = replace(original, subject_sha256=digest("different"))
    else:
        store._volume = store._volume[0], store._volume[1] + 1
    with pytest.raises(CompositionAdmissionError):
        store.read(subject, original)


def test_partial_install_is_never_repaired_or_overwritten(files, monkeypatch):
    store, subject, _ = files
    original = os.link

    def failing(source, destination, **kwargs):
        if destination == "payload.native":
            raise OSError("injected")
        return original(source, destination, **kwargs)

    monkeypatch.setattr(module.os, "link", failing)
    with pytest.raises(CompositionAdmissionError):
        store.write_once(subject, b"source", b"payload")
    path = store.root / subject.attempt.attempt_sha256[7:]
    assert (path / "source.json").read_bytes() == b"source"
    monkeypatch.setattr(module.os, "link", original)
    with pytest.raises(CompositionAdmissionError):
        store.write_once(subject, b"new", b"new")
    assert (path / "source.json").read_bytes() == b"source"


@pytest.mark.parametrize("result", [True, False, b"claimed proof"])
def test_boolean_or_unverified_custody_return_never_authorizes_write(files, result):
    store, subject, _ = files
    store._custody = lambda: result
    with pytest.raises(CompositionAdmissionError):
        store.write_once(subject, b"source", b"payload")
    assert not tuple(store.root.iterdir())


def test_changed_actual_identity_never_authorizes_write(files, monkeypatch):
    store, subject, _ = files
    monkeypatch.setattr(module.os, "geteuid", lambda: store._uid + 1)
    with pytest.raises(CompositionAdmissionError):
        store.write_once(subject, b"source", b"payload")
    assert not tuple(store.root.iterdir())


def test_post_install_custody_drift_does_not_ack_or_refund(files):
    store, subject, _ = files

    def drift():
        if (store.root / subject.attempt.attempt_sha256[7:] / "payload.native").exists():
            raise ValueError("mount changed")

    store._custody = drift
    with pytest.raises(CompositionAdmissionError):
        store.write_once(subject, b"source", b"payload")
    assert (store.root / subject.attempt.attempt_sha256[7:]).is_dir()


def test_root_profile_and_unprotected_ancestry_are_rejected(tmp_path):
    with pytest.raises(CompositionAdmissionError):
        module.ServiceSnapshotFiles(
            tmp_path,
            dispatcher_uid=0,
            dispatcher_gid=0,
            root_device=tmp_path.stat().st_dev,
            root_inode=tmp_path.stat().st_ino,
            require_custody=lambda: None,
        )


def test_read_detects_metadata_change_and_limits_chunk_size(files, monkeypatch):
    store, subject, _ = files
    store.write_once(subject, b"source", b"payload")
    original_read = os.read
    sizes = []

    def changed(fd, size):
        sizes.append(size)
        body = original_read(fd, size)
        os.fchmod(fd, 0o640)
        return body

    monkeypatch.setattr(module.os, "read", changed)
    with pytest.raises(CompositionAdmissionError):
        store.read(subject, record(subject, b"source", b"payload"))
    assert sizes and max(sizes) <= 65536


@pytest.mark.parametrize("damage", ["symlink", "replacement"])
def test_attempt_directory_alias_or_replacement_is_rejected(files, monkeypatch, damage):
    store, subject, _ = files
    store.write_once(subject, b"source", b"payload")
    path = store.root / subject.attempt.attempt_sha256[7:]
    if damage == "symlink":
        path.rename(path.with_name("old"))
        path.symlink_to("old", target_is_directory=True)
    else:
        original = store._read_file

        def replaced(parent, name, maximum):
            body = original(parent, name, maximum)
            if name == "payload.native":
                path.rename(path.with_name("old"))
                path.mkdir(mode=0o700)
            return body

        monkeypatch.setattr(store, "_read_file", replaced)
    with pytest.raises(CompositionAdmissionError):
        store.read(subject, record(subject, b"source", b"payload"))


def test_oversize_write_does_not_allocate_attempt(files):
    store, subject, _ = files
    with pytest.raises(CompositionAdmissionError):
        store.write_once(subject, b"source", b"x" * (subject.limits.max_wire_bytes + 1))
    assert not tuple(store.root.iterdir())


def test_real_traversal_rejects_unprotected_temporary_parent(tmp_path, monkeypatch):
    # Unlike the positive fixture this invokes the production traversal. The
    # unprivileged test process cannot provision a root-owned protected parent.
    monkeypatch.setattr(module.sys, "platform", "linux")
    root = tmp_path / "captures"
    root.mkdir(mode=0o700)
    info = root.stat()
    with pytest.raises(CompositionAdmissionError):
        module.ServiceSnapshotFiles(
            root,
            dispatcher_uid=os.geteuid(),
            dispatcher_gid=os.getegid(),
            root_device=info.st_dev,
            root_inode=info.st_ino,
            require_custody=lambda: None,
        )
