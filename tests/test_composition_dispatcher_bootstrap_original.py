"""Real local descriptor tests with explicit root-ownership boundary doubles."""

import os
from hashlib import sha256

import pytest

from dpone.adapters import composition_dispatcher_bootstrap_original as bootstrap
from dpone.contracts.composition_identity import CompositionAdmissionError


@pytest.fixture
def location(tmp_path, monkeypatch):
    root = tmp_path / "protected"
    root.mkdir(mode=0o750)
    parent = root / "startup"
    parent.mkdir(mode=0o750)
    path = parent / "bootstrap.json"
    original = bootstrap._require_info

    def local(info, gid, *, directory):
        # Host is macOS/nonroot; only ownership is replaced for these FS tests.
        from types import SimpleNamespace

        values = {name: getattr(info, name) for name in bootstrap._STABLE}
        values.update(st_uid=0, st_gid=gid)
        return original(SimpleNamespace(**values), gid, directory=directory)

    monkeypatch.setattr(bootstrap, "_require_info", local)
    monkeypatch.setattr(
        bootstrap, "open_protected", lambda path, **kwargs: os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    )
    return path


def opened(path, **overrides):
    return bootstrap.open_dispatcher_bootstrap(path, dispatcher_gid=1200, deadline=20.0, clock=lambda: 1.0, **overrides)


def put(path, body=b'{"schema":"example"}'):
    path.write_bytes(body)
    path.chmod(0o640)


def test_exact_held_original_and_close(location):
    put(location)
    before = list(location.parent.iterdir())
    owner = opened(location)
    assert owner is not None
    assert owner.document == location.read_bytes()
    assert owner.sha256 == "sha256:" + sha256(owner.document).hexdigest()
    owner.require_current()
    owner.close()
    owner.close()
    with pytest.raises(CompositionAdmissionError):
        owner.require_current()
    assert list(location.parent.iterdir()) == before


def test_only_missing_leaf_waits(location):
    assert opened(location) is None
    location.parent.rmdir()
    with pytest.raises(CompositionAdmissionError):
        opened(location)


@pytest.mark.parametrize("content", [b"", b"{", b'{"a":1,"a":2}', b'{ "a":1}', b"[]", b"x" * (1024 * 1024 + 1)])
def test_bad_original(location, content):
    put(location, content)
    with pytest.raises(CompositionAdmissionError):
        opened(location)


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo"])
def test_unsafe_leaf(location, kind):
    source = location.parent / "other"
    if kind == "fifo":
        os.mkfifo(location)
    else:
        put(source)
        if kind == "symlink":
            location.symlink_to(source)
        else:
            os.link(source, location)
    with pytest.raises(CompositionAdmissionError):
        opened(location)


@pytest.mark.parametrize("kind", ["leaf", "parent", "bytes", "mode"])
def test_changed_original(location, kind):
    put(location)
    owner = opened(location)
    assert owner is not None
    if kind == "leaf":
        location.rename(location.with_name("old"))
        put(location)
    elif kind == "parent":
        location.parent.rename(location.parent.with_name("old"))
        location.parent.mkdir()
        put(location)
    elif kind == "bytes":
        location.write_bytes(b'{"schema":"changed"}')
    else:
        location.chmod(0o660)
    with pytest.raises(CompositionAdmissionError):
        owner.require_current()
    owner.close()


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf"), 0.0, "20"])
def test_invalid_deadline(location, deadline):
    with pytest.raises(CompositionAdmissionError):
        bootstrap.open_dispatcher_bootstrap(location, dispatcher_gid=1200, deadline=deadline, clock=lambda: 1.0)


def test_expired_owner_closes_both(location):
    put(location)
    clock = [1.0]
    owner = bootstrap.open_dispatcher_bootstrap(location, dispatcher_gid=1200, deadline=20, clock=lambda: clock[0])
    assert owner is not None
    descriptors = (owner._parent, owner._descriptor)
    clock[0] = 21
    with pytest.raises(CompositionAdmissionError):
        owner.require_current()
    for fd in descriptors:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_close_failure_attempts_both_descriptors(location, monkeypatch):
    put(location)
    owner = opened(location)
    assert owner is not None
    original = os.close
    closed = []

    def fail_after_close(fd):
        original(fd)
        closed.append(fd)
        if fd == owner._descriptor:
            raise OSError("failure without sensitive details")

    monkeypatch.setattr(bootstrap.os, "close", fail_after_close)
    with pytest.raises(CompositionAdmissionError):
        owner.close()
    assert closed == [owner._descriptor, owner._parent]
    owner.close()


def test_rotation_during_reread_is_rejected(location, monkeypatch):
    put(location)
    owner = opened(location)
    assert owner is not None
    original = bootstrap._read

    def rotate(fd, check):
        content = original(fd, check)
        location.rename(location.with_name("old"))
        put(location)
        return content

    monkeypatch.setattr(bootstrap, "_read", rotate)
    with pytest.raises(CompositionAdmissionError):
        owner.require_current()


@pytest.mark.parametrize("field,value", [("st_uid", 1), ("st_gid", 999), ("st_mode", 0o100660), ("st_nlink", 2)])
def test_actual_file_metadata_policy(field, value):
    from types import SimpleNamespace

    values = dict(st_uid=0, st_gid=1200, st_mode=0o100640, st_nlink=1, st_size=20)
    values[field] = value
    with pytest.raises(CompositionAdmissionError):
        bootstrap._require_info(SimpleNamespace(**values), 1200, directory=False)


def test_late_leaf_open_closes_every_descriptor(location, monkeypatch):
    put(location)
    original = os.open
    descriptors = []
    clock = [1.0]

    def tracked(name, *args, **kwargs):
        descriptor = original(name, *args, **kwargs)
        descriptors.append(descriptor)
        if name == location.name:
            clock[0] = 21.0
        return descriptor

    monkeypatch.setattr(bootstrap.os, "open", tracked)
    with pytest.raises(CompositionAdmissionError):
        bootstrap.open_dispatcher_bootstrap(location, dispatcher_gid=1200, deadline=20, clock=lambda: clock[0])
    for fd in descriptors:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_intermediate_close_failure_does_not_leak_parent(location, monkeypatch):
    put(location)
    original_open, original_close = os.open, os.close
    descriptors = []

    def tracked(*args, **kwargs):
        descriptor = original_open(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor

    def failed(fd):
        original_close(fd)
        if descriptors and fd == descriptors[0]:
            raise OSError("root close error")

    monkeypatch.setattr(bootstrap.os, "open", tracked)
    monkeypatch.setattr(bootstrap.os, "close", failed)
    with pytest.raises(CompositionAdmissionError):
        opened(location)
    assert len(descriptors) == 2
    for fd in descriptors:
        with pytest.raises(OSError):
            os.fstat(fd)


@pytest.mark.parametrize("present", [True, False])
def test_temporary_close_crossing_deadline_rejects(location, monkeypatch, present):
    clock = [1.0]
    owner = None
    if present:
        put(location)
        owner = bootstrap.open_dispatcher_bootstrap(location, dispatcher_gid=1200, deadline=20, clock=lambda: clock[0])
        assert owner is not None
    parents = []
    original_parent, original_close = bootstrap._parent, os.close

    def tracked_parent(*args, **kwargs):
        descriptor = original_parent(*args, **kwargs)
        parents.append(descriptor)
        return descriptor

    def late_close(descriptor):
        original_close(descriptor)
        if len(parents) == 2 and descriptor == parents[-1]:
            clock[0] = 21.0

    monkeypatch.setattr(bootstrap, "_parent", tracked_parent)
    monkeypatch.setattr(bootstrap.os, "close", late_close)
    with pytest.raises(CompositionAdmissionError):
        if owner is not None:
            owner.require_current()
        else:
            bootstrap.open_dispatcher_bootstrap(location, dispatcher_gid=1200, deadline=20, clock=lambda: clock[0])
    descriptors = parents + ([] if owner is None else [owner._parent, owner._descriptor])
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)
