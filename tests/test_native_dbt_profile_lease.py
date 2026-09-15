"""Real filesystem lifetime checks; no credential or execution qualification."""

import os
import stat

import pytest

from dpone.adapters.native_dbt_profile_lease import NativeDbtProfileLease
from dpone.contracts.dbt_publishing import DbtPublishingError


def test_profile_directory_outlives_secret_and_closes_with_lease(tmp_path):
    root = tmp_path.resolve()
    lease = NativeDbtProfileLease(root, max_bytes=64)
    assert list(root.iterdir()) == []
    with lease:
        path = lease.profile_path
        assert path.parent.is_dir() and not path.exists()
        assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
        with lease.materialize(b"synthetic-profile") as written:
            assert written == path and path.read_bytes() == b"synthetic-profile"
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert not path.exists() and path.parent.is_dir()
    assert not path.parent.exists()
    with pytest.raises(DbtPublishingError):
        _ = lease.profile_path
    with pytest.raises(DbtPublishingError):
        with lease:
            pass


@pytest.mark.parametrize("content", [b"", b"x" * 65, "text", bytearray(b"bytes")])
def test_invalid_content_does_not_create_profile(tmp_path, content):
    with NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64) as lease:
        path = lease.profile_path
        with pytest.raises(DbtPublishingError):
            with lease.materialize(content):
                pytest.fail("invalid content yielded")
        assert not path.exists()


def test_store_is_single_use(tmp_path):
    with NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64) as lease:
        with lease.materialize(b"first"):
            pass
        with pytest.raises(DbtPublishingError):
            with lease.materialize(b"second"):
                pytest.fail("second materialization yielded")


def test_symlink_root_or_ancestor_is_rejected(tmp_path):
    root = tmp_path.resolve()
    actual = root / "actual"
    actual.mkdir()
    (actual / "nested").mkdir()
    link = root / "link"
    link.symlink_to(actual, target_is_directory=True)
    for supplied in (link, link / "nested"):
        with pytest.raises(DbtPublishingError):
            with NativeDbtProfileLease(supplied, max_bytes=64):
                pytest.fail("linked root admitted")
    assert list((actual / "nested").iterdir()) == []


def test_substituted_profile_is_not_deleted_and_original_error_is_retained(tmp_path):
    original = RuntimeError("original execution failed")
    path = None
    with pytest.raises(DbtPublishingError) as caught:
        with NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64) as lease:
            path = lease.profile_path
            with lease.materialize(b"original"):
                path.rename(path.with_name("retained-original"))
                path.write_bytes(b"replacement")
                raise original
    assert path.read_bytes() == b"replacement"
    causes = []
    error = caught.value
    while error is not None:
        causes.append(error)
        error = error.__cause__ or error.__context__
    assert original in causes


def test_replaced_directory_does_not_receive_profile_or_get_removed(tmp_path):
    root = tmp_path.resolve()
    with pytest.raises(DbtPublishingError):
        with NativeDbtProfileLease(root, max_bytes=64) as lease:
            path = lease.profile_path
            moved = root / "moved"
            path.parent.rename(moved)
            path.parent.mkdir()
            with lease.materialize(b"secret"):
                pytest.fail("substituted directory admitted")
    assert path.parent.is_dir() and not path.exists()


@pytest.mark.parametrize("failure", ["write", "fsync"])
def test_partial_write_or_fsync_failure_cleans_only_owned_file(tmp_path, monkeypatch, failure):
    with NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64) as lease:
        path = lease.profile_path
        write = os.write
        calls = 0

        def fail_write(fd, payload):
            nonlocal calls
            calls += 1
            if calls == 1:
                return write(fd, payload[:1])
            raise OSError("injected write failure")

        def fail_sync(fd):
            raise OSError("injected fsync failure")

        with monkeypatch.context() as patch:
            patch.setattr(os, failure, fail_write if failure == "write" else fail_sync)
            with pytest.raises(DbtPublishingError):
                with lease.materialize(b"synthetic"):
                    pytest.fail("failed write yielded")
        assert not path.exists()


@pytest.mark.parametrize("bound", [True, 0, -1, 1.0])
def test_invalid_bound_is_rejected_without_io(tmp_path, monkeypatch, bound):
    def forbidden(*args, **kwargs):
        pytest.fail("constructor performed filesystem I/O")

    monkeypatch.setattr(os, "open", forbidden)
    with pytest.raises(DbtPublishingError):
        NativeDbtProfileLease(tmp_path.resolve(), max_bytes=bound)


def test_materialization_requires_open_lease(tmp_path):
    lease = NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64)
    with pytest.raises(DbtPublishingError):
        with lease.materialize(b"before"):
            pytest.fail("unopened lease yielded")
    with lease:
        pass
    with pytest.raises(DbtPublishingError):
        with lease.materialize(b"after"):
            pytest.fail("closed lease yielded")


def test_preexisting_profile_is_not_overwritten_or_deleted(tmp_path):
    with pytest.raises(DbtPublishingError):
        with NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64) as lease:
            path = lease.profile_path
            path.write_bytes(b"foreign-file")
            with lease.materialize(b"secret"):
                pytest.fail("existing file overwritten")
    assert path.read_bytes() == b"foreign-file"


def test_concurrent_materialization_cannot_write_twice(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    ready, release = Event(), Event()
    with NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64) as lease:

        def first():
            with lease.materialize(b"first"):
                ready.set()
                assert release.wait(5)

        def second():
            with lease.materialize(b"second"):
                pytest.fail("second concurrent writer admitted")

        with ThreadPoolExecutor(max_workers=2) as pool:
            one = pool.submit(first)
            assert ready.wait(5)
            two = pool.submit(second)
            release.set()
            one.result(timeout=5)
            with pytest.raises(DbtPublishingError, match="single-use"):
                two.result(timeout=5)


def test_cleanup_oserror_preserves_execution_error_cause(tmp_path, monkeypatch):
    original = RuntimeError("execution failed")
    with pytest.raises(DbtPublishingError) as caught:
        with NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64) as lease:
            with lease.materialize(b"synthetic"):

                def fail_unlink(*args, **kwargs):
                    raise OSError("injected cleanup failure")

                monkeypatch.setattr(os, "unlink", fail_unlink)
                raise original
    error = caught.value
    causes = []
    while error is not None:
        causes.append(error)
        error = error.__cause__ or error.__context__
    assert original in causes


def test_replaced_root_never_redirects_cleanup_to_new_root(tmp_path):
    root = tmp_path.resolve() / "root"
    root.mkdir()
    with NativeDbtProfileLease(root, max_bytes=64) as lease:
        path = lease.profile_path
        moved = root.with_name("moved-root")
        root.rename(moved)
        root.mkdir()
        sentinel = root / "do-not-remove"
        sentinel.write_bytes(b"foreign")
        with pytest.raises(DbtPublishingError):
            _ = lease.profile_path
    assert sentinel.read_bytes() == b"foreign"
    assert list(moved.iterdir()) == []
    assert not path.exists()


def test_close_failure_still_closes_other_descriptor_without_retry(tmp_path, monkeypatch):
    lease = NativeDbtProfileLease(tmp_path.resolve(), max_bytes=64)
    lease.__enter__()
    child_fd, root_fd = lease._child_fd, lease._root_fd
    close = os.close
    attempted = []

    def uncertain_close(descriptor):
        attempted.append(descriptor)
        close(descriptor)
        if descriptor == child_fd:
            raise OSError("close acknowledgement lost")

    with monkeypatch.context() as patch:
        patch.setattr(os, "close", uncertain_close)
        with pytest.raises((DbtPublishingError, OSError)):
            lease.__exit__(None, None, None)
        lease.__exit__(None, None, None)
    assert attempted == [child_fd, root_fd]
    for descriptor in (child_fd, root_fd):
        with pytest.raises(OSError):
            os.fstat(descriptor)
    assert lease._child_fd == lease._root_fd == -1
