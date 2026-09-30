"""Real local exclusion: lock release is never durable owner release."""

import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.contracts.clickhouse_authority import AuthorityConflict, AuthorityError
from tests.test_clickhouse_authority_sqlite import subject


def _exclusion(store):
    from dpone.adapters.clickhouse_authority_execution_lock import LocalPublicationExclusion

    return LocalPublicationExclusion(store)


@pytest.fixture
def store(tmp_path):
    path = tmp_path / "authority.db"
    SQLitePublicationAuthority.provision(path, "deployment")
    authority = SQLitePublicationAuthority(path, "deployment")
    authority.acquire("deployment:one", subject(), "candidate")
    return authority


def _contend(path, results):
    store = SQLitePublicationAuthority(path, "deployment")
    try:
        with _exclusion(store).hold("deployment:one"):
            results.put("acquired")
    except AuthorityConflict:
        results.put("conflict")


def _hold_until_killed(path, ready):
    store = SQLitePublicationAuthority(path, "deployment")
    with _exclusion(store).hold("deployment:one"):
        ready.set()
        multiprocessing.Event().wait(30)


def test_same_subject_threads_and_spawned_processes_conflict(store, tmp_path):
    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    with _exclusion(store).hold("deployment:one") as session:
        session.assert_current()
        with ThreadPoolExecutor(1) as executor:
            executor.submit(_contend, tmp_path / "authority.db", results).result(timeout=20)
        assert results.get(timeout=5) == "conflict"
        process = ctx.Process(target=_contend, args=(tmp_path / "authority.db", results))
        process.start()
        process.join(20)
        if process.is_alive():
            process.kill()
            process.join()
        assert process.exitcode == 0
        assert results.get(timeout=5) == "conflict"
    results.close()
    with _exclusion(store).hold("deployment:one") as next_session:
        next_session.assert_current()


def test_independent_subjects_do_not_share_lock(store):
    store.acquire("deployment:two", replace(subject(), target="other"), "candidate_two")
    lock = _exclusion(store)
    with lock.hold("deployment:one") as first, lock.hold("deployment:two") as second:
        first.assert_current()
        second.assert_current()


def test_exclusion_needs_only_original_identity_and_binding(store):
    from dpone.adapters.clickhouse_authority_execution_lock import PublicationIdentityReader

    class IdentityReader:
        def execution_identity(self):
            return store.execution_identity()

        def binding(self, operation_id):
            return store.binding(operation_id)

    reader: PublicationIdentityReader = IdentityReader()
    with _exclusion(reader).hold("deployment:one") as session:
        session.assert_current()


def test_session_rejects_other_thread_and_use_after_exit(store):
    with _exclusion(store).hold("deployment:one") as session:
        with ThreadPoolExecutor(1) as executor, pytest.raises(AuthorityError):
            executor.submit(session.assert_current).result(timeout=5)
    with pytest.raises(AuthorityError):
        session.assert_current()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX profile")
def test_fork_inherited_session_cannot_authorize_send(store):
    with _exclusion(store).hold("deployment:one") as session:
        pid = os.fork()
        if pid == 0:
            try:
                session.assert_current()
            except AuthorityError:
                os._exit(0)
            os._exit(5)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        session.assert_current()


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "public", "directory"])
def test_lock_identity_and_session_fail_closed(store, tmp_path, kind):
    path = tmp_path / f"authority.db.execution-{subject().key}.lock"
    other = tmp_path / "other"
    other.touch(mode=0o600)
    if kind == "symlink":
        path.symlink_to(other)
    elif kind == "hardlink":
        os.link(other, path)
    elif kind == "directory":
        path.mkdir(mode=0o700)
    else:
        path.touch(mode=0o644)
        path.chmod(0o644)
    with pytest.raises(AuthorityError), _exclusion(store).hold("deployment:one"):
        pytest.fail("Unsafe lock admitted")


def test_replaced_lock_invalidates_live_session(store, tmp_path):
    path = tmp_path / f"authority.db.execution-{subject().key}.lock"
    with _exclusion(store).hold("deployment:one") as session:
        path.rename(tmp_path / "preserved.lock")
        path.touch(mode=0o600)
        with pytest.raises(AuthorityError):
            session.assert_current()


def test_authority_replacement_invalidates_session(store, tmp_path):
    with _exclusion(store).hold("deployment:one") as session:
        path = tmp_path / "authority.db"
        path.rename(tmp_path / "preserved.db")
        path.touch(mode=0o600)
        with pytest.raises(AuthorityError):
            session.assert_current()


def test_lock_persists_across_holds_and_no_new_owner_is_admitted(store, tmp_path):
    path = tmp_path / f"authority.db.execution-{subject().key}.lock"
    with _exclusion(store).hold("deployment:one"):
        before = path.stat()
    with _exclusion(store).hold("deployment:one"):
        assert path.stat().st_ino == before.st_ino
        assert path.stat().st_mode & 0o077 == 0
    with pytest.raises(AuthorityConflict):
        store.acquire("deployment:next", subject(), "next_candidate")


def test_holder_death_releases_only_os_lock(store, tmp_path):
    ctx = multiprocessing.get_context("spawn")
    ready = ctx.Event()
    process = ctx.Process(target=_hold_until_killed, args=(tmp_path / "authority.db", ready))
    process.start()
    try:
        assert ready.wait(20)
        with pytest.raises(AuthorityConflict), _exclusion(store).hold("deployment:one"):
            pytest.fail("Concurrent holder admitted")
    finally:
        process.kill()
        process.join(10)
    with _exclusion(store).hold("deployment:one"):
        assert store.binding("deployment:one").subject == subject()
    with pytest.raises(AuthorityConflict):
        store.acquire("deployment:next", subject(), "next_candidate")


def test_execution_identity_is_read_only_and_rejects_missing_store(store, tmp_path):
    identity = store.execution_identity()
    path = tmp_path / "authority.db"
    assert (identity.path, identity.device, identity.inode, identity.deployment_id) == (
        str(path),
        path.stat().st_dev,
        path.stat().st_ino,
        "deployment",
    )
    path.rename(tmp_path / "preserved.db")
    with pytest.raises(AuthorityError):
        store.execution_identity()
    assert not path.exists()
