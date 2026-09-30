"""Process and thread exclusion over actual private v2 SQLite journals."""

from __future__ import annotations

import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from dpone.contracts.clickhouse_authority import AuthorityConflict, AuthorityError, TransportState
from tests.test_clickhouse_candidate_sqlite import complete_create, enrollment, journal, mutation, request, store


def _enroll_worker(path, value, barrier, results):
    from dpone.adapters.clickhouse_candidate_sqlite import SQLiteCandidateAuthority

    authority = SQLiteCandidateAuthority(path, "deployment")
    readiness = enrollment(value)
    barrier.wait(timeout=15)
    try:
        with authority.enroll(value, readiness):
            results.put("enrolled")
    except AuthorityConflict:
        results.put("conflict")


def _close_worker(path, registered, closed):
    from dpone.adapters.clickhouse_candidate_sqlite import SQLiteCandidateAuthority

    assert registered.wait(timeout=15)
    SQLiteCandidateAuthority(path, "deployment").close_admission("deployment:one")
    closed.set()


def _lost_enroll_ack_worker(path):
    from dpone.adapters.clickhouse_candidate_sqlite import SQLiteCandidateAuthority

    authority = SQLiteCandidateAuthority(path, "deployment")
    transaction = authority._storage.transaction

    @contextmanager
    def committed_exit():
        with transaction() as db:
            yield db
        os._exit(7)

    authority._storage.transaction = committed_exit
    value = request()
    authority.enroll(value, enrollment(value))
    os._exit(99)


def _forked_grant(writes, grant, results):
    try:
        writes.begin_send(grant)
    except AuthorityError:
        results.put("rejected")
    else:
        results.put("sent")


def _join(process, expected=0):
    process.join(timeout=20)
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail("owned candidate process did not finish")
    assert process.exitcode == expected


def test_cross_role_process_race_has_one_atomic_winner(tmp_path):
    authority = store(tmp_path)
    ctx = multiprocessing.get_context("spawn")
    barrier, results = ctx.Barrier(2), ctx.Queue()
    values = (request(), request("deployment:two", "candidate", "target"))
    processes = [
        ctx.Process(target=_enroll_worker, args=(tmp_path / "authority.db", value, barrier, results))
        for value in values
    ]
    for process in processes:
        process.start()
    for process in processes:
        _join(process)
    assert sorted(results.get(timeout=5) for _ in processes) == ["conflict", "enrolled"]
    with authority._storage.connection() as db:
        assert db.execute("SELECT count(*) FROM name_reservations").fetchone()[0] == 2
    results.close()


def test_close_race_keeps_accepted_writer_and_rejects_late_registration(tmp_path):
    authority = store(tmp_path)
    value = request()
    ctx = multiprocessing.get_context("spawn")
    registered, closed = ctx.Event(), ctx.Event()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = complete_create(authority, invocation)
        grant = writes.register(invocation, mutation(invocation, "insert", 1))
        worker = ctx.Process(target=_close_worker, args=(tmp_path / "authority.db", registered, closed))
        worker.start()
        registered.set()
        assert closed.wait(timeout=15)
        _join(worker)
        with pytest.raises(AuthorityConflict):
            writes.register(invocation, mutation(invocation, "insert", 2))
        writes.begin_send(grant)
        assert writes.requests(value.operation_id)[-1].state == TransportState.MAY_HAVE_SENT


def test_enrollment_crash_cannot_reissue_invocation(tmp_path):
    authority = store(tmp_path)
    process = multiprocessing.get_context("spawn").Process(
        target=_lost_enroll_ack_worker, args=(tmp_path / "authority.db",)
    )
    process.start()
    _join(process, 7)
    value = request()
    assert authority.inspect(value.operation_id).lifecycle == "registered"
    with pytest.raises(AuthorityConflict):
        authority.enroll(value, enrollment(value))


def test_grants_reject_foreign_thread_and_fork_before_send_entry(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = journal(authority)
        grant = writes.register(invocation, mutation(invocation))
        with ThreadPoolExecutor(max_workers=1) as executor:
            with pytest.raises(AuthorityError):
                executor.submit(writes.begin_send, grant).result(timeout=10)
        ctx = multiprocessing.get_context("fork")
        results = ctx.Queue()
        process = ctx.Process(target=_forked_grant, args=(writes, grant, results))
        process.start()
        _join(process)
        assert results.get(timeout=5) == "rejected"
        assert writes.requests(value.operation_id)[0].state == TransportState.NOT_STARTED
        results.close()
