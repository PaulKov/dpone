"""Spawned-process proof of real journal races; no ClickHouse authority claim."""

import multiprocessing
import os

import pytest

from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.contracts.clickhouse_authority import AuthorityConflict, TransportState
from tests.test_clickhouse_authority_sqlite import prepared, subject


def _claim(path, barrier, results):
    store = SQLitePublicationAuthority(path, "deployment")
    entry = store.read("deployment:one")
    barrier.wait(timeout=20)
    results.put(store.claim(entry) is not None)


def _transition(path, barrier, results, grant):
    store = SQLitePublicationAuthority(path, "deployment")
    barrier.wait(timeout=20)
    try:
        if grant is None:
            store.close_without_send("deployment:one")
            results.put(("closed", 0))
        else:
            store.begin_send(grant)
            # Test-side transmission sentinel is reachable only after send ACK.
            results.put(("sent", 1))
    except AuthorityConflict:
        results.put(("blocked", 0))


def _crash(path, phase):
    store = SQLitePublicationAuthority(path, "deployment")
    entry = store.read("deployment:one")
    if phase != "prepared":
        grant = store.claim(entry)
        if phase == "sent":
            store.begin_send(grant)
    os._exit(9)


def _join(processes):
    for process in processes:
        process.join(timeout=30)
        if process.is_alive():
            process.kill()
            process.join()
            pytest.fail("authority process did not terminate")
        assert process.exitcode == 0


def _store(tmp_path):
    path = tmp_path / "authority.db"
    SQLitePublicationAuthority.provision(path, "deployment")
    store = SQLitePublicationAuthority(path, "deployment")
    prepared(store)
    return path, store


def test_two_process_claims_have_exactly_one_winner(tmp_path):
    path, store = _store(tmp_path)
    ctx = multiprocessing.get_context("spawn")
    barrier, results = ctx.Barrier(2), ctx.Queue()
    processes = [ctx.Process(target=_claim, args=(path, barrier, results)) for _ in range(2)]
    for process in processes:
        process.start()
    _join(processes)
    assert sorted([results.get(timeout=5), results.get(timeout=5)]) == [False, True]
    assert store.read("deployment:one").record.claim_granted
    results.close()


def test_send_and_close_have_one_durable_winner(tmp_path):
    path, store = _store(tmp_path)
    grant = store.claim(store.read("deployment:one"))
    ctx = multiprocessing.get_context("spawn")
    barrier, results = ctx.Barrier(2), ctx.Queue()
    processes = [ctx.Process(target=_transition, args=(path, barrier, results, choice)) for choice in (grant, None)]
    for process in processes:
        process.start()
    _join(processes)
    outcomes = [results.get(timeout=5), results.get(timeout=5)]
    state = store.transport_state("deployment:one")
    if state == TransportState.MAY_HAVE_SENT:
        assert sorted(outcomes) == [("blocked", 0), ("sent", 1)]
    else:
        assert state == TransportState.CLOSED_WITHOUT_SEND
        assert sorted(outcomes) == [("blocked", 0), ("closed", 0)]
    with pytest.raises(AuthorityConflict):
        store.begin_send(grant)
    results.close()


@pytest.mark.parametrize("phase", ["prepared", "claimed", "sent"])
def test_crash_retains_owner_and_never_regrants_claim(tmp_path, phase):
    path, store = _store(tmp_path)
    process = multiprocessing.get_context("spawn").Process(target=_crash, args=(path, phase))
    process.start()
    process.join(timeout=30)
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail("crash fixture hung")
    assert process.exitcode == 9
    reopened = SQLitePublicationAuthority(path, "deployment")
    with pytest.raises(AuthorityConflict):
        reopened.acquire("deployment:next", subject(), "next_candidate")
    if phase == "sent":
        assert reopened.transport_state("deployment:one") == TransportState.MAY_HAVE_SENT
        with pytest.raises(AuthorityConflict):
            reopened.close_without_send("deployment:one")
    else:
        reopened.close_without_send("deployment:one")
        assert reopened.transport_state("deployment:one") == TransportState.CLOSED_WITHOUT_SEND
    assert reopened.claim(reopened.read("deployment:one")) is None
