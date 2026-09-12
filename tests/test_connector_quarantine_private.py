"""Deterministic linearization and cleanup checks for connection publication."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock

import psycopg
import pytest

from dpone.runtime.connectors.postgres import PostgresConnector


def connector():
    return PostgresConnector("unused", 5432, "unused", "unused", "unused")


class Physical:
    def __init__(self, failure=None):
        self.failure = failure
        self.closed = 0

    def close(self):
        self.closed += 1
        if self.failure is not None:
            raise self.failure


def test_two_acquisitions_publish_one_and_close_loser(monkeypatch):
    owner = connector()
    gate = Barrier(2)
    made = []

    def connect(**kwargs):
        physical = Physical()
        made.append(physical)
        gate.wait(timeout=3)
        return physical

    monkeypatch.setattr(psycopg, "connect", connect)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(lambda: owner.connection) for _ in range(2)]
        results = [future.result(timeout=5) for future in futures]
    assert results[0] is results[1] is owner._connection
    assert sum(item.closed for item in made) == 1
    owner.close()
    assert sum(item.closed for item in made) == 2


def test_quarantine_first_lock_exit_is_already_revoked_and_detached():
    owner = connector()
    physical = Physical()
    owner._connection = physical
    observed = []
    mutex = Lock()

    class ObserveLock:
        def __enter__(self):
            mutex.acquire()

        def __exit__(self, *args):
            observed.append((owner._quarantined, owner._connection))
            mutex.release()

    owner._connection_lock = ObserveLock()
    owner.quarantine()
    assert observed == [(True, None)]
    assert physical.closed == 1


@pytest.mark.parametrize(
    "failure_type", [RuntimeError, asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit]
)
def test_quarantine_failure_never_republishes_handle(failure_type):
    owner = connector()
    primary = failure_type("close failure")
    physical = Physical(primary)
    owner._connection = physical
    if failure_type is RuntimeError:
        owner.quarantine()
    else:
        with pytest.raises(BaseException) as caught:
            owner.quarantine()
        assert caught.value is primary
        assert primary.__cause__ is primary.__context__ is None
    assert owner._connection is None and owner._quarantined
    with pytest.raises(RuntimeError, match="postgres_connector.quarantined"):
        _ = owner.connection
    owner.quarantine()
    assert physical.closed == 1


def test_exact_quarantine_does_not_revoke_empty_or_replacement():
    owner = connector()
    assert owner.quarantine_if_current(None) is False
    current = Physical()
    stale = Physical()
    owner._connection = current
    assert owner.quarantine_if_current(stale) is False
    assert owner.connection is current
    assert current.closed == stale.closed == 0
    owner.close()


def test_loser_close_quarantine_cannot_return_retired_winner(monkeypatch):
    owner = connector()
    winner = Physical()

    class Loser(Physical):
        def close(self):
            super().close()
            owner.quarantine()

    loser = Loser()

    def connect(**kwargs):
        owner._connection = winner
        return loser

    monkeypatch.setattr(psycopg, "connect", connect)
    with pytest.raises(RuntimeError, match="postgres_connector.quarantined"):
        _ = owner.connection
    assert owner._connection is None and owner._quarantined
    assert winner.closed == loser.closed == 1
