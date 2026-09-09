"""Real local SQLite transactions verify CAS, fencing and durable reopen."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.contracts.bounded_window import WindowContractError, WindowLeaseLost


def test_cas_and_reopen(tmp_path):
    path = tmp_path / "journal.sqlite"
    store = SQLiteWindowStore(path, clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 30)
    first = store.save("key", None, "payload", lease)
    with pytest.raises(WindowContractError):
        store.save("key", None, "wrong", lease)
    assert SQLiteWindowStore(path, clock=lambda: 1).load("key") == first
    assert store.save("key", first.revision, "next", lease).revision == 2


def test_expiry_renewal_and_stale_release(tmp_path):
    now = [1.0]
    store = SQLiteWindowStore(tmp_path / "journal.sqlite", clock=lambda: now[0])
    old = store.acquire("target", "owner", 10)
    now[0] = 5
    store.renew(old, 10)
    now[0] = 12
    store.assert_lease(old)
    now[0] = 15
    with pytest.raises(WindowLeaseLost):
        store.renew(old, 10)
    new = store.acquire("target", "owner2", 10)
    assert new.fence == old.fence + 1
    store.release(old)
    store.assert_lease(new)
    with pytest.raises(WindowLeaseLost):
        store.save("key", None, "stale", old)


def test_concurrent_acquire_has_one_winner(tmp_path):
    path = tmp_path / "journal.sqlite"
    store = SQLiteWindowStore(path, clock=lambda: 1)

    def acquire(_):
        try:
            return store.acquire("target", "same-owner", 10)
        except WindowLeaseLost:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        winners = [x for x in pool.map(acquire, range(8)) if x is not None]
    assert len(winners) == 1


@pytest.mark.parametrize("ttl", [0, -1, float("inf"), float("nan")])
def test_invalid_ttl(tmp_path, ttl):
    store = SQLiteWindowStore(tmp_path / "journal.sqlite", clock=lambda: 1)
    with pytest.raises(WindowContractError):
        store.acquire("target", "owner", ttl)
