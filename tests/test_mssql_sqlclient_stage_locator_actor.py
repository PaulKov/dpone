"""Actual actor/context lifecycle gates domain admission and immutable discovery."""

# ruff: noqa: F811

from contextlib import contextmanager
from dataclasses import replace
from threading import Event, current_thread
from time import monotonic

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.app.mssql_sqlclient_stage_locator_composition import (
    admit_sqlclient_state_domain,
    create_sqlclient_stage_locator,
    read_sqlclient_stage_locator,
)
from dpone.contracts.bounded_window import WindowContractError, WindowRecord
from tests.test_mssql_sqlclient_stage_locator import REQUEST
from tests.test_mssql_sqlclient_stage_locator_journal import setup  # noqa: F401


def test_domain_factory_reads_same_context_before_yield_and_closes_actor(setup):
    store, lease, domain, locator, _ = setup
    events = []

    @contextmanager
    def factory():
        events.append(("enter", current_thread()))
        try:
            yield store
        finally:
            events.append(("exit", current_thread()))

    pool = TdsActorPool(capacity=1)
    admitted = admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 2)
    assert pool.live_count == 0 and len(events) == 2
    assert events[0][1] is events[1][1] and events[0][1] is not current_thread()
    assert admitted.domain_id == locator.state_domain_id
    snapshot = create_sqlclient_stage_locator(
        admitted_factory=admitted, locator=locator, request=REQUEST, lease=lease, pool=pool, deadline=monotonic() + 2
    )
    assert (
        read_sqlclient_stage_locator(
            admitted_factory=admitted, lookup=locator.lookup(), pool=pool, deadline=monotonic() + 2
        )
        == snapshot
    )
    assert pool.live_count == 0


@pytest.mark.parametrize("drift", ["absent", "revision", "payload"])
def test_wrapper_rejects_marker_drift_before_yield(setup, monkeypatch, drift):
    store, lease, domain, _, _ = setup

    @contextmanager
    def factory():
        yield store

    pool = TdsActorPool(capacity=1)
    admitted = admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 2)
    current = (
        None
        if drift == "absent"
        else replace(domain, revision=domain.revision + 1)
        if drift == "revision"
        else WindowRecord(1, "{}")
    )
    monkeypatch.setattr(store, "load", lambda key: current)
    with pytest.raises(WindowContractError):
        with admitted():
            pytest.fail("yielded store after domain drift")


def test_failed_context_teardown_never_returns_admission(setup):
    store, lease, *_ = setup

    @contextmanager
    def factory():
        yield store
        raise OSError("context exit failed")

    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsJournalActorUnknown) as failure:
        admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 2)
    assert failure.value.gateway is not None


def test_blocked_teardown_retains_pool_and_gateway_until_actual_exit(setup):
    store, lease, *_ = setup
    waiting, release = Event(), Event()

    @contextmanager
    def factory():
        try:
            yield store
        finally:
            waiting.set()
            release.wait(5)

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(TdsJournalActorUnknown) as failure:
            admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 0.1)
        assert waiting.is_set() and pool.live_count == 1
        assert failure.value.gateway is not None
    finally:
        release.set()
        pool.close(deadline=monotonic() + 2)
    assert pool.live_count == 0


@pytest.mark.parametrize("point", ["before_save", "after_save"])
def test_late_domain_commit_never_returns_admission_or_releases_live_actor(tmp_path, point):
    from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
    from dpone.contracts.mssql_sqlclient_stage_locator import STATE_DOMAIN_KEY

    path = tmp_path / "late.sqlite"
    store = SQLiteWindowStore(path, clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    entered, release = Event(), Event()
    save = store.save

    def blocked(*args):
        if point == "after_save":
            result = save(*args)
        entered.set()
        release.wait(5)
        return result if point == "after_save" else save(*args)

    store.save = blocked

    @contextmanager
    def factory():
        yield store

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(TdsJournalActorUnknown) as failure:
            admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 0.1)
        assert entered.is_set() and pool.live_count == 1
        assert failure.value.gateway is not None
        with pytest.raises(TdsJournalActorUnknown):
            admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 0.1)
    finally:
        release.set()
        pool.close(deadline=monotonic() + 2)
    assert store.load(STATE_DOMAIN_KEY) is not None


def test_cross_target_domain_cas_has_one_winner_without_read_retry(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore

    path = tmp_path / "race.sqlite"
    stores = {target: SQLiteWindowStore(path, clock=lambda: 1.0) for target in ("target-a", "target-b")}
    leases = {target: store.acquire(target, target, 60) for target, store in stores.items()}
    barrier = Barrier(2)
    winners, failures = [], []

    def admit(target):
        store, lease = stores[target], leases[target]
        reads = 0
        load = store.load

        def synchronized(key):
            nonlocal reads
            result = load(key)
            reads += 1
            if reads == 1:
                assert result is None
                barrier.wait(timeout=2)
            return result

        store.load = synchronized

        @contextmanager
        def factory():
            yield store

        pool = TdsActorPool(capacity=1)
        try:
            result = admit_sqlclient_state_domain(
                store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 3
            )
            winners.append((result, reads))
        except TdsJournalActorUnknown as error:
            failures.append((error, reads))
        finally:
            pool.close(deadline=monotonic() + 2)

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(admit, ("target-a", "target-b")))
    assert len(winners) == len(failures) == 1
    assert winners[0][1] == 2 and failures[0][1] == 1
    assert failures[0][0].gateway is not None


def test_same_locator_race_keeps_one_immutable_winner(setup, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from dpone.contracts.mssql_sqlclient_stage_locator import stage_locator_key

    store, lease, _, locator, _ = setup
    barrier = Barrier(2)

    @contextmanager
    def factory():
        yield store

    admitted = admit_sqlclient_state_domain(
        store_factory=factory, lease=lease, pool=TdsActorPool(capacity=1), deadline=monotonic() + 2
    )
    load = store.load
    key = stage_locator_key(locator.lookup())

    def synchronized(name):
        record = load(name)
        if name == key:
            assert record is None
            barrier.wait(timeout=2)
        return record

    monkeypatch.setattr(store, "load", synchronized)

    def create(_):
        pool = TdsActorPool(capacity=1)
        try:
            return create_sqlclient_stage_locator(
                admitted_factory=admitted,
                locator=locator,
                request=REQUEST,
                lease=lease,
                pool=pool,
                deadline=monotonic() + 3,
            )
        except TdsJournalActorUnknown as error:
            return error
        finally:
            pool.close(deadline=monotonic() + 2)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(create, range(2)))
    assert sum(isinstance(result, TdsJournalActorUnknown) for result in results) == 1
    assert load(key).revision == 1


@pytest.mark.parametrize("phase", ["domain", "locator"])
def test_failed_teardown_cannot_be_suppressed_into_success(setup, phase):
    store, lease, _, locator, _ = setup

    @contextmanager
    def good():
        yield store

    pool = TdsActorPool(capacity=1)
    admitted = admit_sqlclient_state_domain(store_factory=good, lease=lease, pool=pool, deadline=monotonic() + 2)

    @contextmanager
    def failed():
        yield store
        raise RuntimeError("teardown failed")

    with pytest.raises(TdsJournalActorUnknown) as failure:
        if phase == "domain":
            admit_sqlclient_state_domain(store_factory=failed, lease=lease, pool=pool, deadline=monotonic() + 2)
        else:
            admitted._factory = failed
            create_sqlclient_stage_locator(
                admitted_factory=admitted,
                locator=locator,
                request=REQUEST,
                lease=lease,
                pool=pool,
                deadline=monotonic() + 2,
            )
    assert failure.value.gateway is not None


@pytest.mark.parametrize("after", [False, True])
def test_late_locator_commit_retains_actual_gateway_and_never_authorizes_create(setup, monkeypatch, after):
    from dpone.contracts.mssql_sqlclient_stage_locator import stage_locator_key

    store, lease, _, locator, _ = setup

    @contextmanager
    def factory():
        yield store

    pool = TdsActorPool(capacity=1)
    admitted = admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 2)
    started, release = Event(), Event()
    save = store.save

    def blocked(*args):
        if after:
            result = save(*args)
        started.set()
        release.wait(5)
        return result if after else save(*args)

    monkeypatch.setattr(store, "save", blocked)
    try:
        with pytest.raises(TdsJournalActorUnknown) as failure:
            create_sqlclient_stage_locator(
                admitted_factory=admitted,
                locator=locator,
                request=REQUEST,
                lease=lease,
                pool=pool,
                deadline=monotonic() + 0.1,
            )
        assert started.is_set() and pool.live_count == 1 and failure.value.gateway is not None
    finally:
        release.set()
        pool.close(deadline=monotonic() + 2)
    assert store.load(stage_locator_key(locator.lookup())).revision == 1


def test_factory_drift_to_different_store_fails_before_body(setup, tmp_path):
    from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore

    store, lease, *_ = setup
    other = SQLiteWindowStore(tmp_path / "other.sqlite", clock=lambda: 1.0)
    current = store

    @contextmanager
    def factory():
        yield current

    admitted = admit_sqlclient_state_domain(
        store_factory=factory, lease=lease, pool=TdsActorPool(capacity=1), deadline=monotonic() + 2
    )
    current = other
    with pytest.raises(WindowContractError):
        with admitted():
            pytest.fail("different store yielded")


def test_backend_cannot_suppress_missing_marker_into_yield(setup, monkeypatch):
    store, lease, *_ = setup

    @contextmanager
    def factory():
        try:
            yield store
        except WindowContractError:
            pass

    admitted = admit_sqlclient_state_domain(
        store_factory=factory, lease=lease, pool=TdsActorPool(capacity=1), deadline=monotonic() + 2
    )
    monkeypatch.setattr(store, "load", lambda _: None)
    with pytest.raises(WindowContractError):
        with admitted():
            pytest.fail("missing marker yielded")


def test_actor_constructs_all_observers_and_service_on_same_backend(setup, monkeypatch):
    import dpone.app.mssql_sqlclient_stage_locator_composition as composition

    store, lease, _, locator, _ = setup
    factories = []
    observed = []

    @contextmanager
    def factory():
        factories.append(current_thread())
        yield store

    pool = TdsActorPool(capacity=1)
    admitted = admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 2)
    for name in (
        "TdsAttemptJournal",
        "TdsCoordinatorDirectoryJournal",
        "TdsCoordinatorJournal",
        "SqlClientStageLocatorJournal",
    ):
        original = getattr(composition, name)

        def capture(store_context, *args, _name=name, _original=original, **kwargs):
            observed.append((_name, store_context, current_thread()))
            return _original(store_context, *args, **kwargs)

        monkeypatch.setattr(composition, name, capture)
    create_sqlclient_stage_locator(
        admitted_factory=admitted, locator=locator, request=REQUEST, lease=lease, pool=pool, deadline=monotonic() + 2
    )
    assert len(factories) == 2 and len(observed) == 4
    assert all(backend is store and thread is factories[1] for _, backend, thread in observed)


@pytest.mark.parametrize("leaf", ["state_domain_id", "object_nonce", "database_guid"])
def test_read_actor_snapshot_owns_independent_uuid_leaves(leaf):
    from contextlib import nullcontext

    from dpone.app.mssql_sqlclient_stage_locator_composition import (
        ReadSqlClientStageLocator,
        SqlClientStageLocatorActor,
    )
    from dpone.contracts.mssql_sqlclient_stage_locator import (
        decode_stage_locator,
        encode_stage_locator,
        encode_state_domain,
        stage_locator_key,
    )
    from tests.test_mssql_sqlclient_stage_locator import LOCATOR

    lookup = decode_stage_locator(encode_stage_locator(LOCATOR)).lookup()
    actor = SqlClientStageLocatorActor(
        lambda: nullcontext(None),
        WindowRecord(1, encode_state_domain(lookup.state_domain_id).decode()),
        ReadSqlClientStageLocator(lookup),
        monotonic() + 2,
        monotonic,
    )
    snapshot = actor._initialization.lookup
    original_id = lookup.database.database_guid if leaf == "database_guid" else getattr(lookup, leaf)
    frozen_id = snapshot.database.database_guid if leaf == "database_guid" else getattr(snapshot, leaf)
    before = stage_locator_key(snapshot)
    assert frozen_id == original_id
    object.__setattr__(original_id, "int", 99)
    assert stage_locator_key(snapshot) == before
    assert frozen_id is not original_id
