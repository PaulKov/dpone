"""Durable observer composition contracts."""

from contextlib import contextmanager
from typing import Any, cast
from uuid import UUID

import pytest

import dpone.app.mssql_sqlclient_native_observer_composition as subject
import dpone.app.mssql_sqlclient_stage_locator_composition as locator_composition


class _Store:
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        self.events = events
        self.fail = fail

    def load(self, key: str):
        self.events.append(f"load:{key}")
        if self.fail:
            raise RuntimeError("unknown")
        return None


def _factory(events: list[str], *, fail: bool = False):
    @contextmanager
    def admitted():
        store = _Store(events, fail=fail)
        events.append(f"open:{id(store)}")
        try:
            yield cast(Any, store)
        finally:
            events.append(f"close:{id(store)}")

    exact = object.__new__(locator_composition._AdmittedSqlClientStoreFactory)
    exact._factory = admitted
    exact._record = object()
    exact._domain_id = UUID(int=1)
    return exact


@pytest.fixture(autouse=True)
def _admitted_marker(monkeypatch):
    monkeypatch.setattr(locator_composition, "require_state_domain", lambda store, record: None)


def test_composition_is_pre_p7_pure_and_each_lifecycle_read_owns_context(monkeypatch):
    events: list[str] = []
    admitted = _factory(events)
    stores: list[object] = []

    class Journal:
        def __init__(self, store: object, *, backend: str) -> None:
            assert backend == "mssql_sqlclient"
            stores.append(store)

        def read(self, identity: object):
            events.append(f"read:{identity}")
            return None

    monkeypatch.setattr(subject, "TdsAttemptJournal", Journal)
    bundle = subject.compose_sqlclient_native_observers(admitted)
    assert events == []

    assert bundle.lifecycle.read(cast(Any, "one")) is None
    assert bundle.lifecycle.read(cast(Any, "two")) is None

    assert stores[0] is not stores[1]
    assert events[0].startswith("open:")
    assert events[2].startswith("close:")
    assert events[3].startswith("open:")
    assert events[5].startswith("close:")


def test_directory_read_binds_lifecycle_to_same_context(monkeypatch):
    events: list[str] = []
    admitted = _factory(events)
    seen: dict[str, object] = {}

    class Lifecycle:
        def __init__(self, store: object, *, backend: str) -> None:
            seen["lifecycle_store"] = store

    class Directory:
        def __init__(self, store: object, *, parent_observer: object) -> None:
            seen["directory_store"] = store
            seen["lifecycle"] = parent_observer

        def read(self, parent: object, limits: object):
            seen["parent"] = parent
            seen["limits"] = limits
            return None

    monkeypatch.setattr(subject, "TdsAttemptJournal", Lifecycle)
    monkeypatch.setattr(subject, "TdsCoordinatorDirectoryJournal", Directory)
    bundle = subject.compose_sqlclient_native_observers(admitted)

    assert bundle.directory.read(cast(Any, "parent"), cast(Any, "limits")) is None
    assert seen["lifecycle_store"] is seen["directory_store"]
    assert events[0].startswith("open:")
    assert events[-1].startswith("close:")


def test_unknown_lifecycle_read_fails_closed_and_closes_context():
    events: list[str] = []
    bundle = subject.compose_sqlclient_native_observers(_factory(events, fail=True))

    with pytest.raises(Exception, match="observation_unknown"):
        bundle.lifecycle.read(cast(Any, "identity"))

    assert events[-1].startswith("close:")


def test_new_bundle_reopens_same_durable_domain_for_source_free_restart(monkeypatch):
    events: list[str] = []
    admitted = _factory(events)
    saved = object()

    class Journal:
        def __init__(self, store: object, *, backend: str) -> None:
            assert backend == "mssql_sqlclient"

        def read(self, identity: object):
            assert identity == "persisted"
            return saved

    monkeypatch.setattr(subject, "TdsAttemptJournal", Journal)

    first = subject.compose_sqlclient_native_observers(admitted)
    restarted = subject.compose_sqlclient_native_observers(admitted)

    assert first.lifecycle is not restarted.lifecycle
    assert restarted.lifecycle.read(cast(Any, "persisted")) is saved
    assert events[0].startswith("open:")
    assert events[-1].startswith("close:")


@pytest.mark.parametrize("observer", ["lifecycle", "directory"])
def test_suppressing_backend_context_cannot_hide_observer_failure(monkeypatch, observer):
    events: list[str] = []

    @contextmanager
    def suppressing():
        events.append("open")
        try:
            yield cast(Any, object())
        except RuntimeError:
            events.append("suppression-attempted")
        finally:
            events.append("close")

    exact = object.__new__(locator_composition._AdmittedSqlClientStoreFactory)
    exact._factory = suppressing
    exact._record = object()
    exact._domain_id = UUID(int=2)

    class FailingLifecycle:
        def __init__(self, store: object, *, backend: str) -> None: ...

        def read(self, identity: object):
            raise RuntimeError("lifecycle failed")

    class FailingDirectory:
        def __init__(self, store: object, *, parent_observer: object) -> None: ...

        def read(self, parent: object, limits: object):
            raise RuntimeError("directory failed")

    monkeypatch.setattr(subject, "TdsAttemptJournal", FailingLifecycle)
    monkeypatch.setattr(subject, "TdsCoordinatorDirectoryJournal", FailingDirectory)
    bundle = subject.compose_sqlclient_native_observers(exact)

    with pytest.raises(RuntimeError, match=f"{observer} failed"):
        if observer == "lifecycle":
            bundle.lifecycle.read(cast(Any, "identity"))
        else:
            bundle.directory.read(cast(Any, "parent"), cast(Any, "limits"))

    assert events == ["open", "suppression-attempted", "close"]


def test_raw_structural_factory_is_rejected_without_opening_it():
    events: list[str] = []

    with pytest.raises(ValueError, match="observer_composition_invalid"):
        subject.compose_sqlclient_native_observers(cast(Any, _factory(events)._factory))

    assert events == []
