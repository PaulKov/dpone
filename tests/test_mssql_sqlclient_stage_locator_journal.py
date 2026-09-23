"""Real original journals precede immutable lookup CAS; failures never replay."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_coordinator_journal import TdsCoordinatorJournal
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown, WindowRecord
from dpone.contracts.mssql_sqlclient_stage_locator import (
    STATE_DOMAIN_KEY,
    stage_locator_key,
    validate_state_domain_record,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_key
from dpone.contracts.mssql_tds_directory import directory_key
from dpone.services.mssql_sqlclient_stage_locator import SqlClientStageLocatorJournal, admit_state_domain
from tests.test_mssql_sqlclient_stage_locator import LIMITS, LOCATOR, OPERATION, OWNER, PARENT, REQUEST


@pytest.fixture
def setup(tmp_path):
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire(PARENT.target_key, OWNER.owner, 60)
    domain = admit_state_domain(store, lease)
    locator = replace(LOCATOR, state_domain_id=validate_state_domain_record(domain))
    parent = TdsAttemptJournal(store, backend="mssql_sqlclient")
    parent.create(PARENT, lease, supervisor_token=OWNER.supervisor_id)
    directories = TdsCoordinatorDirectoryJournal(store, parent_observer=parent)
    writer = directories.create(PARENT, LIMITS, lease, supervisor_token=OWNER.supervisor_id)
    writer.reserve_operation(
        operation_id=OPERATION.operation_id, command=OPERATION.command, command_sha256=OPERATION.command_sha256
    )
    TdsCoordinatorJournal(store, directories).create(OPERATION, LIMITS, lease, supervisor_token=OWNER.supervisor_id)
    yield (
        store,
        lease,
        domain,
        locator,
        SqlClientStageLocatorJournal(
            store,
            domain,
            parent_observer=parent,
            directory_observer=directories,
            coordinator_observer=TdsCoordinatorJournal(store, directories),
        ),
    )


def test_domain_restart_stable_and_locator_create_once(setup):
    store, lease, domain, locator, journal = setup
    assert admit_state_domain(store, lease) == domain
    observed = journal.create(locator, REQUEST, lease)
    assert journal.read(locator.lookup()) == observed
    with pytest.raises(WindowContractError):
        journal.create(locator, REQUEST, lease)
    assert journal.read(locator.lookup()) == observed


@pytest.mark.parametrize("key", ["domain", "parent", "directory", "coordinator"])
def test_missing_originals_fail_before_locator_write(setup, monkeypatch, key):
    store, lease, _, locator, journal = setup
    original = store.load
    selected = {
        "domain": STATE_DOMAIN_KEY,
        "directory": directory_key(PARENT),
        "coordinator": coordinator_key(OPERATION),
    }

    def load(name):
        return (
            None
            if name == selected.get(key) or (key == "parent" and name.startswith("mssql-tds-attempt-v1/"))
            else original(name)
        )

    monkeypatch.setattr(store, "load", load)
    with pytest.raises(WindowContractError):
        journal.create(locator, REQUEST, lease)
    assert original(stage_locator_key(locator.lookup())) is None


@pytest.mark.parametrize("bad", [None, True, 0, 2**63, 1.0, "payload"])
def test_bad_cas_ack_is_unknown_even_when_committed(setup, monkeypatch, bad):
    store, lease, _, locator, journal = setup
    save = store.save

    def altered(key, expected, payload, current):
        save(key, expected, payload, current)
        return (
            None if bad is None else WindowRecord(1 if bad == "payload" else bad, "{}" if bad == "payload" else payload)
        )

    monkeypatch.setattr(store, "save", altered)
    with pytest.raises(WindowOutcomeUnknown):
        journal.create(locator, REQUEST, lease)
    assert journal.read(locator.lookup()).locator == locator


@pytest.mark.parametrize("after", [False, True])
def test_ambiguous_save_preserves_absence_or_committed_original(setup, monkeypatch, after):
    store, lease, _, locator, journal = setup
    save = store.save

    def failed(*args):
        if after:
            save(*args)
        raise OSError("lost acknowledgement")

    monkeypatch.setattr(store, "save", failed)
    with pytest.raises(WindowOutcomeUnknown):
        journal.create(locator, REQUEST, lease)
    assert (store.load(stage_locator_key(locator.lookup())) is not None) == after


def test_wrong_request_nonce_and_digest_have_no_effect(setup):
    store, lease, _, locator, journal = setup
    with pytest.raises((ValueError, WindowContractError)):
        journal.create(locator, replace(REQUEST, object_nonce=UUID(int=99)), lease)
    assert store.load(stage_locator_key(locator.lookup())) is None


def test_database_name_drift_does_not_select_another_record(setup):
    _, lease, _, locator, journal = setup
    journal.create(locator, REQUEST, lease)
    with pytest.raises(WindowContractError):
        journal.read(replace(locator.lookup(), database=replace(locator.database, name="renamed")))


@pytest.mark.parametrize("part", ["parent", "directory", "coordinator"])
def test_newer_owner_cannot_create_locator_for_original_owner(setup, part):
    store, lease, _, locator, journal = setup
    parents = TdsAttemptJournal(store, backend="mssql_sqlclient")
    directories = TdsCoordinatorDirectoryJournal(store, parent_observer=parents)
    coordinator = TdsCoordinatorJournal(store, directories)
    parent_snapshot = parents.read(PARENT)
    directory_snapshot = directories.read(PARENT, LIMITS)
    coordinator_snapshot = coordinator.read(OPERATION)
    store.release(lease)
    successor = store.acquire(PARENT.target_key, "new-owner", 60)
    token = str(UUID(int=40))
    parents.take_over(parent_snapshot, successor, supervisor_token=token)
    if part != "parent":
        directories.take_over(directory_snapshot, successor, supervisor_token=token)
    if part == "coordinator":
        coordinator.take_over(coordinator_snapshot, LIMITS, successor, supervisor_token=token)
    with pytest.raises(WindowContractError):
        journal.create(locator, REQUEST, successor)
    assert store.load(stage_locator_key(locator.lookup())) is None


@pytest.mark.parametrize("field,value", [("work_sealed", True), ("admission_closed", True), ("schema_version", 1)])
def test_changed_directory_blocks_creation(setup, field, value):
    store, lease, _, locator, journal = setup
    record = store.load(directory_key(PARENT))
    from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

    body = strict_json_object(record.payload)
    if field == "schema_version":
        body["directory"]["schema"] = "dpone.tds.coordinator-directory.v1"
    else:
        body["directory"][field] = value
    store.save(directory_key(PARENT), record.revision, canonical_json_bytes(body).decode(), lease)
    with pytest.raises((ValueError, WindowContractError)):
        journal.create(locator, REQUEST, lease)
    assert store.load(stage_locator_key(locator.lookup())) is None


@pytest.mark.parametrize(
    "mutation", ["domain", "nonce", "implementation", "request_digest", "limits", "operation_uuid", "owner", "fence"]
)
def test_original_bindings_reject_drift_before_save(setup, mutation):
    store, lease, _, locator, journal = setup
    changes = {
        "domain": {"state_domain_id": UUID(int=999)},
        "nonce": {"object_nonce": UUID(int=999)},
        "implementation": {"create_operation": replace(OPERATION, implementation_sha256="e" * 64)},
        "request_digest": {"create_operation": replace(OPERATION, command_sha256="e" * 64)},
        "limits": {"directory_limits": replace(LIMITS, max_entries=11)},
        "operation_uuid": {"create_operation": replace(OPERATION, operation_id=UUID(int=999))},
        "owner": {"execution_owner": replace(OWNER, owner="other")},
        "fence": {"execution_owner": replace(OWNER, fence=2), "create_operation": replace(OPERATION, original_fence=2)},
    }
    with pytest.raises((ValueError, WindowContractError)):
        journal.create(replace(locator, **changes[mutation]), REQUEST, lease)
    assert store.load(stage_locator_key(locator.lookup())) is None


def test_recovery_reads_original_after_takeover_without_repointing(setup):
    store, lease, _, locator, journal = setup
    recorded = journal.create(locator, REQUEST, lease)
    parents = TdsAttemptJournal(store, backend="mssql_sqlclient")
    directories = TdsCoordinatorDirectoryJournal(store, parent_observer=parents)
    coordinators = TdsCoordinatorJournal(store, directories)
    p, d, c = parents.read(PARENT), directories.read(PARENT, LIMITS), coordinators.read(OPERATION)
    store.release(lease)
    successor = store.acquire(PARENT.target_key, "new-owner", 60)
    token = str(UUID(int=40))
    parents.take_over(p, successor, supervisor_token=token)
    directories.take_over(d, successor, supervisor_token=token)
    coordinators.take_over(c, LIMITS, successor, supervisor_token=token)
    assert journal.read(locator.lookup()) == recorded
    with pytest.raises(WindowContractError):
        journal.create(locator, REQUEST, successor)


@pytest.mark.parametrize(
    "error", ["readback", "readback_revision", "readback_payload", "save_before", "save_after", "ack", "assert"]
)
def test_domain_initialization_unknown_and_invalid_faults(tmp_path, monkeypatch, error):
    store = SQLiteWindowStore(tmp_path / "domain.sqlite", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    load, save, assert_lease = store.load, store.save, store.assert_lease
    saved = False

    def failure_save(*args):
        nonlocal saved
        if error == "save_before":
            raise OSError("unknown")
        record = save(*args)
        saved = True
        if error == "save_after":
            raise OSError("unknown")
        return replace(record, revision=True) if error == "ack" else record

    def failure_load(key):
        record = load(key)
        if saved:
            if error == "readback":
                raise OSError("unknown")
            if error == "readback_revision":
                return replace(record, revision=record.revision + 1)
            if error == "readback_payload":
                return replace(record, payload="{}")
        return record

    def failure_assert(current):
        assert_lease(current)
        if saved and error == "assert":
            raise OSError("unknown")

    monkeypatch.setattr(store, "save", failure_save)
    monkeypatch.setattr(store, "load", failure_load)
    monkeypatch.setattr(store, "assert_lease", failure_assert)
    with pytest.raises((WindowContractError, WindowOutcomeUnknown)):
        admit_state_domain(store, lease)
    assert (load(STATE_DOMAIN_KEY) is not None) == (error != "save_before")


def test_actual_lost_lease_prevents_locator_save(setup):
    store, lease, _, locator, journal = setup
    store.release(lease)
    with pytest.raises(WindowContractError):
        journal.create(locator, REQUEST, lease)
    assert store.load(stage_locator_key(locator.lookup())) is None


@pytest.mark.parametrize("key_kind", ["parent", "directory", "coordinator"])
def test_existing_locator_with_missing_original_is_not_recovery_authority(setup, monkeypatch, key_kind):
    store, lease, _, locator, journal = setup
    journal.create(locator, REQUEST, lease)
    load = store.load

    def missing(key):
        selected = (
            key.startswith("mssql-tds-attempt-v1/")
            if key_kind == "parent"
            else key == directory_key(PARENT)
            if key_kind == "directory"
            else key == coordinator_key(OPERATION)
        )
        return None if selected else load(key)

    monkeypatch.setattr(store, "load", missing)
    with pytest.raises(WindowContractError):
        journal.read(locator.lookup())


def test_service_uses_injected_original_observers(setup):
    from dpone.services.mssql_sqlclient_stage_locator import SqlClientStageLocatorJournal as Service

    store, lease, domain, locator, _ = setup
    parents = TdsAttemptJournal(store, backend="mssql_sqlclient")
    directories = TdsCoordinatorDirectoryJournal(store, parent_observer=parents)
    coordinators = TdsCoordinatorJournal(store, directories)
    calls = []

    class Observer:
        def __init__(self, label, delegate):
            self.label, self.delegate = label, delegate

        def read(self, *args):
            calls.append((self.label, args))
            return self.delegate.read(*args)

    service = Service(
        store,
        domain,
        parent_observer=Observer("parent", parents),
        directory_observer=Observer("directory", directories),
        coordinator_observer=Observer("coordinator", coordinators),
    )
    observed = service.create(locator, REQUEST, lease)
    assert service.read(locator.lookup()) == observed
    assert [label for label, _ in calls] == ["parent", "directory", "coordinator"] * 2


@pytest.mark.parametrize("selected", ["parent", "directory", "coordinator"])
def test_injected_observer_cannot_substitute_another_original_identity(setup, selected):
    store, lease, domain, locator, original = setup
    recorded = original.create(locator, REQUEST, lease)
    parents = TdsAttemptJournal(store, backend="mssql_sqlclient")
    directories = TdsCoordinatorDirectoryJournal(store, parent_observer=parents)
    coordinators = TdsCoordinatorJournal(store, directories)
    other = replace(PARENT, run_id="other-run")

    class Changed:
        def __init__(self, delegate):
            self.delegate = delegate

        def read(self, *args):
            snapshot = self.delegate.read(*args)
            state = snapshot.state
            if selected == "parent":
                state = replace(state, identity=other)
            elif selected == "directory":
                state = replace(state, parent=other)
            else:
                state = replace(state, identity=replace(state.identity, parent=other))
            return replace(snapshot, state=state)

    service = SqlClientStageLocatorJournal(
        store,
        domain,
        parent_observer=Changed(parents) if selected == "parent" else parents,
        directory_observer=Changed(directories) if selected == "directory" else directories,
        coordinator_observer=Changed(coordinators) if selected == "coordinator" else coordinators,
    )
    with pytest.raises(WindowContractError):
        service.read(locator.lookup())
    assert original.read(locator.lookup()) == recorded
