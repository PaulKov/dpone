"""Exact coordinator CAS preserves unknown outcomes without SQL-side claims."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_coordinator_journal import TdsCoordinatorJournal
from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown, WindowRecord
from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorCredentialIntent,
    CoordinatorFailed,
    CoordinatorProcessRegistered,
    TdsCoordinatorPhase,
    coordinator_key,
)
from dpone.contracts.mssql_tds_directory import directory_key
from dpone.contracts.mssql_tds_directory_codec import decode_directory_record, encode_directory_record
from dpone.contracts.mssql_tds_worker import TdsAttemptError, TdsAttemptOwnership
from tests.test_mssql_tds_coordinator import PROCESS, identity, reservation
from tests.test_mssql_tds_directory import LIMITS, PARENT

TOKEN = str(UUID(int=40))


class DirectoryReader:
    """Injected read-only fixture using real directory bytes, no constructor coupling."""

    def __init__(self, store):
        self.store = store

    def read(self, parent, limits):
        record = self.store.load(directory_key(parent))
        return (
            None
            if record is None
            else decode_directory_record(
                record.payload.encode(), revision=record.revision, parent=parent, limits=limits
            )
        )


@pytest.fixture
def setup(tmp_path):
    store = SQLiteWindowStore(tmp_path / "coordinator.sqlite", clock=lambda: 1.0)
    lease = store.acquire(PARENT.target_key, "owner", 30)
    owner = TdsAttemptOwnership(lease.owner, lease.fence, TOKEN)
    reserved = reservation()
    reserved = replace(reserved, slots=(replace(reserved.slots[0], owner_fence=lease.fence),))
    store.save(directory_key(PARENT), None, encode_directory_record(reserved, owner).decode(), lease)
    reader = DirectoryReader(store)
    journal = TdsCoordinatorJournal(store, reader)
    yield store, lease, journal, reader, replace(identity(), original_fence=lease.fence)


def create(setup):
    _, lease, journal, _, operation = setup
    return journal.create(operation, LIMITS, lease, supervisor_token=TOKEN)


def test_real_create_and_read_acknowledge_only_intent(setup):
    store, _, journal, _, operation = setup
    assert journal.read(operation) is None
    writer = create(setup)
    assert writer.snapshot.state.phase is TdsCoordinatorPhase.INTENT
    assert writer.snapshot.revision == 1
    assert journal.read(operation) == writer.snapshot
    assert store.load(coordinator_key(operation)) is not None


def register(writer):
    return writer.advance(CoordinatorProcessRegistered(PROCESS, "a" * 64), expected_phase=TdsCoordinatorPhase.INTENT)


def successor(setup):
    store, lease, _, reader, _ = setup
    observed = reader.read(PARENT, LIMITS)
    store.release(lease)
    lease = store.acquire(PARENT.target_key, "successor", 30)
    owner = TdsAttemptOwnership(lease.owner, lease.fence, str(UUID(int=41)))
    store.save(directory_key(PARENT), observed.revision, encode_directory_record(observed.state, owner).decode(), lease)
    return lease, owner.supervisor_id


@pytest.mark.parametrize("bad", ["missing", "slot", "owner", "fence", "target", "limits"])
def test_create_rejects_bad_admission_before_save(setup, monkeypatch, bad):
    store, lease, journal, reader, operation = setup
    limits = LIMITS
    token = TOKEN
    if bad == "missing":
        monkeypatch.setattr(reader, "read", lambda *args: None)
    elif bad == "slot":
        operation = replace(operation, operation_id=UUID(int=90))
    elif bad == "owner":
        token = str(UUID(int=90))
    elif bad == "fence":
        lease = replace(lease, fence=lease.fence + 1)
    elif bad == "target":
        lease = replace(lease, target_id="other")
    else:
        limits = replace(limits, max_entries=limits.max_entries + 1)
    monkeypatch.setattr(store, "save", lambda *args: pytest.fail("invalid admission saved"))
    with pytest.raises((WindowContractError, WindowOutcomeUnknown)):
        journal.create(operation, limits, lease, supervisor_token=token)


def test_stable_key_collision_and_changed_identity_read_never_replace_original(setup):
    _, lease, journal, _, operation = setup
    original = create(setup).snapshot
    changed = replace(operation, implementation_sha256="f" * 64)
    assert coordinator_key(changed) == coordinator_key(operation)
    with pytest.raises(WindowContractError):
        journal.create(changed, LIMITS, lease, supervisor_token=TOKEN)
    with pytest.raises(WindowContractError):
        journal.read(changed)
    assert journal.read(operation) == original


@pytest.mark.parametrize("phase", ["create", "advance", "takeover"])
@pytest.mark.parametrize(
    "bad", ["type", "boolean", "nonincreasing", "negative", "overflow", "float", "payload", "bytes"]
)
def test_invalid_ack_never_authorizes_or_refreshes_writer(setup, monkeypatch, phase, bad):
    store, _, journal, _, operation = setup
    writer = None if phase == "create" else create(setup)
    before = None if writer is None else writer.snapshot
    lease, token = successor(setup) if phase == "takeover" else (setup[1], TOKEN)
    original = store.save

    def save(key, expected, payload, current_lease):
        original(key, expected, payload, current_lease)
        responses = {
            "type": object(),
            "boolean": WindowRecord(True, payload),
            "nonincreasing": WindowRecord(expected or 0, payload),
            "negative": WindowRecord(-1, payload),
            "overflow": WindowRecord(2**63, payload),
            "float": WindowRecord(1.0, payload),
            "payload": WindowRecord((expected or 0) + 1, "{}"),
            "bytes": WindowRecord((expected or 0) + 1, payload.encode()),
        }
        return responses[bad]

    monkeypatch.setattr(store, "save", save)
    with pytest.raises(WindowOutcomeUnknown, match="ack_unknown"):
        if phase == "create":
            create(setup)
        elif phase == "advance":
            register(writer)
        else:
            journal.take_over(before, LIMITS, lease, supervisor_token=token)
    assert journal.read(operation) is not None
    if phase == "advance":
        assert writer.snapshot == before
        monkeypatch.setattr(store, "load", lambda *args: pytest.fail("poisoned writer read"))
        with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
            writer.assert_authority()
        with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
            register(writer)


@pytest.mark.parametrize("phase", ["create", "advance", "takeover"])
@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt])
def test_interruption_after_possible_durable_write_does_not_erase_state(setup, monkeypatch, phase, error_type):
    store, _, journal, _, operation = setup
    writer = None if phase == "create" else create(setup)
    before = None if writer is None else writer.snapshot
    lease, token = successor(setup) if phase == "takeover" else (setup[1], TOKEN)
    original = store.save

    def interrupted(*args):
        original(*args)
        raise error_type("lost reply")

    monkeypatch.setattr(store, "save", interrupted)
    with pytest.raises(WindowOutcomeUnknown if error_type is OSError else KeyboardInterrupt):
        if phase == "create":
            create(setup)
        elif phase == "advance":
            register(writer)
        else:
            journal.take_over(before, LIMITS, lease, supervisor_token=token)
    observed = journal.read(operation)
    assert observed is not None
    if phase == "advance":
        assert observed.state.phase is TdsCoordinatorPhase.PROCESS_REGISTERED
        assert writer.snapshot == before
        with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
            writer.assert_authority()


@pytest.mark.parametrize(
    "bad",
    [
        "type",
        "revision_bool",
        "revision_zero",
        "revision_overflow",
        "payload_type",
        "oversized",
        "duplicate",
        "malformed",
    ],
)
def test_invalid_durable_record_fails_closed(setup, monkeypatch, bad):
    store, _, journal, _, operation = setup
    create(setup)
    real = store.load(coordinator_key(operation))
    cases = {
        "type": object(),
        "revision_bool": WindowRecord(True, real.payload),
        "revision_zero": WindowRecord(0, real.payload),
        "revision_overflow": WindowRecord(2**63, real.payload),
        "payload_type": WindowRecord(1, b"{}"),
        "oversized": WindowRecord(1, " " * 16385),
        "duplicate": WindowRecord(1, real.payload.replace('"sequence":0', '"sequence":0,"sequence":0')),
        "malformed": WindowRecord(1, "{"),
    }
    monkeypatch.setattr(store, "load", lambda key: cases[bad])
    with pytest.raises(WindowContractError, match="record_invalid"):
        journal.read(operation)


def test_takeover_preserves_exact_state_and_cannot_resume_credentials(setup):
    _, _, journal, _, _ = setup
    writer = create(setup)
    before = register(writer)
    lease, token = successor(setup)
    recovered = journal.take_over(before, LIMITS, lease, supervisor_token=token)
    assert recovered.snapshot.state.execution_owner == before.state.execution_owner
    assert recovered.snapshot.state.process == before.state.process
    with pytest.raises(WindowContractError, match="transition_invalid"):
        recovered.advance(CoordinatorCredentialIntent(), expected_phase=TdsCoordinatorPhase.PROCESS_REGISTERED)
    result = recovered.advance(
        CoordinatorFailed(TdsAttemptError.FENCING), expected_phase=TdsCoordinatorPhase.PROCESS_REGISTERED
    )
    assert result.state.error is TdsAttemptError.FENCING


@pytest.mark.parametrize("bad", ["stale", "equal_fence", "changed_identity"])
def test_invalid_takeover_rejects_before_save(setup, monkeypatch, bad):
    store, _, journal, _, _ = setup
    observed = create(setup).snapshot
    lease, token = successor(setup)
    if bad == "stale":
        observed = replace(observed, revision=observed.revision + 1)
    elif bad == "equal_fence":
        lease = replace(lease, fence=observed.state.ownership.fence)
    else:
        changed = replace(observed.state.identity, implementation_sha256="f" * 64)
        observed = replace(observed, state=replace(observed.state, identity=changed))
    monkeypatch.setattr(store, "save", lambda *args: pytest.fail("rejected takeover saved"))
    with pytest.raises(WindowContractError):
        journal.take_over(observed, LIMITS, lease, supervisor_token=token)


def test_invalid_pure_transition_does_no_io_and_writer_can_continue(setup, monkeypatch):
    writer = create(setup)
    with monkeypatch.context() as patch:
        patch.setattr(setup[0], "load", lambda *args: pytest.fail("invalid event read"))
        patch.setattr(setup[0], "save", lambda *args: pytest.fail("invalid event saved"))
        with pytest.raises(WindowContractError):
            writer.advance(CoordinatorCredentialIntent(), expected_phase=TdsCoordinatorPhase.INTENT)
    assert register(writer).state.phase is TdsCoordinatorPhase.PROCESS_REGISTERED


def test_true_thread_pid_and_nonreentrant_confinement(setup, monkeypatch):
    import os
    from concurrent.futures import ThreadPoolExecutor

    writer = create(setup)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with pytest.raises(WindowContractError, match="owner_mismatch"):
            executor.submit(writer.assert_authority).result()
    pid = os.getpid()
    with monkeypatch.context() as patch:
        patch.setattr(os, "getpid", lambda: pid + 1)
        patch.setattr(setup[0], "load", lambda *args: pytest.fail("fork read"))
        with pytest.raises(WindowContractError, match="owner_mismatch"):
            writer.assert_authority()
    original = setup[0].load
    count = []

    def load(key):
        with pytest.raises(WindowContractError, match="reentrant"):
            writer.assert_authority()
        count.append(1)
        return original(key)

    monkeypatch.setattr(setup[0], "load", load)
    writer.assert_authority()
    assert len(count) == 2


def test_reservation_without_operation_record_remains_gap_after_newer_fence(setup):
    _, _, journal, _, operation = setup
    assert journal.read(operation) is None
    lease, token = successor(setup)
    with pytest.raises(WindowContractError, match="creation_rejected"):
        journal.create(operation, LIMITS, lease, supervisor_token=token)
    with pytest.raises(WindowContractError, match="observation_required"):
        journal.take_over(None, LIMITS, lease, supervisor_token=token)
    assert journal.read(operation) is None


@pytest.mark.parametrize("source", ["load", "lease", "directory"])
def test_uncertain_authority_read_poisoned_without_snapshot_refresh(setup, monkeypatch, source):
    store, _, _, reader, _ = setup
    writer = create(setup)
    before = writer.snapshot

    def unavailable(*args):
        raise OSError("storage unavailable")

    target, method = (
        (reader, "read") if source == "directory" else (store, "assert_lease" if source == "lease" else "load")
    )
    with monkeypatch.context() as patch:
        patch.setattr(target, method, unavailable)
        with pytest.raises(WindowOutcomeUnknown):
            writer.assert_authority()
    assert writer.snapshot == before
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        writer.assert_authority()


def test_changed_operation_revision_permanently_invalidates_writer(setup):
    store, lease, _, _, operation = setup
    writer = create(setup)
    before = writer.snapshot
    record = store.load(coordinator_key(operation))
    store.save(coordinator_key(operation), record.revision, record.payload, lease)
    with pytest.raises(WindowContractError, match="revision_changed"):
        writer.assert_authority()
    assert writer.snapshot == before
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        register(writer)


def test_observed_directory_slot_blocks_forward_barrier_before_save(setup):
    from dpone.contracts.mssql_tds_directory import TdsLocalContainment, parent_digest, record_local_containment

    store, lease, journal, reader, operation = setup
    writer = create(setup)
    before = register(writer)
    directory = reader.read(PARENT, LIMITS)
    proof = TdsLocalContainment(parent_digest(PARENT), operation.operation_id, "a" * 64, "b" * 64)
    state = record_local_containment(directory.state, operation.slot_index, proof)
    store.save(
        directory_key(PARENT), directory.revision, encode_directory_record(state, directory.ownership).decode(), lease
    )
    with pytest.raises(WindowContractError, match="reservation_changed"):
        writer.advance(CoordinatorCredentialIntent(), expected_phase=TdsCoordinatorPhase.PROCESS_REGISTERED)
    assert writer.snapshot == before and journal.read(operation) == before
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        writer.assert_authority()
