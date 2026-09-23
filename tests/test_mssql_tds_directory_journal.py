"""Directory storage envelopes retain ownership without granting write authority."""

import json
from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_directory import TdsDirectorySnapshot
from dpone.contracts.mssql_tds_directory_codec import decode_directory_record, encode_directory_record
from dpone.contracts.mssql_tds_worker import TdsAttemptOwnership
from tests.test_mssql_tds_directory import LIMITS, PARENT, initial, reserve

OWNER = TdsAttemptOwnership("owner", 2, str(UUID(int=42)))


def read(body, revision=3, **kwargs):
    return decode_directory_record(
        body, revision=revision, parent=kwargs.get("parent", PARENT), limits=kwargs.get("limits", LIMITS)
    )


def test_storage_roundtrip_keeps_revision_external_and_owner_bound():
    snapshot = TdsDirectorySnapshot(reserve(initial()), OWNER, 3)
    payload = encode_directory_record(snapshot.state, snapshot.ownership)
    assert read(payload) == snapshot
    assert "revision" not in json.loads(payload)
    assert read(payload, revision=4) == replace(snapshot, revision=4)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(extra="secret-canary"),
        lambda d: d.update(schema="other"),
        lambda d: d["ownership"].update(extra=1),
        lambda d: d["ownership"].update(fence=True),
        lambda d: d["ownership"].update(fence=0),
        lambda d: d["ownership"].update(supervisor_id="{00000000-0000-0000-0000-00000000002a}"),
        lambda d: d["ownership"].update(owner=""),
        lambda d: d["directory"]["limits"].update(max_entries=100),
        lambda d: d["directory"]["parent"].update(run_id="other"),
    ],
)
def test_storage_rejects_ambiguous_or_drifted_record(mutation):
    raw = json.loads(encode_directory_record(initial(), OWNER))
    mutation(raw)
    with pytest.raises(ValueError) as caught:
        read(json.dumps(raw).encode())
    assert str(caught.value) == "mssql_native.tds_directory_storage_record_invalid"


@pytest.mark.parametrize("revision", [True, 0, -1, "3", None])
def test_invalid_store_revision(revision):
    with pytest.raises(ValueError):
        read(encode_directory_record(initial(), OWNER), revision)


def test_owner_cannot_precede_reserved_operation_fence():
    with pytest.raises(ValueError):
        encode_directory_record(reserve(initial(), fence=3), OWNER)
    raw = json.loads(encode_directory_record(reserve(initial()), OWNER))
    raw["directory"]["slots"][0]["owner_fence"] = 3
    with pytest.raises(ValueError):
        read(json.dumps(raw).encode())


def test_envelope_is_bounded_before_json_parse(monkeypatch):
    import dpone.contracts.mssql_tds_directory_codec as codec

    def forbidden(*args):
        pytest.fail("oversized input parsed")

    monkeypatch.setattr(codec, "strict_json_object", forbidden)
    with pytest.raises(ValueError):
        read(b"x" * (LIMITS.max_encoded_bytes + 2049))


@pytest.mark.parametrize("body", [b"[]", b"null", b"\xff", b'{"x":1,"x":2}', "text", bytearray(b"{}")])
def test_malformed_envelope(body):
    with pytest.raises(ValueError, match="mssql_native.tds_directory_storage_record_invalid"):
        read(body)


@pytest.mark.parametrize("owner", ["😀" * 256, '"' * 256, "\\" * 256])
def test_maximum_valid_owner_roundtrip_fits_envelope(owner):
    admitted = replace(OWNER, owner=owner)
    payload = encode_directory_record(initial(), admitted)
    assert len(payload) <= LIMITS.max_encoded_bytes + 2048
    assert read(payload).ownership == admitted


def test_invalid_unicode_owner_is_redacted():
    raw = json.loads(encode_directory_record(initial(), OWNER))
    raw["ownership"]["owner"] = "\ud800"
    with pytest.raises(ValueError) as caught:
        read(json.dumps(raw).encode())
    assert str(caught.value) == "mssql_native.tds_directory_storage_record_invalid"


@pytest.mark.parametrize("storage", [False, True])
def test_saved_history_rejects_ordinary_work_after_retirement(storage):
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand, encode_directory
    from dpone.contracts.mssql_tds_directory_codec import decode_directory
    from tests.test_mssql_tds_directory import authority, authorize_retirement, seal_work, settled

    state = authorize_retirement(seal_work(settled(reserve(initial()))), authority(True))
    state = settled(reserve(state, command=TdsCoordinatorCommand.RETIRE, number=2), 1)
    state = settled(reserve(state, command=TdsCoordinatorCommand.RETIRE, number=3), 2)
    raw = json.loads(encode_directory_record(state, OWNER) if storage else encode_directory(state))
    directory = raw["directory"] if storage else raw
    directory["slots"][-1]["command"] = TdsCoordinatorCommand.CREATE.value
    with pytest.raises(ValueError):
        if storage:
            read(json.dumps(raw).encode())
        else:
            decode_directory(json.dumps(raw).encode(), parent=PARENT, limits=LIMITS)
    with pytest.raises(ValueError):
        replace(state, slots=state.slots[:-1] + (replace(state.slots[-1], command=TdsCoordinatorCommand.CREATE),))


@pytest.fixture
def storage(tmp_path):
    from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
    from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
    from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal

    store = SQLiteWindowStore(tmp_path / "directory.sqlite", clock=lambda: 1.0)
    lease = store.acquire(PARENT.target_key, "owner", 30)
    TdsAttemptJournal(store).create(PARENT, lease, supervisor_token=OWNER.supervisor_id)
    return store, lease, TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))


def create(storage):
    _, lease, journal = storage
    return journal.create(PARENT, LIMITS, lease, supervisor_token=OWNER.supervisor_id)


def operate(writer, number=1):
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand

    return writer.reserve_operation(
        operation_id=UUID(int=number), command=TdsCoordinatorCommand.CREATE, command_sha256="a" * 64
    )


def test_create_read_reopen_and_duplicate_rejection(storage):
    from dpone.contracts.bounded_window import WindowContractError

    writer = create(storage)
    assert storage[2].read(PARENT, LIMITS) == writer.snapshot
    assert writer.snapshot.revision == 1
    assert not hasattr(storage[2].read(PARENT, LIMITS), "reserve_operation")
    with pytest.raises(WindowContractError):
        create(storage)
    observed = operate(writer)
    assert observed.revision > 1
    assert storage[2].read(PARENT, LIMITS) == observed


def test_parent_creation_intent_required(storage):
    from dpone.contracts.bounded_window import WindowContractError

    store, lease, journal = storage
    other = replace(PARENT, run_id="missing")
    with pytest.raises(WindowContractError, match="parent"):
        journal.create(other, LIMITS, lease, supervisor_token=OWNER.supervisor_id)
    assert journal.read(other, LIMITS) is None
    with pytest.raises(WindowContractError, match="parent"):
        journal.create(PARENT, LIMITS, lease, supervisor_token=str(UUID(int=999)))


def test_takeover_requires_new_epoch_and_new_supervisor(storage):
    from dpone.contracts.bounded_window import WindowContractError

    writer = create(storage)
    store, lease, journal = storage
    observed = writer.snapshot
    with pytest.raises(WindowContractError):
        journal.take_over(observed, lease, supervisor_token=str(UUID(int=3)))
    store.release(lease)
    new = store.acquire(PARENT.target_key, "new", 30)
    with pytest.raises(WindowContractError):
        journal.take_over(observed, new, supervisor_token=OWNER.supervisor_id)
    successor = journal.take_over(observed, new, supervisor_token=str(UUID(int=3)))
    assert successor.snapshot.ownership.fence == new.fence
    with pytest.raises(WindowContractError):
        operate(successor)
    with pytest.raises(WindowContractError):
        operate(writer)
    with pytest.raises(WindowContractError):
        journal.take_over(observed, new, supervisor_token=str(UUID(int=4)))


@pytest.mark.parametrize("failure", [RuntimeError("private-canary"), KeyboardInterrupt()])
def test_lost_save_ack_poisoned_and_never_replayed(storage, monkeypatch, failure):
    from dpone.contracts.bounded_window import WindowOutcomeUnknown

    writer = create(storage)
    store, _, journal = storage
    original = store.save

    def lose(*args):
        original(*args)
        raise failure

    monkeypatch.setattr(store, "save", lose)
    with pytest.raises(KeyboardInterrupt if isinstance(failure, KeyboardInterrupt) else WindowOutcomeUnknown):
        operate(writer)
    monkeypatch.setattr(store, "save", original)
    assert len(journal.read(PARENT, LIMITS).state.slots) == 1
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        writer.assert_authority()


@pytest.mark.parametrize("bad", [None, "not-record", 0, 1, True, "payload"])
def test_invalid_save_ack_poisoned(storage, monkeypatch, bad):
    from dpone.contracts.bounded_window import WindowOutcomeUnknown, WindowRecord

    writer = create(storage)
    store, _, _ = storage
    original = store.save

    def corrupt(*args):
        result = original(*args)
        if bad == "payload":
            return WindowRecord(result.revision, "wrong")
        if type(bad) in (int, bool):
            return WindowRecord(bad, result.payload)
        return bad

    monkeypatch.setattr(store, "save", corrupt)
    with pytest.raises(WindowOutcomeUnknown):
        operate(writer)
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        writer.assert_authority()


def test_unknown_read_poison_and_noop_revision(storage, monkeypatch):
    from dpone.contracts.bounded_window import WindowOutcomeUnknown

    writer = create(storage)
    sealed = writer.seal_work()
    assert writer.seal_work() == sealed
    store, _, _ = storage
    original = store.load

    def fail(*args):
        raise OSError("private-canary")

    monkeypatch.setattr(store, "load", fail)
    with pytest.raises(WindowOutcomeUnknown):
        writer.assert_authority()
    monkeypatch.setattr(store, "load", original)
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        writer.assert_authority()


def test_writer_thread_and_process_are_confined(storage, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    import dpone.adapters.mssql_tds_directory_journal as adapter
    from dpone.contracts.bounded_window import WindowContractError

    writer = create(storage)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(WindowContractError, match="thread"):
            pool.submit(writer.assert_authority).result()
    writer.assert_authority()
    old_pid = adapter.os.getpid()
    monkeypatch.setattr(adapter.os, "getpid", lambda: old_pid + 1)
    with pytest.raises(WindowContractError, match="process"):
        writer.assert_authority()


def test_retirement_authority_must_match_durable_parent(storage):
    from dpone.contracts.bounded_window import WindowContractError
    from tests.test_mssql_tds_directory import authority

    writer = create(storage)
    before = writer.seal_work()
    with pytest.raises(WindowContractError, match="parent"):
        writer.authorize_retirement(authority())
    assert writer.snapshot == before


def test_recovery_chain_keeps_original_pending(storage):
    from dpone.contracts.bounded_window import WindowContractError
    from tests.test_mssql_tds_directory import local, remote

    old = create(storage)
    observed = operate(old)
    store, lease, journal = storage
    store.release(lease)
    new = store.acquire(PARENT.target_key, "new", 30)
    writer = journal.take_over(observed, new, supervisor_token=str(UUID(int=3)))
    snapshot = writer.reserve_reconciliation(
        operation_id=UUID(int=2), command_sha256="a" * 64, reconciles_slot=0, containment=local(observed.state)
    )
    writer.record_local_containment(1, local(snapshot.state, 1))
    writer.record_remote_settlement(1, remote(snapshot.state, 1))
    with pytest.raises(WindowContractError):
        writer.seal_work()
    writer.record_remote_settlement(0, remote(snapshot.state, 0))
    sealed = writer.seal_work()
    assert sealed.state.work_sealed
    assert writer.close_admission().state.admission_closed


def test_retirement_requires_observed_parent_and_preserves_idempotency(storage):
    from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
    from dpone.contracts.mssql_tds_worker import Contained, ContainmentRequired, TdsAttemptError, TdsAttemptPhase
    from tests.test_mssql_tds_directory import local, remote

    writer = create(storage)
    store, lease, _ = storage
    old = TdsAttemptJournal(store).read(PARENT)
    store.release(lease)
    new = store.acquire(PARENT.target_key, "new", 30)
    token = str(UUID(int=3))
    parent = TdsAttemptJournal(store).take_over(old, new, supervisor_token=token)
    parent.advance(
        ContainmentRequired(TdsAttemptError.OPERATION_TIMEOUT), expected_phase=TdsAttemptPhase.CREATION_INTENT
    )
    authority = parent.advance(Contained("a" * 64), expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED).state
    writer = storage[2].take_over(writer.snapshot, new, supervisor_token=token)
    writer.seal_work()
    authorized = writer.authorize_retirement(authority)
    assert writer.authorize_retirement(authority) == authorized
    reserved = writer.reserve_operation(
        operation_id=UUID(int=1), command=TdsCoordinatorCommand.RETIRE, command_sha256="a" * 64
    )
    writer.record_local_containment(0, local(reserved.state))
    writer.record_remote_settlement(0, remote(reserved.state))
    closed = writer.close_admission()
    assert writer.close_admission() == closed


def test_old_save_cannot_land_after_takeover(storage, monkeypatch):
    from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown

    old = create(storage)
    store, lease, journal = storage
    original = store.save
    successor = []

    def interleave(*args):
        monkeypatch.setattr(store, "save", original)
        store.release(lease)
        new = store.acquire(PARENT.target_key, "new", 30)
        successor.append(journal.take_over(old.snapshot, new, supervisor_token=str(UUID(int=3))))
        return original(*args)

    monkeypatch.setattr(store, "save", interleave)
    with pytest.raises(WindowContractError):
        operate(old)
    assert journal.read(PARENT, LIMITS) == successor[0].snapshot
    assert not successor[0].snapshot.state.slots
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        old.assert_authority()


@pytest.mark.parametrize("bad", [None, "wrong", 0, True, "oversized", "unicode"])
def test_malformed_read_rejects_and_poisons(storage, monkeypatch, bad):
    from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown, WindowRecord

    writer = create(storage)
    store, _, _ = storage
    if bad == "oversized":
        value = WindowRecord(1, "x" * (LIMITS.max_encoded_bytes + 2049))
    elif bad == "unicode":
        value = WindowRecord(1, "\ud800")
    elif type(bad) in (bool, int):
        value = WindowRecord(bad, encode_directory_record(writer.snapshot.state, writer.snapshot.ownership).decode())
    else:
        value = bad
    monkeypatch.setattr(store, "load", lambda key: value)
    with pytest.raises(WindowContractError):
        writer.assert_authority()
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        writer.assert_authority()


def test_create_rejects_parent_that_advanced_beyond_creation(storage):
    from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
    from dpone.contracts.bounded_window import WindowContractError
    from dpone.contracts.mssql_tds_worker import ContainmentRequired, TdsAttemptError, TdsAttemptPhase

    store, lease, journal = storage
    parent = replace(PARENT, run_id="advanced")
    writer = TdsAttemptJournal(store).create(parent, lease, supervisor_token=OWNER.supervisor_id)
    writer.advance(
        ContainmentRequired(TdsAttemptError.OPERATION_TIMEOUT), expected_phase=TdsAttemptPhase.CREATION_INTENT
    )
    with pytest.raises(WindowContractError, match="parent_creation"):
        journal.create(parent, LIMITS, lease, supervisor_token=OWNER.supervisor_id)
    assert journal.read(parent, LIMITS) is None


@pytest.mark.parametrize("mode", ["missing", "wrong_identity", "failure"])
def test_injected_parent_observer_controls_creation_without_directory_write(storage, mode):
    from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
    from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
    from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown

    store, lease, _ = storage
    calls = []
    observed = TdsAttemptJournal(store).read(PARENT)

    class Observer:
        def read(self, identity):
            calls.append(identity)
            if mode == "failure":
                raise WindowOutcomeUnknown("observer_unavailable")
            if mode == "missing":
                return None
            return replace(observed, state=replace(observed.state, identity=replace(PARENT, run_id="other")))

    journal = TdsCoordinatorDirectoryJournal(store, parent_observer=Observer())
    with pytest.raises((WindowContractError, WindowOutcomeUnknown)):
        journal.create(PARENT, LIMITS, lease, supervisor_token=OWNER.supervisor_id)
    assert calls == [PARENT]
    assert journal.read(PARENT, LIMITS) is None


def test_injected_parent_observer_failure_during_retirement_poisons_writer(storage):
    from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
    from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
    from dpone.contracts.bounded_window import WindowOutcomeUnknown
    from tests.test_mssql_tds_directory import authority

    store, lease, _ = storage
    calls = []

    class Observer:
        def read(self, identity):
            calls.append(identity)
            if len(calls) > 1:
                raise WindowOutcomeUnknown("observer_unavailable")
            return TdsAttemptJournal(store).read(identity)

    journal = TdsCoordinatorDirectoryJournal(store, parent_observer=Observer())
    writer = journal.create(PARENT, LIMITS, lease, supervisor_token=OWNER.supervisor_id)
    before = writer.seal_work()
    with pytest.raises(WindowOutcomeUnknown, match="observer_unavailable"):
        writer.authorize_retirement(authority())
    assert calls == [PARENT, PARENT]
    assert writer.snapshot == before
    assert journal.read(PARENT, LIMITS) == before
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        writer.assert_authority()
