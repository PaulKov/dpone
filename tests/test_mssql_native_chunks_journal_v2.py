"""V2 custody, event links, and legacy isolation use a real CAS store."""

import hashlib
import json
from threading import Event, Thread, current_thread

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.adapters.mssql_native_chunks_journal_v2 import NativeChunkJournalV2
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan, NativeChunkReceipt
from dpone.contracts.mssql_native_verification import NativeVerificationBackend, NativeVerificationIdentityV2


def _identity(plan):
    return NativeVerificationIdentityV2(
        plan,
        "bcp",
        NativeVerificationBackend.TARGET_LOCAL,
        "a" * 64,
        "b" * 64,
        "c" * 64,
        "mssql-native-sha256-sum-v1",
        "d" * 64,
    )


def _journal(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    return store, lease, plan, NativeChunkJournalV2(store, lease, _identity(plan))


def _file(tmp_path):
    return EncodedNativeFile(tmp_path / "sealed.bcp", 0, 1, 2, "e" * 64, "f" * 64)


def _stage(journal):
    return {
        "stage_id": journal.opaque_stage_id("[schema].[table]"),
        "owner_binding_sha256": "1" * 64,
        "object_id": 1,
        "schema_sha256": "2" * 64,
    }


def _writer():
    return {
        "import_backend": "bcp",
        "writer_proof_capability": "bcp-supervised-stage-barrier-v1",
        "protocol_sha256": "a" * 64,
        "package_sha256": "b" * 64,
        "capability_sha256": "c" * 64,
        "grant_token_sha256": "3" * 64,
        "timeout_policy_sha256": "d" * 64,
    }


def _observation():
    return {
        "writer_outcome": "success",
        "input_rows_consumed": 1,
        "row_count": 1,
        "count_overflow": False,
        "limbs": ["0"] * 8,
        "quiescence": "proved",
        "diagnostic_code": "mssql_native.verified",
    }


def _verified_chain(journal, tmp_path):
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    journal.append_event(0, attempt_id, "GRANTED", writer_binding=_writer())
    journal.append_event(0, attempt_id, "WRITING")
    journal.append_event(0, attempt_id, "WRITER_TERMINAL", observation={**_observation(), "quiescence": "unverified"})
    journal.append_event(0, attempt_id, "QUIESCENT", observation=_observation())
    journal.append_event(0, attempt_id, "VERIFIED")
    return attempt_id


def test_v2_key_and_identity_are_isolated_from_v1(tmp_path):
    store, lease, plan, journal = _journal(tmp_path)
    legacy = NativeChunkJournal(store, lease, plan)
    assert journal.key.startswith("mssql-native-chunks-v2/")
    assert legacy.key.startswith("mssql-native-chunks-v1/")
    assert journal.key != legacy.key
    journal.begin()
    assert legacy.data is None
    assert journal.data["identity"] == _identity(plan).document()
    drifted = journal.data
    drifted["identity"]["timeout_policy_sha256"] = "4" * 64
    store.save(journal.key, journal.revision, json.dumps(drifted), lease)
    with pytest.raises(WindowContractError, match="identity"):
        NativeChunkJournalV2(store, lease, _identity(plan))


def test_event_chain_is_closed_hash_linked_and_reloadable(tmp_path):
    store, lease, plan, journal = _journal(tmp_path)
    attempt_id = _verified_chain(journal, tmp_path)
    events = journal.data["events"][attempt_id]
    assert [event["event"] for event in events] == [
        "INTENT",
        "STAGE_OWNED",
        "GRANTED",
        "WRITING",
        "WRITER_TERMINAL",
        "QUIESCENT",
        "VERIFIED",
    ]
    assert set(events[0]) == {
        "schema_version",
        "kind",
        "invocation_key",
        "ordinal",
        "attempt_id",
        "sequence",
        "previous_sha256",
        "event",
        "stage_binding",
        "artifact_binding",
        "writer_binding",
        "observation",
        "created_at",
    }
    assert events[0]["previous_sha256"] is None
    assert events[0]["artifact_binding"]["file_sha256"] == "e" * 64
    assert "[schema].[table]" not in json.dumps(events)
    assert events[1]["stage_binding"]["stage_id"] == journal.opaque_stage_id("[schema].[table]")
    for sequence, event in enumerate(events[1:], 1):
        assert event["sequence"] == sequence
        expected = json.dumps(events[sequence - 1], ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        assert event["previous_sha256"] == hashlib.sha256(expected).hexdigest()
        assert (
            store.load(f"{journal.key}/{0:020d}/{attempt_id}/{sequence:020d}").payload.encode()
            == json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        )
    assert NativeChunkJournalV2(store, lease, _identity(plan)).data == journal.data
    receipt = NativeChunkReceipt(0, attempt_id, "[schema].[table]", 1, 2, "e" * 64, "f" * 64)
    journal.verified(receipt)
    complete = journal.complete(source_eof=True, completion_metadata={"note": "café"})
    assert complete.rows == 1
    assert complete.metadata_digest == "4f04d229f04347a677771f9c19db24439d899760249282ba081fa345cf491b72"
    assert NativeChunkJournalV2(store, lease, _identity(plan)).completed().rows == 1


def test_verified_requires_a_real_terminal_event_and_matching_binding(tmp_path):
    _, _, _, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    receipt = NativeChunkReceipt(0, journal.attempt_id(0, 0), "[schema].[table]", 1, 2, "e" * 64, "f" * 64)
    with pytest.raises(WindowContractError, match="event"):
        journal.verified(receipt)
    with pytest.raises(WindowContractError, match="transition"):
        journal.append_event(0, receipt.attempt_id, "VERIFIED")


def test_invalid_event_shape_transition_and_tampered_link_fail_closed(tmp_path):
    store, lease, plan, journal = _journal(tmp_path)
    _verified_chain(journal, tmp_path)
    attempt_id = journal.attempt_id(0, 0)
    with pytest.raises(WindowContractError, match="transition"):
        journal.append_event(0, attempt_id, "WRITING")
    with pytest.raises(WindowContractError, match="event"):
        journal.append_event(0, attempt_id, "UNKNOWN", observation={**_observation(), "secret": "bad"})
    corrupted = journal.data
    corrupted["events"][attempt_id][1]["previous_sha256"] = "0" * 64
    store.save(journal.key, journal.revision, json.dumps(corrupted), lease)
    with pytest.raises(WindowContractError, match="event"):
        NativeChunkJournalV2(store, lease, _identity(plan))


def test_pre_eof_restart_cannot_begin_again_or_claim_complete(tmp_path):
    store, lease, plan, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    restarted = NativeChunkJournalV2(store, lease, _identity(plan))
    with pytest.raises(WindowContractError, match="reextract_required"):
        restarted.begin()
    with pytest.raises(WindowContractError):
        restarted.complete(source_eof=True)


def test_identical_event_retry_is_idempotent_but_changed_event_is_rejected(tmp_path):
    _, _, _, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    first = journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    revision = journal.revision
    assert journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal)) == first
    assert journal.revision == revision
    with pytest.raises(WindowContractError, match="transition"):
        journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding={**_stage(journal), "object_id": 2})


def test_lost_ack_cannot_reconcile_from_stage_contents_or_retry(tmp_path):
    store, lease, plan, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    first_id = journal.attempt_id(0, 0)
    journal.append_event(0, first_id, "STAGE_OWNED", stage_binding=_stage(journal))
    journal.append_event(0, first_id, "GRANTED", writer_binding=_writer())
    unknown = {**_observation(), "writer_outcome": "lost_ack", "quiescence": "unverified"}
    journal.append_event(0, first_id, "UNKNOWN", observation=unknown)
    with pytest.raises(WindowContractError, match="retry"):
        journal.attempt(0, 1, _file(tmp_path))
    with pytest.raises(WindowContractError, match="transition"):
        journal.append_event(0, first_id, "RETIRED")
    proved = {**unknown, "quiescence": "proved", "row_count": 0}
    with pytest.raises(WindowContractError):
        journal.append_event(0, first_id, "PARTIAL_PROVED", observation=proved)
    with pytest.raises(WindowContractError):
        journal.append_event(0, first_id, "QUIESCENT", observation=proved)
    journal.append_event(0, first_id, "INCIDENT_RETAINED")
    with pytest.raises(WindowContractError, match="retry"):
        journal.attempt(0, 1, _file(tmp_path))
    assert set(journal.attempts()) == {first_id}
    tampered = journal.data
    tampered["nonces"][first_id] = "0" * 64
    store.save(journal.key, journal.revision, json.dumps(tampered), lease)
    with pytest.raises(WindowContractError, match="event"):
        NativeChunkJournalV2(store, lease, _identity(plan))


def test_reopened_bcp_unknown_requires_positive_terminal_and_observation_only_barrier(tmp_path):
    from contextlib import nullcontext

    store, lease, plan, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    journal.append_event(0, attempt_id, "GRANTED", writer_binding=_writer())
    journal.append_event(0, attempt_id, "WRITING")
    journal.append_event(0, attempt_id, "WRITER_TERMINAL", observation={**_observation(), "quiescence": "unverified"})
    journal.append_event(0, attempt_id, "UNKNOWN", observation={**_observation(), "quiescence": "failed"})
    reopened = NativeChunkJournalV2(store, lease, _identity(plan))
    with pytest.raises(WindowContractError, match="dedicated_recovery_required"):
        reopened.append_event(0, attempt_id, "QUIESCENT", observation=_observation())
    observed = reopened.observe_bcp_recovery(0, attempt_id, barrier=nullcontext, observe=_observation)
    assert observed["event"] == "QUIESCENT"
    with pytest.raises(WindowContractError, match="reextract_required"):
        reopened.attempt(0, 1, _file(tmp_path))


def test_recovered_bcp_receipt_is_committed_inside_observation_barrier(tmp_path):
    from contextlib import contextmanager

    store, lease, plan, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    journal.append_event(0, attempt_id, "GRANTED", writer_binding=_writer())
    journal.append_event(0, attempt_id, "WRITING")
    journal.append_event(0, attempt_id, "WRITER_TERMINAL", observation={**_observation(), "quiescence": "unverified"})
    journal.append_event(0, attempt_id, "UNKNOWN", observation={**_observation(), "quiescence": "failed"})
    reopened = NativeChunkJournalV2(store, lease, _identity(plan))
    ordering = []

    @contextmanager
    def barrier():
        ordering.append("locked")
        yield
        ordering.append("released")

    def observe():
        ordering.append("digest")
        receipt = NativeChunkReceipt(0, attempt_id, "[schema].[table]", 1, 2, "e" * 64, "f" * 64)
        return _observation(), receipt

    receipt = reopened.recover_bcp_verified(0, attempt_id, barrier=barrier, observe=observe)
    assert receipt.attempt_id == attempt_id
    assert ordering == ["locked", "digest", "released"]
    assert reopened.data["chunks"]["0"]["phase"] == "verified"
    assert reopened.data["events"][attempt_id][-1]["event"] == "VERIFIED"
    assert reopened.recover_bcp_verified(0, attempt_id, barrier=barrier, observe=observe) == receipt
    assert ordering == ["locked", "digest", "released", "locked", "digest", "released"]


def test_reopened_lost_ack_may_only_retain_incident(tmp_path):
    from contextlib import nullcontext

    store, lease, plan, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    journal.append_event(0, attempt_id, "GRANTED", writer_binding=_writer())
    journal.append_event(
        0,
        attempt_id,
        "UNKNOWN",
        observation={**_observation(), "writer_outcome": "lost_ack", "quiescence": "unverified"},
    )
    reopened = NativeChunkJournalV2(store, lease, _identity(plan))
    with pytest.raises(WindowContractError, match="proof_missing"):
        reopened.observe_bcp_recovery(0, attempt_id, barrier=nullcontext, observe=_observation)
    reopened.retain_bcp_incident(0, attempt_id)
    assert reopened.data["events"][attempt_id][-1]["event"] == "INCIDENT_RETAINED"
    with pytest.raises(WindowContractError):
        reopened.attempt(0, 1, _file(tmp_path))


def test_verified_pre_eof_retirement_requires_durable_nonpublication_and_exact_drop(tmp_path):
    store, lease, plan, journal = _journal(tmp_path)
    attempt_id = _verified_chain(journal, tmp_path)
    journal.verified(NativeChunkReceipt(0, attempt_id, "[schema].[table]", 1, 2, "e" * 64, "f" * 64))
    reopened = NativeChunkJournalV2(store, lease, _identity(plan))
    with pytest.raises(WindowContractError, match="nonpublication"):
        reopened.retire_verified(0, attempt_id, drop_exact_owned=lambda: None)
    reopened.record_nonpublication("4" * 64, assert_nonpublication=lambda: None)
    events = []
    reopened.retire_verified(0, attempt_id, drop_exact_owned=lambda: events.append("dropped"))
    assert events == ["dropped"]
    assert reopened.data["events"][attempt_id][-1]["event"] == "RETIRED"
    with pytest.raises(WindowContractError):
        reopened.complete(source_eof=True)


@pytest.mark.parametrize(
    "bad_observation",
    [
        {"row_count": 2},
        {"count_overflow": True},
        {"limbs": None},
    ],
)
def test_verified_event_requires_proved_matching_aggregate(tmp_path, bad_observation):
    _, _, _, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    journal.append_event(0, attempt_id, "GRANTED", writer_binding=_writer())
    journal.append_event(0, attempt_id, "WRITING")
    journal.append_event(0, attempt_id, "WRITER_TERMINAL", observation={**_observation(), "quiescence": "unverified"})
    journal.append_event(0, attempt_id, "QUIESCENT", observation={**_observation(), **bad_observation})
    with pytest.raises(WindowContractError, match="event"):
        journal.append_event(0, attempt_id, "VERIFIED")


def test_quiescent_event_requires_proved_barrier(tmp_path):
    _, _, _, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    journal.append_event(0, attempt_id, "GRANTED", writer_binding=_writer())
    journal.append_event(0, attempt_id, "WRITING")
    journal.append_event(0, attempt_id, "WRITER_TERMINAL", observation={**_observation(), "quiescence": "unverified"})
    with pytest.raises(WindowContractError, match="event"):
        journal.append_event(0, attempt_id, "QUIESCENT", observation={**_observation(), "quiescence": "failed"})


def test_shared_instance_serializes_snapshot_through_root_cas(tmp_path):
    _, _, _, journal = _journal(tmp_path)
    journal.begin()
    first_snapshot, release_first, second_started, second_done = Event(), Event(), Event(), Event()
    failures = []
    original_staging = journal._staging

    def paused_staging():
        snapshot = original_staging()
        if current_thread().name == "observations":
            first_snapshot.set()
            assert release_first.wait(5)
        return snapshot

    journal._staging = paused_staging

    def observations():
        try:
            journal.record_observations(({"worker": "complete"},))
        except Exception as error:
            failures.append(error)

    def limits():
        second_started.set()
        try:
            journal.bind_limits({"capacity": 1})
        except Exception as error:
            failures.append(error)
        finally:
            second_done.set()

    first = Thread(target=observations, name="observations")
    second = Thread(target=limits, name="limits")
    first.start()
    assert first_snapshot.wait(5)
    second.start()
    assert second_started.wait(5)
    try:
        assert not second_done.wait(0.2), "second mutation entered before the first CAS finished"
    finally:
        release_first.set()
        first.join(5)
        second.join(5)
    assert not first.is_alive() and not second.is_alive()
    assert not failures
    assert journal.data["observations"] == [{"worker": "complete"}]
    assert journal.data["limits"] == {"capacity": 1}


def test_publication_callbacks_are_serialized_with_snapshot_and_save(tmp_path):
    store, _, _, journal = _journal(tmp_path)
    attempt_id = _verified_chain(journal, tmp_path)
    journal.verified(NativeChunkReceipt(0, attempt_id, "[schema].[table]", 1, 2, "e" * 64, "f" * 64))
    journal.complete(source_eof=True)
    first_save, release_first, second_started, second_done = Event(), Event(), Event(), Event()
    failures = []
    original_save = store.save

    def paused_save(key, expected, payload, lease):
        if key == journal.key and current_thread().name == "preparing":
            first_save.set()
            assert release_first.wait(5)
        return original_save(key, expected, payload, lease)

    store.save = paused_save

    def preparing():
        try:
            journal.publication.preparation_started({"stage": "bound"})
        except Exception as error:
            failures.append(error)

    def prepared():
        second_started.set()
        try:
            journal.publication.prepared({"stage": "bound"})
        except Exception as error:
            failures.append(error)
        finally:
            second_done.set()

    first = Thread(target=preparing, name="preparing")
    second = Thread(target=prepared, name="prepared")
    first.start()
    assert first_save.wait(5)
    second.start()
    assert second_started.wait(5)
    try:
        assert not second_done.wait(0.2), "publication callback ran before the first CAS finished"
    finally:
        release_first.set()
        first.join(5)
        second.join(5)
    assert not first.is_alive() and not second.is_alive()
    assert not failures
    assert journal.publication.state()["phase"] == "prepared"


def test_identical_orphan_event_is_adopted_but_changed_bytes_are_rejected(tmp_path):
    store, lease, _, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    original_save = store.save
    failed = False

    def fail_root_once(key, expected, payload, writer_lease):
        nonlocal failed
        if key == journal.key and not failed:
            failed = True
            raise WindowContractError("injected root CAS interruption")
        return original_save(key, expected, payload, writer_lease)

    store.save = fail_root_once
    with pytest.raises(WindowContractError, match="injected"):
        journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    assert journal.data["events"][attempt_id][-1]["event"] == "INTENT"
    orphan_key = f"{journal.key}/{0:020d}/{attempt_id}/{1:020d}"
    orphan = store.load(orphan_key)
    assert orphan is not None
    with pytest.raises(WindowContractError):
        journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding={**_stage(journal), "object_id": 2})
    adopted = journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    assert orphan.payload == json.dumps(adopted, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert NativeChunkJournalV2(store, lease, journal.identity).data == journal.data


def test_reopened_pre_eof_journal_adopts_only_durable_identical_orphan(tmp_path):
    store, lease, _, journal = _journal(tmp_path)
    journal.begin()
    journal.attempt(0, 0, _file(tmp_path))
    attempt_id = journal.attempt_id(0, 0)
    original_save = store.save
    interrupted = False

    def interrupt_root_once(key, expected, payload, writer_lease):
        nonlocal interrupted
        if key == journal.key and not interrupted:
            interrupted = True
            raise WindowContractError("injected root CAS interruption")
        return original_save(key, expected, payload, writer_lease)

    store.save = interrupt_root_once
    with pytest.raises(WindowContractError, match="injected"):
        journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    reopened = NativeChunkJournalV2(store, lease, journal.identity)
    orphan_key = f"{journal.key}/{0:020d}/{attempt_id}/{1:020d}"
    orphan = store.load(orphan_key)
    assert orphan is not None
    assert reopened.data["events"][attempt_id][-1]["event"] == "INTENT"

    with pytest.raises(WindowContractError):
        reopened.append_event(0, attempt_id, "STAGE_OWNED", stage_binding={**_stage(journal), "object_id": 2})
    with pytest.raises(WindowContractError, match="orphan_event_changed"):
        reopened.append_event(0, attempt_id, "GRANTED", writer_binding=_writer())
    adopted = reopened.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    assert orphan.payload == json.dumps(adopted, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert NativeChunkJournalV2(store, lease, journal.identity).data == reopened.data

    with pytest.raises(WindowContractError, match="reextract_required"):
        reopened.append_event(0, attempt_id, "GRANTED", writer_binding=_writer())
    with pytest.raises(WindowContractError, match="reextract_required"):
        reopened.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=_stage(journal))
    with pytest.raises(WindowContractError, match="reextract_required"):
        reopened.attempt(1, 0, _file(tmp_path))
    with pytest.raises(WindowContractError, match="reextract_required"):
        reopened.record_observations(())
    with pytest.raises(WindowContractError, match="reextract_required"):
        reopened.complete(source_eof=True)


def test_reopened_verified_pre_eof_journal_is_recovery_only(tmp_path):
    store, lease, plan, journal = _journal(tmp_path)
    attempt_id = _verified_chain(journal, tmp_path)
    journal.verified(NativeChunkReceipt(0, attempt_id, "[schema].[table]", 1, 2, "e" * 64, "f" * 64))
    reopened = NativeChunkJournalV2(store, lease, _identity(plan))
    with pytest.raises(WindowContractError, match="reextract_required"):
        reopened.complete(source_eof=True)
    assert reopened.completed() is None
    assert journal.complete(source_eof=True).rows == 1
