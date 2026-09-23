"""A durable commit with an invalid acknowledgement never grants authority."""

from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown, WindowRecord
from dpone.contracts.mssql_tds_worker import Contained, ContainmentRequired, TdsAttemptError, TdsAttemptPhase
from tests.test_mssql_tds_lifecycle import environment, identity


@pytest.mark.parametrize("operation", ["create", "advance", "take_over"])
@pytest.mark.parametrize(
    "fault",
    [
        "payload",
        "payload_type",
        "record_type",
        "true",
        "false",
        "zero",
        "negative",
        "text",
        "float",
        "null",
        "same",
        "decreased",
        "large",
    ],
)
def test_invalid_save_acknowledgement_preserves_unknown_outcome(tmp_path, operation, fault):
    store, lease, journal = environment(tmp_path)
    original = None
    if operation != "create":
        prior = journal.create(identity(), lease, supervisor_token=str(uuid4()))
        original = prior.advance(
            ContainmentRequired(TdsAttemptError.CONNECTION), expected_phase=TdsAttemptPhase.CREATION_INTENT
        )
    if operation == "take_over":
        store.release(lease)
        lease = store.acquire("target", "recovery", 60)

    class InvalidReply:
        enabled = False
        accesses = 0

        def __getattr__(self, name):
            self.accesses += 1
            return getattr(store, name)

        def save(self, key, revision, payload, current_lease):
            self.accesses += 1
            record = store.save(key, revision, payload, current_lease)
            if not self.enabled:
                return record
            if fault == "payload":
                return replace(record, payload=payload + " ")
            if fault == "payload_type":
                return replace(record, payload=payload.encode())
            if fault == "record_type":
                return SimpleNamespace(revision=record.revision, payload=payload)
            value = {
                "true": True,
                "false": False,
                "zero": 0,
                "negative": -1,
                "text": "2",
                "float": 2.0,
                "null": None,
                "same": revision or 0,
                "decreased": max(0, (revision or 0) - 1),
                "large": 2**63,
            }[fault]
            return replace(record, revision=value)

    backend = InvalidReply()
    tested = TdsAttemptJournal(backend)
    writer = None
    if operation == "advance":
        # Acquire the tested writer before enabling the malformed response.
        # A new identity avoids bypassing create-once semantics.
        tested_identity = identity(attempt=1)
        writer = tested.create(tested_identity, lease, supervisor_token=str(uuid4()))
        original = writer.advance(
            ContainmentRequired(TdsAttemptError.CONNECTION), expected_phase=TdsAttemptPhase.CREATION_INTENT
        )
    else:
        tested_identity = identity()
    backend.enabled = True
    with pytest.raises(WindowOutcomeUnknown, match="ack_unknown"):
        if operation == "create":
            tested.create(tested_identity, lease, supervisor_token=str(uuid4()))
        elif operation == "take_over":
            tested.take_over(original, lease, supervisor_token=str(uuid4()))
        else:
            writer.advance(Contained("a" * 64), expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED)
    persisted = journal.read(tested_identity)
    assert persisted is not None
    assert persisted.state.ownership.fence == lease.fence
    if writer is not None:
        assert writer.snapshot == original
        assert persisted.state.phase is TdsAttemptPhase.CONTAINED
        backend.enabled = False
        calls = backend.accesses
        with pytest.raises(WindowOutcomeUnknown, match="writer_poisoned"):
            writer.assert_authority()
        with pytest.raises(WindowOutcomeUnknown, match="writer_poisoned"):
            writer.advance(Contained("a" * 64), expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED)
        assert backend.accesses == calls
    with pytest.raises(WindowContractError):
        journal.create(tested_identity, lease, supervisor_token=str(uuid4()))
    with pytest.raises(WindowContractError):
        journal.take_over(persisted, lease, supervisor_token=str(uuid4()))
    store.release(lease)
    newer = store.acquire("target", "newer-recovery", 60)
    recovered = journal.take_over(persisted, newer, supervisor_token=str(uuid4()))
    recovered.assert_authority()
    assert recovered.snapshot.state.ownership.fence > persisted.state.ownership.fence


@pytest.mark.parametrize("expected", [None, 1, 50])
def test_valid_acknowledgement_allows_noncontiguous_revisions(tmp_path, expected):
    from dpone.adapters.mssql_tds_lifecycle import _persist

    _, lease, _ = environment(tmp_path)
    record = WindowRecord((expected or 0) + 7, "exact payload")

    class Store:
        def save(self, key, revision, payload, current_lease):
            assert (key, revision, payload, current_lease) == ("key", expected, record.payload, lease)
            return record

    assert _persist(Store(), "key", expected, record.payload, lease) is record
