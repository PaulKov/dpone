"""SqlClient authority ordering in the existing durable attempt journal."""

from dataclasses import asdict, replace
from uuid import uuid4

import pytest

from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown
from dpone.contracts.mssql_sqlclient_attempt import (
    SqlClientAttemptEvidence,
    SqlClientCredentialIntent,
    SqlClientGrantIntent,
    SqlClientWriterObserved,
)
from dpone.contracts.mssql_tds_worker import Exited, Running, advance_state, initial_state, replace_ownership
from dpone.contracts.mssql_tds_worker_codec import decode_state, encode_state
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_tds_lifecycle import environment, identity
from tests.test_mssql_tds_worker_contracts import H, initial, normal_states


def spawned():
    value = list(normal_states())[3]
    return replace(value, schema_version=2, backend="mssql_sqlclient")


def advance(state, event):
    return advance_state(state, event, expected_phase=state.phase)


def credentials(empty=False):
    return SqlClientCredentialIntent(SqlClientAttemptEvidence(H, "b" * 64, empty))


def test_legacy_wire_is_unchanged_and_cannot_accept_new_events():
    state = initial()
    old = asdict(state)
    del old["backend"], old["sqlclient"]
    assert encode_state(state) == canonical_json_bytes(old)
    assert decode_state(encode_state(state)) == state
    with pytest.raises(ValueError):
        advance(list(normal_states())[3], credentials())


def test_explicit_backend_creates_version_two_only():
    old = initial()
    state = initial_state(old.identity, old.ownership, backend="mssql_sqlclient")
    assert state.schema_version == 2 and state.backend == "mssql_sqlclient"
    assert decode_state(encode_state(state)) == state
    with pytest.raises(ValueError):
        initial_state(old.identity, old.ownership, backend="unknown")


def test_nonempty_grant_order_and_one_shot_evidence():
    state = spawned()
    with pytest.raises(ValueError):
        advance(state, Running())
    state = advance(state, credentials())
    with pytest.raises(ValueError):
        advance(state, credentials())
    with pytest.raises(ValueError):
        advance(state, SqlClientGrantIntent(H))
    with pytest.raises(ValueError):
        advance(state, Exited(0, H))
    state = advance(state, SqlClientWriterObserved("c" * 64))
    with pytest.raises(ValueError):
        advance(state, SqlClientWriterObserved("d" * 64))
    state = advance(state, SqlClientGrantIntent("e" * 64))
    with pytest.raises(ValueError):
        advance(state, SqlClientGrantIntent("f" * 64))
    state = advance(state, Exited(0, H))
    assert decode_state(encode_state(state)) == state
    assert state.sqlclient.grant_intent_sha256 == "e" * 64


def test_empty_requires_no_session_or_grant_and_failed_pregrant_exit_is_readable():
    empty = advance(spawned(), credentials(True))
    with pytest.raises(ValueError):
        advance(empty, SqlClientWriterObserved(H))
    assert advance(empty, Exited(0, H)).sqlclient.input_empty
    failed = advance(advance(spawned(), credentials()), Exited(1, None))
    assert decode_state(encode_state(failed)) == failed


def test_takeover_preserves_original_evidence_references():
    state = advance(advance(advance(spawned(), credentials()), SqlClientWriterObserved(H)), SqlClientGrantIntent(H))
    owner = replace(state.ownership, fence=2, supervisor_id="22222222-2222-4222-8222-222222222222")
    assert replace_ownership(state, owner).sqlclient == state.sqlclient


@pytest.mark.parametrize("observed,granted", [(True, False), (True, True)])
def test_codec_rejects_evidence_without_its_separate_cas_sequence(observed, granted):
    state = advance(spawned(), credentials())
    if observed:
        state = advance(state, SqlClientWriterObserved(H))
    if granted:
        state = advance(state, SqlClientGrantIntent(H))
    payload = asdict(state)
    payload["sequence"] = state.sequence - 1
    with pytest.raises(ValueError, match="evidence_sequence"):
        decode_state(canonical_json_bytes(payload))


def test_containment_sequence_accounts_for_credential_intent():
    from dpone.contracts.mssql_tds_worker import ContainmentRequired, TdsAttemptError

    state = advance(advance(spawned(), credentials()), ContainmentRequired(TdsAttemptError.DRIVER))
    payload = asdict(state)
    payload["sequence"] = state.sequence - 1
    with pytest.raises(ValueError, match="evidence_sequence"):
        decode_state(canonical_json_bytes(payload))


@pytest.mark.parametrize(
    "change", [{"input_empty": 1}, {"grant_intent_sha256": H}, {"writer_observation_sha256": "bad"}]
)
def test_evidence_rejects_invalid_shape_and_order(change):
    with pytest.raises(ValueError):
        SqlClientAttemptEvidence(H, H, False, **change) if "input_empty" not in change else SqlClientAttemptEvidence(
            H, H, change["input_empty"]
        )


@pytest.mark.parametrize("lost_step", [0, 1, 2])
def test_durable_sqlite_ack_loss_never_allows_resend_and_takeover_retains_refs(tmp_path, lost_step):
    store, lease, observer = environment(tmp_path)

    class LostAck:
        enabled = False

        def __getattr__(self, name):
            return getattr(store, name)

        def save(self, key, revision, payload, current_lease):
            result = store.save(key, revision, payload, current_lease)
            if self.enabled:
                raise OSError("synthetic lost commit acknowledgement")
            return result

    backend = LostAck()
    journal = TdsAttemptJournal(backend, backend="mssql_sqlclient")
    writer = journal.create(identity(), lease, supervisor_token=str(uuid4()))
    from dpone.contracts.mssql_tds_worker import LaunchIntent, Prepared, ProcessRegistered

    prior = list(normal_states())[3]
    for event in (Prepared(prior.object_identity, H), LaunchIntent(H), ProcessRegistered(prior.process)):
        writer.advance(event, expected_phase=writer.snapshot.state.phase)
    events = [credentials(), SqlClientWriterObserved("c" * 64), SqlClientGrantIntent("d" * 64)]
    for event in events[:lost_step]:
        writer.advance(event, expected_phase=writer.snapshot.state.phase)
    acknowledged = writer.snapshot
    backend.enabled = True
    with pytest.raises(WindowOutcomeUnknown):
        writer.advance(events[lost_step], expected_phase=acknowledged.state.phase)
    persisted = observer.read(identity())
    assert persisted.state.sequence == acknowledged.state.sequence + 1
    assert writer.snapshot == acknowledged
    backend.enabled = False
    with pytest.raises(WindowOutcomeUnknown, match="poisoned"):
        writer.advance(events[lost_step], expected_phase=acknowledged.state.phase)
    store.release(lease)
    recovery_lease = store.acquire("target", "recovery", 60)
    recovery = journal.take_over(persisted, recovery_lease, supervisor_token=str(uuid4()))
    assert recovery.snapshot.state.sqlclient == persisted.state.sqlclient
    with pytest.raises(WindowContractError, match="recovery_requires_settlement"):
        recovery.advance(SqlClientGrantIntent(H), expected_phase=recovery.snapshot.state.phase)
