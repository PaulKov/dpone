"""Transactional SQL fault model; never live gate or ClickHouse proof."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.adapters import composition_clickhouse_dispatch_queries as queries
from dpone.adapters.composition_clickhouse_dispatch_store import MssqlClickHouseDispatchStore
from dpone.adapters.composition_clickhouse_transport import ClickHouseDispatchObservation
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.composition_clickhouse_dispatch import (
    ClickHouseDispatchColumn,
    CreateGenerationDispatch,
    InsertGenerationDispatch,
)
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptProof, composition_attempt_epoch_subject
from tests.composition_mssql_catalog_helpers import install_offline_catalog_references
from tests.composition_mssql_store_fault_model import Connection, Cursor, FaultDatabase
from tests.composition_snapshot_helpers import digest, intent, occurrence

SQL_SERVICE = "10000000-0000-4000-8000-000000000002"


class DispatchCursor(Cursor):
    def _select_or_mutate(self, sql, p):
        data = self.connection.data
        if "composition_ch_" not in sql and "/* dispatch_issued */" not in sql:
            return super()._select_or_mutate(sql, p)
        assert self.connection.transaction_id == self.connection.database.lock_owner
        if "/* dispatch_issued */" in sql:
            return [
                (key,)
                for key, rows in data["issued_authorities"].items()
                for connector, service, principal in rows
                if (connector, service, principal) == ("clickhouse", p[0], p[1])
            ][:2]
        if sql.startswith("INSERT"):
            if "composition_ch_dispatches]" in sql:
                assert p[0] not in data["dispatches"] and not any(
                    row[1] == p[1] or row[4] == p[4] for row in data["dispatches"].values()
                )
                assert not any(key[0] == p[3] for key in data["closures"])
                data["dispatches"][p[0]] = p
            elif "composition_ch_dispatch_terminals]" in sql:
                assert p[0] in data["dispatches"] and p[0] not in data["terminals"]
                data["terminals"][p[0]] = p[1:]
            else:
                assert (p[0], p[1]) not in data["closures"]
                data["closures"][p[0], p[1]] = p[2:]
            return []
        if "composition_ch_dispatch_closures]" in sql:
            if len(p) == 1:
                return [(phase,) for gate, phase in data["closures"] if gate == p[0]][:3]
            found = data["closures"].get(tuple(p))
            return [] if found is None else [found]
        if "composition_ch_dispatch_terminals]" in sql:
            found = data["terminals"].get(p[0])
            return [] if found is None else [found]
        rows = sorted(data["dispatches"].values())
        if "WHERE claim_key" in sql:
            return [row for row in rows if row[1] == p[0] or row[0] == p[1] or row[4] == p[2]][:4]
        if "WHERE gate_id" in sql:
            return [row for row in rows if row[3] == p[0] and (len(p) == 1 or row[0] > p[1])][:1]
        return [row for row in rows if row[0] == p[0]][:2]


class DispatchConnection(Connection):
    def cursor(self):
        return DispatchCursor(self)


class DispatchDatabase(FaultDatabase):
    def connect(self):
        connection = DispatchConnection(self)
        self.connections.append(connection)
        return connection


class Authority:
    def __init__(self, value):
        self.value, self.events, self.change_transaction = value, [], None

    def _check(self, context, phase):
        self.events.append(phase)
        if self.change_transaction == phase:
            context.cursor.connection.commit()
            context.begin(SQL_SERVICE)

    def require_dispatch(self, context, dispatch):
        self._check(context, "admit")

    def observe_closed(self, context, *, attempt, target, gate_id):
        self._check(context, "closed")
        return tuple(
            CompositionAttemptProof(
                kind,
                attempt.attempt_sha256,
                attempt.activation_request_sha256,
                composition_attempt_epoch_subject(attempt),
                (self.value.ingest_principal,),
                digest(kind),
            )
            for kind in ("CLOSED_GATES", "QUIESCENCE")
        )


@pytest.fixture
def active(monkeypatch):
    install_offline_catalog_references(monkeypatch)
    monkeypatch.setattr(queries, "require_clickhouse_dispatch_schema", lambda cursor, schema: None)
    value = intent()
    db = DispatchDatabase(occurrence().request, SQL_SERVICE)
    db.data.update(dispatches={}, terminals={}, closures={})
    activation = MssqlCompositionActivationStore(db.connect, expected_service_id=SQL_SERVICE)
    activation.prepare(db.request)
    activation.activate(db.request)
    MssqlCompositionAttemptStore(db.connect, expected_service_id=SQL_SERVICE).admit_once(value.attempt)
    db.data["issued_authorities"][value.attempt.attempt_sha256] = (
        ("clickhouse", value.target.service_id, value.ingest_principal.principal_id),
    )
    authority = Authority(value)
    store = MssqlClickHouseDispatchStore(
        db.connect,
        expected_service_id=SQL_SERVICE,
        attempt=value.attempt,
        target=value.target,
        gate_id=value.ingest_principal.principal_id.removeprefix("clickhouse-user:"),
        authority=authority,
    )
    dispatch = CreateGenerationDispatch(
        value.attempt, value.target, value.generation.new_generation_uuid, (ClickHouseDispatchColumn("id", "Int32"),)
    )
    return db, store, dispatch, authority


def observation(dispatch):
    return ClickHouseDispatchObservation(
        dispatch.dispatch_sha256,
        dispatch.claim_key,
        dispatch.query_id,
        dispatch.payload_bytes if isinstance(dispatch, InsertGenerationDispatch) else 0,
        0,
        "sha256:" + sha256(b"").hexdigest(),
        "content-length",
    )


def test_claim_completion_and_protected_closure_have_exact_durable_originals(active):
    db, store, dispatch, authority = active
    store.claim_once(dispatch)
    assert db.data["dispatches"][dispatch.dispatch_sha256][-1] == dispatch.to_bytes()
    store.record_completed(dispatch, observation(dispatch))
    store.begin_closure()
    closed = store.require_drained()
    assert closed.startswith("sha256:")
    assert len(db.data["closures"]) == 2
    assert authority.events == ["admit", "closed"]
    assert store.require_drained() == closed


def test_replay_and_changed_bytes_cannot_get_second_permit(active):
    db, store, dispatch, authority = active
    store.claim_once(dispatch)
    for value in (dispatch, replace(dispatch, columns=(ClickHouseDispatchColumn("id", "Int64"),))):
        with pytest.raises(CompositionAdmissionError):
            store.claim_once(value)
    assert len(db.data["dispatches"]) == 1 and authority.events == ["admit"]


def test_lost_commit_ack_does_not_grant_send_or_replay(active):
    db, store, dispatch, _ = active
    db.fail_commit = True
    with pytest.raises(CompositionAdmissionError, match="unknown"):
        store.claim_once(dispatch)
    assert dispatch.dispatch_sha256 in db.data["dispatches"]
    db.fail_commit = False
    with pytest.raises(CompositionAdmissionError):
        store.claim_once(dispatch)


def test_closing_blocks_claims_and_pending_request_blocks_closed(active):
    db, store, dispatch, authority = active
    store.claim_once(dispatch)
    store.begin_closure()
    with pytest.raises(CompositionAdmissionError):
        store.claim_once(replace(dispatch, columns=(ClickHouseDispatchColumn("id", "Int64"),)))
    with pytest.raises(CompositionAdmissionError, match="unresolved"):
        store.require_drained()
    assert len(db.data["closures"]) == 1 and authority.events == ["admit"]
    store.record_completed(dispatch, observation(dispatch))
    assert store.require_drained().startswith("sha256:")


@pytest.mark.parametrize("phase", ["admit", "closed"])
def test_authority_callback_cannot_replace_actual_sql_transaction(active, phase):
    db, store, dispatch, authority = active
    if phase == "closed":
        store.claim_once(dispatch)
        store.record_completed(dispatch, observation(dispatch))
        store.begin_closure()
    authority.change_transaction = phase
    with pytest.raises(CompositionAdmissionError, match="transaction_identity"):
        store.claim_once(dispatch) if phase == "admit" else store.require_drained()
    assert not any(key[1] == "CLOSED" for key in db.data["closures"])


def test_changed_completion_or_missing_claim_is_rejected(active):
    db, store, dispatch, _ = active
    with pytest.raises(CompositionAdmissionError):
        store.record_completed(dispatch, observation(dispatch))
    store.claim_once(dispatch)
    with pytest.raises(CompositionAdmissionError):
        store.record_completed(dispatch, replace(observation(dispatch), response_body_bytes=1))
    assert not db.data["terminals"]
    store.record_completed(dispatch, observation(dispatch))
    store.record_completed(dispatch, observation(dispatch))
    assert len(db.data["terminals"]) == 1


def test_claim_commit_without_durable_original_cannot_ack(active):
    db, store, dispatch, _ = active
    db.commit_applies = False
    with pytest.raises(CompositionAdmissionError):
        store.claim_once(dispatch)
    assert not db.data["dispatches"]


def test_every_claim_original_and_terminal_is_required_not_just_counts(active):
    db, store, dispatch, _ = active
    store.claim_once(dispatch)
    store.record_completed(dispatch, observation(dispatch))
    second = InsertGenerationDispatch(
        dispatch.attempt,
        dispatch.target,
        dispatch.generation_uuid,
        dispatch.columns,
        digest("native"),
        len(b"native"),
        0,
    )
    store.claim_once(second)
    store.begin_closure()
    with pytest.raises(CompositionAdmissionError, match="unresolved"):
        store.require_drained()
    store.record_completed(second, observation(second))
    key = dispatch.dispatch_sha256
    terminal_hash, document = db.data["terminals"][key]
    db.data["terminals"][key] = (terminal_hash, document + b" ")
    with pytest.raises(CompositionAdmissionError, match="terminal_original"):
        store.require_drained()
    assert not any(key[1] == "CLOSED" for key in db.data["closures"])


def test_terminal_readback_identity_is_not_replaceable(active):
    db, store, dispatch, _ = active
    store.claim_once(dispatch)
    store.record_completed(dispatch, observation(dispatch))
    with pytest.raises(CompositionAdmissionError, match="terminal_replay"):
        store.record_completed(dispatch, replace(observation(dispatch), response_framing="chunked"))


@pytest.mark.parametrize("invalid", ["boolean", "other_principal", "other_attempt"])
def test_closed_requires_protected_exact_proof_originals(active, invalid):
    db, store, dispatch, authority = active
    store.begin_closure()
    original = authority.observe_closed

    def malformed(context, **kwargs):
        values = original(context, **kwargs)
        if invalid == "boolean":
            return True
        if invalid == "other_principal":
            return tuple(replace(value, authorities=(authority.value.publisher_principal,)) for value in values)
        return tuple(replace(value, attempt_sha256=digest("another attempt")) for value in values)

    authority.observe_closed = malformed
    with pytest.raises(CompositionAdmissionError):
        store.require_drained()
    assert len(db.data["closures"]) == 1


def test_gate_uuid_cannot_be_reused_for_another_operation(active):
    db, store, dispatch, authority = active
    db.data["issued_authorities"][digest("other operation")] = db.data["issued_authorities"][
        dispatch.attempt.attempt_sha256
    ]
    with pytest.raises(CompositionAdmissionError, match="issued_identity"):
        store.claim_once(dispatch)
    assert not db.data["dispatches"] and not authority.events


def test_unknown_business_outcome_remains_unknown_after_transport_closure(active):
    db, store, dispatch, _ = active
    store.claim_once(dispatch)
    store.record_completed(dispatch, observation(dispatch))
    record = db.data["operations"][dispatch.attempt.attempt_sha256]
    db.data["operations"][dispatch.attempt.attempt_sha256] = (*record[:6], "COMMIT_UNKNOWN", *record[7:])
    store.begin_closure()
    store.require_drained()
    assert db.data["operations"][dispatch.attempt.attempt_sha256][6] == "COMMIT_UNKNOWN"


def test_failed_budget_callback_cannot_leave_a_dispatch_claim(active):
    db, store, dispatch, authority = active

    def exhausted(context, value):
        raise CompositionAdmissionError("campaign_budget")

    authority.require_dispatch = exhausted
    with pytest.raises(CompositionAdmissionError, match="campaign_budget"):
        store.claim_once(dispatch)
    assert not db.data["dispatches"]


def test_changed_insert_payload_cannot_reset_the_ordinal_claim(active):
    db, store, dispatch, authority = active
    original = InsertGenerationDispatch(
        dispatch.attempt,
        dispatch.target,
        dispatch.generation_uuid,
        dispatch.columns,
        digest("native"),
        len(b"native"),
        0,
    )
    store.claim_once(original)
    changed = replace(original, payload_sha256=digest("changed"))
    assert changed.claim_key == original.claim_key and changed.dispatch_sha256 != original.dispatch_sha256
    with pytest.raises(CompositionAdmissionError, match="dispatch_replay"):
        store.claim_once(changed)
    assert len(db.data["dispatches"]) == 1 and authority.events == ["admit"]


def test_callback_cannot_switch_cursor_even_inside_same_transaction(active):
    db, store, dispatch, authority = active

    def switch(context, value):
        context.cursor = context.cursor.connection.cursor()

    authority.require_dispatch = switch
    with pytest.raises(CompositionAdmissionError, match="context_identity"):
        store.claim_once(dispatch)
    assert not db.data["dispatches"]


def test_callback_cannot_change_admitted_attempt_state(active):
    db, store, dispatch, authority = active

    def change(context, value):
        rows = context.cursor.connection.data["operations"]
        key = dispatch.attempt.attempt_sha256
        original = rows[key]
        rows[key] = (*original[:6], "COMMIT_UNKNOWN", *original[7:])

    authority.require_dispatch = change
    with pytest.raises(CompositionAdmissionError, match="attempt_state"):
        store.claim_once(dispatch)
    assert not db.data["dispatches"]
    assert db.data["operations"][dispatch.attempt.attempt_sha256][6] == "RUNNING"


def test_lost_terminal_ack_can_reopen_only_identical_completed_evidence(active):
    db, store, dispatch, _ = active
    store.claim_once(dispatch)
    db.fail_commit = True
    with pytest.raises(CompositionAdmissionError, match="unknown"):
        store.record_completed(dispatch, observation(dispatch))
    assert len(db.data["terminals"]) == 1
    db.fail_commit = False
    store.record_completed(dispatch, observation(dispatch))
    with pytest.raises(CompositionAdmissionError, match="dispatch_replay"):
        store.claim_once(dispatch)
