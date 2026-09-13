"""Offline transactional fault tests, not live SQL durability certification."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Lock

import pytest

from dpone.adapters import composition_snapshot_sql_store as module
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot import SnapshotPublisherClosure
from tests.composition_mssql_catalog_helpers import install_offline_catalog_references
from tests.composition_mssql_store_fault_model import Connection, Cursor, FaultDatabase
from tests.composition_snapshot_helpers import digest, intent, observation, occurrence

SERVICE = "10000000-0000-4000-8000-000000000002"


class SnapshotCursor(Cursor):
    def _select_or_mutate(self, sql, p):
        if "composition_snapshot_" not in sql:
            return super()._select_or_mutate(sql, p)
        data = self.connection.data
        assert self.connection.transaction_id == self.connection.database.lock_owner
        if sql.startswith("INSERT"):
            name = "intents" if "snapshot_intents]" in sql else "history"
            key = p[0] if name == "intents" else p[:2]
            assert key not in data[name]
            if name == "intents":
                assert not any(row[1:3] == p[1:3] or row[3] == p[3] for row in data[name].values())
            data[name][key] = p
            return []
        if "snapshot_intents]" in sql:
            rows = list(data["intents"].values())
            if "write_subject_sha256=?" in sql:
                return [r for r in rows if r[0] == p[0] or r[1:3] == p[1:3] or r[3] == p[3]]
            if "operation_key=?" in sql:
                return sorted(r for r in rows if r[1] == p[0])
            return [r for r in rows if r[0] == p[0]]
        return sorted((r[1:] for r in data["history"].values() if r[0] == p[0]), key=lambda r: r[0])


class SnapshotConnection(Connection):
    def cursor(self):
        return SnapshotCursor(self)


class SnapshotDatabase(FaultDatabase):
    def connect(self):
        connection = SnapshotConnection(self)
        self.connections.append(connection)
        return connection


class Authority:
    def __init__(self):
        self.closed = False
        self.fail = False
        self.change_transaction = False

    def require_ready(self, context, value):
        if self.fail or self.closed:
            raise CompositionAdmissionError("test_gate_closed")
        if self.change_transaction:
            context.cursor.connection.commit()
            context.begin(SERVICE)

    def require_closure(self, context, value, closure):
        if not self.closed or closure != closure_for(value):
            raise CompositionAdmissionError("test_closure_original")


def closure_for(value):
    return SnapshotPublisherClosure(value.intent_sha256, digest("closed"), digest("quiet"))


@pytest.fixture
def active(monkeypatch):
    install_offline_catalog_references(monkeypatch)
    monkeypatch.setattr(module, "require_snapshot_publication_schema", lambda cursor, schema: None)
    value = intent()
    db = SnapshotDatabase(occurrence().request, SERVICE)
    db.data.update(intents={}, history={})
    activation = MssqlCompositionActivationStore(db.connect, expected_service_id=SERVICE)
    activation.prepare(db.request)
    activation.activate(db.request)
    MssqlCompositionAttemptStore(db.connect, expected_service_id=SERVICE).admit_once(value.attempt)
    authority = Authority()
    store = module.MssqlSnapshotPublicationStore(
        db.connect, expected_service_id=SERVICE, authority=authority, attempt=value.attempt
    )
    return db, store, value, authority


def test_exact_immutable_history_and_conflicting_intent(active):
    db, store, value, authority = active
    prepared = store.prepare(value)
    assert store.prepare(value) == prepared
    with pytest.raises(CompositionAdmissionError):
        store.prepare(replace(value, closed_ingest_sha256=digest("different")))
    claimed = store.claim_exchange(prepared)
    assert claimed is not None and store.claim_exchange(prepared) is None
    authority.closed = True
    resolved = store.resolve(
        claimed, state="PUBLISHED", closure=closure_for(value), observation=observation(value, published=True)
    )
    assert store.read(value.intent_sha256) == resolved
    assert store.records() == (resolved,)
    assert [row[-1] for row in db.data["history"].values()] == [r.to_bytes() for r in (prepared, claimed, resolved)]


def test_lost_ack_is_never_a_claim_success(active):
    db, store, value, _ = active
    prepared = store.prepare(value)
    db.fail_commit = True
    with pytest.raises(CompositionAdmissionError, match="unknown"):
        store.claim_exchange(prepared)
    db.fail_commit = False
    assert store.read(value.intent_sha256).state == "EXCHANGE_INTENT"
    assert store.claim_exchange(prepared) is None


def test_failed_authority_rolls_back_and_changed_transaction_rejected(active):
    db, store, value, authority = active
    prepared = store.prepare(value)
    authority.fail = True
    with pytest.raises(CompositionAdmissionError):
        store.claim_exchange(prepared)
    assert store.read(value.intent_sha256) == prepared
    authority.fail = False
    authority.change_transaction = True
    with pytest.raises(CompositionAdmissionError):
        store.claim_exchange(prepared)
    assert len(db.data["history"]) == 1


def test_concurrent_claims_have_one_winner(active, monkeypatch):
    _, store, value, _ = active
    prepared = store.prepare(value)
    # FaultDatabase's SQL lock rejects contenders instead of waiting. Model
    # SQL Server's lock wait only; all production CAS/authority code still runs.
    from contextlib import contextmanager

    original = module.composition_control_transaction
    lock = Lock()

    @contextmanager
    def serialized(*args):
        with lock, original(*args) as ledger:
            yield ledger

    monkeypatch.setattr(module, "composition_control_transaction", serialized)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(store.claim_exchange, (prepared, prepared)))
    assert sum(result is not None for result in results) == 1


def test_stale_epoch_and_unbacked_closure_reject(active):
    _, store, value, authority = active
    stale = replace(value, attempt=replace(value.attempt, guard_epochs=((value.target.guard_id, 2),)))
    with pytest.raises(CompositionAdmissionError):
        store.prepare(stale)
    prepared = store.prepare(value)
    with pytest.raises(CompositionAdmissionError, match="closure"):
        store.resolve(prepared, state="NOT_PUBLISHED", closure=closure_for(value), observation=observation(value))
    authority.closed = True
    resolved = store.resolve(
        prepared, state="NOT_PUBLISHED", closure=closure_for(value), observation=observation(value)
    )
    assert resolved.state == "NOT_PUBLISHED"
    assert store.claim_exchange(prepared) is None


def test_insert_failure_rolls_back_intent_and_history_together(active):
    db, store, value, _ = active
    db.fail_sql = "INSERT INTO [dpone_control].[composition_snapshot_history]"
    with pytest.raises(CompositionAdmissionError, match="unknown"):
        store.prepare(value)
    assert db.data["intents"] == {} and db.data["history"] == {}
    assert db.connections[-1].rollbacks == 1 and db.connections[-1].closed
    db.fail_sql = None
    assert store.prepare(value).state == "PREPARED"


@pytest.mark.parametrize("applied", [False, True])
def test_prepare_ack_loss_never_manufactures_ack(active, applied):
    db, store, value, _ = active
    db.fail_commit, db.commit_applies = True, applied
    with pytest.raises(CompositionAdmissionError, match="unknown"):
        store.prepare(value)
    assert bool(db.data["intents"]) is applied
    assert bool(db.data["history"]) is applied
    db.fail_commit, db.commit_applies = False, True
    assert store.prepare(value).state == "PREPARED"


def test_durable_epoch_change_blocks_claim(active):
    db, store, value, _ = active
    prepared = store.prepare(value)
    domain = db.data["domains"][value.target.guard_id]
    db.data["domains"][value.target.guard_id] = (*domain[:3], 2, domain[4])
    with pytest.raises(CompositionAdmissionError):
        store.claim_exchange(prepared)
    assert len(db.data["history"]) == 1


@pytest.mark.parametrize("corruption", ["missing", "bytes", "state", "revision", "intent_index"])
def test_complete_history_is_reopened_and_corruption_rejects(active, corruption):
    db, store, value, _ = active
    prepared = store.prepare(value)
    store.claim_exchange(prepared)
    key = (value.intent_sha256, 1)
    row = db.data["history"][key]
    if corruption == "missing":
        del db.data["history"][key]
    elif corruption == "bytes":
        db.data["history"][key] = (*row[:-1], row[-1] + b" ")
    elif corruption == "state":
        db.data["history"][key] = (*row[:2], "PUBLISHED", *row[3:])
    elif corruption == "revision":
        db.data["history"][key] = (row[0], 4, *row[2:])
    else:
        original = db.data["intents"][value.intent_sha256]
        db.data["intents"][value.intent_sha256] = (*original[:3], "another-query", original[4])
    with pytest.raises(CompositionAdmissionError):
        store.read(value.intent_sha256)


def test_unknown_is_retained_then_explicitly_resolved(active):
    _, store, value, authority = active
    claimed = store.claim_exchange(store.prepare(value))
    unknown = store.resolve(claimed, state="COMMIT_UNKNOWN", closure=None, observation=None)
    assert store.read(value.intent_sha256) == unknown
    assert store.claim_exchange(unknown) is None
    authority.closed = True
    resolved = store.resolve(unknown, state="NOT_PUBLISHED", closure=closure_for(value), observation=observation(value))
    assert resolved.revision == 4
    with pytest.raises(CompositionAdmissionError):
        store.resolve(unknown, state="NOT_PUBLISHED", closure=closure_for(value), observation=observation(value))


def test_schema_retains_uniqueness_lock_and_immutable_trigger_definitions():
    from dpone.adapters.composition_snapshot_sql_schema import SNAPSHOT_TABLES, render_snapshot_publication_schema

    sql = render_snapshot_publication_schema()
    assert len(SNAPSHOT_TABLES) == 2
    for required in (
        "uq_composition_snapshot_subject",
        "uq_composition_snapshot_query",
        "UPDATE, DELETE",
        "APPLOCK_MODE",
        "HASHBYTES",
        "h.revision=i.revision-1",
    ):
        assert required in sql


def test_retiring_unknown_attempt_allows_only_recovery(active):
    db, store, value, authority = active
    prepared = store.prepare(value)
    claimed = store.claim_exchange(prepared)
    MssqlCompositionActivationStore(db.connect, expected_service_id=SERVICE).begin_retirement(db.request)
    operation = db.data["operations"][value.attempt.attempt_sha256]
    db.data["operations"][value.attempt.attempt_sha256] = (*operation[:6], "COMMIT_UNKNOWN", *operation[7:])
    with pytest.raises(CompositionAdmissionError):
        store.prepare(value)
    authority.closed = True
    resolved = store.resolve(claimed, state="NOT_PUBLISHED", closure=closure_for(value), observation=observation(value))
    assert resolved.state == "NOT_PUBLISHED"
    assert db.data["operations"][value.attempt.attempt_sha256][6] == "COMMIT_UNKNOWN"


def test_resolve_commit_ack_loss_does_not_return_terminal_success(active):
    db, store, value, authority = active
    claimed = store.claim_exchange(store.prepare(value))
    authority.closed = True
    db.fail_commit = True
    with pytest.raises(CompositionAdmissionError, match="unknown"):
        store.resolve(
            claimed, state="PUBLISHED", closure=closure_for(value), observation=observation(value, published=True)
        )
    db.fail_commit = False
    assert store.read(value.intent_sha256).state == "PUBLISHED"
    with pytest.raises(CompositionAdmissionError, match="conflict"):
        store.resolve(
            claimed, state="PUBLISHED", closure=closure_for(value), observation=observation(value, published=True)
        )
