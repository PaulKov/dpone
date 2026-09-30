"""Real journal and execution lock around fault-injected synchronous transport."""

import hashlib
import multiprocessing
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.adapters.clickhouse_authority_execution_lock import LocalPublicationExclusion
from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.contracts.clickhouse_authority import AuthorityConflict, TransportState
from dpone.contracts.clickhouse_publication import PublicationState
from dpone.ports.clickhouse_publication_transport import NativePublicationCompletion
from dpone.runtime.sinks.clickhouse_guarded_publication import PublicationUnknown
from tests.test_clickhouse_authority_sqlite import subject
from tests.test_clickhouse_native_publication import original_publication


class Transport:
    def __init__(self, store, error=None, changed=None):
        self.store, self.error, self.changed = store, error, changed
        self.requests = []

    def execute(self, original):
        self.requests.append(original)
        assert self.store.transport_state(original.binding.operation_id) == TransportState.MAY_HAVE_SENT
        # A real second SQLite transaction proves network work is outside any
        # outstanding write transaction, not just outside a mocked context.
        with sqlite3.connect(self.store.execution_identity().path, timeout=0) as db:
            db.execute("BEGIN IMMEDIATE")
            db.rollback()
        if self.error:
            raise self.error
        completion = NativePublicationCompletion(
            original.binding.operation_id,
            original.query_id,
            "server",
            hashlib.sha256(original.statement.encode()).hexdigest(),
            (24, 8, 14),
            54470,
            "0.2.10",
        )
        return replace(completion, **self.changed) if self.changed else completion


def publisher(store, transport):
    from dpone.adapters.clickhouse_authority_publisher import AuthorityPublicationPublisher

    return AuthorityPublicationPublisher(store, LocalPublicationExclusion(store), transport)


def test_publication_unknown_preserves_existing_runtime_import():
    from dpone.contracts.clickhouse_publication import PublicationUnknown as CanonicalUnknown

    assert PublicationUnknown is CanonicalUnknown
    assert PublicationUnknown.safe_to_retry is False
    assert PublicationUnknown.operator_verification_required is True


def prepared_store(tmp_path, method="replace_partition"):
    path = tmp_path / "authority.db"
    SQLitePublicationAuthority.provision(path, "deployment")
    store = SQLitePublicationAuthority(path, "deployment")
    original_binding, original_intent = original_publication(method)
    binding = store.acquire(original_binding.operation_id, original_binding.subject, original_binding.candidate)
    entry = store.prepare(binding, original_intent)
    return store, store.claim(entry)


def test_acknowledged_send_eos_and_terminal_order_retains_owner(tmp_path):
    store, grant = prepared_store(tmp_path)
    transport = Transport(store)
    service = publisher(store, transport)
    assert service.execute_once(grant) is None
    assert store.transport_state(grant.operation_id) == TransportState.CLOSED_TERMINAL
    assert len(transport.requests) == 1
    before = store.diagnostics(grant.operation_id)
    service.close_and_drain(grant.operation_id)
    service.close_and_drain(grant.operation_id)
    assert store.diagnostics(grant.operation_id) == before
    assert store.read(grant.operation_id).record.state == PublicationState.CLAIMED
    with pytest.raises(PublicationUnknown):
        service.execute_once(grant)
    with pytest.raises(AuthorityConflict):
        store.acquire("deployment:next", subject(), "next_candidate")
    assert len(transport.requests) == 1
    assert grant.secret not in repr(before)


def test_lost_begin_send_ack_sends_nothing(tmp_path, monkeypatch):
    store, grant = prepared_store(tmp_path)
    transition = store.begin_send

    def lost_ack(value):
        transition(value)
        raise OSError("synthetic-sensitive-error")

    monkeypatch.setattr(store, "begin_send", lost_ack)
    transport = Transport(store)
    service = publisher(store, transport)
    with pytest.raises(PublicationUnknown) as error:
        service.execute_once(grant)
    assert not transport.requests
    assert "synthetic-sensitive-error" not in str(error.value)
    assert error.value.safe_to_retry is False
    assert store.transport_state(grant.operation_id) == TransportState.MAY_HAVE_SENT
    with pytest.raises(PublicationUnknown):
        service.close_and_drain(grant.operation_id)


@pytest.mark.parametrize("committed", [False, True])
def test_terminal_ack_loss_never_replays(tmp_path, monkeypatch, committed):
    store, grant = prepared_store(tmp_path)
    terminal = store.record_terminal

    def lost_ack(value, digest):
        if committed:
            terminal(value, digest)
        raise OSError("lost terminal ack")

    monkeypatch.setattr(store, "record_terminal", lost_ack)
    transport = Transport(store)
    with pytest.raises(PublicationUnknown):
        publisher(store, transport).execute_once(grant)
    reopened = SQLitePublicationAuthority(tmp_path / "authority.db", "deployment")
    recovery = publisher(reopened, transport)
    if committed:
        recovery.close_and_drain(grant.operation_id)
        with pytest.raises(AuthorityConflict):
            reopened.record_terminal(grant, "e" * 64)
    else:
        with pytest.raises(PublicationUnknown):
            recovery.close_and_drain(grant.operation_id)
    with pytest.raises(PublicationUnknown):
        recovery.execute_once(grant)
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    "error", [TimeoutError("private"), ConnectionError("private"), RuntimeError("private"), KeyboardInterrupt()]
)
def test_transport_failure_and_cancellation_stay_possible_send(tmp_path, error):
    store, grant = prepared_store(tmp_path)
    transport = Transport(store, error)
    service = publisher(store, transport)
    expected = KeyboardInterrupt if isinstance(error, KeyboardInterrupt) else PublicationUnknown
    with pytest.raises(expected):
        service.execute_once(grant)
    assert store.transport_state(grant.operation_id) == TransportState.MAY_HAVE_SENT
    with pytest.raises(PublicationUnknown):
        service.close_and_drain(grant.operation_id)
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    "change",
    [
        dict(query_id="foreign"),
        dict(operation_id="deployment:other"),
        dict(server_id="elsewhere"),
        dict(statement_digest="e" * 64),
        dict(driver_version="0.2.11"),
        dict(server_version=(24, 8, 15)),
    ],
)
def test_unrelated_completion_cannot_close_original(tmp_path, change):
    store, grant = prepared_store(tmp_path)
    with pytest.raises(PublicationUnknown):
        publisher(store, Transport(store, changed=change)).execute_once(grant)
    assert store.transport_state(grant.operation_id) == TransportState.MAY_HAVE_SENT


@pytest.mark.parametrize("change", [dict(operation_id="deployment:other"), dict(epoch=2), dict(secret="wrong")])
def test_foreign_and_stale_grants_never_dispatch(tmp_path, change):
    store, grant = prepared_store(tmp_path)
    transport = Transport(store)
    with pytest.raises(PublicationUnknown):
        publisher(store, transport).execute_once(replace(grant, **change))
    assert not transport.requests
    assert store.transport_state(grant.operation_id) == TransportState.NOT_STARTED


def test_noop_and_closure_state_matrix(tmp_path):
    store, grant = prepared_store(tmp_path, "noop")
    transport = Transport(store)
    service = publisher(store, transport)
    service.execute_once(grant)
    assert store.transport_state(grant.operation_id) == TransportState.CLOSED_WITHOUT_SEND
    before = store.diagnostics(grant.operation_id)
    service.close_and_drain(grant.operation_id)
    assert store.diagnostics(grant.operation_id) == before
    assert not transport.requests
    with pytest.raises(AuthorityConflict):
        store.acquire("deployment:next", subject(), "next_candidate")


def _delayed(path, grant, ready, proceed, result):
    store = SQLitePublicationAuthority(path, "deployment")
    transport = Transport(store)
    service = publisher(store, transport)
    ready.set()
    assert proceed.wait(20)
    try:
        service.execute_once(grant)
    except PublicationUnknown:
        result.put(len(transport.requests))


def test_delayed_claimant_loses_to_close(tmp_path):
    store, grant = prepared_store(tmp_path)
    ctx = multiprocessing.get_context("spawn")
    ready, proceed, result = ctx.Event(), ctx.Event(), ctx.Queue()
    process = ctx.Process(target=_delayed, args=(tmp_path / "authority.db", grant, ready, proceed, result))
    process.start()
    try:
        assert ready.wait(20)
        publisher(store, Transport(store)).close_and_drain(grant.operation_id)
        proceed.set()
        assert result.get(timeout=20) == 0
        process.join(20)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        result.close()
    assert store.transport_state(grant.operation_id) == TransportState.CLOSED_WITHOUT_SEND


def test_divergent_binding_rejected_before_transport(tmp_path, monkeypatch):
    store, grant = prepared_store(tmp_path)
    original = store.binding(grant.operation_id)
    monkeypatch.setattr(store, "binding", lambda operation: replace(original, candidate="elsewhere"))
    transport = Transport(store)
    with pytest.raises(PublicationUnknown):
        publisher(store, transport).execute_once(grant)
    assert not transport.requests


def test_documented_offline_example_closes_without_send(capsys):
    guide = Path(__file__).resolve().parents[1] / "docs/clickhouse-native-publication.md"
    snippet = guide.read_text(encoding="utf-8").split("```python\n", 1)[1].split("```", 1)[0]
    exec(compile(snippet, str(guide), "exec"), {})
    assert capsys.readouterr().out == "closed_without_send True\n"
