"""Read-only pinned journal regressions; offline SQL is not live certification."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.test_composition_snapshot_capture import capture_env as capture_env
from tests.test_composition_snapshot_capture import runtime as runtime
from tests.test_composition_snapshot_sql_store import SERVICE
from tests.test_composition_snapshot_sql_store import active as active


def test_capture_supplied_ledger_callback_and_reads_never_open_connection(capture_env):
    env = capture_env
    seen = []
    env.store._enrollment = lambda ledger, subject: seen.append((ledger, subject))
    with env.store._transaction() as ledger:
        count = len(env.runtime.db.connections)
        assert env.store.read_in(ledger, env.subject.attempt) == (env.subject, {})
        assert len(env.runtime.db.connections) == count
        assert seen and all(item == (ledger, env.subject) for item in seen)


@pytest.mark.parametrize("replacement", ["transaction", "cursor", "subject"])
def test_capture_rejects_callback_context_or_subject_replacement(capture_env, replacement):
    env = capture_env
    with pytest.raises(CompositionAdmissionError), env.store._transaction() as ledger:

        def change(current, subject):
            if replacement == "transaction":
                current.cursor.connection.commit()
                current.begin(current.expected_service_id)
            elif replacement == "cursor":
                current.cursor = current.cursor.connection.cursor()

        env.store._enrollment = change
        if replacement == "subject":
            env.store._verify = lambda _: replace(env.subject, attempt=replace(env.subject.attempt, try_number=9))
        env.store.read_in(ledger, env.subject.attempt)


def test_publication_same_ledger_reads_neither_connect_nor_call_mutation_authority(active):
    db, store, value, authority = active
    prepared = store.prepare(value)
    authority.fail = True
    before = deepcopy(db.data)
    with composition_control_transaction(db.connect, "dpone_control", SERVICE) as ledger:
        count = len(db.connections)
        assert store.read_in(ledger, value.intent_sha256) == prepared
        assert store.records_in(ledger) == (prepared,)
        assert len(db.connections) == count
    assert db.data == before


@pytest.mark.parametrize("wrong", ["service", "schema", "attempt", "history"])
def test_publication_pinned_reads_reject_changed_identity_or_history(active, wrong):
    db, store, value, _ = active
    store.prepare(value)
    if wrong == "attempt":
        store._attempt = replace(value.attempt, try_number=value.attempt.try_number + 1)
    if wrong == "history":
        key = next(iter(db.data["history"]))
        row = db.data["history"][key]
        db.data["history"][key] = (*row[:-1], row[-1] + b" ")
    with composition_control_transaction(db.connect, "dpone_control", SERVICE) as ledger:
        if wrong == "service":
            store._service = "10000000-0000-4000-8000-000000000099"
        if wrong == "schema":
            store._schema = "foreign"
        with pytest.raises(CompositionAdmissionError):
            store.read_in(ledger, value.intent_sha256)


def test_publication_callback_transaction_replacement_is_rejected(active, monkeypatch):
    from dpone.adapters import composition_snapshot_sql_store as module

    db, store, value, _ = active
    store.prepare(value)
    original = module.require_existing_execution_in

    def replace_transaction(ledger, *args, **kwargs):
        result = original(ledger, *args, **kwargs)
        ledger.cursor.connection.commit()
        ledger.begin(SERVICE)
        return result

    monkeypatch.setattr(module, "require_existing_execution_in", replace_transaction)
    with (
        pytest.raises(CompositionAdmissionError),
        composition_control_transaction(db.connect, "dpone_control", SERVICE) as ledger,
    ):
        store.read_in(ledger, value.intent_sha256)


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED"])
def test_publication_terminal_reader_requires_terminal_validation(active, monkeypatch, state):
    from types import SimpleNamespace

    from dpone.adapters import composition_snapshot_sql_store as module

    db, store, value, _ = active
    store.prepare(value)
    original = module.require_existing_execution_in

    def terminal(ledger, *args, **kwargs):
        occurrence, _ = original(ledger, *args, **kwargs)
        return occurrence, SimpleNamespace(state=state)

    monkeypatch.setattr(module, "require_existing_execution_in", terminal)
    with composition_control_transaction(db.connect, "dpone_control", SERVICE) as ledger:

        def reject(*args):
            raise CompositionAdmissionError("test_missing_terminal_proof")

        monkeypatch.setattr(type(ledger), "terminal_validator", property(lambda self: reject))
        with pytest.raises(CompositionAdmissionError, match="test_missing_terminal_proof"):
            store.records_in(ledger)


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED"])
def test_capture_terminal_reader_requires_terminal_validation(capture_env, monkeypatch, state):
    from types import SimpleNamespace

    from dpone.adapters import composition_snapshot_capture_store as module

    original = module.require_existing_execution_in

    def terminal(ledger, *args, **kwargs):
        occurrence, _ = original(ledger, *args, **kwargs)
        return occurrence, SimpleNamespace(state=state)

    monkeypatch.setattr(module, "require_existing_execution_in", terminal)
    with capture_env.store._transaction() as ledger:

        def reject(*args):
            raise CompositionAdmissionError("test_missing_terminal_proof")

        monkeypatch.setattr(type(ledger), "terminal_validator", property(lambda self: reject))
        with pytest.raises(CompositionAdmissionError, match="test_missing_terminal_proof"):
            capture_env.store.read_in(ledger, capture_env.subject.attempt)
