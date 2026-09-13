"""No credential replay, late OPEN success, or acknowledgement before persistence."""

from dataclasses import replace
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from dpone.app.composition_dispatcher_sessions import DispatcherGateComponents, DispatcherSessions
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.test_composition_clickhouse_dispatch import create

SHA = "sha256:" + "a" * 64
ATTEMPT = create().attempt


def setup_sessions(*, issue=None, execute=None, terminal=None, capacity=2):
    log, now = [], [1.0]
    selected = SimpleNamespace(
        attempt=ATTEMPT,
        write="write",
        manifest_document=b"original",
        context=SimpleNamespace(occurrence="parent", binding="binding", target_binding_ref="target"),
    )

    def record(*args):
        log.append("terminal")
        if terminal:
            terminal()

    journal = SimpleNamespace(record_completed=record)

    def issuing(_):
        log.append("issue")
        if issue:
            issue()
        return SimpleNamespace(user_id="uuid", username="name", password="secret")

    gate = SimpleNamespace(
        issue_once=issuing,
        journal=lambda *_: journal,
        close=lambda _: log.append("close") or "closed-proof",
        prove_quiescence=lambda _: log.append("quiescence") or "quiet-proof",
    )

    def sending(dispatch, *, payload):
        log.append("send")
        if execute:
            execute()
        return "observation"

    transport = SimpleNamespace(execute=sending)

    def factory(*_):
        log.append("factory")
        return DispatcherGateComponents(gate, lambda *_: transport)

    sessions = DispatcherSessions(
        loader=SimpleNamespace(load_attempt=lambda *_: selected),
        factory=factory,
        max_sessions=capacity,
        clock=lambda: now[0],
    )
    return sessions, log, now, selected


def test_open_returns_only_public_identity_and_duplicate_is_blocked():
    sessions, log, _, _ = setup_sessions()
    result = sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    assert result.gate_id == "uuid" and "secret" not in repr(result)
    with pytest.raises(CompositionAdmissionError):
        sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    assert log == ["factory", "issue"]


def test_failed_issuance_retains_slot_and_capacity():
    def fail():
        raise RuntimeError("secret")

    sessions, log, _, _ = setup_sessions(issue=fail, capacity=1)
    for attempt in (ATTEMPT, ATTEMPT, replace(ATTEMPT, try_number=2)):
        with pytest.raises(CompositionAdmissionError) as error:
            sessions.open(SHA, attempt, "INGEST", deadline=10)
        assert "secret" not in str(error.value)
    assert log == ["factory", "issue"]


def test_late_issuance_cannot_return_success_or_retry():
    sessions, log, now, _ = setup_sessions()

    def late(_):
        now[0] = 11
        return DispatcherGateComponents(SimpleNamespace(issue_once=lambda _: None), lambda *_: None)

    sessions._factory = late
    with pytest.raises(CompositionAdmissionError):
        sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    with pytest.raises(CompositionAdmissionError):
        sessions.open(SHA, ATTEMPT, "INGEST", deadline=20)


def test_close_during_issuance_never_acknowledges_open():
    entered, resume = Event(), Event()

    def pause():
        entered.set()
        assert resume.wait(2)

    sessions, log, _, _ = setup_sessions(issue=pause)
    errors = []

    def opening():
        try:
            sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
        except CompositionAdmissionError:
            errors.append("unknown")

    thread = Thread(target=opening)
    thread.start()
    try:
        assert entered.wait(2)
        with pytest.raises(CompositionAdmissionError):
            sessions.close(SHA, ATTEMPT, "INGEST", deadline=10)
    finally:
        resume.set()
        thread.join(2)
    assert not thread.is_alive() and errors == ["unknown"]
    sessions.close(SHA, ATTEMPT, "INGEST", deadline=10)
    assert log[-2:] == ["close", "quiescence"]


def test_closed_session_cannot_reopen():
    sessions, _, _, _ = setup_sessions()
    sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    assert sessions.close(SHA, ATTEMPT, "INGEST", deadline=10).quiescence == "quiet-proof"
    with pytest.raises(CompositionAdmissionError):
        sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)


def test_dispatch_terminal_precedes_ack_and_foreign_context_blocks_send():
    sessions, log, _, selected = setup_sessions()
    sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    assert sessions.dispatch(SHA, create(), deadline=10) == "observation"
    assert log[-2:] == ["send", "terminal"]
    sessions._loader.load_attempt = lambda *_: SimpleNamespace(**(vars(selected) | {"write": "foreign"}))
    with pytest.raises(CompositionAdmissionError):
        sessions.dispatch(SHA, create(), deadline=10)
    assert log[-2:] == ["send", "terminal"]


def test_failed_terminal_or_expired_response_never_acknowledges_or_resends():
    def fail():
        raise RuntimeError("secret")

    sessions, log, _, _ = setup_sessions(terminal=fail)
    sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    for _ in range(2):
        with pytest.raises(CompositionAdmissionError):
            sessions.dispatch(SHA, create(), deadline=10)
    assert log.count("send") == 1
    sessions, log, now, _ = setup_sessions(execute=lambda: now.__setitem__(0, 11))
    sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    with pytest.raises(CompositionAdmissionError):
        sessions.dispatch(SHA, create(), deadline=10)
    assert log[-2:] == ["send", "terminal"]


def test_close_does_not_hold_global_lock_while_dispatch_is_paused():
    entered, resume = Event(), Event()

    def pause():
        entered.set()
        assert resume.wait(2)

    sessions, log, _, _ = setup_sessions(execute=pause)
    sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    errors = []

    def sending():
        try:
            sessions.dispatch(SHA, create(), deadline=10)
        except Exception as exc:
            errors.append(exc)

    thread = Thread(target=sending)
    thread.start()
    try:
        assert entered.wait(2)
        sessions.close(SHA, ATTEMPT, "INGEST", deadline=10)
        with pytest.raises(CompositionAdmissionError):
            sessions.dispatch(SHA, create(), deadline=10)
    finally:
        resume.set()
        thread.join(2)
    assert not thread.is_alive() and not errors
    assert log.index("close") < log.index("terminal")
    # The double intentionally provides no SQL barrier; the production gate
    # must reject unresolved claims rather than certify this ordering closed.


def test_fresh_process_has_no_issued_session():
    sessions, _, _, _ = setup_sessions()
    with pytest.raises(CompositionAdmissionError):
        sessions.dispatch(SHA, create(), deadline=10)


def test_actual_transport_claims_before_network_and_records_before_return(monkeypatch):
    from dpone.adapters.composition_clickhouse_transport import (
        ClickHouseDispatchTransport,
        ClickHouseTransportCredentials,
    )

    sessions, log, _, _ = setup_sessions()
    sessions.open(SHA, ATTEMPT, "INGEST", deadline=10)
    session = next(iter(sessions._sessions.values()))
    claimed = set()

    def claim(dispatch):
        if dispatch.claim_key in claimed:
            raise CompositionAdmissionError("duplicate")
        claimed.add(dispatch.claim_key)
        log.append("claim")

    journal = session.components.gate.journal(ATTEMPT, "uuid")
    journal.claim_once = claim
    transport = ClickHouseDispatchTransport(
        endpoint="http://127.0.0.1:8123",
        credentials=ClickHouseTransportCredentials("user", "secret"),
        journal=journal,
        timeout_seconds=1,
    )

    def request(**_):
        assert log[-1] == "claim"
        log.append("network")
        return SimpleNamespace(body=b"", request_body_bytes=0, framing="content-length")

    monkeypatch.setattr(transport._http, "request", request)
    session.components = DispatcherGateComponents(session.components.gate, lambda *_: transport)
    result = sessions.dispatch(SHA, create(), deadline=10)
    assert result.dispatch_sha256 == create().dispatch_sha256
    assert log[-3:] == ["claim", "network", "terminal"]
    with pytest.raises(CompositionAdmissionError):
        sessions.dispatch(SHA, create(), deadline=10)
    assert log.count("network") == 1


def test_attempt_selection_uses_exact_workload_not_relation_name(monkeypatch, tmp_path):
    from dpone.app import composition_dispatcher_context as module
    from dpone.contracts.composition_control import dbt_relation_write_subject
    from dpone.contracts.composition_persistence import CompositionAttemptIdentity
    from dpone.contracts.dbt_relation_writes import DbtRelationWrite
    from dpone.contracts.strict_json import canonical_json_bytes
    from tests.test_composition_dispatcher_context import _staged

    loader, arguments, _, _, _ = _staged(monkeypatch, tmp_path)
    write = DbtRelationWrite("p", "w", "orders", "transfer", "clickhouse", "target", "db", "dbo", "same")
    other = DbtRelationWrite("p", "w", "other", "transfer", "clickhouse", "foreign", "db", "dbo", "same")
    attempt = CompositionAttemptIdentity(
        SHA, "orders", "standalone", SHA, arguments["plan_sha256"], "run", "task", 1, -1, ((SHA, 1),)
    )
    manifest = canonical_json_bytes(
        {
            "name": "orders",
            "sink": {
                "type": "clickhouse",
                "connection_ref": "target",
                "table": {"database": "db", "schema": "dbo", "name": "same"},
            },
        }
    )
    plan = SimpleNamespace(
        sources=SimpleNamespace(subject_sha256=arguments["plan_sha256"], transfer_manifests=(("orders", manifest),)),
        writes=(write, other),
        workloads=(
            SimpleNamespace(
                workload_id="orders",
                constituent_id="standalone",
                pack_sha256=SHA,
                execution_cell="mssql_clickhouse_full_refresh_v1",
                write_subjects=(dbt_relation_write_subject(write),),
            ),
        ),
    )
    monkeypatch.setattr(module, "reopen_composition_plan", lambda *_: plan)
    selected = loader.load_attempt(SHA, attempt)
    assert selected.write == write
    assert selected.context.target_binding_ref == "target"
    assert selected.manifest["name"] == "orders"
    selected.manifest["name"] = "changed"
    assert selected.manifest["name"] == "orders"
    for changes in (
        {"pack_sha256": "sha256:" + "f" * 64},
        {"constituent_id": "native"},
        {"workload_id": "other"},
        {"plan_sha256": SHA},
    ):
        with pytest.raises(CompositionAdmissionError):
            loader.load_attempt(SHA, replace(attempt, **changes))
    plan.workloads[0].write_subjects = (dbt_relation_write_subject(other),)
    with pytest.raises(CompositionAdmissionError):
        loader.load_attempt(SHA, attempt)
