from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.app import mssql_sqlclient_fresh_chunk_executor as module
from dpone.contracts.mssql_tds_worker import TdsAttemptError
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_writer_execution_custody import SqlClientWriterHandledFailure


def _execution(**changes):
    values = dict(
        prepared_attempt=lambda *args: module.SqlClientPreparedAttempt(
            attempt=object.__new__(TdsAttempt), release_input=lambda: None
        ),
        authorization=lambda attempt: object(),
        pool=object(),
        evidence_writer_factory=lambda: None,
        startup_deadline=2.0,
        operation_deadline=3.0,
        termination_timeout_seconds=4,
        max_worker_address_space_bytes=1024,
        writer_profile=object(),
        writer_credentials=lambda: object(),
        session_nonce=lambda: b"n" * 32,
        observer_admission=object(),
        observer_profile=object(),
        observer_credentials=lambda: object(),
        observer_launcher=object(),
        observer_startup_timeout=1.0,
        observer_termination_timeout=1.0,
        grant_id=lambda: UUID("11111111-1111-4111-8111-111111111111"),
        settlement_verifier=lambda: object(),
        clock_ns=lambda: 1,
        retirement_custody=SimpleNamespace(retain=lambda attempt, **kwargs: None),
    )
    values.update(changes)
    return module.FreshSqlClientChunkExecution(**values)


def test_executor_owns_exact_phase_order_and_returns_only_terminal(monkeypatch):
    events = []
    terminal_type = type("Terminal", (), {})
    terminal = terminal_type()
    observer_request = object()
    observer = object()
    monkeypatch.setattr(module, "SqlClientWriterVerified", terminal_type)
    monkeypatch.setattr(module, "SqlClientCredentials", object)
    monkeypatch.setattr(module, "SqlClientWriterGrantReady", object)
    monkeypatch.setattr(module, "SqlClientWriterLocallyExited", object)
    monkeypatch.setattr(
        module,
        "authorize_mssql_sqlclient_writer",
        lambda attempt, inputs: events.append("authorize") or object(),
    )
    monkeypatch.setattr(module, "admit_mssql_sqlclient_writer", lambda *a, **k: events.append("admit") or object())
    monkeypatch.setattr(module, "launch_mssql_sqlclient_writer", lambda *a, **k: events.append("launch") or object())
    monkeypatch.setattr(
        module,
        "prepare_mssql_sqlclient_writer",
        lambda *a, **k: events.append("pregrant") or object(),
    )
    monkeypatch.setattr(
        module,
        "sqlclient_writer_observer_request",
        lambda *a, **k: events.append("request") or observer_request,
    )
    monkeypatch.setattr(module, "preload_sqlclient_credentials", lambda *a: events.append("preload") or object())
    monkeypatch.setattr(
        module,
        "SqlClientWriterObservationInputs",
        lambda observer, grant_id, now_ns: SimpleNamespace(observer=observer, grant_id=grant_id, now_ns=now_ns),
    )
    monkeypatch.setattr(
        module,
        "open_sqlclient_writer_observer",
        lambda *a, **k: events.append("open-observer") or observer,
    )

    def observation(pregrant, *, session_inputs):
        events.append("observation")
        inputs = session_inputs()
        assert inputs.observer is observer
        return object()

    monkeypatch.setattr(module, "compose_sqlclient_writer_observation", observation)
    monkeypatch.setattr(module, "execute_sqlclient_writer", lambda *a, **k: events.append("execute") or object())
    monkeypatch.setattr(
        module,
        "settle_sqlclient_writer",
        lambda *a, **k: events.append("settle") or terminal,
    )
    execution = _execution(
        prepared_attempt=lambda *args: (
            events.append("prepared")
            or module.SqlClientPreparedAttempt(
                attempt=object.__new__(TdsAttempt), release_input=lambda: events.append("release-input")
            )
        )
    )

    result = module.FreshSqlClientChunkExecutor(execution).execute(
        SimpleNamespace(), SimpleNamespace(), "attempt", object(), SimpleNamespace()
    )

    assert result is terminal
    assert events == [
        "prepared",
        "authorize",
        "admit",
        "launch",
        "pregrant",
        "request",
        "observation",
        "preload",
        "open-observer",
        "execute",
        "settle",
        "release-input",
    ]


def test_early_result_route_never_opens_observer(monkeypatch):
    terminal_type = type("Terminal", (), {})
    monkeypatch.setattr(module, "SqlClientWriterVerified", terminal_type)
    monkeypatch.setattr(module, "SqlClientCredentials", object)
    monkeypatch.setattr(module, "SqlClientWriterGrantReady", type("GrantReady", (), {}))
    monkeypatch.setattr(module, "SqlClientWriterLocallyExited", object)
    monkeypatch.setattr(module, "authorize_mssql_sqlclient_writer", lambda *a: object())
    monkeypatch.setattr(module, "admit_mssql_sqlclient_writer", lambda *a, **k: object())
    monkeypatch.setattr(module, "launch_mssql_sqlclient_writer", lambda *a, **k: object())
    monkeypatch.setattr(module, "prepare_mssql_sqlclient_writer", lambda *a, **k: object())
    monkeypatch.setattr(module, "sqlclient_writer_observer_request", lambda *a, **k: None)
    monkeypatch.setattr(
        module,
        "open_sqlclient_writer_observer",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("observer opened")),
    )
    monkeypatch.setattr(
        module,
        "compose_sqlclient_writer_observation",
        lambda pregrant, *, session_inputs: object(),
    )
    monkeypatch.setattr(module, "execute_sqlclient_writer", lambda *a, **k: object())
    monkeypatch.setattr(module, "settle_sqlclient_writer", lambda *a: terminal_type())

    with pytest.raises(ValueError, match="fresh_chunk_execution_invalid"):
        module.FreshSqlClientChunkExecutor(_execution()).execute(object(), object(), "attempt", object(), object())


def test_executor_releases_input_when_execution_fails(monkeypatch):
    events = []
    prepared = module.SqlClientPreparedAttempt(
        attempt=object.__new__(TdsAttempt), release_input=lambda: events.append("released")
    )
    monkeypatch.setattr(
        module,
        "authorize_mssql_sqlclient_writer",
        lambda *args: (_ for _ in ()).throw(RuntimeError("execution failed")),
    )

    with pytest.raises(RuntimeError, match="execution failed"):
        module.FreshSqlClientChunkExecutor(_execution(prepared_attempt=lambda *args: prepared)).execute(
            object(), object(), "attempt", object(), object()
        )

    assert events == ["released"]


def test_executor_releases_input_for_invalid_wrapped_attempt():
    events = []
    prepared = module.SqlClientPreparedAttempt(attempt=object(), release_input=lambda: events.append("released"))

    with pytest.raises(ValueError, match="fresh_chunk_execution_invalid"):
        module.FreshSqlClientChunkExecutor(_execution(prepared_attempt=lambda *args: prepared)).execute(
            object(), object(), "attempt", object(), object()
        )

    assert events == ["released"]


def test_executor_preserves_execution_and_release_failures(monkeypatch):
    prepared = module.SqlClientPreparedAttempt(
        attempt=object.__new__(TdsAttempt),
        release_input=lambda: (_ for _ in ()).throw(OSError("release failed")),
    )
    monkeypatch.setattr(
        module,
        "authorize_mssql_sqlclient_writer",
        lambda *args: (_ for _ in ()).throw(RuntimeError("execution failed")),
    )

    with pytest.raises(BaseExceptionGroup) as raised:
        module.FreshSqlClientChunkExecutor(_execution(prepared_attempt=lambda *args: prepared)).execute(
            object(), object(), "attempt", object(), object()
        )

    assert [str(error) for error in raised.value.exceptions] == ["execution failed", "release failed"]


def _handled_failure_path(monkeypatch, code: TdsAttemptError) -> None:
    monkeypatch.setattr(module, "SqlClientCredentials", object)
    monkeypatch.setattr(module, "SqlClientWriterGrantReady", object)
    monkeypatch.setattr(module, "authorize_mssql_sqlclient_writer", lambda *a: object())
    monkeypatch.setattr(module, "admit_mssql_sqlclient_writer", lambda *a, **k: object())
    monkeypatch.setattr(module, "launch_mssql_sqlclient_writer", lambda *a, **k: object())
    monkeypatch.setattr(module, "prepare_mssql_sqlclient_writer", lambda *a, **k: object())
    monkeypatch.setattr(module, "sqlclient_writer_observer_request", lambda *a, **k: object())
    monkeypatch.setattr(module, "preload_sqlclient_credentials", lambda *a: object())
    monkeypatch.setattr(module, "open_sqlclient_writer_observer", lambda *a, **k: object())
    monkeypatch.setattr(module, "SqlClientWriterObservationInputs", lambda *a: object())
    monkeypatch.setattr(module, "compose_sqlclient_writer_observation", lambda *a, **k: object())
    monkeypatch.setattr(
        module,
        "execute_sqlclient_writer",
        lambda *a, **k: SqlClientWriterHandledFailure(code),
    )


@pytest.mark.parametrize(
    "code",
    (
        TdsAttemptError.CONNECTION,
        TdsAttemptError.DRIVER,
        TdsAttemptError.STARTUP_TIMEOUT,
        TdsAttemptError.OPERATION_TIMEOUT,
    ),
)
def test_acknowledged_retry_candidate_requires_settlement_before_retry(monkeypatch, code):
    _handled_failure_path(monkeypatch, code)

    with pytest.raises(module.SqlClientRetryPendingSettlement) as raised:
        module.FreshSqlClientChunkExecutor(_execution()).execute(object(), object(), "attempt", object(), object())

    assert raised.value.code is code


@pytest.mark.parametrize("code", tuple(set(TdsAttemptError) - module._RETRYABLE))
def test_acknowledged_deterministic_failure_is_terminal(monkeypatch, code):
    _handled_failure_path(monkeypatch, code)

    with pytest.raises(module.SqlClientTerminalAttemptError) as raised:
        module.FreshSqlClientChunkExecutor(_execution()).execute(object(), object(), "attempt", object(), object())

    assert raised.value.code is code
