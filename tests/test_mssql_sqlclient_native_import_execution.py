from types import SimpleNamespace

from dpone.app import mssql_sqlclient_native_import_execution as subject
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import SqlClientInputCustody


def _custody():
    return SqlClientInputCustody.bind(
        plan_sha256="a" * 64,
        target_id="target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="run-0-0",
        ordinal=0,
        rows=1,
        encoded_bytes=2,
        file_sha256="b" * 64,
        typed_digest="c" * 64,
        durable_object_id="object",
        durable_location_sha256="d" * 64,
    )


def test_execution_consumes_only_the_exact_terminal_once(monkeypatch):
    terminal = object()
    projection = object()
    calls = []
    monkeypatch.setattr(subject, "SqlClientWriterVerified", object)
    monkeypatch.setattr(subject, "project_sqlclient_native_chunk", lambda value: calls.append(value) or projection)
    execution = subject.SqlClientNativeImportExecution(lambda *args: terminal)

    assert execution.execute(SimpleNamespace(), SimpleNamespace(), "run-0-0", object(), _custody()) is projection
    assert calls == [terminal]


def test_execution_rejects_noncanonical_custody_before_effect():
    calls = []
    execution = subject.SqlClientNativeImportExecution(lambda *args: calls.append(args))
    try:
        execution.execute(object(), object(), "run-0-0", object(), object())
    except ValueError as error:
        assert str(error) == "mssql_native.sqlclient_import_execution_invalid"
    else:
        raise AssertionError("invalid custody accepted")
    assert calls == []
