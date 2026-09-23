import os
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.app import mssql_sqlclient_prepared_attempt_context as module
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import (
    EncodedNativeFile,
    NativeBulkTransportPolicy,
    NativeChunkPlan,
)
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import SqlClientInputCustodyReceipt, plan_sha256
from dpone.contracts.mssql_tds_create import TdsCreateColumn, TdsCreateType
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.contracts.native_wire_layout import NativeWireColumnLayout

H = "a" * 64


class Custody:
    def __init__(self, fd: int) -> None:
        self.fd = fd
        self.events: list[str] = []

    @contextmanager
    def open_pinned(self, _receipt):
        self.events.append("open_input")
        try:
            yield self.fd
        finally:
            self.events.append("close_input")


def _inputs(tmp_path: Path):
    path = tmp_path / "input.bin"
    path.write_bytes((1).to_bytes(8, "little", signed=True))
    descriptor = path.open("rb")
    policy = NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire", transport=policy)
    file = EncodedNativeFile(path, 4, 1, 8, sha256(path.read_bytes()).hexdigest(), "b" * 64)
    receipt = SqlClientInputCustodyReceipt.bind(
        plan_sha256=plan_sha256(plan),
        target_id="target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="run-4-1",
        ordinal=4,
        rows=1,
        encoded_bytes=8,
        file_sha256=file.file_sha256,
        typed_digest=file.typed_digest,
        durable_object_id="object",
        durable_location_sha256=H,
    )
    return path, descriptor, plan, file, receipt


def _deployment(tmp_path: Path, custody: Custody):
    admitted = object.__new__(_AdmittedSqlClientStoreFactory)
    admitted._factory = lambda: None
    admitted._record = object()
    admitted._domain_id = UUID(int=2)
    return module.SqlClientPreparedAttemptDeployment(
        input_custody=custody,
        pool=object(),
        admitted_factory=admitted,
        directory_limits=TdsDirectoryLimits(8, 3, 50000, 10000),
        supervisor_token=str(UUID(int=1)),
        implementation_sha256=H,
        database="db",
        schema="dbo",
        table="stage",
        owner_binding=H,
        create_columns=(TdsCreateColumn("value", TdsCreateType.BIGINT, False),),
        wire_columns=(NativeWireColumnLayout("value", "bigint", "Int64", False, "bigint", 0, 8, None, None, None),),
        max_row_bytes=1024,
        create_launcher=object(),
        departure_launcher=object(),
        observe_launcher=object(),
        creator_admission=object(),
        creator_principal=object(),
        writer_admission=object(),
        writer_principal=object(),
        create_connection_material=lambda: object(),
        observer_connection_material=lambda: object(),
        baseline=object(),
        baseline_authority=object(),
        build=object(),
        evidence_root=tmp_path / "evidence",
        operation_deadline=1000.0,
        create_startup_timeout=10.0,
        helper_startup_timeout=10.0,
        observe_startup_timeout=10.0,
        termination_timeout=5.0,
    )


def test_factory_rejects_structural_state_domain_before_opening_context(tmp_path):
    path = tmp_path / "input.bin"
    path.write_bytes(b"input")
    with path.open("rb") as stream:
        deployment = _deployment(tmp_path, Custody(stream.fileno()))
        object.__setattr__(deployment, "admitted_factory", lambda: None)

        with pytest.raises(ValueError, match="prepared_attempt_context_invalid"):
            module.compose_sqlclient_prepared_attempt_factory(deployment)


def test_context_composes_exact_create_observe_and_settlement(monkeypatch, tmp_path):
    _, stream, plan, file, receipt = _inputs(tmp_path)
    custody = Custody(stream.fileno())
    deployment = _deployment(tmp_path, custody)
    events = []
    attempt = SimpleNamespace(
        reserve_operation=lambda *args, **kwargs: events.append(("reserve", args, kwargs)),
        close=lambda **kwargs: events.append(("attempt_close", kwargs)),
    )
    evidence = object()
    outcome = SimpleNamespace(create_outcome=SimpleNamespace(response=SimpleNamespace(evidence=evidence)))
    stage = object()
    handle = SimpleNamespace(attempt=attempt, request=SimpleNamespace(parent=None), close=lambda **kwargs: None)

    monkeypatch.setattr(
        module, "create_tds_attempt", lambda *a, **k: events.append(("create_attempt", a, k)) or attempt
    )
    monkeypatch.setattr(module, "run_sqlclient_create_departure_v2", lambda *a, **k: events.append("create") or outcome)
    monkeypatch.setattr(module, "settle_sqlclient_create_departure", lambda *a, **k: events.append("create_settle"))
    monkeypatch.setattr(module, "stage_identity_from_create", lambda value: stage if value is evidence else None)
    monkeypatch.setattr(module, "SqlClientObserveRequest", lambda **values: SimpleNamespace(**values))

    def opened(_attempt, request, **kwargs):
        handle.request = request
        events.append("observe")
        return handle

    monkeypatch.setattr(module, "open_sqlclient_observe", opened)
    monkeypatch.setattr(module, "settle_prepared_observe", lambda *a, **k: events.append("observe_settle"))

    with module.open_sqlclient_prepared_attempt_context(
        deployment, plan, file, "run-4-1", WindowLease("target", "owner", 1), receipt
    ) as context:
        events.append("yield")
        assert context.input_fd != stream.fileno()
        assert context.parent_input.fd == context.input_fd
        assert context.parent_input.file_identity.size == 8
        assert context.parent_input.expected.file_sha256 == file.file_sha256
        assert context.handle is handle

    assert events[2:6] == ["create", "create_settle", "observe", "yield"]
    assert events[-1] == "observe_settle"
    assert custody.events == ["open_input", "close_input"]
    assert not any(isinstance(value, tuple) and value[0] == "attempt_close" for value in events)
    context.release_input()
    with pytest.raises(OSError):
        os.fstat(context.input_fd)
    stream.close()


def test_context_closes_owned_attempt_when_create_fails(monkeypatch, tmp_path):
    _, stream, plan, file, receipt = _inputs(tmp_path)
    custody = Custody(stream.fileno())
    deployment = _deployment(tmp_path, custody)
    closed = []
    attempt = SimpleNamespace(
        reserve_operation=lambda *args, **kwargs: None,
        close=lambda **kwargs: closed.append(kwargs["deadline"]),
    )
    monkeypatch.setattr(module, "create_tds_attempt", lambda *a, **k: attempt)
    monkeypatch.setattr(
        module, "run_sqlclient_create_departure_v2", lambda *a, **k: (_ for _ in ()).throw(RuntimeError())
    )

    with pytest.raises(RuntimeError):
        with module.open_sqlclient_prepared_attempt_context(
            deployment, plan, file, "run-4-1", WindowLease("target", "owner", 1), receipt
        ):
            pytest.fail("unreachable")

    assert closed == [deployment.operation_deadline]
    assert custody.events == ["open_input", "close_input"]
    stream.close()


def test_context_preserves_operation_and_cleanup_failures(monkeypatch, tmp_path):
    _, stream, plan, file, receipt = _inputs(tmp_path)
    custody = Custody(stream.fileno())
    deployment = _deployment(tmp_path, custody)
    attempt = SimpleNamespace(
        reserve_operation=lambda *args, **kwargs: None,
        close=lambda **kwargs: (_ for _ in ()).throw(OSError("attempt close failed")),
    )
    monkeypatch.setattr(module, "create_tds_attempt", lambda *a, **k: attempt)
    monkeypatch.setattr(
        module,
        "run_sqlclient_create_departure_v2",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("create failed")),
    )

    with pytest.raises(BaseExceptionGroup) as raised:
        with module.open_sqlclient_prepared_attempt_context(
            deployment, plan, file, "run-4-1", WindowLease("target", "owner", 1), receipt
        ):
            pytest.fail("unreachable")

    assert [str(error) for error in raised.value.exceptions] == ["create failed", "attempt close failed"]
    stream.close()


def test_context_preserves_operation_cleanup_and_input_release_failures(monkeypatch, tmp_path):
    _, stream, plan, file, receipt = _inputs(tmp_path)
    custody = Custody(stream.fileno())
    deployment = _deployment(tmp_path, custody)
    attempt = SimpleNamespace(
        reserve_operation=lambda *args, **kwargs: None,
        close=lambda **kwargs: (_ for _ in ()).throw(OSError("attempt close failed")),
    )
    monkeypatch.setattr(module, "create_tds_attempt", lambda *a, **k: attempt)
    monkeypatch.setattr(
        module,
        "run_sqlclient_create_departure_v2",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("create failed")),
    )
    actual_close = os.close
    leaked = []

    def failed_close(descriptor):
        leaked.append(descriptor)
        raise PermissionError("input release failed")

    monkeypatch.setattr(module.os, "close", failed_close)
    with pytest.raises(BaseExceptionGroup) as raised:
        with module.open_sqlclient_prepared_attempt_context(
            deployment, plan, file, "run-4-1", WindowLease("target", "owner", 1), receipt
        ):
            pytest.fail("unreachable")

    assert [str(error) for error in raised.value.exceptions] == [
        "create failed",
        "attempt close failed",
        "input release failed",
    ]
    assert len(leaked) == 1
    actual_close(leaked[0])
    stream.close()


def test_cleanup_attempts_both_resources_when_handle_close_fails():
    events = []

    class Handle:
        def close(self, *, deadline):
            events.append(("handle", deadline))
            raise RuntimeError("handle close failed")

    class Attempt:
        def close(self, *, deadline):
            events.append(("attempt", deadline))

    with pytest.raises(RuntimeError, match="handle close failed"):
        module._cleanup(Handle(), Attempt(), deadline=3.0)

    assert events == [("handle", 3.0), ("attempt", 3.0)]


def test_cleanup_preserves_both_close_failures():
    class Handle:
        def close(self, *, deadline):
            raise RuntimeError("handle close failed")

    class Attempt:
        def close(self, *, deadline):
            raise OSError("attempt close failed")

    with pytest.raises(BaseExceptionGroup) as raised:
        module._cleanup(Handle(), Attempt(), deadline=3.0)

    assert [str(error) for error in raised.value.exceptions] == ["handle close failed", "attempt close failed"]


@pytest.mark.parametrize("attempt_id", ["bad", "run-4-3", "run-3-1"])
def test_context_rejects_noncanonical_attempt_before_effects(tmp_path, attempt_id):
    _, stream, plan, file, receipt = _inputs(tmp_path)
    custody = Custody(stream.fileno())
    with pytest.raises(ValueError, match=module.ERROR):
        with module.open_sqlclient_prepared_attempt_context(
            _deployment(tmp_path, custody), plan, file, attempt_id, WindowLease("target", "owner", 1), receipt
        ):
            pytest.fail("unreachable")
    assert custody.events == []
    stream.close()
