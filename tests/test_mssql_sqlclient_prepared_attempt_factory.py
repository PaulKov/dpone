from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
from dpone.adapters.mssql_sqlclient_installation import AdmittedSqlClientInstallation
from dpone.app import mssql_sqlclient_prepared_attempt_factory as module
from dpone.app.mssql_sqlclient_observe_composition import SqlClientRetainedObserve
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import (
    EncodedNativeFile,
    NativeBulkTransportPolicy,
    NativeChunkPlan,
    TdsInputReceipt,
)
from dpone.contracts.mssql_sqlclient_input import SqlClientFileIdentity, SqlClientInputDescriptor
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import SqlClientInputCustody, plan_sha256
from dpone.contracts.mssql_sqlclient_preparation_qualification import (
    AdmittedPreparationBaseline,
    PreparationBaselineAuthority,
)
from dpone.contracts.mssql_tds_worker_identity import TdsAttemptIdentity, TdsAttemptPhase
from dpone.contracts.native_wire_layout import NativeWireColumnLayout
from dpone.services.mssql_tds_attempt import TdsAttempt

H = "a" * 64


def _fixture(tmp_path: Path):
    policy = NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire", transport=policy)
    file = EncodedNativeFile(tmp_path / "input.native", 3, 2, 16, "b" * 64, "c" * 64)
    lease = WindowLease("target", "owner", 1)
    custody = SqlClientInputCustody.bind(
        plan_sha256=plan_sha256(plan),
        target_id="target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="attempt-1",
        ordinal=3,
        rows=2,
        encoded_bytes=16,
        file_sha256="b" * 64,
        typed_digest="c" * 64,
        durable_object_id="object",
        durable_location_sha256=H,
    )
    descriptor = SqlClientInputDescriptor(
        1,
        7,
        (NativeWireColumnLayout("id", "bigint", "Int64", False, "bigint", fixed_length=8),),
        TdsInputReceipt(2, 16, "b" * 64),
        16,
        SqlClientFileIdentity(1, 2, 16, 3, 4),
    )
    parent = TdsAttemptIdentity(
        "target",
        "run",
        3,
        0,
        plan_sha256(plan),
        plan_sha256(policy),
        H,
        "b" * 64,
        "db",
        "stage",
        "chunk",
        H,
    )
    snapshot = SimpleNamespace(state=SimpleNamespace(phase=TdsAttemptPhase.PREPARED, identity=parent))
    attempt = TdsAttempt(SimpleNamespace(snapshot=snapshot), SimpleNamespace())
    handle = object.__new__(SqlClientRetainedObserve)
    handle.attempt = attempt
    handle.request = SimpleNamespace(parent=parent)
    context = module.SqlClientPreparedAttemptContext(
        handle=handle,
        input_fd=7,
        parent_input=descriptor,
        baseline=tuple.__new__(AdmittedPreparationBaseline),
        baseline_authority=PreparationBaselineAuthority(H),
        build=object.__new__(AdmittedSqlClientInstallation),
        reader_factory=PinnedEvidenceReadFactory(tmp_path),
        evidence_root=tmp_path,
        release_input=lambda: None,
    )
    return plan, file, lease, custody, context


def test_factory_prepares_exact_context_and_returns_original_attempt(monkeypatch, tmp_path):
    plan, file, lease, custody, context = _fixture(tmp_path)
    events = []

    @contextmanager
    def opened(*coordinates):
        events.append(("open", coordinates))
        yield context
        events.append(("close",))

    def prepared(handle, **inputs):
        events.append(("prepare", handle, inputs))
        return object()

    monkeypatch.setattr(module, "prepare_sqlclient_attempt", prepared)
    result = module.SqlClientPreparedAttemptFactory(opened).prepare(plan, file, "attempt-1", lease, custody)

    assert result.attempt is context.handle.attempt
    assert events[0] == ("open", (plan, file, "attempt-1", lease, custody))
    assert events[1][0:2] == ("prepare", context.handle)
    assert events[1][2]["expected_typed_digest"] == file.typed_digest
    assert events[-1] == ("close",)


@pytest.mark.parametrize("difference", ["attempt", "digest", "lease", "transport"])
def test_factory_rejects_mismatched_coordinates_before_opening(difference, tmp_path):
    plan, file, lease, custody, _ = _fixture(tmp_path)
    attempt_id = "attempt-1"
    if difference == "attempt":
        attempt_id = "attempt-2"
    elif difference == "digest":
        file = EncodedNativeFile(file.path, file.ordinal, file.rows, file.encoded_bytes, "d" * 64, file.typed_digest)
    elif difference == "lease":
        lease = WindowLease("other", "owner", 1)
    else:
        plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")

    def opened(*args):
        raise AssertionError("context opened")

    with pytest.raises(ValueError, match=module.ERROR):
        module.SqlClientPreparedAttemptFactory(opened).prepare(plan, file, attempt_id, lease, custody)


def test_factory_rejects_context_parent_mismatch_without_preparing(monkeypatch, tmp_path):
    plan, file, lease, custody, context = _fixture(tmp_path)
    context.handle.request = SimpleNamespace(parent=replace(context.handle.request.parent, run_id="other"))

    @contextmanager
    def opened(*args):
        yield context

    monkeypatch.setattr(module, "prepare_sqlclient_attempt", lambda *a, **k: pytest.fail("prepared"))
    with pytest.raises(ValueError, match=module.ERROR):
        module.SqlClientPreparedAttemptFactory(opened).prepare(plan, file, "attempt-1", lease, custody)


@pytest.mark.parametrize("difference", ["phase", "identity"])
def test_factory_requires_exact_prepared_ack(monkeypatch, difference, tmp_path):
    plan, file, lease, custody, context = _fixture(tmp_path)
    snapshot = context.handle.attempt._lifecycle.snapshot
    if difference == "phase":
        snapshot.state.phase = TdsAttemptPhase.CREATION_INTENT
    else:
        snapshot.state.identity = replace(snapshot.state.identity, run_id="other")

    @contextmanager
    def opened(*args):
        yield context

    monkeypatch.setattr(module, "prepare_sqlclient_attempt", lambda *a, **k: object())
    with pytest.raises(ValueError, match=module.ERROR):
        module.SqlClientPreparedAttemptFactory(opened).prepare(plan, file, "attempt-1", lease, custody)
