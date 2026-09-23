"""Production SqlClient route composition closes every invocation authority."""

from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, cast

import pytest

import dpone.app.mssql_sqlclient_native_route_composition as subject
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.adapters.mssql_sqlclient_input_custody import FileSqlClientInputCustody
from dpone.app.mssql_sqlclient_fresh_chunk_executor import FreshSqlClientChunkExecutor
from dpone.app.mssql_sqlclient_native_invocation_producer import (
    SqlClientNativeInvocationProducer,
    SqlClientNativeProductionDeployment,
    SqlClientNativeProductionInvocationDescription,
)
from dpone.app.mssql_sqlclient_native_parent_composition import SqlClientNativeParentDeployment
from dpone.app.mssql_sqlclient_native_runtime_composition import (
    SqlClientNativeImportCapabilities,
    SqlClientNativeInputCustody,
)
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkPlan
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.ports.mssql_native_route_backend import NativeActorCapacity
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService
from tests.test_mssql_native_policy import config


class Store:
    def acquire(self, target_id, owner, ttl):
        return WindowLease(target_id, owner, 1)

    def assert_lease(self, lease):
        return None

    def renew(self, lease, ttl):
        return None

    def release(self, lease):
        return None

    def load(self, key):
        return None

    def save(self, key, expected, payload, lease):
        raise AssertionError("save not expected")


def _fixture(tmp_path: Path):
    store = Store()
    lease = WindowLease("target", "owner", 7)
    plan = NativeChunkPlan(
        "run",
        "target",
        "query",
        "window",
        "schema",
        "wire",
        transport=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
    )
    journal = object.__new__(NativeChunkJournal)
    journal.store = store
    journal.lease = lease
    journal.plan = plan
    journal.parent_schema_version = 4
    custody = FileSqlClientInputCustody(tmp_path / "custody")
    imports = SqlClientNativeImportCapabilities(
        custody=SqlClientNativeInputCustody(custody),
        executor=object.__new__(FreshSqlClientChunkExecutor),
        inspect_projection=lambda *args: None,
        observe_failed_eligibility=lambda *args: None,
        settle_failed=lambda *args: None,
        allocated_bytes=lambda: 0,
    )
    parent = SqlClientNativeParentDeployment(
        journal=journal,
        lifecycle_observer=cast(Any, object()),
        directory_observer=cast(Any, object()),
        rollback_no_commit=lambda: {},
        evidence_reader=cast(Any, object()),
        retirement=cast(Any, object()),
        file_custody=custody,
        input_custody=cast(Any, object()),
        checkpoint=cast(Any, object()),
        directory_limits=cast(Any, object()),
    )

    @contextmanager
    def open_imports():
        yield imports

    stage = subject.SqlClientNativeStageDeployment(
        plan=plan,
        wire=object(),
        limits=SimpleNamespace(effective_import_parallelism=2),
        work_dir=tmp_path,
        sink=cast(Any, SimpleNamespace(_strategy_map={})),
        target_connector=SimpleNamespace(
            get_records=lambda *args: [],
            get_records_iterator=lambda *args: iter(()),
            execute_query=lambda *args: None,
            open_session=lambda *args, **kwargs: object(),
        ),
        database="db",
        schema="dbo",
        row_source=lambda: pytest.fail("source opened during composition"),
        journal_factory=lambda: journal,
        lease=lease,
        cancelled=Event(),
        target_headroom=1024,
    )
    invocation = subject.SqlClientNativeInvocationDeployment(
        stage=stage,
        imports=imports,
        parent=parent,
        open_import_capabilities=open_imports,
        capacity=NativeActorCapacity(9, 2, 2, 1),
        implementation_sha256="a" * 64,
        admission=object.__new__(MssqlTransactionAdmission),
    )
    return store, invocation


def _producer(invocation, *, open_invocation=None):
    stage = invocation.stage
    producer = object.__new__(SqlClientNativeInvocationProducer)
    production = object.__new__(SqlClientNativeProductionDeployment)
    for name, value in {
        "store": invocation.parent.journal.store,
        "plan": stage.plan,
        "capacity": invocation.capacity,
        "file_custody": invocation.parent.file_custody,
        "implementation_sha256": invocation.implementation_sha256,
    }.items():
        object.__setattr__(production, name, value)

    def open_default(lease, cancelled):
        invocation.parent.journal.lease = lease
        return replace(invocation, stage=replace(stage, lease=lease, cancelled=cancelled))

    def describe(lease, cancelled):
        return SqlClientNativeProductionInvocationDescription(
            target_id=stage.plan.target_id,
            plan=stage.plan,
            lease=lease,
            cancelled=cancelled,
            capacity=invocation.capacity,
            parallelism=stage.limits.effective_import_parallelism,
            custody=invocation.parent.file_custody,
            deployment=production,
            producer_identity=producer,
        )

    cast(Any, producer).describe = describe
    cast(Any, producer).open = lambda description: (open_invocation or open_default)(
        description.lease, description.cancelled
    )
    return producer


def test_compose_bindings_uses_parent_runtime_and_stage_composers(monkeypatch, tmp_path):
    _store, invocation = _fixture(tmp_path)
    invocation = replace(invocation, stage=replace(invocation.stage, row_source=lambda payload: payload))
    parent_capabilities, runtime_binding, stage_context = object(), object(), object()
    calls = []

    def parent(value):
        calls.append(("parent", value))
        return parent_capabilities

    monkeypatch.setattr(subject, "compose_sqlclient_native_parent_capabilities", parent)

    def runtime(**values):
        calls.append(("runtime", values))
        return runtime_binding

    def stage(**values):
        calls.append(("stage", values))
        return stage_context

    monkeypatch.setattr(subject, "compose_sqlclient_native_runtime_binding", runtime)
    monkeypatch.setattr(subject, "compose_sqlclient_native_stage_context", stage)

    result = subject.compose_sqlclient_native_bindings(invocation)

    assert result.stage_context is stage_context
    assert type(result.service) is MssqlNativeStagedLoadService
    assert result.service._sink is invocation.stage.sink
    assert result.service._preparer._sink is invocation.stage.sink
    assert not hasattr(invocation.stage.sink, "get_records")
    assert not hasattr(invocation.stage.target_connector, "_strategy_map")
    assert result.admission is invocation.admission
    assert [name for name, _value in calls] == ["parent", "runtime", "stage"]
    runtime_inputs = calls[1][1]
    assert runtime_inputs["parent"] is parent_capabilities
    assert runtime_inputs["imports"] is invocation.imports
    assert runtime_inputs["capacity"] is invocation.capacity
    assert runtime_inputs["open_import_capabilities"] is invocation.open_import_capabilities
    assert calls[2][1]["sqlclient_binding"] is runtime_binding
    assert calls[2][1]["store"] is invocation.parent.journal.store
    assert calls[2][1]["journal_factory"]() is invocation.parent.journal
    with pytest.raises(RuntimeError, match="recovery_source_forbidden"):
        calls[2][1]["row_source"]()
    payload = object()
    result.service._preparer._context_factory(object(), payload, object())
    assert calls[-1][1]["row_source"]() is payload


@pytest.mark.parametrize(
    "change,error",
    [
        ("journal", "route_composition_invalid"),
        ("lease", "route_composition_invalid"),
        ("cancel", "route_composition_invalid"),
        ("cancelled", "route_composition_invalid"),
        ("custody", "route_composition_invalid"),
        ("transport", "route_composition_invalid"),
        ("capacity", "capacity_insufficient"),
        ("implementation", "route_composition_invalid"),
    ],
)
def test_composition_rejects_incoherent_authority_before_composers(monkeypatch, tmp_path, change, error):
    _store, invocation = _fixture(tmp_path)
    stage, parent, imports = invocation.stage, invocation.parent, invocation.imports
    if change == "journal":
        other = object.__new__(NativeChunkJournal)
        stage = replace(stage, journal_factory=lambda: other)
    elif change == "lease":
        stage = replace(stage, lease=WindowLease("target", "owner", 8))
    elif change == "cancel":
        stage = replace(stage, cancelled=object())
    elif change == "cancelled":
        cancelled = Event()
        cancelled.set()
        stage = replace(stage, cancelled=cancelled)
    elif change == "custody":
        imports = replace(
            imports,
            custody=SqlClientNativeInputCustody(FileSqlClientInputCustody(tmp_path / "other")),
        )
    elif change == "transport":
        stage = replace(stage, plan=replace(stage.plan, transport=None))
    elif change == "capacity":
        invocation = replace(invocation, capacity=NativeActorCapacity(1, 2, 2, 1))
    else:
        invocation = replace(invocation, implementation_sha256="invalid")
    invocation = replace(invocation, stage=stage, parent=parent, imports=imports)
    monkeypatch.setattr(
        subject,
        "compose_sqlclient_native_parent_capabilities",
        lambda value: pytest.fail("parent composer reached"),
    )

    with pytest.raises(ValueError, match=error):
        subject.compose_sqlclient_native_bindings(invocation)


def test_runtime_root_preserves_target_only_binding_and_forbids_legacy_checkpoint(tmp_path):
    store, invocation = _fixture(tmp_path)
    deployment = subject.SqlClientNativeRouteDeployment(
        store=store,
        target_id="target",
        invocation_factory=lambda config, lease, cancelled: _producer(invocation).open(
            _producer(invocation).describe(lease, cancelled)
        ),
        source=lambda config, bindings: pytest.fail("source must not open while composing"),
        preflight=lambda config: None,
        quality=lambda config, handle, lease: None,
        evidence=lambda config, result, context, lease: None,
    )

    runtime = subject.compose_sqlclient_native_runtime(deployment)

    assert type(runtime) is NativeMssqlRuntime
    with pytest.raises(RuntimeError, match="checkpoint_bypassed_parent_settlement"):
        runtime.advance_state(object(), cast(Any, object()), invocation.stage.lease)


def test_runtime_root_rejects_incomplete_store_before_runtime_creation(tmp_path):
    _store, invocation = _fixture(tmp_path)
    deployment = subject.SqlClientNativeRouteDeployment(
        store=cast(Any, object()),
        target_id="target",
        invocation_factory=lambda config, lease, cancelled: _producer(invocation).open(
            _producer(invocation).describe(lease, cancelled)
        ),
        source=lambda config, bindings: pytest.fail("source opened"),
        preflight=lambda config: None,
        quality=lambda config, handle, lease: None,
        evidence=lambda config, result, context, lease: None,
    )
    with pytest.raises(ValueError, match="route_composition_invalid"):
        subject.compose_sqlclient_native_runtime(deployment)


def test_runtime_producer_owns_fresh_to_checkpoint_and_second_run_is_source_free(monkeypatch, tmp_path):
    store, template = _fixture(tmp_path)
    events = []
    source_opens = 0
    result = LoadResult(
        inserted_rows=2,
        updated_rows=0,
        total_rows=2,
        staging_rows=2,
        commit_receipt_id="receipt",
    )
    handle = StagedLoadHandle(None, (), 2)

    class Publication:
        phase = "published"

        @property
        def data(self):
            return {"version": 4}

        @property
        def publication(self):
            return self

        def state(self):
            return {"phase": self.phase}

    publication = Publication()

    class Service:
        resumes = 0

        def resume(self, *_args):
            events.append("resume")
            self.resumes += 1
            return None if self.resumes == 1 else result

        def stage(self, *_args):
            events.append("fresh-p7-stage")
            return handle

        def finalize(self, *_args):
            events.append("parent-publication")
            return result

        def cleanup(self, *_args):
            events.append("cleanup")

        def cleanup_recovered(self, *_args):
            events.append("cleanup-recovered")

    service = Service()

    def invocation(lease, cancelled):
        events.append("producer-open")
        assert template.parent.journal.store is store
        template.parent.journal.lease = lease
        stage = replace(template.stage, lease=lease, cancelled=cancelled)
        opened = replace(template, stage=stage)
        return opened

    def bindings(deployment):
        events.append("bindings")
        assert deployment.stage.lease is lease_seen[-1]
        assert deployment.stage.cancelled is cancelled_seen[-1]
        assert deployment.parent.journal.store is store

        def settle_parent():
            events.extend(("retirement", "custody-release", "checkpoint"))
            publication.phase = "succeeded"

        context = SimpleNamespace(
            plan=deployment.stage.plan,
            lease=deployment.stage.lease,
            cancelled=deployment.stage.cancelled,
            journal_factory=lambda: publication,
            settle_parent=settle_parent,
        )
        return NativeRuntimeBindings(service, context, deployment.admission)

    original_invocation = invocation
    lease_seen = []
    cancelled_seen = []

    def recording_invocation(lease, cancelled):
        lease_seen.append(lease)
        cancelled_seen.append(cancelled)
        return original_invocation(lease, cancelled)

    @contextmanager
    def source(_config, binding):
        nonlocal source_opens
        source_opens += 1
        events.append("source")
        assert binding.service is service
        yield object()

    monkeypatch.setattr(subject, "compose_sqlclient_native_bindings", bindings)
    deployment = subject.SqlClientNativeRouteDeployment(
        store=store,
        target_id="target",
        invocation_factory=lambda config, lease, cancelled: recording_invocation(lease, cancelled),
        source=source,
        preflight=lambda _config: events.append("preflight"),
        quality=lambda *_args: events.append("quality"),
        evidence=lambda *_args: events.append("evidence"),
    )
    runtime = subject.compose_sqlclient_native_runtime(deployment)
    load_config = config(
        transport={
            "backend": "mssql_sqlclient",
            "input": "rows",
            "max_worker_address_space_bytes": 8 << 30,
        }
    )

    first = runtime.run(load_config, owner="worker")
    second = runtime.run(load_config, owner="worker")

    assert first.status == second.status == "success"
    assert source_opens == 1
    assert events == [
        "preflight",
        "producer-open",
        "bindings",
        "resume",
        "source",
        "fresh-p7-stage",
        "quality",
        "parent-publication",
        "evidence",
        "retirement",
        "custody-release",
        "checkpoint",
        "cleanup",
        "preflight",
        "producer-open",
        "bindings",
        "resume",
        "cleanup-recovered",
    ]
    with pytest.raises(RuntimeError, match="checkpoint_bypassed_parent_settlement"):
        runtime.advance_state(load_config, result, lease_seen[0])
