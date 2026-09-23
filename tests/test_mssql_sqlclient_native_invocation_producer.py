"""Two-phase production invocation producer contract tests."""

from contextlib import contextmanager
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

import dpone.app.mssql_sqlclient_native_invocation_producer as subject
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.adapters.mssql_sqlclient_input_custody import FileSqlClientInputCustody
from dpone.app.mssql_sqlclient_fresh_chunk_execution_composition import SqlClientFreshChunkDeployment
from dpone.app.mssql_sqlclient_fresh_chunk_executor import (
    FreshSqlClientChunkExecution,
    FreshSqlClientChunkExecutor,
)
from dpone.app.mssql_sqlclient_native_observer_composition import SqlClientNativeObserverBundle
from dpone.app.mssql_sqlclient_native_parent_composition import SqlClientNativeParentDeployment
from dpone.app.mssql_sqlclient_native_retirement_composition import SqlClientNativeRetirementDeployment
from dpone.app.mssql_sqlclient_native_runtime_composition import (
    SqlClientNativeImportCapabilities,
    SqlClientNativeInputCustody,
)
from dpone.app.mssql_sqlclient_prepared_attempt_context import SqlClientPreparedAttemptDeployment
from dpone.app.mssql_sqlclient_prepared_attempt_factory import SqlClientPreparedAttemptFactory
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkLimits, NativeChunkPlan
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.ports.mssql_native_route_backend import NativeActorCapacity
from dpone.runtime.native_wire_models import SourceNativeWireContract
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody


class Store:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def assert_lease(self, _lease: object) -> None:
        self.events.append("journal.assert_lease")

    def load(self, _key: str) -> None:
        self.events.append("journal.load")
        return None


def _deployment(tmp_path: Path, events: list[str]) -> subject.SqlClientNativeProductionDeployment:
    plan = NativeChunkPlan(
        "run",
        "target",
        "query",
        "window",
        "schema",
        "wire",
        transport=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
    )
    custody = FileSqlClientInputCustody(tmp_path / "custody")

    @contextmanager
    def raw_factory():
        events.append("observer.open")
        try:
            yield cast(Any, Store(events))
        finally:
            events.append("observer.close")

    admitted_factory = object.__new__(_AdmittedSqlClientStoreFactory)
    admitted_factory._factory = raw_factory
    admitted_factory._record = object()
    admitted_factory._domain_id = UUID(int=1)

    prepared_deployment = object.__new__(SqlClientPreparedAttemptDeployment)
    object.__setattr__(prepared_deployment, "admitted_factory", admitted_factory)
    object.__setattr__(prepared_deployment, "directory_limits", cast(Any, object()))

    def parent_factory(
        journal: NativeChunkJournal,
        file_custody: FileSqlClientInputCustody,
        observers: SqlClientNativeObserverBundle,
        attempt_retirement_custody: SqlClientAttemptRetirementCustody,
        admitted_factory: object,
    ) -> SqlClientNativeParentDeployment:
        events.append("parent")
        return SqlClientNativeParentDeployment(
            journal=journal,
            lifecycle_observer=observers.lifecycle,
            directory_observer=observers.directory,
            rollback_no_commit=lambda: {},
            evidence_reader=cast(Any, object()),
            retirement=cast(Any, object()),
            file_custody=file_custody,
            input_custody=cast(Any, object()),
            checkpoint=cast(Any, object()),
            directory_limits=cast(Any, object()),
            attempt_retirement_custody=attempt_retirement_custody,
            recover_verified_retirement=lambda request, deadline: None,
        )

    def admission_factory(candidate: NativeChunkPlan, lease: WindowLease) -> MssqlTransactionAdmission:
        assert candidate is plan
        assert lease.target_id == "target"
        events.append("admission")
        return object.__new__(MssqlTransactionAdmission)

    return subject.SqlClientNativeProductionDeployment(
        store=cast(Any, Store(events)),
        plan=plan,
        wire=object.__new__(SourceNativeWireContract),
        limits=NativeChunkLimits(1000, 1000, import_parallelism=2),
        work_dir=tmp_path,
        sink=cast(Any, SimpleNamespace(_strategy_map={})),
        target_connector=cast(
            Any,
            SimpleNamespace(
                get_records=lambda *args: (),
                get_records_iterator=lambda *args: iter(()),
                execute_query=lambda *args: None,
                open_session=lambda *args: object(),
            ),
        ),
        database="database",
        schema="schema",
        row_source=lambda _payload: (),
        target_headroom=1024,
        capacity=NativeActorCapacity(9, 2, 2, 1),
        implementation_sha256="a" * 64,
        file_custody=custody,
        admitted_factory=admitted_factory,
        prepared_deployment=prepared_deployment,
        fresh_chunk=object.__new__(SqlClientFreshChunkDeployment),
        retirement=object.__new__(SqlClientNativeRetirementDeployment),
        inspect_projection=lambda *args: None,
        failed_attempt=subject.SqlClientFailedAttemptAuthority(
            resolve_attempt=lambda *args: None,
            observe_stage=lambda *args: None,
            observe_input_custody=lambda *args: "a" * 64,
        ),
        allocated_bytes=lambda: 0,
        parent_factory=parent_factory,
        admission_factory=admission_factory,
    )


def _patch_composers(monkeypatch: pytest.MonkeyPatch, events: list[str]) -> None:
    execution = object.__new__(FreshSqlClientChunkExecution)
    prepared = object.__new__(SqlClientPreparedAttemptFactory)
    failed_settlement = SimpleNamespace(settle=lambda *_args: None)

    def prepared_factory(deployment: object) -> SqlClientPreparedAttemptFactory:
        assert type(deployment) is SqlClientPreparedAttemptDeployment
        events.append("prepared")
        return prepared

    def fresh(
        deployment: object,
        prepared: object,
        retirement_custody: SqlClientAttemptRetirementCustody,
    ) -> FreshSqlClientChunkExecution:
        assert type(deployment) is SqlClientFreshChunkDeployment
        assert type(prepared) is SqlClientPreparedAttemptFactory
        assert type(retirement_custody) is SqlClientAttemptRetirementCustody
        events.append("fresh")
        return execution

    def imports(**values: object) -> SqlClientNativeImportCapabilities:
        events.append("imports")
        assert values["failed_settlement"] is failed_settlement
        custody = cast(FileSqlClientInputCustody, values["input_custody"])
        return SqlClientNativeImportCapabilities(
            custody=SqlClientNativeInputCustody(custody),
            executor=object.__new__(FreshSqlClientChunkExecutor),
            inspect_projection=cast(Any, values["inspect_projection"]),
            failed_settlement=cast(Any, values["failed_settlement"]),
            allocated_bytes=cast(Any, values["allocated_bytes"]),
        )

    def failed_retirement(_deployment):
        events.append("failed_retirement")
        return object(), object()

    def failed_attempt(_deployment):
        events.append("failed_attempt")
        return failed_settlement

    monkeypatch.setattr(subject, "compose_sqlclient_fresh_chunk_execution", fresh)
    monkeypatch.setattr(subject, "compose_sqlclient_native_import_capabilities", imports)
    monkeypatch.setattr(subject, "compose_sqlclient_prepared_attempt_factory", prepared_factory)
    monkeypatch.setattr(subject, "compose_sqlclient_failed_retirement", failed_retirement)
    monkeypatch.setattr(subject, "compose_sqlclient_failed_attempt_settlement", failed_attempt)


def test_describe_is_pure_and_open_binds_exact_schema_v4_identities(monkeypatch, tmp_path):
    events: list[str] = []
    deployment = _deployment(tmp_path, events)
    _patch_composers(monkeypatch, events)
    producer = subject.SqlClientNativeInvocationProducer(deployment)
    lease = subject.WindowLease("target", "owner", 7)
    cancelled = Event()

    description = producer.describe(lease, cancelled)

    assert events == []
    assert description.plan is deployment.plan
    assert description.lease is lease
    assert description.cancelled is cancelled
    invocation = producer.open(description)

    assert events == [
        "journal.assert_lease",
        "journal.load",
        "prepared",
        "failed_retirement",
        "failed_attempt",
        "fresh",
        "imports",
        "parent",
        "admission",
    ]
    journal = invocation.parent.journal
    assert journal.parent_schema_version == 4
    assert journal.plan is deployment.plan
    assert journal.lease is lease
    assert invocation.stage.plan is deployment.plan
    assert invocation.stage.lease is lease
    assert invocation.stage.cancelled is cancelled
    assert invocation.stage.journal_factory() is journal
    assert invocation.parent.file_custody is deployment.file_custody
    assert invocation.imports.custody.storage is deployment.file_custody
    with invocation.open_import_capabilities() as reopened:
        assert reopened is invocation.imports


def test_foreign_or_cancelled_description_is_rejected_before_recovery(monkeypatch, tmp_path):
    events: list[str] = []
    deployment = _deployment(tmp_path, events)
    _patch_composers(monkeypatch, events)
    producer = subject.SqlClientNativeInvocationProducer(deployment)
    foreign = subject.SqlClientNativeInvocationProducer(deployment)
    lease = subject.WindowLease("target", "owner", 7)
    description = producer.describe(lease, Event())

    with pytest.raises(ValueError, match="invocation_producer_invalid"):
        foreign.open(description)
    assert events == []

    description.cancelled.set()
    with pytest.raises(ValueError, match="invocation_producer_invalid"):
        producer.open(description)
    assert events == []


def test_invalid_lease_is_rejected_without_opening_factories(tmp_path):
    events: list[str] = []
    producer = subject.SqlClientNativeInvocationProducer(_deployment(tmp_path, events))

    with pytest.raises(ValueError, match="invocation_producer_invalid"):
        producer.describe(subject.WindowLease("other", "owner", 1), Event())

    assert events == []


def test_mismatched_prepared_state_domain_is_rejected_before_source(monkeypatch, tmp_path):
    events: list[str] = []
    deployment = _deployment(tmp_path, events)
    foreign = object.__new__(SqlClientPreparedAttemptDeployment)
    other = object.__new__(_AdmittedSqlClientStoreFactory)
    other._factory = lambda: None
    other._record = object()
    other._domain_id = UUID(int=2)
    object.__setattr__(foreign, "admitted_factory", other)
    object.__setattr__(deployment, "prepared_deployment", foreign)
    _patch_composers(monkeypatch, events)
    with pytest.raises(ValueError, match="invocation_producer_invalid"):
        subject.SqlClientNativeInvocationProducer(deployment)

    assert events == []


def test_raw_structural_state_domain_is_rejected_before_effects(tmp_path):
    events: list[str] = []
    deployment = _deployment(tmp_path, events)
    object.__setattr__(deployment, "admitted_factory", lambda: None)

    with pytest.raises(ValueError, match="invocation_producer_invalid"):
        subject.SqlClientNativeInvocationProducer(deployment)

    assert events == []
