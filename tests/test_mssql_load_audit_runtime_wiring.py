from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.dag.config_models import ETLProcessConfig
from dpone.ports.runtime_hydrator import RuntimeBindings
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.bootstrap_runner import DefaultProcessRunner
from dpone.runtime.bootstrap_state import RuntimeStateBootstrap
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.state.factory import StateFactory


@pytest.mark.parametrize(
    ("database", "schema"),
    [
        pytest.param("DWH_Dev", "system", id="dev"),
        pytest.param("Example_System", "dbo", id="prod"),
    ],
)
def test_resolved_mssql_audit_is_preflighted_at_exact_registry_location(
    monkeypatch: pytest.MonkeyPatch,
    database: str,
    schema: str,
) -> None:
    connector = object()
    events: list[tuple[str, object]] = []
    audit_storage = _PreflightAuditStorage(events)

    monkeypatch.setattr(ResolvedConnectorFactory, "create", lambda *_args, **_kwargs: connector)
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_xmin_state_storage",
        lambda **kwargs: SimpleNamespace(location=kwargs),
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_kafka_offset_state_storage",
        lambda **_kwargs: object(),
    )

    def create_audit_storage(**kwargs: object) -> _PreflightAuditStorage:
        events.append(("resolved", kwargs))
        return audit_storage

    monkeypatch.setattr(StateFactory, "create_mssql_load_audit_storage", create_audit_storage)
    monkeypatch.setattr(
        "dpone.runtime.bootstrap_state.build_sql_partition_checkpoint_store",
        lambda **_kwargs: None,
    )

    bindings = RuntimeStateBootstrap().build_resolved(
        state_cfg={
            "type": "mssql",
            "atomicity": "target_atomic",
            "provisioning": "external",
            "table": {"name": "dpone_source_state"},
            "run_table": {"name": "dpone_run_state"},
            "receipt_table": {"name": "dpone_commit_receipt"},
            "audit_table": {"name": "dpone_load_audit"},
        },
        state_connection=_resolved_mssql_connection(database=database, schema=schema),
        load_config=_load_config(),
    )

    assert bindings.load_audit_storage is audit_storage
    assert bindings.xmin_state_storage.location["run_table"] == "dpone_run_state"
    assert bindings.xmin_state_storage.location["audit_table"] == "dpone_load_audit"
    assert events == [
        (
            "resolved",
            {
                "mssql_connector": connector,
                "state_table": "dpone_load_audit",
                "schema": schema,
                "database": database,
                "provisioning": "external",
            },
        ),
        ("preflight", audit_storage),
    ]


def test_hydrator_exposes_one_identity_service_bound_to_state_audit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_storage = object()
    state_bootstrap = _HydratorStateBootstrap(audit_storage)
    connections = SimpleNamespace(
        strict=True,
        source=None,
        sink=None,
        state=None,
        proxy=None,
        receipts=(),
    )
    monkeypatch.setattr(
        "dpone.runtime.bootstrap_hydrator.resolve_runtime_connections",
        lambda **_kwargs: connections,
    )

    bindings = DefaultRuntimeHydrator(
        state_bootstrap=state_bootstrap,
        endpoint_factory=_EndpointFactory(),
        connection_context_loader=_ContextLoader(),
    ).build(
        config={
            "name": "sample_metrics_metrics_value",
            "source": {"type": "postgres"},
            "sink": {"type": "mssql"},
            "state": {"type": "mssql"},
        },
        load_config=_load_config(),
    )

    assert isinstance(bindings.load_identity_service, LoadIdentityService)
    assert bindings.load_identity_service.audit_storage is audit_storage


def test_hydrator_accepts_target_atomic_connection_aliases_and_defers_identity_to_sql_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_connector = SimpleNamespace(host="dwh-write.internal", port=1433, user="target-alias")
    state_connector = SimpleNamespace(host="192.0.2.42", port=11433, user="state-alias")
    storage = _TransactionAwareStateStorage()
    bindings = _build_target_atomic_runtime(
        monkeypatch,
        storage=storage,
        state_connector=state_connector,
        target_connector=target_connector,
    )

    assert bindings.sink_obj.connector is target_connector
    assert storage.bound_transaction_connector is target_connector
    assert storage.bound_database_authority is not None


@pytest.mark.parametrize(
    ("state_connector", "target_connector"),
    [
        pytest.param(None, object(), id="missing-state-connector"),
        pytest.param(object(), None, id="missing-target-connector"),
    ],
)
def test_hydrator_rejects_missing_target_atomic_connectors(
    monkeypatch: pytest.MonkeyPatch,
    state_connector: object | None,
    target_connector: object | None,
) -> None:
    with pytest.raises(RuntimeConfigurationError, match="requires MSSQL state and target connectors"):
        _build_target_atomic_runtime(
            monkeypatch,
            storage=_TransactionAwareStateStorage(),
            state_connector=state_connector,
            target_connector=target_connector,
        )


def test_hydrator_rejects_target_atomic_storage_without_transaction_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeConfigurationError, match="transaction-aware MSSQL source-state storage"):
        _build_target_atomic_runtime(
            monkeypatch,
            storage=object(),
            state_connector=object(),
            target_connector=object(),
        )


def test_process_config_and_runner_inject_the_same_identity_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity_service = LoadIdentityService()
    config = ETLProcessConfig(name="sample_metrics_metrics_value", load_config=_load_config())
    config.apply_runtime_bindings(
        RuntimeBindings(
            source_obj=object(),
            sink_obj=object(),
            etl_logger=object(),
            load_identity_service=identity_service,
        )
    )
    captured: dict[str, object] = {}

    class _Processor:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr("dpone.runtime.etl.processor.ETLProcessor", _Processor)
    monkeypatch.setattr(
        "dpone.runtime.etl.backfill_orchestrator.execute_process_with_backfill",
        lambda *_args, **_kwargs: {
            "status": "success",
            "inserted_rows": 0,
            "updated_rows": 0,
            "final_rows": 0,
            "extracted_rows": 0,
            "duration_seconds": 0.0,
            "errors": [],
        },
    )
    process = SimpleNamespace(config=config, current_state=None)

    DefaultProcessRunner(route_capability_factory=_RouteFactory()).run(process)

    assert captured["load_identity_service"] is identity_service


def test_receipt_backed_commit_audit_failure_is_warning_not_false_failure() -> None:
    storage = _FailCommittedAuditStorage()
    warnings: list[str] = []
    service = LoadIdentityService(
        audit_storage=storage,
        on_receipt_backed_commit_audit_failure=lambda record: warnings.append(record.load_id),
    )
    started = service.start(_load_config(), process_name="sample_metrics_metrics_value")
    staged = service.mark_staged(started, extracted_rows=10)

    committed = service.mark_committed(
        staged,
        LoadResult(
            inserted_rows=1,
            updated_rows=0,
            total_rows=10,
            commit_receipt_id="receipt-1",
            commit_outcome=AtomicCommitOutcome.COMMITTED,
        ),
    )

    assert committed.status == "committed"
    assert committed.commit_receipt_id == "receipt-1"
    assert storage.events == ["started", "staged", "committed"]
    assert warnings == [committed.load_id]

    preserved = service.mark_failed(started, RuntimeError("post-commit hook failed"))

    assert preserved is committed
    assert storage.events == ["started", "staged", "committed"]


def test_non_receipted_commit_audit_failure_keeps_fail_closed_semantics() -> None:
    storage = _FailCommittedAuditStorage()
    service = LoadIdentityService(audit_storage=storage)
    started = service.start(_load_config())
    staged = service.mark_staged(started, extracted_rows=1)

    with pytest.raises(RuntimeError, match="audit unavailable"):
        service.mark_committed(
            staged,
            LoadResult(inserted_rows=1, updated_rows=0, total_rows=1),
        )


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_sample_metrics_source",
        target_conn_id="mssql_sample_metrics_target",
        source_schema="public",
        source_table="metrics_value",
        target_schema="sample_metrics",
        target_table="metrics_value",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["guid"],
    )


def _resolved_mssql_connection(*, database: str, schema: str) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(database=database, schema=schema),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor(
            connection_type="mssql",
            properties={"database": database, "schema": schema},
        ),
    )


class _PreflightAuditStorage:
    def __init__(self, events: list[tuple[str, object]]) -> None:
        self._events = events

    def create_load_table(self) -> None:
        self._events.append(("preflight", self))


class _HydratorStateBootstrap:
    def __init__(self, audit_storage: object) -> None:
        self.audit_storage = audit_storage

    def build_resolved(self, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            xmin_state_storage=None,
            kafka_offset_state_storage=None,
            partition_checkpoint_store=None,
            shared_bq_connector=None,
            shared_mssql_state_connector=None,
            mssql_state_location=None,
            load_audit_storage=self.audit_storage,
        )

    def build_run_state_storage(self, **_kwargs: object) -> None:
        return None


class _EndpointFactory:
    @staticmethod
    def build_sink_resolved(*_args: object, **_kwargs: object) -> object:
        return object()

    @staticmethod
    def build_source_resolved(*_args: object, **_kwargs: object) -> object:
        return object()


class _TransactionAwareStateStorage:
    def __init__(self) -> None:
        self.bound_transaction_connector: object | None = None
        self.bound_database_authority: object | None = None

    def bind_transaction_connector(self, connector: object) -> None:
        self.bound_transaction_connector = connector

    def bind_database_authority(self, verifier: object) -> None:
        self.bound_database_authority = verifier


class _DatabaseAuthorityVerifier:
    authority_sha256 = "a" * 64

    @staticmethod
    def verify_pins() -> None:
        return None

    @staticmethod
    def verify(*, target_connector: object, state_connector: object) -> None:
        del target_connector, state_connector


class _TargetAtomicStateBootstrap:
    def __init__(self, *, storage: object, state_connector: object | None) -> None:
        self._storage = storage
        self._state_connector = state_connector

    def build_resolved(self, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            xmin_state_storage=self._storage,
            kafka_offset_state_storage=None,
            partition_checkpoint_store=None,
            shared_bq_connector=None,
            shared_mssql_state_connector=self._state_connector,
            mssql_state_location=SimpleNamespace(atomicity="target_atomic"),
            load_audit_storage=None,
        )

    def build_run_state_storage(self, **_kwargs: object) -> None:
        return None


class _TargetAtomicEndpointFactory:
    def __init__(self, target_connector: object | None) -> None:
        self._target_connector = target_connector

    def build_sink_resolved(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(connector=self._target_connector)

    @staticmethod
    def build_source_resolved(*_args: object, **_kwargs: object) -> object:
        return object()


def _build_target_atomic_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    storage: object,
    state_connector: object | None,
    target_connector: object | None,
) -> RuntimeBindings:
    connections = SimpleNamespace(
        strict=True,
        source=None,
        sink=_resolved_mssql_connection(database="DWH_Dev", schema="sample_metrics"),
        state=_resolved_mssql_connection(database="DWH_Dev", schema="system"),
        proxy=None,
        receipts=(),
    )
    monkeypatch.setattr(
        "dpone.runtime.bootstrap_hydrator.resolve_runtime_connections",
        lambda **_kwargs: connections,
    )
    return DefaultRuntimeHydrator(
        state_bootstrap=_TargetAtomicStateBootstrap(
            storage=storage,
            state_connector=state_connector,
        ),
        endpoint_factory=_TargetAtomicEndpointFactory(target_connector),
        connection_context_loader=_ContextLoader(),
        mssql_database_authority_verifier_factory=lambda **_kwargs: _DatabaseAuthorityVerifier(),
    ).build(
        config={
            "name": "sample_metrics_metrics_value",
            "source": {"type": "postgres"},
            "sink": {"type": "mssql"},
            "state": {
                "type": "mssql",
                "atomicity": "target_atomic",
                "provisioning": "external",
            },
        },
        load_config=_load_config(),
    )


class _ContextLoader:
    @staticmethod
    def load() -> None:
        return None


class _RouteFactory:
    @staticmethod
    def build(**_kwargs: object) -> None:
        return None


@dataclass
class _FailCommittedAuditStorage:
    events: list[str] | None = None

    def __post_init__(self) -> None:
        self.events = []

    def record_load_started(self, _record: object) -> None:
        assert self.events is not None
        self.events.append("started")

    def record_load_staged(self, _record: object) -> None:
        assert self.events is not None
        self.events.append("staged")

    def record_load_committed(self, _record: object) -> None:
        assert self.events is not None
        self.events.append("committed")
        raise RuntimeError("audit unavailable")

    def record_load_failed(self, _record: object) -> None:
        assert self.events is not None
        self.events.append("failed")
