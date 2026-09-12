"""Composition root that executes one ordinary PostgreSQL→MSSQL full_refresh.

This is the only place where the protected parent attempt store, login gate,
issued sink/state overlays, generic admission, parent fence and independent
OUTCOME observer are wired together. Every collaborator is injected, so a
deployment can substitute a real protected adapter without changing policy here.

Ordering is a security property, not an implementation detail:

1. reopen the ACTIVE parent occurrence and derive the exact attempt identity;
2. reserve the attempt atomically, then issue one-time MSSQL login material;
3. overlay issued sink/state credentials and run parent-fenced ``full_refresh``;
4. close the login gate, prove quiescence, reopen actual business evidence and
   persist an immutable OUTCOME proof before the attempt is finalized.

A generic unprotected ``dpone run`` is never constructed for an authenticated
v3 transfer, and no process return value alone can terminalize a parent attempt.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
)
from dpone.contracts.composition_persistence import CompositionAttemptIdentity
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.services.composition_transfer_attempt import build_composition_transfer_attempt
from dpone.services.composition_worker import CompositionWorker

ISSUED_LOGIN_RESOLVER = "composition-issued-login"


@dataclass(frozen=True, slots=True)
class CompositionTransferExecutionRequest:
    """One verified ordinary transfer invocation bound to its parent identity."""

    manifest: Mapping[str, Any]
    plan_sha256: str
    run_identity: AirflowRunIdentity
    airflow_attempt: AirflowAttemptCorrelation
    raw_config: Mapping[str, Any]
    load_config: Any
    selector: str | None = None


@dataclass(frozen=True, slots=True)
class CompositionTransferResult:
    """Executor metrics; terminal proof lives on the worker receipt."""

    rows_written: int


@dataclass(frozen=True, slots=True)
class CompositionTransferExecutionDependencies:
    """Injected protected capabilities; this root owns no ambient client."""

    read_active: Callable[[], CompositionActivationOccurrence]
    attempts: Any
    gate: Any
    hydrator: Any
    runner: Any
    outcome_observer: Any
    sink_target: ResolvedBindingConnection
    state_target: ResolvedBindingConnection
    expected_service_id: str
    admission_service: Any | None = None
    composition_fence: Any | None = None
    operation_registrar: Callable[..., Any] | None = None


class CompositionTransferExecutionRoot:
    """Execute one ordinary PostgreSQL→MSSQL transfer as a supervised attempt."""

    def __init__(self, dependencies: CompositionTransferExecutionDependencies) -> None:
        self._deps = dependencies

    def execute(self, request: CompositionTransferExecutionRequest) -> CompositionTransferResult:
        """Run the complete admission, fenced load and seal sequence once."""

        occurrence = self._deps.read_active()
        occurrence.require_state("ACTIVE")
        attempt = build_composition_transfer_attempt(
            occurrence,
            manifest=request.manifest,
            plan_sha256=request.plan_sha256,
            run_identity=request.run_identity,
            airflow_attempt=request.airflow_attempt,
        )
        worker: CompositionWorker[Any] = CompositionWorker(
            attempts=self._deps.attempts,
            gate=self._deps.gate,
            outcome_observer=self._deps.outcome_observer,
        )
        result = worker.run(attempt, execute=lambda credentials: self._transfer(request, attempt, credentials))
        return result.value

    def _transfer(
        self,
        request: CompositionTransferExecutionRequest,
        attempt: CompositionAttemptIdentity,
        credentials: Any,
    ) -> CompositionTransferResult:
        """Hydrate issued overlays, register the generic operation, then fence DML."""

        sink = issued_transfer_connection(self._deps.sink_target, attempt, credentials)
        state = issued_transfer_connection(self._deps.state_target, attempt, credentials)
        bindings = self._deps.hydrator.build(
            config=request.raw_config,
            load_config=request.load_config,
            sink_connection=sink,
            state_connection=state,
            mssql_transaction_admission_service=self._deps.admission_service,
            composition_transaction_fence=self._deps.composition_fence,
        )
        process = _invocation_process(request, bindings)
        result = self._deps.runner.run(
            process,
            sink_connection=sink,
            state_connection=state,
            mssql_transaction_admission_service=self._admission(attempt, bindings.sink_obj),
            composition_transaction_fence=self._deps.composition_fence,
        )
        return CompositionTransferResult(rows_written=int(result.inserted_rows))

    def _admission(self, attempt: CompositionAttemptIdentity, sink: Any) -> Any:
        if self._deps.admission_service is not None:
            return self._deps.admission_service
        from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService

        return MssqlTransactionAdmissionService(
            operation_registrar=self._registrar(attempt, sink),
            composition_fence=self._deps.composition_fence,
            fence_connector=getattr(sink, "connector", None),
        )

    def _registrar(self, attempt: CompositionAttemptIdentity, sink: Any) -> Callable[..., None] | None:
        bind = self._deps.operation_registrar
        if bind is None:
            return None

        def register(admission: Any, mutation_plan_sha256: bytes) -> None:
            fence = bind(attempt, admission, mutation_plan_sha256)
            if fence is not None:
                bind_composition_fence(sink, fence)

        return register


def issued_transfer_connection(
    target: ResolvedBindingConnection,
    attempt: CompositionAttemptIdentity,
    credentials: Any,
) -> ResolvedBindingConnection:
    """Copy only endpoint/TLS fields; issued login is the sole sink/state secret."""

    attempt.__post_init__()
    if (
        credentials.login_name != "dpone_v3_" + attempt.attempt_sha256[7:]
        or type(credentials.login_sid) is not bytes
        or len(credentials.login_sid) != 16
        or not isinstance(credentials.password, str)
        or not 1 <= len(credentials.password) <= 128
    ):
        raise CompositionAdmissionError("worker_issued_identity")
    original = target.credentials
    return ResolvedBindingConnection(
        CredentialsConfig(
            host=original.host,
            port=original.port,
            database=original.database,
            schema=original.schema,
            driver=original.driver,
            encrypt=original.encrypt,
            trust_server_certificate=original.trust_server_certificate,
            username=credentials.login_name,
            password=credentials.password,
        ),
        {"resolver": ISSUED_LOGIN_RESOLVER},
        target.descriptor,
    )


def bind_composition_fence(sink: Any, fence: Any) -> None:
    """Attach the parent fence to every generic MSSQL strategy on this sink."""

    from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import (
        MssqlGenericTransactionFinalizer,
    )

    def factory(strategy: Any, state_storage: Any, **kwargs: Any) -> Any:
        return MssqlGenericTransactionFinalizer(strategy, state_storage, composition_fence=fence, **kwargs)

    for strategy in getattr(sink, "_strategy_map", {}).values():
        strategy.transaction_finalizer_factory = factory


def _invocation_process(request: CompositionTransferExecutionRequest, bindings: Any) -> Any:
    """Bind already-hydrated runtime objects without ambient hydrator fallback."""

    config = SimpleNamespace(
        name=str(request.manifest.get("name") or "composition-transfer"),
        load_config=request.load_config,
        raw_config=dict(request.raw_config),
        source_obj=bindings.source_obj,
        sink_obj=bindings.sink_obj,
        etl_logger=bindings.etl_logger,
        run_state_storage=bindings.run_state_storage,
        xmin_handoff_state_storage=getattr(bindings, "xmin_handoff_state_storage", None),
        partition_checkpoint_store=bindings.partition_checkpoint_store,
        load_identity_service=getattr(bindings, "load_identity_service", None),
        credential_resolution_receipts=tuple(getattr(bindings, "credential_resolution_receipts", ()) or ()),
        ensure_runtime_bindings=lambda: None,
    )
    return SimpleNamespace(config=config, current_state=None)


__all__ = [
    "ISSUED_LOGIN_RESOLVER",
    "CompositionTransferExecutionDependencies",
    "CompositionTransferExecutionRequest",
    "CompositionTransferExecutionRoot",
    "CompositionTransferResult",
    "bind_composition_fence",
    "issued_transfer_connection",
]
