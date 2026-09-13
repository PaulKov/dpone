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
from dataclasses import asdict, dataclass
from types import SimpleNamespace
from typing import Any

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
)
from dpone.contracts.composition_persistence import CompositionAttemptIdentity, encode_attempt_identity
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.contracts.strict_json import canonical_json_bytes
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
    execution_date: Any | None = None


@dataclass(frozen=True, slots=True)
class CompositionTransferResult:
    """Executor metrics; terminal proof lives on the worker receipt."""

    rows_written: int


@dataclass(frozen=True, slots=True)
class CompositionTransferExecutionDependencies:
    """Injected protected capabilities; this root owns no ambient client.

    ``operation_registrar`` is ``MssqlCompositionTransactionBindings.bind`` (or a
    compatible 4-arg callback). The admission seam is
    ``Callable[[MssqlTransactionAdmission, bytes], MssqlCompositionTransactionFence | None]``
    after :func:`composition_transfer_binding_registrar` adapts the binder.
    Missing ``operation_registrar`` is ``CompositionAdmissionError``. A missing
    fence is rejected unless that adapted callback returns a fence bound before DML.
    """

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
    capture_lifecycle: Callable[[CompositionAttemptIdentity], Any] | None = None
    preplan_factory: Callable[..., Any] | None = None


class CompositionTransferExecutionRoot:
    """Execute one ordinary PostgreSQL→MSSQL transfer as a supervised attempt."""

    def __init__(self, dependencies: CompositionTransferExecutionDependencies) -> None:
        self._deps = dependencies

    def can_execute_attempt(self) -> bool:
        """Refuse login when independent receipt/row/content observation is absent."""

        prove = getattr(self._deps.outcome_observer, "can_prove_outcome", None)
        return callable(prove) and bool(prove())

    def execute(self, request: CompositionTransferExecutionRequest) -> CompositionTransferResult:
        """Run the complete admission, fenced load and seal sequence once."""

        if self._deps.operation_registrar is None:
            raise CompositionAdmissionError("operation_registrar")
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

        if self._deps.operation_registrar is None:
            raise CompositionAdmissionError("operation_registrar")
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
        from dpone.runtime.bootstrap_hydrator import close_runtime_resources

        try:
            process = _invocation_process(request, bindings)
            result = self._deps.runner.run(
                process,
                context=_scheduler_context(request),
                dag_id=request.airflow_attempt.dag_id,
                execution_date=request.execution_date,
                sink_connection=sink,
                state_connection=state,
                mssql_transaction_admission_service=self._admission(request, attempt, bindings.sink_obj),
                composition_transaction_fence=self._deps.composition_fence,
                source_extraction_lifecycle_service=(
                    self._deps.capture_lifecycle(attempt) if self._deps.capture_lifecycle else None
                ),
            )
            return CompositionTransferResult(rows_written=int(result.inserted_rows))

        finally:
            close_runtime_resources(bindings)

    def _admission(
        self,
        request: CompositionTransferExecutionRequest,
        attempt: CompositionAttemptIdentity,
        sink: Any,
    ) -> Any:
        if self._deps.operation_registrar is None:
            raise CompositionAdmissionError("operation_registrar")
        from dpone.contracts.dbt_relation_writes import transfer_relation_write
        from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService

        write = transfer_relation_write(
            project_path=attempt.constituent_id,
            workflow_id=request.airflow_attempt.dag_id,
            workload_id=attempt.workload_id,
            manifest=request.manifest,
        )
        registrar = self._registrar(attempt, sink, write)
        service = self._deps.admission_service
        if service is None:
            return MssqlTransactionAdmissionService(
                operation_registrar=registrar,
                preplan_verifier=(
                    self._deps.preplan_factory(attempt, write).verify_preplan if self._deps.preplan_factory else None
                ),
                composition_fence=self._deps.composition_fence,
                fence_connector=getattr(sink, "connector", None),
            )
        if self._deps.preplan_factory is not None and getattr(service, "_preplan_verifier", None) is None:
            raise CompositionAdmissionError("transfer_preplan_configuration")
        if getattr(service, "_operation_registrar", None) is None:
            raise CompositionAdmissionError("operation_registrar")
        return service

    def _registrar(self, attempt: CompositionAttemptIdentity, sink: Any, write: Any) -> Callable[..., None]:
        bind = self._deps.operation_registrar
        if bind is None:
            raise CompositionAdmissionError("operation_registrar")
        register = composition_transfer_binding_registrar(bind, attempt=attempt, write=write)

        def attach(admission: Any, mutation_plan_sha256: bytes, *, preplan_reference: Any = None) -> None:
            fence = register(admission, mutation_plan_sha256, preplan_reference=preplan_reference)
            if fence is None:
                fence = self._deps.composition_fence
            if fence is None:
                raise CompositionAdmissionError("composition_fence")
            bind_composition_fence(sink, fence)

        return attach


def composition_transfer_binding_registrar(
    bind: Callable[..., Any],
    *,
    attempt: CompositionAttemptIdentity,
    write: Any,
) -> Callable[..., Any]:
    """Adapt ``MssqlCompositionTransactionBindings.bind`` to the admission registrar.

    The returned callback is
    ``Callable[[MssqlTransactionAdmission, bytes], MssqlCompositionTransactionFence | None]``.
    The legacy four-argument binder remains supported. Trusted admission adds
    a keyword-only retained preplan reference whose whole digest is pinned in
    the v2 binding before the resulting composition fence can be attached.
    """

    def register(admission: Any, mutation_plan_sha256: bytes, *, preplan_reference: Any = None) -> Any:
        from dpone.adapters.composition_mssql_transaction_fence import MssqlCompositionTransactionFence
        from dpone.contracts.composition_mssql_binding import (
            CompositionMssqlOperationBinding,
            stable_operation_document,
        )
        from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission

        if type(admission) is not MssqlTransactionAdmission or admission.operation is None:
            raise CompositionAdmissionError("transfer_operation")
        extra = {}
        if preplan_reference is not None:
            from dpone.runtime.composition_transfer_preplan_store import RetainedTransferPreplanReference

            if (
                type(preplan_reference) is not RetainedTransferPreplanReference
                or preplan_reference.mutation_plan_sha256 != mutation_plan_sha256
            ):
                raise CompositionAdmissionError("transfer_preplan_original")
            original = preplan_reference.body
            if (
                canonical_json_bytes(original["attempt_original"]) != encode_attempt_identity(attempt)
                or canonical_json_bytes(original["operation_original"])
                != stable_operation_document(admission.operation)
                or original["write"] != asdict(write)
            ):
                raise CompositionAdmissionError("transfer_preplan_original")
            extra["preplan_document_sha256"] = "sha256:" + preplan_reference.document_sha256.hex()
        result = bind(attempt, admission.operation, write, mutation_plan_sha256, **extra)
        if result is None:
            return None
        if isinstance(result, MssqlCompositionTransactionFence):
            return result
        if type(result) is CompositionMssqlOperationBinding:
            return MssqlCompositionTransactionFence(result)
        raise CompositionAdmissionError("composition_fence")

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


def _scheduler_context(request: CompositionTransferExecutionRequest) -> Any:
    """Preserve scheduler invocation across retries and distinguish mapped tasks."""
    from dpone.app.composition_transfer_invocation import transfer_scheduler_context

    scheduler = request.airflow_attempt
    return transfer_scheduler_context(
        run_id=scheduler.run_id,
        workflow_id=scheduler.dag_id,
        task_id=scheduler.task_id,
        map_index=scheduler.map_index,
        run_identity=request.run_identity,
        airflow_attempt=scheduler,
    )


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
    "composition_transfer_binding_registrar",
    "issued_transfer_connection",
]
