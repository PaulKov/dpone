"""Offline parent-fenced PostgreSQL→MSSQL transfer; live SQL remains UNVERIFIED."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.adapters.composition_mssql_gate_proofs import gate_proof
from dpone.adapters.composition_mssql_issuance import MssqlIssuedCredentials
from dpone.adapters.composition_mssql_transfer_outcome import (
    CompositionMssqlTransferOutcomeObserver,
    CompositionTransferObservation,
)
from dpone.app.composition_transfer_execution import (
    CompositionTransferExecutionDependencies,
    CompositionTransferExecutionRequest,
    CompositionTransferExecutionRoot,
)
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptProof, CompositionAttemptReceipt
from dpone.contracts.process_types import ProcessResult
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.services.composition_transfer_attempt import build_composition_transfer_attempt
from tests.composition_mssql_gate_helpers import SERVICE, occurrence
from tests.test_composition_activation_contract import digest

ISSUED_PASSWORD = "issued-one-time-secret"


def _manifest() -> dict[str, Any]:
    return {
        "name": "b_ordinary",
        "source": {
            "type": "postgres",
            "connection_ref": "pg-source",
            "table": {"database": "sales", "schema": "public", "name": "orders"},
        },
        "sink": {
            "type": "mssql",
            "connection_ref": "mssql-sink",
            "table": {"database": "warehouse", "schema": "dbo", "name": "orders"},
            "strategy": {"mode": "full_refresh"},
        },
        "state": {
            "type": "mssql",
            "atomicity": "target_atomic",
            "provisioning": "external",
            "connection_ref": "mssql-state",
        },
    }


def _target(*, database: str) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        CredentialsConfig(
            host="sql",
            database=database,
            schema="dbo",
            username="ambient-binding",
            password="ambient-binding-secret",
        ),
        {},
        ResolvedConnectionDescriptor("mssql", {}),
    )


@dataclass
class _Ledger:
    events: list[str] = field(default_factory=list)
    admitted: bool = False
    issued: bool = False
    closed: str | None = None
    quiescence: str | None = None
    source_reads: int = 0
    sink_connection: ResolvedBindingConnection | None = None
    fence: Any = None


class _Attempts:
    def __init__(self, ledger: _Ledger) -> None:
        self._ledger = ledger
        self.return_running_replay = False
        self.receipt: CompositionAttemptReceipt | None = None

    def admit_once(self, attempt) -> CompositionAttemptReceipt:
        self._ledger.events.append("admit_attempt")
        if self.return_running_replay or self._ledger.admitted:
            raise CompositionAdmissionError("attempt_replay")
        self._ledger.admitted = True
        self.receipt = CompositionAttemptReceipt(attempt, "RUNNING")
        return self.receipt

    def finalize(self, attempt, *, state: str, outcome_evidence_sha256: str) -> CompositionAttemptReceipt:
        self._ledger.events.append("finalize_attempt")
        self.receipt = CompositionAttemptReceipt(
            attempt, state, self._ledger.closed, self._ledger.quiescence, outcome_evidence_sha256
        )
        return self.receipt


class _Gate:
    def __init__(self, ledger: _Ledger) -> None:
        self._ledger = ledger
        self.sid = bytes.fromhex("61" * 16)
        self.issued: list[MssqlIssuedCredentials] = []

    def issue_once(self, attempt) -> MssqlIssuedCredentials:
        self._ledger.events.append("issue_login")
        if self._ledger.issued:
            raise CompositionAdmissionError("login_issuance_replay")
        self._ledger.issued = True
        credentials = MssqlIssuedCredentials("dpone_v3_" + attempt.attempt_sha256[7:], self.sid, ISSUED_PASSWORD)
        self.issued.append(credentials)
        return credentials

    def close(self, attempt):
        self._ledger.events.append("close_gate")
        proof = self._proof(attempt, "CLOSED_GATES")
        self._ledger.closed = proof.proof_sha256
        return proof

    def prove_quiescence(self, attempt):
        self._ledger.events.append("prove_quiescence")
        proof = self._proof(attempt, "QUIESCENCE")
        self._ledger.quiescence = proof.proof_sha256
        return proof

    def _proof(self, attempt, kind: str) -> CompositionAttemptProof:
        return gate_proof(
            attempt,
            service_id=SERVICE,
            sid=self.sid,
            kind=kind,
            evidence={
                "schema": "dpone.composition-mssql-gate-observation.v1",
                "attempt_sha256": attempt.attempt_sha256,
                "kind": kind,
                "login_sid": self.sid.hex(),
            },
        )


class _Hydrator:
    def __init__(self, ledger: _Ledger) -> None:
        self._ledger = ledger

    def build(self, *, config, load_config, sink_connection=None, state_connection=None, **_kwargs):
        del config, load_config, state_connection
        self._ledger.sink_connection = sink_connection
        return SimpleNamespace(
            source_obj=SimpleNamespace(extract=self._extract),
            sink_obj=SimpleNamespace(_strategy_map={}),
            etl_logger=object(),
            run_state_storage=None,
            xmin_handoff_state_storage=None,
            partition_checkpoint_store=None,
            load_identity_service=None,
            credential_resolution_receipts=(),
        )

    def _extract(self, *_args, **_kwargs) -> None:
        self._ledger.source_reads += 1


class _Runner:
    def __init__(self, ledger: _Ledger) -> None:
        self._ledger = ledger

    def run(self, process, *, sink_connection=None, composition_transaction_fence=None, **_kwargs) -> ProcessResult:
        self._ledger.fence = composition_transaction_fence
        if sink_connection is not None:
            self._ledger.sink_connection = sink_connection
        process.config.source_obj.extract()
        self._ledger.events.extend(
            [
                "target_fence_before_mutation",
                "target_dml",
                "target_fence_before_receipt",
                "receipt_insert",
            ]
        )
        return ProcessResult(
            status="success",
            inserted_rows=3,
            updated_rows=0,
            final_rows=3,
            extracted_rows=3,
            duration_seconds=0.0,
            errors=[],
        )


class _Outcome:
    def __init__(self, ledger: _Ledger, gate: _Gate) -> None:
        self._ledger = ledger
        self._gate = gate

    def observe(self, attempt) -> CompositionAttemptProof:
        self._ledger.events.append("observe_outcome")
        return CompositionAttemptProof(
            "OUTCOME",
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            gate_proof(
                attempt,
                service_id=SERVICE,
                sid=self._gate.sid,
                kind="CLOSED_GATES",
                evidence={"kind": "OUTCOME"},
            ).guard_epochs_sha256,
            (
                gate_proof(
                    attempt,
                    service_id=SERVICE,
                    sid=self._gate.sid,
                    kind="CLOSED_GATES",
                    evidence={"kind": "authority"},
                ).authorities[0],
            ),
            digest("transfer-outcome"),
            "SUCCEEDED",
        )


@dataclass
class TransferRuntime:
    root: CompositionTransferExecutionRoot
    request: CompositionTransferExecutionRequest
    attempts: _Attempts
    gate: _Gate
    ledger: _Ledger

    @property
    def events(self) -> list[str]:
        return self.ledger.events

    @property
    def source_reads(self) -> int:
        return self.ledger.source_reads

    @property
    def sink_credentials(self) -> Any:
        connection = self.ledger.sink_connection
        if connection is None:
            return SimpleNamespace(resolver=None)
        return SimpleNamespace(resolver=connection.safe_metadata.get("resolver"))

    def execute(self):
        return self.root.execute(self.request)


@pytest.fixture
def runtime() -> TransferRuntime:
    active = occurrence()
    workload = active.request.workloads[1]
    ledger = _Ledger()
    attempts = _Attempts(ledger)
    gate = _Gate(ledger)
    dependencies = CompositionTransferExecutionDependencies(
        read_active=lambda: active,
        attempts=attempts,
        gate=gate,
        hydrator=_Hydrator(ledger),
        runner=_Runner(ledger),
        outcome_observer=_Outcome(ledger, gate),
        sink_target=_target(database="warehouse"),
        state_target=_target(database="warehouse_state"),
        expected_service_id=SERVICE,
        composition_fence=object(),
        operation_registrar=lambda *_args, **_kwargs: None,
    )
    request = CompositionTransferExecutionRequest(
        manifest=_manifest(),
        plan_sha256=digest("ordinary plan"),
        run_identity=AirflowRunIdentity(
            active.request.release_id,
            active.request.deployment_id,
            AirflowArtifactIdentity(workload.workload_id, workload.pack_sha256),
        ),
        airflow_attempt=AirflowAttemptCorrelation(
            dag_id="b_ordinary",
            task_id="b_ordinary__dpone_runtime",
            run_id="scheduled__2026-09-12T00:00:00+00:00",
            try_number=1,
            map_index=-1,
        ),
        raw_config=_manifest(),
        load_config=SimpleNamespace(options={}),
    )
    return TransferRuntime(CompositionTransferExecutionRoot(dependencies), request, attempts, gate, ledger)


def test_transfer_root_issues_sink_and_fences_actual_transaction(runtime):
    result = runtime.execute()
    assert result.rows_written == 3
    assert runtime.sink_credentials.resolver == "composition-issued-login"
    assert runtime.ledger.fence is not None
    assert runtime.events.index("target_fence_before_mutation") < runtime.events.index("target_dml")
    assert runtime.events.index("target_fence_before_receipt") < runtime.events.index("receipt_insert")


def test_running_replay_never_reads_postgres(runtime):
    runtime.attempts.return_running_replay = True
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        runtime.execute()
    assert runtime.source_reads == 0


def test_issued_overlay_never_copies_ambient_secrets(runtime):
    runtime.execute()
    connection = runtime.ledger.sink_connection
    assert connection is not None
    assert connection.credentials.username == runtime.gate.issued[0].login_name
    assert connection.credentials.password == ISSUED_PASSWORD
    assert "ambient-binding-secret" not in str(connection.safe_metadata)
    assert ISSUED_PASSWORD not in runtime.events


@pytest.mark.parametrize(
    "observed,state",
    [
        (CompositionTransferObservation(True, True, True, False, False), "SUCCEEDED"),
        (CompositionTransferObservation(False, False, False, True, True), "FAILED"),
        (CompositionTransferObservation(True, False, True, False, False), "COMMIT_UNKNOWN"),
        (CompositionTransferObservation(False, False, False, True, False), "COMMIT_UNKNOWN"),
    ],
)
def test_transfer_outcome_classifies_only_from_independent_evidence(runtime, observed, state):
    writes: list[tuple[bool, str]] = []

    def persist(_ledger, proof, _document, *, create: bool = True) -> None:
        writes.append((create, proof.outcome_state))

    observer = CompositionMssqlTransferOutcomeObserver(
        transaction=_transaction,
        expected_service_id=SERVICE,
        principal_id="mssql-sid:" + "61" * 16,
        observe=lambda _attempt: observed,
        persist=persist,
    )
    proof = observer.observe(_attempt(runtime))
    assert proof.outcome_state == state
    assert writes == [(True, state), (False, state)]


def test_uncertain_observation_is_commit_unknown(runtime):
    observer = CompositionMssqlTransferOutcomeObserver(
        transaction=_transaction,
        expected_service_id=SERVICE,
        principal_id="mssql-sid:" + "61" * 16,
        observe=lambda _attempt: (_ for _ in ()).throw(RuntimeError("driver")),
        persist=lambda *_args, **_kwargs: None,
    )
    proof = observer.observe(_attempt(runtime))
    assert proof.outcome_state == "COMMIT_UNKNOWN"


def test_issued_overlay_rejects_ambient_resolver():
    from dpone.runtime.bootstrap_hydrator import _require_issued_overlay
    from dpone.runtime.errors import RuntimeConfigurationError

    with pytest.raises(RuntimeConfigurationError, match="composition_issued_login_overlay"):
        _require_issued_overlay(_target(database="warehouse"))


def test_admission_replay_default_does_not_require_fence():
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
    from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION, MssqlTransactionAdmissionService
    from dpone.runtime.sinks.load_result import AtomicCommitOutcome
    from tests.test_mssql_generic_transaction_governance import _config, _operation, _receipt

    admission = MssqlTransactionAdmission(replay_receipt=_receipt(_operation()))
    load_config = replace(_config(), options={ADMISSION_OPTION: admission})
    result = MssqlTransactionAdmissionService().replay_result(load_config)
    assert result is not None
    assert result.commit_outcome == AtomicCommitOutcome.REPLAY_SUPPRESSED


def test_admission_replay_applies_parent_fence():
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
    from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION, MssqlTransactionAdmissionService
    from tests.test_mssql_generic_transaction_governance import _config, _operation, _receipt

    receipt = _receipt(_operation())
    events: list[str] = []

    class _Fence:
        def require_current(self, connector, receipt=None, **_kwargs):
            del connector
            events.append("target_fence_before_receipt")
            assert receipt is not None
            return 7

    connector = SimpleNamespace(begin=lambda: events.append("begin"), rollback=lambda: events.append("rollback"))
    service = MssqlTransactionAdmissionService(composition_fence=_Fence(), fence_connector=connector)
    load_config = replace(_config(), options={ADMISSION_OPTION: MssqlTransactionAdmission(replay_receipt=receipt)})
    service.replay_result(load_config)
    assert events == ["begin", "target_fence_before_receipt", "rollback"]


def test_missing_operation_registrar_is_rejected(runtime):
    runtime.root._deps = replace(runtime.root._deps, operation_registrar=None)
    with pytest.raises(CompositionAdmissionError, match="operation_registrar"):
        runtime.execute()
    assert runtime.source_reads == 0


def test_missing_composition_fence_is_rejected_when_registrar_returns_none(runtime):
    from dpone.app.composition_transfer_execution import composition_transfer_binding_registrar
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
    from tests.test_mssql_composition_transaction_fence import binding

    original = binding()
    register = composition_transfer_binding_registrar(
        lambda *_args, **_kwargs: None,
        attempt=original.attempt,
        write=original.write,
    )
    assert register(MssqlTransactionAdmission(operation=original.operation), original.mutation_plan_sha256) is None
    runtime.root._deps = replace(
        runtime.root._deps,
        composition_fence=None,
        operation_registrar=lambda *_args, **_kwargs: None,
    )
    service = runtime.root._admission(runtime.request, _attempt(runtime), SimpleNamespace(_strategy_map={}))
    with pytest.raises(CompositionAdmissionError, match="composition_fence"):
        service._operation_registrar(
            MssqlTransactionAdmission(operation=original.operation),
            original.mutation_plan_sha256,
        )


def test_strict_issued_overlay_is_used_to_build_sink(monkeypatch):
    from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator

    overlay = _issued_overlay("warehouse")
    endpoints = _RecordingEndpoints()
    _install_hydrator_connections(monkeypatch, strict=True)
    bindings = DefaultRuntimeHydrator(
        state_bootstrap=_IdleStateBootstrap(),
        endpoint_factory=endpoints,
        connection_context_loader=_IdleContextLoader(),
    ).build(
        config=_overlay_config(),
        load_config=_overlay_load_config(),
        sink_connection=overlay,
        state_connection=_issued_overlay("warehouse_state"),
    )
    assert endpoints.sink_connection is overlay
    assert bindings.sink_obj == "sink"


def test_non_strict_issued_overlay_is_rejected(monkeypatch):
    from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
    from dpone.runtime.errors import RuntimeConfigurationError

    endpoints = _RecordingEndpoints()
    _install_hydrator_connections(monkeypatch, strict=False)
    with pytest.raises(RuntimeConfigurationError, match="composition_issued_login_overlay_required"):
        DefaultRuntimeHydrator(
            state_bootstrap=_IdleStateBootstrap(),
            endpoint_factory=endpoints,
            connection_context_loader=_IdleContextLoader(),
        ).build(
            config=_overlay_config(),
            load_config=_overlay_load_config(),
            sink_connection=_issued_overlay("warehouse"),
            state_connection=_issued_overlay("warehouse_state"),
        )
    assert endpoints.sink_connection is None
    assert endpoints.legacy_sink is False


def test_runner_rejects_issued_overlays_when_hydrator_cannot_apply_them(monkeypatch):
    from dpone.runtime.bootstrap_runner import _hydrate_invocation
    from dpone.runtime.errors import RuntimeConfigurationError

    monkeypatch.setattr("dpone.ports.runtime_hydrator.ensure_runtime_hydrator", lambda: object())
    called: list[str] = []
    process = SimpleNamespace(
        config=SimpleNamespace(source_obj=None, ensure_runtime_bindings=lambda: called.append("overlay-free"))
    )
    with pytest.raises(RuntimeConfigurationError, match="composition_issued_login_overlay_required"):
        _hydrate_invocation(
            process,
            sink_connection=_issued_overlay("warehouse"),
            state_connection=_issued_overlay("warehouse_state"),
        )
    assert called == []


def test_admission_registrar_runs_after_preplan_with_admission_and_digest():
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
    from dpone.contracts.run_context import RunContext
    from dpone.runtime.etl.mssql_schema_preplan import MSSQL_SCHEMA_PREPLAN_OPTION
    from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION, MssqlTransactionAdmissionService
    from tests.test_mssql_generic_transaction_governance import (
        _admission_sink,
        _AdmissionState,
        _certified_source,
        _config,
    )

    calls: list[tuple[Any, bytes]] = []

    def registrar(admission: Any, digest: bytes) -> None:
        calls.append((admission, digest))

    captured: list[Any] = []
    prepared = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: _AdmissionState(captured),
        operation_registrar=registrar,
    ).prepare(
        _config(),
        source=_certified_source(),
        sink=_admission_sink(
            SimpleNamespace(
                atomicity="target_atomic",
                provisioning="external",
                require_database_authority_binding=lambda: None,
            )
        ),
        run_context=RunContext("run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )
    assert len(calls) == 1
    admission, digest = calls[0]
    assert isinstance(admission, MssqlTransactionAdmission)
    assert admission is prepared.options[ADMISSION_OPTION]
    preplan = prepared.options[MSSQL_SCHEMA_PREPLAN_OPTION]
    assert digest == preplan.target_mutation_plan.digest
    assert isinstance(digest, bytes) and len(digest) == 32
    assert captured, "preplan must run after admission"


def test_composition_transfer_binding_adapter_wraps_binder_as_fence():
    from dpone.adapters.composition_mssql_transaction_fence import MssqlCompositionTransactionFence
    from dpone.app.composition_transfer_execution import composition_transfer_binding_registrar
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
    from tests.test_mssql_composition_transaction_fence import binding

    original = binding()
    calls: list[tuple[Any, Any, Any, bytes]] = []

    def bind(attempt, operation, write, mutation_plan_sha256):
        calls.append((attempt, operation, write, mutation_plan_sha256))
        return original

    register = composition_transfer_binding_registrar(bind, attempt=original.attempt, write=original.write)
    admission = MssqlTransactionAdmission(operation=original.operation)
    fence = register(admission, original.mutation_plan_sha256)
    assert isinstance(fence, MssqlCompositionTransactionFence)
    assert fence.binding is original
    assert calls == [(original.attempt, original.operation, original.write, original.mutation_plan_sha256)]


@contextmanager
def _transaction():
    yield SimpleNamespace()


def _attempt(runtime: TransferRuntime):
    return build_composition_transfer_attempt(
        runtime.root._deps.read_active(),
        manifest=runtime.request.manifest,
        plan_sha256=runtime.request.plan_sha256,
        run_identity=runtime.request.run_identity,
        airflow_attempt=runtime.request.airflow_attempt,
    )


def _issued_overlay(database: str) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        CredentialsConfig(host="sql", database=database, schema="dbo", username="issued", password=ISSUED_PASSWORD),
        {"resolver": "composition-issued-login"},
        ResolvedConnectionDescriptor("mssql", {}),
    )


def _overlay_config() -> dict[str, Any]:
    return {
        "source": {"type": "postgres", "connection_ref": "pg-source"},
        "sink": {"type": "mssql", "connection_ref": "mssql-sink"},
        "state": {"type": "disabled"},
    }


def _overlay_load_config():
    from dpone.config.load_config import LoadConfig
    from dpone.config.load_strategy import LoadStrategy

    return LoadConfig(
        source_conn_id="pg-source",
        target_conn_id="mssql-sink",
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )


def _install_hydrator_connections(monkeypatch, *, strict: bool) -> None:
    from dpone.runtime.credentials.authority import RuntimeResolvedConnections

    monkeypatch.setattr(
        "dpone.runtime.bootstrap_hydrator.resolve_runtime_connections",
        lambda **_kwargs: RuntimeResolvedConnections(
            strict=strict,
            source=_target(database="sales"),
            sink=_target(database="warehouse"),
            state=_target(database="warehouse_state"),
        ),
    )


class _IdleContextLoader:
    @staticmethod
    def load() -> Any:
        return SimpleNamespace(environment="test")


class _IdleStateBootstrap:
    @staticmethod
    def _bindings() -> Any:
        from dpone.runtime.bootstrap_state_models import RuntimeStateBindings

        return RuntimeStateBindings(state_type="disabled", proxy_config={}, xmin_state_storage=None)

    def build_resolved(self, **_kwargs: Any) -> Any:
        return self._bindings()

    def build(self, **_kwargs: Any) -> Any:
        return self._bindings()

    def build_run_state_storage(self, **_kwargs: Any) -> None:
        return None


class _RecordingEndpoints:
    def __init__(self) -> None:
        self.sink_connection: ResolvedBindingConnection | None = None
        self.legacy_sink = False

    def build_sink_resolved(self, _cfg: Any, connection: ResolvedBindingConnection, *_args: Any, **_kwargs: Any) -> str:
        self.sink_connection = connection
        return "sink"

    def build_source_resolved(self, *_args: Any, **_kwargs: Any) -> str:
        return "source"

    def build_sink(self, *_args: Any, **_kwargs: Any) -> str:
        self.legacy_sink = True
        return "legacy-sink"

    def build_source(self, *_args: Any, **_kwargs: Any) -> str:
        return "legacy-source"
