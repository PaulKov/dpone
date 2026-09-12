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
        del composition_transaction_fence
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
