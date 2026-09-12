"""Offline MSSQL→ClickHouse parent root; live Docker/Linux/SQL remains UNVERIFIED."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from hashlib import sha256
from typing import Any

import pytest

from dpone.adapters.composition_clickhouse_principal import IssuedClickHouseCredentials
from dpone.adapters.composition_clickhouse_transport import (
    ClickHouseDispatchObservation,
    ClickHouseDispatchTransportError,
)
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import (
    ClickHouseDispatchColumn,
    CreateGenerationDispatch,
    ExchangeSnapshotDispatch,
    InsertGenerationDispatch,
)
from dpone.contracts.composition_persistence import (
    CompositionAttemptProof,
    CompositionAttemptReceipt,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
from dpone.runtime.composition_snapshot import ClickHouseAtomicSnapshotPublisher
from tests.composition_snapshot_helpers import (
    SERVICE,
    Authority,
    Catalog,
    MemoryStore,
    digest,
    intent,
    observation,
    occurrence,
)

COLUMNS = (
    ClickHouseDispatchColumn("id", "Int32"),
    ClickHouseDispatchColumn("name", "Nullable(String)"),
)
USER_ID = "10000000-0000-4000-8000-000000000021"
AUTHORITY = (CompositionProofAuthority("clickhouse", SERVICE, "clickhouse-user:" + USER_ID),)


def _manifest() -> dict[str, Any]:
    return {
        "name": "a_native",
        "source": {
            "type": "mssql",
            "connection_ref": "mssql-source",
            "table": {"database": "sales", "schema": "dbo", "name": "orders"},
        },
        "sink": {
            "type": "clickhouse",
            "connection_ref": "ch-sink",
            "table": {"database": "analytics", "schema": "default", "name": "orders"},
            "mode": "replace",
            "strategy": {"mode": "full_refresh", "max_source_bytes": 1_000_000},
        },
        "runtime": {},
        "quality": {},
        "gitops": {},
    }


def _proof(attempt, kind: str, state: str | None = None) -> CompositionAttemptProof:
    return CompositionAttemptProof(
        kind,
        attempt.attempt_sha256,
        attempt.activation_request_sha256,
        composition_attempt_epoch_subject(attempt),
        AUTHORITY,
        digest(kind + attempt.attempt_sha256 + (state or "")),
        state,
    )


@dataclass
class _Tables:
    target: list[tuple[object, ...]] = field(default_factory=list)
    source: list[tuple[object, ...]] = field(default_factory=list)
    generation: list[tuple[object, ...]] | None = None

    def seed_target(self, rows: list[tuple[object, ...]]) -> None:
        self.target = list(rows)

    def seed_source(self, rows: list[tuple[object, ...]]) -> None:
        self.source = list(rows)

    def exchange(self) -> None:
        self.target, self.generation = list(self.generation or []), self.target


@dataclass
class _Ledger:
    events: list[str] = field(default_factory=list)
    enrollment_phases: list[str] = field(default_factory=list)
    dispatches: list[tuple[Any, bytes]] = field(default_factory=list)
    admitted: bool = False
    issued: bool = False
    closed: str | None = None
    quiescence: str | None = None
    fault: str | None = None
    enrollment_document: bytes = b'{"schema":"dpone.composition-clickhouse-supervisor-enrollment.v1"}'


class _Attempts:
    def __init__(self, ledger: _Ledger) -> None:
        self._ledger = ledger
        self.receipt: CompositionAttemptReceipt | None = None

    def admit_once(self, attempt) -> CompositionAttemptReceipt:
        self._ledger.events.append("admit_attempt")
        if self._ledger.admitted:
            raise CompositionAdmissionError("attempt_replay")
        self._ledger.admitted = True
        self.receipt = CompositionAttemptReceipt(attempt, "RUNNING")
        return self.receipt

    def finalize(self, attempt, *, state: str, outcome_evidence_sha256: str) -> CompositionAttemptReceipt:
        self._ledger.events.append("attempt_terminal")
        self.receipt = CompositionAttemptReceipt(
            attempt, state, self._ledger.closed, self._ledger.quiescence, outcome_evidence_sha256
        )
        return self.receipt


class _Gate:
    def __init__(self, ledger: _Ledger, require_enrollment) -> None:
        self._ledger = ledger
        self._require_enrollment = require_enrollment
        self.issued: list[IssuedClickHouseCredentials] = []

    def issue_once(self, attempt) -> IssuedClickHouseCredentials:
        self._require_enrollment(attempt, "principal")
        self._ledger.events.append("issue_principal")
        if self._ledger.issued:
            raise CompositionAdmissionError("gate_issuance_replay")
        self._ledger.issued = True
        credentials = IssuedClickHouseCredentials("dpone_ch_" + "ab" * 32, USER_ID, "issued-clickhouse-secret")
        self.issued.append(credentials)
        return credentials

    def close(self, attempt) -> CompositionAttemptProof:
        self._ledger.events.append("close_dispatch")
        self._ledger.events.append("close_principal")
        proof = _proof(attempt, "CLOSED_GATES")
        self._ledger.closed = proof.proof_sha256
        return proof

    def prove_quiescence(self, attempt) -> CompositionAttemptProof:
        proof = _proof(attempt, "QUIESCENCE")
        self._ledger.quiescence = proof.proof_sha256
        return proof


class _Transport:
    def __init__(self, ledger: _Ledger, tables: _Tables) -> None:
        self._ledger = ledger
        self._tables = tables

    def execute(self, dispatch, *, payload: bytes = b"") -> ClickHouseDispatchObservation:
        self._ledger.dispatches.append((dispatch, payload))
        fault = self._ledger.fault
        if fault == "malformed_http":
            raise ClickHouseDispatchTransportError(dispatch_sha256=dispatch.dispatch_sha256, request_body_bytes=0)
        if fault == "partial_response":
            raise ClickHouseDispatchTransportError(
                dispatch_sha256=dispatch.dispatch_sha256, request_body_bytes=max(len(payload) // 2, 1)
            )
        if type(dispatch) is CreateGenerationDispatch:
            self._tables.generation = []
        elif type(dispatch) is InsertGenerationDispatch:
            self._tables.generation = list(self._tables.source)
        elif type(dispatch) is ExchangeSnapshotDispatch:
            self._tables.exchange()
        else:
            raise CompositionAdmissionError("clickhouse_dispatch_operation")
        body_digest = "sha256:" + sha256(b"").hexdigest()
        return ClickHouseDispatchObservation(
            dispatch.dispatch_sha256,
            dispatch.claim_key,
            dispatch.query_id,
            len(payload),
            0,
            body_digest,
            "content-length",
        )


class _Executor:
    def __init__(self, ledger: _Ledger, tables: _Tables, catalog: Catalog, transport: _Transport) -> None:
        self._ledger = ledger
        self._tables = tables
        self._catalog = catalog
        self._transport = transport
        self.calls: list[str] = []

    def exchange_once(self, value) -> None:
        self.calls.append(value.exchange_query_id)
        if self._ledger.fault == "unknown_publication":
            self._transport.execute(ExchangeSnapshotDispatch(value))
            self._catalog.value = replace(self._catalog.value, target_uuid=None)
            return
        if self._ledger.fault == "missing_reconciliation":
            self._transport.execute(ExchangeSnapshotDispatch(value))
            self._catalog.error = True
            return
        self._transport.execute(ExchangeSnapshotDispatch(value))
        self._catalog.value = observation(value, published=True)


class _Outcome:
    def __init__(self, ledger: _Ledger, store: MemoryStore) -> None:
        self._ledger = ledger
        self._store = store

    def observe(self, attempt) -> CompositionAttemptProof:
        self._ledger.events.append("outcome_proof")
        records = tuple(self._store.records.values())
        state = "COMMIT_UNKNOWN"
        if records and records[-1].state == "PUBLISHED" and self._ledger.fault is None:
            state = "SUCCEEDED"
        return _proof(attempt, "OUTCOME", state)


@dataclass
class ClickHouseRuntime:
    root: Any
    request: Any
    ledger: _Ledger
    tables: _Tables
    attempts: _Attempts
    store: MemoryStore
    authority: Authority

    def seed_target(self, rows: list[tuple[object, ...]]) -> None:
        self.tables.seed_target(rows)

    def seed_source(self, rows: list[tuple[object, ...]]) -> None:
        self.tables.seed_source(rows)

    def execute(self):
        from dpone.app.composition_clickhouse_execution import CompositionClickHouseExecutionRoot

        assert isinstance(self.root, CompositionClickHouseExecutionRoot)
        return self.root.execute(self.request)

    def target_rows(self) -> list[tuple[object, ...]]:
        return list(self.tables.target)

    @property
    def events(self) -> list[str]:
        return self.ledger.events


@pytest.fixture
def runtime() -> ClickHouseRuntime:
    from dpone.app.composition_clickhouse_execution import (
        CompositionClickHouseExecutionDependencies,
        CompositionClickHouseExecutionRequest,
        CompositionClickHouseExecutionRoot,
        build_composition_clickhouse_attempt,
    )

    active = occurrence()
    workload = active.request.workloads[0]
    ledger = _Ledger()
    tables = _Tables()
    attempt_holder: dict[str, Any] = {}

    def require_enrollment(attempt, phase: str) -> bytes:
        if phase not in {"principal", "dispatch", "publication"}:
            raise CompositionAdmissionError("enrollment_phase")
        ledger.enrollment_phases.append(phase)
        if ledger.fault == "supervisor_drift" and phase != "principal":
            raise CompositionAdmissionError("enrollment_drift")
        return ledger.enrollment_document

    def read_source(attempt):
        assert attempt_holder["attempt"] == attempt
        return tuple(tables.source)

    store = MemoryStore()
    prepared = intent(rows=2)
    authority = Authority(prepared)
    catalog = Catalog(prepared)
    transport = _Transport(ledger, tables)
    executor = _Executor(ledger, tables, catalog, transport)
    publisher = ClickHouseAtomicSnapshotPublisher(authority=authority, store=store, catalog=catalog, executor=executor)
    attempts = _Attempts(ledger)
    gate = _Gate(ledger, require_enrollment)
    request = CompositionClickHouseExecutionRequest(
        manifest=_manifest(),
        plan_sha256=digest("clickhouse plan"),
        run_identity=AirflowRunIdentity(
            active.request.release_id,
            active.request.deployment_id,
            AirflowArtifactIdentity(workload.workload_id, workload.pack_sha256),
        ),
        airflow_attempt=AirflowAttemptCorrelation(
            dag_id="a_native",
            task_id="a_native__dpone_runtime",
            run_id="run",
            try_number=1,
            map_index=-1,
        ),
        generation_ref=prepared.generation.record_sha256,
        generation_uuid=prepared.generation.new_generation_uuid,
        columns=COLUMNS,
    )
    attempt = build_composition_clickhouse_attempt(
        active,
        manifest=request.manifest,
        plan_sha256=request.plan_sha256,
        run_identity=request.run_identity,
        airflow_attempt=request.airflow_attempt,
    )
    attempt_holder["attempt"] = attempt
    authority.value = replace(prepared, attempt=attempt)
    catalog.value = observation(authority.value)
    dependencies = CompositionClickHouseExecutionDependencies(
        read_active=lambda: active,
        attempts=attempts,
        gate=gate,
        publisher=publisher,
        transport=transport,
        read_source=read_source,
        require_enrollment=require_enrollment,
        outcome_observer=_Outcome(ledger, store),
        target=prepared.target,
        expected_service_id=SERVICE,
    )
    return ClickHouseRuntime(
        CompositionClickHouseExecutionRoot(dependencies),
        request,
        ledger,
        tables,
        attempts,
        store,
        authority,
    )


def test_clickhouse_root_publishes_atomic_snapshot_and_deletes_disappeared_rows(runtime):
    runtime.seed_target([(1, "old"), (2, "disappeared")])
    runtime.seed_source([(1, "new"), (3, None)])
    runtime.execute()
    assert runtime.target_rows() == [(1, "new"), (3, None)]
    assert runtime.events[-4:] == ["close_dispatch", "close_principal", "outcome_proof", "attempt_terminal"]


@pytest.mark.parametrize(
    "fault",
    ["malformed_http", "partial_response", "unknown_publication", "missing_reconciliation", "supervisor_drift"],
)
def test_clickhouse_faults_are_commit_unknown_and_close_dispatch_first(runtime, fault):
    runtime.ledger.fault = fault
    runtime.seed_target([(1, "old"), (2, "disappeared")])
    runtime.seed_source([(1, "new"), (3, None)])
    with pytest.raises(CompositionAdmissionError, match="worker_commit_unknown"):
        runtime.execute()
    assert runtime.events[-4:] == ["close_dispatch", "close_principal", "outcome_proof", "attempt_terminal"]
    assert runtime.events.index("close_dispatch") < runtime.events.index("close_principal")
    assert runtime.attempts.receipt is not None
    assert runtime.attempts.receipt.state == "COMMIT_UNKNOWN"


def test_enrollment_original_is_required_before_principal_dispatch_and_publication(runtime):
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new")])
    runtime.execute()
    assert runtime.ledger.enrollment_phases == ["principal", "dispatch", "publication"]


def test_query_ids_and_request_bodies_are_bound_to_the_attempt(runtime):
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new"), (3, None)])
    runtime.execute()
    assert runtime.ledger.dispatches
    attempt = runtime.attempts.receipt.attempt
    for dispatch, payload in runtime.ledger.dispatches:
        assert dispatch.attempt == attempt
        assert dispatch.query_id
        if type(dispatch) is ExchangeSnapshotDispatch:
            assert dispatch.query_id == dispatch.intent.exchange_query_id
        else:
            assert dispatch.query_id == "dpone-ch-" + dispatch.claim_key.removeprefix("sha256:")
        if type(dispatch) is InsertGenerationDispatch:
            assert payload
            assert dispatch.payload_sha256 == "sha256:" + sha256(payload).hexdigest()
            assert dispatch.payload_bytes == len(payload)
        else:
            assert payload == b""
