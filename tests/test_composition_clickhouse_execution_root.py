"""Offline MSSQL→ClickHouse parent root; live Docker/Linux/SQL remains UNVERIFIED."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from hashlib import sha256
from typing import Any

import pytest

from dpone.adapters import composition_clickhouse_dispatch_queries as dispatch_queries
from dpone.adapters import composition_clickhouse_gate_queries as gate_queries
from dpone.adapters.composition_clickhouse_gate import MssqlClickHouseGate
from dpone.adapters.composition_clickhouse_http import ClickHouseTransportCredentials
from dpone.adapters.composition_clickhouse_principal import IssuedClickHouseCredentials
from dpone.adapters.composition_clickhouse_transport import (
    ClickHouseDispatchObservation,
    ClickHouseDispatchTransportError,
)
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
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
from tests.composition_mssql_catalog_helpers import install_offline_catalog_references
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
from tests.test_composition_clickhouse_gate import (
    SQL_SERVICE,
    Admin,
    GateDatabase,
    Policy,
    Supervisor,
    enrolled_supervisor,
)

COLUMNS = (
    ClickHouseDispatchColumn("id", "Int32"),
    ClickHouseDispatchColumn("name", "Nullable(String)"),
)
USER_ID = "10000000-0000-4000-8000-000000000021"


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


def _proof(attempt, kind: str, state: str | None = None, *, user_id: str = USER_ID) -> CompositionAttemptProof:
    return CompositionAttemptProof(
        kind,
        attempt.attempt_sha256,
        attempt.activation_request_sha256,
        composition_attempt_epoch_subject(attempt),
        (CompositionProofAuthority("clickhouse", SERVICE, "clickhouse-user:" + user_id),),
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
    claims: list[str] = field(default_factory=list)
    issued: list[IssuedClickHouseCredentials] = field(default_factory=list)
    admitted: bool = False
    closed: str | None = None
    quiescence: str | None = None
    fault: str | None = None
    ingest_user_id: str = USER_ID
    ingest_closed_before_prepare: bool = False
    ingest_quiescent_before_prepare: bool = False
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


class _ObservedGate:
    """Worker-facing wrapper that still drives ``MssqlClickHouseGate.journal``."""

    def __init__(self, inner: MssqlClickHouseGate, ledger: _Ledger, require_enrollment, *, record_events: bool) -> None:
        self._inner = inner
        self._ledger = ledger
        self._require_enrollment = require_enrollment
        self._record_events = record_events
        self.issued: list[IssuedClickHouseCredentials] = []

    def issue_once(self, attempt) -> IssuedClickHouseCredentials:
        if self._record_events:
            self._require_enrollment(attempt, "principal")
            self._ledger.events.append("issue_principal")
        credentials = self._inner.issue_once(attempt)
        self.issued.append(credentials)
        self._ledger.issued.append(credentials)
        if self._record_events:
            self._ledger.ingest_user_id = credentials.user_id
        return credentials

    def journal(self, attempt, user_id):
        return self._inner.journal(attempt, user_id)

    def close(self, attempt) -> CompositionAttemptProof:
        if self._record_events:
            self._ledger.events.append("close_dispatch")
        proof = self._inner.close(attempt)
        if self._record_events:
            self._ledger.events.append("close_principal")
            self._ledger.closed = proof.proof_sha256
        return proof

    def prove_quiescence(self, attempt) -> CompositionAttemptProof:
        proof = self._inner.prove_quiescence(attempt)
        if self._record_events:
            self._ledger.quiescence = proof.proof_sha256
        return proof


class _BoundTransport:
    """Requires constructor credentials from ``issue_once`` and journals claims."""

    def __init__(self, credentials: ClickHouseTransportCredentials, journal, ledger: _Ledger, tables: _Tables) -> None:
        if type(credentials) is not ClickHouseTransportCredentials:
            raise CompositionAdmissionError("clickhouse_transport_credentials")
        credentials.__post_init__()
        issued = next(
            (
                row
                for row in ledger.issued
                if row.username == credentials.username and row.password == credentials.password
            ),
            None,
        )
        if issued is None:
            raise CompositionAdmissionError("clickhouse_transport_credentials")
        self._credentials = credentials
        self._journal = journal
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
        self._journal.claim_once(dispatch)
        self._ledger.claims.append(dispatch.claim_key)
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
    def __init__(self, ledger: _Ledger, tables: _Tables, catalog: Catalog, transport: Any) -> None:
        self._ledger = ledger
        self._tables = tables
        self._catalog = catalog
        self._transport = transport
        self._journal: Any = None
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
        seen = self._transport.execute(ExchangeSnapshotDispatch(value))
        journal = self._journal
        if journal is not None:
            journal.record_completed(ExchangeSnapshotDispatch(value), seen)
        self._catalog.value = observation(value, published=True)


class _Outcome:
    def __init__(self, ledger: _Ledger, store: MemoryStore) -> None:
        self._ledger = ledger
        self._store = store

    def observe(self, attempt) -> CompositionAttemptProof:
        self._ledger.events.append("outcome_proof")
        records = tuple(self._store.records.values())
        state = "COMMIT_UNKNOWN"
        if records and records[-1].state == "PUBLISHED" and records[-1].observation is not None:
            state = "SUCCEEDED"
        return _proof(attempt, "OUTCOME", state, user_id=self._ledger.ingest_user_id)


@dataclass
class ClickHouseRuntime:
    root: Any
    request: Any
    ledger: _Ledger
    tables: _Tables
    attempts: _Attempts
    store: MemoryStore
    authority: Authority
    db: Any

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
def runtime(monkeypatch) -> ClickHouseRuntime:
    from dpone.app.composition_clickhouse_execution import (
        CompositionClickHouseExecutionDependencies,
        CompositionClickHouseExecutionRequest,
        CompositionClickHouseExecutionRoot,
        build_composition_clickhouse_attempt,
    )

    install_offline_catalog_references(monkeypatch)
    monkeypatch.setattr(gate_queries, "require_clickhouse_gate_schema", lambda *_: None)
    monkeypatch.setattr(dispatch_queries, "require_clickhouse_dispatch_schema", lambda *_: None)
    monkeypatch.setattr(
        "dpone.adapters.composition_clickhouse_supervisor_enrollment.require_clickhouse_supervisor_schema",
        lambda *_: None,
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
    enrollment_key, enrollment_row = enrolled_supervisor(prepared)
    db = GateDatabase(active.request, SQL_SERVICE)
    db.data.update(
        ch_gates={},
        ch_gate_bindings={},
        ch_gate_events={},
        dispatches={},
        terminals={},
        closures={},
        enrollments={enrollment_key: enrollment_row},
    )
    activation = MssqlCompositionActivationStore(db.connect, expected_service_id=SQL_SERVICE)
    activation.prepare(db.request)
    activation.activate(db.request)
    authority = Authority(prepared)
    catalog = Catalog(prepared)
    executor = _Executor(ledger, tables, catalog, None)
    publisher = ClickHouseAtomicSnapshotPublisher(authority=authority, store=store, catalog=catalog, executor=executor)
    attempts = _Attempts(ledger)
    admin, supervisor, policy = Admin(), Supervisor(prepared), Policy()
    ingest_inner = MssqlClickHouseGate(
        db.connect,
        expected_service_id=SQL_SERVICE,
        target=prepared.target,
        purpose="ingest",
        principal_admin=admin,
        supervisor=supervisor,
        dispatch_policy=policy,
        enrollment_sha256=enrollment_key,
    )
    publisher_inner = MssqlClickHouseGate(
        db.connect,
        expected_service_id=SQL_SERVICE,
        target=prepared.target,
        purpose="publisher",
        principal_admin=admin,
        supervisor=supervisor,
        dispatch_policy=policy,
        enrollment_sha256=enrollment_key,
    )
    gate = _ObservedGate(ingest_inner, ledger, require_enrollment, record_events=True)
    publisher_gate = _ObservedGate(publisher_inner, ledger, require_enrollment, record_events=False)
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
    MssqlCompositionAttemptStore(db.connect, expected_service_id=SQL_SERVICE).admit_once(attempt)
    authority.value = replace(prepared, attempt=attempt)
    catalog.value = observation(authority.value)
    original_load = authority.load_prepared

    def load_prepared(observed_attempt, generation_ref):
        phases = [phase for _key, phase in db.data["ch_gate_events"]]
        ledger.ingest_closed_before_prepare = "CLOSED" in phases
        ledger.ingest_quiescent_before_prepare = any(key[1] == "QUIESCENCE" for key in db.data["proofs"])
        ledger.events.append("prepare")
        return original_load(observed_attempt, generation_ref)

    authority.load_prepared = load_prepared  # type: ignore[method-assign]

    def bind_transport(credentials: ClickHouseTransportCredentials, journal) -> _BoundTransport:
        return _BoundTransport(credentials, journal, ledger, tables)

    def attach_publisher_transport(transport, journal=None) -> None:
        executor._transport = transport
        executor._journal = journal
        ingest, publisher_issued = ledger.issued[0], ledger.issued[1]
        authority.value = replace(
            authority.value,
            ingest_principal=CompositionProofAuthority("clickhouse", SERVICE, "clickhouse-user:" + ingest.user_id),
            publisher_principal=CompositionProofAuthority(
                "clickhouse", SERVICE, "clickhouse-user:" + publisher_issued.user_id
            ),
        )
        catalog.value = observation(authority.value)

    dependencies = CompositionClickHouseExecutionDependencies(
        read_active=lambda: active,
        attempts=attempts,
        gate=gate,
        publisher_gate=publisher_gate,
        publisher=publisher,
        bind_transport=bind_transport,
        attach_publisher_transport=attach_publisher_transport,
        read_source=read_source,
        require_enrollment=require_enrollment,
        outcome_observer=_Outcome(ledger, store),
        target=prepared.target,
        expected_service_id=SERVICE,
        can_classify_publication=True,
    )
    return ClickHouseRuntime(
        CompositionClickHouseExecutionRoot(dependencies),
        request,
        ledger,
        tables,
        attempts,
        store,
        authority,
        db,
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
    records = tuple(runtime.store.records.values())
    assert not records or records[-1].state != "PUBLISHED"


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


def test_issued_credentials_are_bound_and_journaled_before_publish(runtime):
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new")])
    runtime.execute()
    assert runtime.ledger.issued
    assert runtime.ledger.claims
    assert runtime.db.data["dispatches"]
    assert runtime.db.data["terminals"]
    assert any(type(dispatch) is CreateGenerationDispatch for dispatch, _ in runtime.ledger.dispatches)
    assert any(type(dispatch) is InsertGenerationDispatch for dispatch, _ in runtime.ledger.dispatches)
    assert len(runtime.db.data["terminals"]) >= 2


def test_ingest_is_closed_and_quiescent_before_prepare(runtime):
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new")])
    runtime.execute()
    assert runtime.ledger.ingest_closed_before_prepare
    assert runtime.ledger.ingest_quiescent_before_prepare
    assert runtime.events.index("close_dispatch") < runtime.events.index("prepare")


def test_publication_reopens_enrollment_inside_require_current(runtime):
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new")])
    runtime.execute()
    assert runtime.authority.enrollment_calls
    assert runtime.authority.enrollment_calls[0] == (runtime.attempts.receipt.attempt, runtime.root._deps.target)


def test_outcome_follows_publisher_record_not_fixture_knob(runtime):
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new")])
    runtime.execute()
    runtime.ledger.fault = "malformed_http"
    proof = runtime.root._deps.outcome_observer.observe(runtime.attempts.receipt.attempt)
    assert proof.outcome_state == "SUCCEEDED"
    assert tuple(runtime.store.records.values())[-1].state == "PUBLISHED"


def test_root_rejects_missing_dispatch_journal(runtime):
    from dataclasses import replace as replace_deps

    from dpone.app.composition_clickhouse_execution import CompositionClickHouseExecutionRoot

    inner = runtime.root._deps.gate

    class _NoJournal:
        def issue_once(self, attempt):
            return inner.issue_once(attempt)

        def close(self, attempt):
            return inner.close(attempt)

        def prove_quiescence(self, attempt):
            return inner.prove_quiescence(attempt)

    runtime.root = CompositionClickHouseExecutionRoot(replace_deps(runtime.root._deps, gate=_NoJournal()))
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new")])
    with pytest.raises(CompositionAdmissionError, match="worker_commit_unknown"):
        runtime.execute()
    assert not runtime.store.records


def test_root_rejects_missing_publisher_gate(runtime):
    from dataclasses import replace as replace_deps

    from dpone.app.composition_clickhouse_execution import CompositionClickHouseExecutionRoot

    runtime.root = CompositionClickHouseExecutionRoot(replace_deps(runtime.root._deps, publisher_gate=None))
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new")])
    with pytest.raises(CompositionAdmissionError, match="worker_commit_unknown"):
        runtime.execute()
    assert "prepare" not in runtime.events or not runtime.store.records


def test_root_rejects_missing_transport_binding(runtime):
    from dataclasses import replace as replace_deps

    from dpone.app.composition_clickhouse_execution import CompositionClickHouseExecutionRoot

    runtime.root = CompositionClickHouseExecutionRoot(replace_deps(runtime.root._deps, bind_transport=None))
    runtime.seed_target([(1, "old")])
    runtime.seed_source([(1, "new")])
    with pytest.raises(CompositionAdmissionError, match="worker_commit_unknown"):
        runtime.execute()
    assert not runtime.db.data["dispatches"]
