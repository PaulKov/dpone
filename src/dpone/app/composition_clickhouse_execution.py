"""Composition root that publishes one MSSQL→ClickHouse Atomic full-refresh.

This is the only place where the protected parent attempt store, ClickHouse
gate, enrolled supervisor original, read-only MSSQL source, closed HTTP
dispatch and snapshot publisher are wired together. Every collaborator is
injected, so a deployment can substitute a real protected adapter without
changing policy here.

Ordering is a security property, not an implementation detail:

1. reopen the ACTIVE parent occurrence and derive the exact attempt identity;
2. reserve the attempt atomically, then issue one-time ingest credentials;
3. require the exact supervisor enrollment original before dispatch;
4. bind those issued credentials to ingest transport, journal CREATE/INSERT
   (claim → send → record_completed), then close ingest and prove quiescence;
5. require enrollment again, issue a distinct publisher principal, and publish
   through that publisher journal/transport;
6. persist OUTCOME proof from the publisher record and seal the attempt.

Malformed HTTP, a partial response, unknown publication, missing
reconciliation or supervisor drift never become success: the independent
observer classifies ``COMMIT_UNKNOWN``. Live Docker, Linux and SQL remain
UNVERIFIED.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from dpone.adapters.composition_clickhouse_http import ClickHouseTransportCredentials
from dpone.adapters.composition_clickhouse_principal import IssuedClickHouseCredentials
from dpone.adapters.composition_clickhouse_transport import ClickHouseDispatchTransportError
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
)
from dpone.contracts.composition_clickhouse_dispatch import (
    ClickHouseDispatchColumn,
    CreateGenerationDispatch,
    InsertGenerationDispatch,
)
from dpone.contracts.composition_execution import composition_generated_transfer_cell
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    require_composition_attempt_scope,
)
from dpone.contracts.composition_snapshot import SnapshotTarget
from dpone.runtime.clickhouse_native import ClickHouseNativeEncoder
from dpone.services.composition_worker import CompositionWorker

CLICKHOUSE_CELL = "mssql_clickhouse_full_refresh_v1"


@dataclass(frozen=True, slots=True)
class CompositionClickHouseExecutionRequest:
    """One generated MSSQL→ClickHouse invocation bound to its parent identity."""

    manifest: Mapping[str, Any]
    plan_sha256: str
    run_identity: AirflowRunIdentity
    airflow_attempt: AirflowAttemptCorrelation
    generation_ref: str
    generation_uuid: str
    columns: tuple[ClickHouseDispatchColumn, ...]


@dataclass(frozen=True, slots=True)
class CompositionClickHouseResult:
    """Observed source rows and publisher state; terminal proof is on the receipt."""

    rows: tuple[tuple[object, ...], ...]
    publication_state: str


@dataclass(frozen=True, slots=True)
class CompositionClickHouseExecutionDependencies:
    """Injected protected capabilities; this root owns no ambient client."""

    read_active: Callable[[], CompositionActivationOccurrence]
    attempts: Any
    gate: Any
    publisher_gate: Any
    publisher: Any
    bind_transport: Callable[[ClickHouseTransportCredentials, Any], Any] | None
    read_source: Callable[[CompositionAttemptIdentity], Sequence[tuple[object, ...]]]
    require_enrollment: Callable[[CompositionAttemptIdentity, str], Any]
    outcome_observer: Any
    target: SnapshotTarget
    expected_service_id: str
    attach_publisher_transport: Callable[..., None] | None = None


class CompositionClickHouseExecutionRoot:
    """Execute one MSSQL→ClickHouse full-refresh as a supervised parent attempt."""

    def __init__(self, dependencies: CompositionClickHouseExecutionDependencies) -> None:
        self._deps = dependencies

    def execute(self, request: CompositionClickHouseExecutionRequest) -> CompositionClickHouseResult:
        """Run admission, enrolled dispatch, atomic publication and seal once."""

        occurrence = self._deps.read_active()
        occurrence.require_state("ACTIVE")
        attempt = build_composition_clickhouse_attempt(
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
        result = worker.run(attempt, execute=lambda credentials: self._refresh(request, attempt, credentials))
        return result.value

    def _refresh(
        self,
        request: CompositionClickHouseExecutionRequest,
        attempt: CompositionAttemptIdentity,
        credentials: IssuedClickHouseCredentials,
    ) -> CompositionClickHouseResult:
        """Read the source, dispatch attempt-bound work, then publish one snapshot."""

        try:
            if type(credentials) is not IssuedClickHouseCredentials:
                raise CompositionAdmissionError("worker_issued_identity")
            self._deps.require_enrollment(attempt, "dispatch")
            rows = tuple(self._deps.read_source(attempt))
            journal = self._journal(self._deps.gate, attempt, credentials.user_id)
            transport = self._bind_transport(credentials, journal)
            self._ingest(request, attempt, rows, transport, journal)
            self._close_purpose(self._deps.gate, attempt)
            self._deps.require_enrollment(attempt, "publication")
            publisher_gate = self._deps.publisher_gate
            if publisher_gate is None:
                raise CompositionAdmissionError("clickhouse_publisher_gate")
            publisher_credentials = publisher_gate.issue_once(attempt)
            if type(publisher_credentials) is not IssuedClickHouseCredentials:
                raise CompositionAdmissionError("worker_issued_identity")
            publisher_journal = self._journal(publisher_gate, attempt, publisher_credentials.user_id)
            publisher_transport = self._bind_transport(publisher_credentials, publisher_journal)
            attach = self._deps.attach_publisher_transport
            if attach is not None:
                attach(publisher_transport, publisher_journal)
            prepared = self._deps.publisher.prepare(attempt, request.generation_ref)
            published = self._deps.publisher.publish(prepared.intent.intent_sha256)
            return CompositionClickHouseResult(rows, published.state)
        except (ClickHouseDispatchTransportError, CompositionAdmissionError):
            return CompositionClickHouseResult((), "COMMIT_UNKNOWN")

    def _journal(self, gate: Any, attempt: CompositionAttemptIdentity, user_id: str) -> Any:
        journal = getattr(gate, "journal", None)
        if not callable(journal):
            raise CompositionAdmissionError("clickhouse_dispatch_journal")
        return journal(attempt, user_id)

    def _bind_transport(self, credentials: IssuedClickHouseCredentials, journal: Any) -> Any:
        bind = self._deps.bind_transport
        if not callable(bind):
            raise CompositionAdmissionError("clickhouse_transport_binding")
        return bind(ClickHouseTransportCredentials(credentials.username, credentials.password), journal)

    def _close_purpose(self, gate: Any, attempt: CompositionAttemptIdentity) -> None:
        close = getattr(gate, "close", None)
        prove = getattr(gate, "prove_quiescence", None)
        if not callable(close) or not callable(prove):
            raise CompositionAdmissionError("clickhouse_ingest_close")
        closed = close(attempt)
        if getattr(closed, "kind", None) != "CLOSED_GATES":
            raise CompositionAdmissionError("clickhouse_ingest_close")
        quiet = prove(attempt)
        if getattr(quiet, "kind", None) != "QUIESCENCE":
            raise CompositionAdmissionError("clickhouse_ingest_quiescence")

    def _ingest(
        self,
        request: CompositionClickHouseExecutionRequest,
        attempt: CompositionAttemptIdentity,
        rows: tuple[tuple[object, ...], ...],
        transport: Any,
        journal: Any,
    ) -> None:
        target = self._deps.target
        self._send(
            transport,
            journal,
            CreateGenerationDispatch(attempt, target, request.generation_uuid, request.columns),
        )
        if not rows:
            return
        payload = _native_payload(request.columns, rows)
        self._send(
            transport,
            journal,
            InsertGenerationDispatch(
                attempt,
                target,
                request.generation_uuid,
                request.columns,
                "sha256:" + sha256(payload).hexdigest(),
                len(payload),
                0,
            ),
            payload,
        )

    def _send(
        self,
        transport: Any,
        journal: Any,
        dispatch: CreateGenerationDispatch | InsertGenerationDispatch,
        payload: bytes = b"",
    ) -> None:
        observation = transport.execute(dispatch, payload=payload)
        record = getattr(journal, "record_completed", None)
        if not callable(record):
            raise CompositionAdmissionError("clickhouse_dispatch_journal")
        record(dispatch, observation)


def build_composition_clickhouse_attempt(
    occurrence: CompositionActivationOccurrence,
    *,
    manifest: Mapping[str, Any],
    plan_sha256: str,
    run_identity: AirflowRunIdentity,
    airflow_attempt: AirflowAttemptCorrelation,
) -> CompositionAttemptIdentity:
    """Bind the verified generated transfer to the ACTIVE parent membership."""

    occurrence.require_state("ACTIVE")
    cell = composition_generated_transfer_cell(manifest)
    if cell != CLICKHOUSE_CELL:
        raise CompositionAdmissionError("worker_clickhouse_cell")
    workload_id = str(manifest.get("name") or "")
    if (run_identity.release_id, run_identity.deployment_id) != (
        occurrence.request.release_id,
        occurrence.request.deployment_id,
    ):
        raise CompositionAdmissionError("worker_parent_identity")
    workload = next((row for row in occurrence.request.workloads if row.workload_id == workload_id), None)
    if (
        workload is None
        or workload.execution_cell != cell
        or run_identity.workload_pack.id != workload_id
        or run_identity.workload_pack.sha256 != workload.pack_sha256
    ):
        raise CompositionAdmissionError("worker_clickhouse_membership")
    guards = {
        row.guard_id
        for row in occurrence.request.resources
        if set(workload.write_subjects).intersection(row.write_subjects)
    }
    result = CompositionAttemptIdentity(
        occurrence.request.request_sha256,
        workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        plan_sha256,
        airflow_attempt.run_id,
        airflow_attempt.task_id,
        airflow_attempt.try_number,
        airflow_attempt.map_index,
        tuple(pair for pair in occurrence.receipt.guard_epochs if pair[0] in guards),
    )
    require_composition_attempt_scope(occurrence, result)
    return result


def _native_payload(columns: tuple[ClickHouseDispatchColumn, ...], rows: Sequence[tuple[object, ...]]) -> bytes:
    try:
        schema = tuple((column.name, column.type_name) for column in columns)
        payload = b"".join(ClickHouseNativeEncoder(schema, target_schema=schema).iter_batches(rows))
    except Exception:
        raise CompositionAdmissionError("clickhouse_source_payload") from None
    if type(payload) is not bytes or not payload:
        raise CompositionAdmissionError("clickhouse_source_payload")
    return payload


__all__ = [
    "CLICKHOUSE_CELL",
    "CompositionClickHouseExecutionDependencies",
    "CompositionClickHouseExecutionRequest",
    "CompositionClickHouseExecutionRoot",
    "CompositionClickHouseResult",
    "build_composition_clickhouse_attempt",
]
