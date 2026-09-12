"""Observe sealed snapshot publication; never invent principals or generations."""

from __future__ import annotations

from collections.abc import Mapping
from ipaddress import ip_address
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from dpone.adapters.composition_clickhouse_enrollment import clickhouse_physical_domain
from dpone.adapters.composition_clickhouse_gate_queries import ClickHouseGateBinding
from dpone.adapters.composition_clickhouse_http import ClickHouseTransportCredentials
from dpone.adapters.composition_clickhouse_supervisor_enrollment import require_attempt_enrollment_original
from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ExchangeSnapshotDispatch, InsertGenerationDispatch
from dpone.contracts.composition_control import CompositionProofAuthority
from dpone.contracts.composition_snapshot import (
    SnapshotLimits,
    SnapshotPublicationIntent,
    SnapshotPublisherClosure,
    SnapshotTarget,
)
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject


def clickhouse_http_endpoint(resolved: Any) -> tuple[str, ClickHouseTransportCredentials, str | None]:
    """Literal-IP HTTP endpoint from the enrolled sink; DNS aliases are rejected."""

    credentials = resolved.credentials
    host = str(credentials.host or "")
    if host == "localhost":
        host = "127.0.0.1"
    try:
        if "%" in host:
            raise ValueError
        ip_address(host)
    except ValueError:
        raise CompositionAdmissionError("clickhouse_transport_endpoint") from None
    secure = bool(getattr(credentials, "secure", False))
    port = int(credentials.port or (8443 if secure else 8123))
    scheme = "https" if secure else "http"
    endpoint = f"{scheme}://{host}:{port}"
    parsed = urlsplit(endpoint)
    if scheme == "http" and host not in {"127.0.0.1", "::1"}:
        raise CompositionAdmissionError("clickhouse_transport_endpoint")
    if parsed.hostname != host or parsed.port != port:
        raise CompositionAdmissionError("clickhouse_transport_endpoint")
    username, password = credentials.username, credentials.password
    if not isinstance(username, str) or not isinstance(password, str):
        raise CompositionAdmissionError("clickhouse_transport_credentials")
    params = credentials.additional_params if isinstance(credentials.additional_params, Mapping) else {}
    ca_file = params.get("ca_cert") or params.get("tls_ca_cert")
    if ca_file is not None and (type(ca_file) is not str or not ca_file):
        raise CompositionAdmissionError("clickhouse_transport_endpoint")
    return endpoint, ClickHouseTransportCredentials(username, password), ca_file


def snapshot_limits_from_manifest(manifest: Mapping[str, Any]) -> SnapshotLimits:
    """Reuse the sealed source-byte ceiling; do not invent a larger budget."""

    sink = manifest.get("sink")
    if not isinstance(sink, Mapping) or not isinstance(sink.get("strategy"), Mapping):
        raise CompositionAdmissionError("bounded_generated_full_refresh_required")
    ceiling = sink["strategy"].get("max_source_bytes")
    if type(ceiling) is not int or ceiling <= 0 or ceiling > 2**62:
        raise CompositionAdmissionError("bounded_generated_full_refresh_required")
    return SnapshotLimits(ceiling, ceiling, ceiling, ceiling, ceiling, min(ceiling * 3, 2**63 - 1))


def clickhouse_plan_write(plan: Any, manifest: Mapping[str, Any]) -> DbtRelationWrite:
    """Select the exact producer write for this ClickHouse sink table."""

    sink = manifest.get("sink")
    table = sink.get("table") if isinstance(sink, Mapping) else None
    name = table.get("name") if isinstance(table, Mapping) else None
    matches = tuple(
        write
        for write in getattr(plan, "writes", ())
        if type(write) is DbtRelationWrite and write.connector == "clickhouse" and write.relation == name
    )
    if len(matches) != 1:
        raise CompositionAdmissionError("clickhouse_source_payload")
    return matches[0]


def snapshot_target_for_write(enrollment: Any, write: DbtRelationWrite) -> SnapshotTarget:
    """Bind the enrolled database to the producer write and reserved generation slot."""

    body = enrollment.body
    generation = write.relation + "__dpone_gen"
    return SnapshotTarget(
        body["service_id"],
        clickhouse_physical_domain(body["service_id"], body["database_uuid"]).physical_subject_sha256,
        body["database_uuid"],
        write.schema,
        write.relation,
        generation,
        dbt_relation_write_subject(write),
        body["target_enrollment_sha256"],
    )


class ClickHouseDispatchBudgetPolicy:
    """Reserve wire bytes against the sealed source ceiling."""

    def __init__(self, limits: SnapshotLimits) -> None:
        self._limits = limits

    def require_dispatch(self, context: Any, dispatch: Any) -> None:
        del context
        if type(dispatch) is InsertGenerationDispatch and dispatch.payload_bytes > self._limits.max_wire_bytes:
            raise CompositionAdmissionError("clickhouse_dispatch_payload_budget")


class ObservingSnapshotPublicationAuthority:
    """Rebuild the prepared intent from issued principals and sealed generation."""

    def __init__(
        self,
        *,
        read_active: Any,
        connection_factory: Any,
        control_schema: str,
        target: SnapshotTarget,
        limits: SnapshotLimits,
        enrollment_sha256: str,
        load_generation: Any,
        publisher_gate: Any,
    ) -> None:
        self._read_active = read_active
        self._factory = connection_factory
        self._schema = control_schema
        self._target = target
        self._limits = limits
        self._enrollment = enrollment_sha256
        self._load_generation = load_generation
        self._publisher_gate = publisher_gate

    def load_prepared(self, attempt: Any, generation_ref: str) -> SnapshotPublicationIntent:
        generation = self._load_generation(attempt, generation_ref)
        ingest, publisher, closed = self._observe_principals(attempt)
        intent = SnapshotPublicationIntent(attempt, self._target, generation, self._limits, ingest, publisher, closed)
        intent.require_prepared_subject(attempt, generation_ref)
        return intent

    def require_current(self, intent: SnapshotPublicationIntent, *, recovery: bool) -> Any:
        del recovery
        return self._read_active()

    def require_enrollment(self, attempt: Any, target: SnapshotTarget) -> None:
        if target != self._target:
            raise CompositionAdmissionError("clickhouse_enrollment")
        with composition_control_transaction(self._factory, self._schema, self._target.service_id) as ledger:
            require_attempt_enrollment_original(ledger, self._enrollment, attempt, target)

    def ingest_authority(self, attempt: Any) -> CompositionProofAuthority:
        """Reopen the issued ingest principal; this does not close or reissue it."""

        with composition_control_transaction(self._factory, self._schema, self._target.service_id) as ledger:
            return self._gate_principal(ledger, attempt, "ingest")

    def close_publisher(self, intent: SnapshotPublicationIntent) -> SnapshotPublisherClosure:
        closed = self._publisher_gate.close(intent.attempt)
        quiet = self._publisher_gate.prove_quiescence(intent.attempt)
        return SnapshotPublisherClosure(intent.intent_sha256, closed.evidence_sha256, quiet.evidence_sha256)

    def _observe_principals(self, attempt: Any) -> tuple[CompositionProofAuthority, CompositionProofAuthority, str]:
        with composition_control_transaction(self._factory, self._schema, self._target.service_id) as ledger:
            ingest = self._gate_principal(ledger, attempt, "ingest")
            publisher = self._gate_principal(ledger, attempt, "publisher")
            ledger.cursor.execute(
                f"SELECT TOP (2) evidence_sha256 FROM {ledger.table('proofs')} WITH (HOLDLOCK) "
                "WHERE operation_key=? AND kind='CLOSED_GATES';",
                attempt.attempt_sha256,
            )
            rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
        if len(rows) != 1 or type(rows[0][0]) is not str:
            raise CompositionAdmissionError("snapshot_prepare_unavailable")
        return ingest, publisher, rows[0][0]

    def _gate_principal(self, ledger: Any, attempt: Any, purpose: str) -> CompositionProofAuthority:
        binding = ClickHouseGateBinding(attempt, self._target, purpose)
        ledger.cursor.execute(
            f"SELECT TOP (2) LOWER(CONVERT(char(36),gate_id)) FROM {ledger.table('ch_gate_bindings')} "
            "WITH (HOLDLOCK) WHERE gate_key=?;",
            binding.key,
        )
        rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
        if len(rows) != 1 or str(UUID(str(rows[0][0]))) != rows[0][0]:
            raise CompositionAdmissionError("snapshot_prepare_unavailable")
        return CompositionProofAuthority("clickhouse", self._target.service_id, "clickhouse-user:" + rows[0][0])


class ClickHouseHttpSnapshotExecutor:
    """One EXCHANGE through the attached issued publisher transport."""

    def __init__(self) -> None:
        self._transport: Any = None
        self._journal: Any = None

    def attach(self, transport: Any, journal: Any = None) -> None:
        self._transport = transport
        self._journal = journal

    def exchange_once(self, intent: SnapshotPublicationIntent) -> None:
        if self._transport is None:
            raise CompositionAdmissionError("clickhouse_transport_binding")
        dispatch = ExchangeSnapshotDispatch(intent)
        observation = self._transport.execute(dispatch)
        record = getattr(self._journal, "record_completed", None)
        if callable(record):
            record(dispatch, observation)


__all__ = [
    "ClickHouseDispatchBudgetPolicy",
    "ClickHouseHttpSnapshotExecutor",
    "ObservingSnapshotPublicationAuthority",
    "clickhouse_http_endpoint",
    "clickhouse_plan_write",
    "snapshot_limits_from_manifest",
    "snapshot_target_for_write",
]
