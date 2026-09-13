"""Protected ClickHouse gate originals and exact transaction-scoped SQL reads."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from ipaddress import ip_address
from typing import Any

from dpone.adapters.composition_clickhouse_dispatch_queries import document_sha256, require
from dpone.adapters.composition_clickhouse_gate_schema import require_clickhouse_gate_schema
from dpone.adapters.composition_clickhouse_principal import require_uuid
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_identity import require_digest
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    encode_attempt_proof,
)
from dpone.contracts.composition_snapshot import SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


@dataclass(frozen=True, slots=True)
class ClickHouseSupervisorObservation:
    """Retained original from the mandatory protected Linux supervisor observer.

    Shape validation is not authority. The observer must independently reopen
    enrolled server/database, boot and isolated sole-writer network evidence.
    Its evidence document contains these exact fixed fields plus concrete facts.
    """

    service_id: str
    database_uuid: str
    boot_id: str
    isolation_id: str
    supervisor_ip: str
    evidence_sha256: str
    evidence_document: bytes = field(repr=False)

    def __post_init__(self) -> None:
        for value in (self.service_id, self.database_uuid, self.boot_id, self.isolation_id):
            require_uuid(value)
        require_digest(self.evidence_sha256)
        require(type(self.evidence_document) is bytes and 0 < len(self.evidence_document) <= 65536, "supervisor_budget")
        require(document_sha256(self.evidence_document) == self.evidence_sha256, "supervisor_hash")
        require(
            str(ip_address(self.supervisor_ip)) == self.supervisor_ip
            and not ip_address(self.supervisor_ip).is_unspecified
            and not ip_address(self.supervisor_ip).is_loopback,
            "supervisor_ip",
        )
        body = strict_json_object(self.evidence_document)
        expected = {
            name: getattr(self, name)
            for name in ("service_id", "database_uuid", "boot_id", "isolation_id", "supervisor_ip")
        }
        require(
            set(body) == {"schema", "facts", *expected}
            and body["schema"] == "dpone.composition-clickhouse-supervisor.v1"
            and all(body[key] == value for key, value in expected.items())
            and type(body["facts"]) is dict
            and bool(body["facts"])
            and canonical_json_bytes(body) == self.evidence_document,
            "supervisor_original",
        )

    def to_dict(self) -> dict[str, object]:
        return {"evidence_sha256": self.evidence_sha256, "original": strict_json_object(self.evidence_document)}


@dataclass(frozen=True, slots=True)
class ClickHouseLocalSupervisorObservation:
    """Exact protected enrollment for loopback-only CH in a private namespace.

    LOCAL spans namespace-local interfaces; this DTO confers no authority.
    Its producer must freshly inspect listener, process and mount isolation.
    """

    service_id: str
    database_uuid: str
    boot_id: str
    isolation_id: str
    enrollment_sha256: str
    network_namespace_id: str
    evidence_sha256: str
    evidence_document: bytes = field(repr=False)

    def __post_init__(self) -> None:
        for value in (self.service_id, self.database_uuid, self.boot_id, self.isolation_id):
            require_uuid(value)
        for value in (self.enrollment_sha256, self.evidence_sha256):
            require_digest(value)
        require(
            type(self.network_namespace_id) is str
            and self.network_namespace_id.isascii()
            and self.network_namespace_id.isdecimal()
            and 0 < int(self.network_namespace_id) < 2**64
            and str(int(self.network_namespace_id)) == self.network_namespace_id,
            "supervisor_namespace",
        )
        require(type(self.evidence_document) is bytes and 0 < len(self.evidence_document) <= 65536, "supervisor_budget")
        require(document_sha256(self.evidence_document) == self.evidence_sha256, "supervisor_hash")
        expected = {
            name: getattr(self, name)
            for name in (
                "service_id",
                "database_uuid",
                "boot_id",
                "isolation_id",
                "enrollment_sha256",
                "network_namespace_id",
            )
        }
        body = strict_json_object(self.evidence_document)
        require(
            set(body) == {"schema", "facts", *expected}
            and body["schema"] == "dpone.composition-clickhouse-local-supervisor.v1"
            and all(body[key] == value for key, value in expected.items())
            and type(body["facts"]) is dict
            and bool(body["facts"])
            and canonical_json_bytes(body) == self.evidence_document,
            "supervisor_original",
        )

    def to_dict(self) -> dict[str, object]:
        return {"evidence_sha256": self.evidence_sha256, "original": strict_json_object(self.evidence_document)}


SupervisorObservation = ClickHouseSupervisorObservation | ClickHouseLocalSupervisorObservation


@dataclass(frozen=True, slots=True)
class ClickHouseGateBinding:
    """Root-bound purpose distinguishes ingest and EXCHANGE principals."""

    attempt: CompositionAttemptIdentity
    target: SnapshotTarget
    purpose: str

    def __post_init__(self) -> None:
        self.attempt.__post_init__()
        self.target.__post_init__()
        require(
            self.purpose in {"ingest", "publisher"} and self.target.guard_id in dict(self.attempt.guard_epochs),
            "gate_binding",
        )

    @property
    def key(self) -> str:
        return document_sha256(canonical_json_bytes({"schema": "dpone.composition-clickhouse-gate.v1", **asdict(self)}))

    @property
    def username(self) -> str:
        return "dpone_ch_" + self.key.removeprefix("sha256:")

    def document(self, observation: SupervisorObservation) -> bytes:
        require(
            (observation.service_id, observation.database_uuid) == (self.target.service_id, self.target.database_id),
            "supervisor_subject",
        )
        return canonical_json_bytes(
            {"schema": "dpone.composition-clickhouse-gate.v1", **asdict(self), "supervisor": observation.to_dict()}
        )


class ClickHouseGateQueries:
    """All callback boundaries pin actual SQL transaction, cursor and service."""

    def __init__(self, ledger: CompositionMssqlLedger, binding: ClickHouseGateBinding) -> None:
        self.ledger, self.binding = ledger, binding
        self._cursor, self._schema, self._service = ledger.cursor, ledger.schema, ledger.expected_service_id
        self._transaction = ledger.require_transaction()
        require_clickhouse_gate_schema(ledger.cursor, ledger.schema)
        self.check()

    def check(self) -> None:
        require(
            self.ledger.cursor is self._cursor
            and (self.ledger.schema, self.ledger.expected_service_id) == (self._schema, self._service),
            "gate_context",
        )
        self.ledger.require_transaction(self._transaction)

    def rows(self, sql: str, *parameters: Any) -> tuple[tuple[Any, ...], ...]:
        self.check()
        self.ledger.cursor.execute(sql, *parameters)
        result = tuple(tuple(row) for row in self.ledger.cursor.fetchall())
        self.check()
        return result

    def append(self, table: str, columns: str, values: tuple[object, ...]) -> None:
        self.check()
        self.ledger.cursor.execute(
            f"INSERT INTO {self.ledger.table(table)} ({columns}) VALUES ({','.join('?' for _ in values)});", *values
        )
        self.check()

    def scope(self, *, recovery: bool):
        self.check()
        attempt, target = self.binding.attempt, self.binding.target
        occurrence, receipt = require_existing_execution_in(
            self.ledger, attempt, expected_service_id=self._service, terminal_validator=self.ledger.terminal_validator
        )
        self.check()
        require(
            occurrence.receipt.state in ({"ACTIVE", "RETIRING"} if recovery else {"ACTIVE"})
            and receipt.state in ({"RUNNING", "COMMIT_UNKNOWN"} if recovery else {"RUNNING"}),
            "gate_scope_state",
        )
        workload = next(row for row in occurrence.request.workloads if row.workload_id == attempt.workload_id)
        resource = next((row for row in occurrence.request.resources if row.guard_id == target.guard_id), None)
        require(
            resource is not None
            and (resource.connector, resource.service_id, resource.physical_subject_sha256)
            == ("clickhouse", target.service_id, target.physical_subject_sha256)
            and target.write_subject_sha256 in resource.write_subjects
            and target.write_subject_sha256 in workload.write_subjects
            and workload.execution_cell == "mssql_clickhouse_full_refresh_v1",
            "gate_target_scope",
        )
        return occurrence, receipt

    def original(self, table: str, *, phase: str | None = None) -> bytes | None:
        condition = "" if phase is None else " AND phase=?"
        parameters = (self.binding.key,) if phase is None else (self.binding.key, phase)
        rows = self.rows(
            f"SELECT TOP (2) evidence_sha256, CASE WHEN DATALENGTH(evidence_document) BETWEEN 1 AND 8388608 "
            f"THEN evidence_document END FROM {self.ledger.table(table)} WITH (UPDLOCK,HOLDLOCK) WHERE gate_key=?{condition};",
            *parameters,
        )
        require(len(rows) <= 1, "gate_original_count")
        if not rows:
            return None
        require(
            len(rows[0]) == 2 and type(rows[0][1]) is bytes and document_sha256(rows[0][1]) == rows[0][0],
            "gate_original",
        )
        require(canonical_json_bytes(strict_json_object(rows[0][1])) == rows[0][1], "gate_original_canonical")
        return rows[0][1]

    def require_subject(self, observation: SupervisorObservation) -> bytes:
        document = self.binding.document(observation)
        require(self.original("ch_gates") == document, "gate_subject_changed")
        rows = self.rows(
            f"SELECT TOP (2) operation_key,login_name FROM {self.ledger.table('ch_gates')} WITH (HOLDLOCK) WHERE gate_key=?;",
            self.binding.key,
        )
        require(rows == ((self.binding.attempt.attempt_sha256, self.binding.username),), "gate_subject_index")
        return document

    def event(self, phase: str) -> bytes | None:
        require(phase in {"ENABLING", "READY", "CLOSING", "CLOSED"}, "gate_phase")
        return self.original("ch_gate_events", phase=phase)

    def put_event(self, phase: str, document: bytes) -> None:
        previous = self.event(phase)
        if previous is None:
            self.append(
                "ch_gate_events",
                "gate_key,phase,evidence_sha256,evidence_document",
                (self.binding.key, phase, document_sha256(document), document),
            )
        else:
            require(previous == document, "gate_event_replay")
        require(self.event(phase) == document, "gate_event_readback")

    def binding_id(self) -> str:
        document = self.original("ch_gate_bindings")
        require(document is not None, "gate_unbound")
        assert document is not None
        body = strict_json_object(document)
        require(set(body) == {"gate_key", "gate_id"} and body["gate_key"] == self.binding.key, "gate_uuid_original")
        value = body["gate_id"]
        require_uuid(value)
        rows = self.rows(
            f"SELECT TOP (2) LOWER(CONVERT(char(36),gate_id)) FROM {self.ledger.table('ch_gate_bindings')} WITH (HOLDLOCK) WHERE gate_key=?;",
            self.binding.key,
        )
        require(rows == ((value,),), "gate_uuid_index")
        issued = self.rows(
            f"SELECT TOP (2) operation_key FROM {self.ledger.table('issued_authorities')} WITH (HOLDLOCK) "
            "WHERE connector='clickhouse' AND service_id=? AND principal_id=?;",
            self.binding.target.service_id,
            "clickhouse-user:" + value,
        )
        require(issued == ((self.binding.attempt.attempt_sha256,),), "gate_issued")
        return value

    def persist_proof(self, proof: CompositionAttemptProof, *, create: bool = False) -> None:
        """Separate UUIDs produce separate rows; never overwrite a purpose's proof."""
        document = encode_attempt_proof(proof)
        rows = self.rows(
            f"SELECT TOP (2) operation_family,proof_document FROM {self.ledger.table('proofs')} WITH (HOLDLOCK) "
            "WHERE operation_key=? AND kind=? AND proof_sha256=?;",
            proof.attempt_sha256,
            proof.kind,
            proof.proof_sha256,
        )
        expected = (("execution", document),)
        if not rows:
            require(create, "gate_proof_missing")
            self.append(
                "proofs",
                "operation_key,kind,proof_sha256,operation_family,proof_document",
                (proof.attempt_sha256, proof.kind, proof.proof_sha256, "execution", document),
            )
            rows = self.rows(
                f"SELECT TOP (2) operation_family,proof_document FROM {self.ledger.table('proofs')} WITH (HOLDLOCK) "
                "WHERE operation_key=? AND kind=? AND proof_sha256=?;",
                proof.attempt_sha256,
                proof.kind,
                proof.proof_sha256,
            )
        require(rows == expected, "gate_proof_original")
