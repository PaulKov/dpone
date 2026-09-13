"""Aggregate protected ClickHouse originals before actual attempt finalization.

All authority comes from original readers on one pinned ledger, plus the capture
loader's independent file custody verification. No HTTP outcome, worker count or
caller-provided evidence dictionary is accepted. Commit uncertainty is not ACK.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dpone.adapters.composition_clickhouse_gate_queries import ClickHouseGateBinding, ClickHouseGateQueries
from dpone.adapters.composition_clickhouse_historical_closure import read_purpose_closure_in
from dpone.adapters.composition_mssql_attempts import ConnectionFactory, composition_control_transaction
from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.composition_mssql_terminal import read_issued_authorities_in
from dpone.adapters.composition_snapshot_capture_store import MssqlSnapshotCaptureStore
from dpone.adapters.composition_snapshot_sql_store import MssqlSnapshotPublicationStore
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionAttemptReceipt,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
    encode_attempt_identity,
    encode_attempt_proof,
)
from dpone.contracts.composition_remote_transfer_result import (
    RemoteTransferResult,
    decode_publication_originals,
    decode_result,
    evidence_digest,
)
from dpone.contracts.composition_snapshot import SnapshotGeneration, SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def _require(value: bool) -> None:
    if not value:
        raise CompositionAdmissionError("clickhouse_terminal_originals")


def _ref(document: bytes) -> dict[str, Any]:
    return {"document": strict_json_object(document), "sha256": evidence_digest(document)}


def _proof_ref(proof: CompositionAttemptProof, document: bytes) -> dict[str, Any]:
    return {**_ref(encode_attempt_proof(proof)), "evidence_document": strict_json_object(document)}


@dataclass(frozen=True, slots=True)
class TerminalProofTriplet:
    """Persisted aggregate references; not an actual terminal receipt."""

    closed_gates: CompositionAttemptProof
    quiescence: CompositionAttemptProof
    outcome: CompositionAttemptProof

    @property
    def proofs(self) -> tuple[CompositionAttemptProof, ...]:
        return self.closed_gates, self.quiescence, self.outcome


@dataclass(frozen=True, slots=True)
class _Originals:
    body: bytes
    proofs: TerminalProofTriplet
    documents: tuple[bytes, ...]


class _Read:
    def __init__(self, ledger: CompositionMssqlLedger, service: str, schema: str) -> None:
        self.ledger, self.service, self.schema = ledger, service, schema
        self.cursor, self.transaction = ledger.cursor, ledger.require_transaction()
        self.check()

    def check(self) -> None:
        _require(
            self.ledger.cursor is self.cursor
            and (self.ledger.expected_service_id, self.ledger.schema) == (self.service, self.schema)
        )
        self.ledger.require_transaction(self.transaction)

    def scope(self, attempt: CompositionAttemptIdentity):
        self.check()
        original = require_existing_execution_in(
            self.ledger, attempt, expected_service_id=self.service, terminal_validator=self.ledger.terminal_validator
        )
        self.check()
        if original[1].state in {"SUCCEEDED", "FAILED"}:
            self.ledger.terminal_validator(self.ledger, *original)
            self.check()
        return original


class ClickHouseTerminalStore:
    """Nested observation, aggregate persistence and independently derived results.

    The supplied generation loader must be capture.load_generation_in: it uses
    this ledger and verifies source originals on the enrolled capture volume.
    It must not open another SQL transaction or substitute cached authority.
    """

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        expected_service_id: str,
        target: SnapshotTarget,
        capture_store: MssqlSnapshotCaptureStore,
        publication_store: MssqlSnapshotPublicationStore,
        load_generation_in: Callable[[CompositionMssqlLedger, CompositionAttemptIdentity, str], SnapshotGeneration],
        control_schema: str = "dpone_control",
    ) -> None:
        target.__post_init__()
        self._factory, self._service, self._schema = (
            connection_factory,
            expected_service_id,
            require_control_schema(control_schema),
        )
        self._target, self._capture, self._publication, self._generation = (
            target,
            capture_store,
            publication_store,
            load_generation_in,
        )

    def _transaction(self):
        return composition_control_transaction(self._factory, self._schema, self._service)

    def require_issued(self, attempt: CompositionAttemptIdentity) -> None:
        """Require both retained bindings without issuing or reconstructing users."""
        with self._transaction() as ledger:
            q = _Read(ledger, self._service, self._schema)
            before = q.scope(attempt)
            expected = tuple(
                sorted(
                    CompositionProofAuthority(
                        "clickhouse",
                        self._target.service_id,
                        "clickhouse-user:"
                        + ClickHouseGateQueries(ledger, ClickHouseGateBinding(attempt, self._target, p)).binding_id(),
                    )
                    for p in ("ingest", "publisher")
                )
            )
            _require(
                len(set(expected)) == 2
                and read_issued_authorities_in(
                    ledger, attempt, expected_service_id=self._service, transaction_id=q.transaction
                )
                == expected
            )
            _require(q.scope(attempt) == before)
            q.check()

    def _originals(self, q: _Read, attempt: CompositionAttemptIdentity) -> _Originals:
        q.check()
        subject, events = self._capture.read_in(q.ledger, attempt)
        q.check()
        _require(subject.attempt == attempt and subject.target == self._target)
        capture = canonical_json_bytes(
            {
                "schema": "dpone.composition-remote-capture-originals.v1",
                **{
                    name: _ref(events[phase])
                    for name, phase in (
                        ("subject", "CLAIMED"),
                        ("captured", "CAPTURED"),
                        ("generation_seal", "GENERATION_SEALED"),
                    )
                },
            }
        )
        publications = self._publication.records_in(q.ledger)
        q.check()
        _require(len(publications) == 1 and publications[0].state == "PUBLISHED")
        publication = publications[0]
        generation = self._generation(q.ledger, attempt, publication.intent.generation.record_sha256)
        q.check()
        _require(generation == publication.intent.generation)
        purpose = {}
        for name in ("ingest", "publisher"):
            purpose[name] = read_purpose_closure_in(q.ledger, attempt, self._target, name)
            q.check()
        closures = {
            field: {name: _proof_ref(purpose[name].proofs[index], purpose[name].evidence_document) for name in purpose}
            for index, field in enumerate(("closed_gates", "quiescence"))
        }
        nested = decode_publication_originals(
            capture,
            publication.to_bytes(),
            attempt=attempt,
            closed_gates=closures["closed_gates"],
            quiescence=closures["quiescence"],
        )
        authorities = tuple(sorted((publication.intent.ingest_principal, publication.intent.publisher_principal)))
        _require(
            len(set(authorities)) == 2
            and read_issued_authorities_in(
                q.ledger, attempt, expected_service_id=self._service, transaction_id=q.transaction
            )
            == authorities
        )
        q.check()
        documents = tuple(
            canonical_json_bytes(
                {
                    "schema": "dpone.composition-clickhouse-terminal-closure.v1",
                    "kind": kind,
                    "attempt_sha256": attempt.attempt_sha256,
                    "ingest_proof_sha256": closures[field]["ingest"]["sha256"],
                    "publisher_proof_sha256": closures[field]["publisher"]["sha256"],
                }
            )
            for kind, field in (("CLOSED_GATES", "closed_gates"), ("QUIESCENCE", "quiescence"))
        )
        documents += (
            canonical_json_bytes(
                {
                    "schema": "dpone.composition-clickhouse-terminal-outcome.v1",
                    "attempt_sha256": attempt.attempt_sha256,
                    "state": "SUCCEEDED",
                    "capture_sha256": evidence_digest(capture),
                    "publication_sha256": publication.record_sha256,
                }
            ),
        )
        proofs = tuple(
            CompositionAttemptProof(
                kind,
                attempt.attempt_sha256,
                attempt.activation_request_sha256,
                composition_attempt_epoch_subject(attempt),
                authorities,
                evidence_digest(document),
                "SUCCEEDED" if kind == "OUTCOME" else None,
            )
            for kind, document in zip(("CLOSED_GATES", "QUIESCENCE", "OUTCOME"), documents, strict=True)
        )
        for index, field in enumerate(("closed_gates", "quiescence")):
            closures[field]["terminal"] = _proof_ref(proofs[index], documents[index])
        body = {
            "schema": "dpone.composition-remote-transfer-result.v1",
            "attempt_document": strict_json_object(encode_attempt_identity(attempt)),
            "attempt_sha256": attempt.attempt_sha256,
            "capture_document": strict_json_object(capture),
            "capture_sha256": evidence_digest(capture),
            "publication_document": strict_json_object(publication.to_bytes()),
            "publication_sha256": publication.record_sha256,
            **closures,
            "outcome": _proof_ref(proofs[2], documents[2]),
            "rows": nested.captured.rows,
        }
        return _Originals(canonical_json_bytes(body), TerminalProofTriplet(*proofs), documents)

    def _persist(self, q: _Read, originals: _Originals, *, create: bool) -> None:
        for proof, document in zip(originals.proofs.proofs, originals.documents, strict=True):
            q.check()
            persist_execution_proof(q.ledger, proof, document, create=create)
            q.check()

    def persist_success(self, attempt: CompositionAttemptIdentity) -> TerminalProofTriplet:
        """Persist all three originals while SQL still says ACTIVE/RUNNING."""
        with self._transaction() as ledger:
            q = _Read(ledger, self._service, self._schema)
            before = q.scope(attempt)
            _require(before[0].receipt.state == "ACTIVE" and before[1].state == "RUNNING")
            originals = self._originals(q, attempt)
            self._persist(q, originals, create=True)
            _require(self._originals(q, attempt) == originals and q.scope(attempt) == before)
        with self._transaction() as ledger:
            q = _Read(ledger, self._service, self._schema)
            _require(q.scope(attempt) == before)
            _require(self._originals(q, attempt) == originals)
            self._persist(q, originals, create=False)
            _require(q.scope(attempt) == before)
        return originals.proofs

    def _read(self, q: _Read, attempt: CompositionAttemptIdentity) -> tuple[_Originals, CompositionAttemptReceipt]:
        before = q.scope(attempt)
        originals = self._originals(q, attempt)
        self._persist(q, originals, create=False)
        _require(self._originals(q, attempt) == originals and q.scope(attempt) == before)
        return originals, before[1]

    def read_proofs(self, attempt: CompositionAttemptIdentity) -> TerminalProofTriplet:
        with self._transaction() as ledger:
            originals, _ = self._read(_Read(ledger, self._service, self._schema), attempt)
        return originals.proofs

    def read_result(self, attempt: CompositionAttemptIdentity) -> RemoteTransferResult:
        """Build a successful result only from the actual SUCCEEDED SQL receipt."""
        with self._transaction() as ledger:
            originals, receipt = self._read(_Read(ledger, self._service, self._schema), attempt)
            _require(receipt.state == "SUCCEEDED")
            body = strict_json_object(originals.body)
            observation = canonical_json_bytes(
                {
                    "schema": "dpone.composition-remote-attempt-observation.v1",
                    "attempt_document": body["attempt_document"],
                    "attempt_sha256": attempt.attempt_sha256,
                    "state": receipt.state,
                    "closed_gates_sha256": receipt.closed_gates_sha256,
                    "quiescence_sha256": receipt.quiescence_sha256,
                    "outcome_evidence_sha256": receipt.outcome_evidence_sha256,
                }
            )
            body.update(
                terminal_receipt_document=strict_json_object(observation),
                terminal_receipt_sha256=evidence_digest(observation),
            )
            document = canonical_json_bytes(body)
            result = decode_result(document, evidence_digest(document), attempt=attempt)
        return result
