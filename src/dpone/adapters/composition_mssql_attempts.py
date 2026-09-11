"""SQL Server attempt admission against the complete protected composition ledger.

The transaction lock serializes short control operations across both backends;
execution never holds it. A RUNNING replay is reconciliation input, never a new
executor permit. These adapters require a trusted, separately provisioned control
connection and do not create schema, enrollment or business credentials.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.dbapi_lifecycle import (
    close,
    rollback,
    row,
)
from dpone.contracts.composition_control import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionAttemptReceipt,
    composition_attempt_epoch_subject,
    decode_attempt_identity,
    decode_attempt_proof,
    encode_attempt_identity,
    require_composition_attempt_admission,
    require_composition_attempt_scope,
)
from dpone.ports.sql_connection import SqlControlConnection

ConnectionFactory = Callable[[], SqlControlConnection]


@contextmanager
def composition_control_transaction(
    factory: ConnectionFactory,
    schema: str,
    service_id: str,
) -> Iterator[CompositionMssqlLedger]:
    """Commit once; sanitize uncertain driver failures without fabricating ACKs."""
    connection = None
    cursor = None
    try:
        connection = factory()
        connection.autocommit = False
        cursor = connection.cursor()
        ledger = CompositionMssqlLedger(cursor, schema)
        ledger.begin(service_id)
        yield ledger
        connection.commit()
    except CompositionAdmissionError:
        rollback(connection)
        raise
    except Exception:
        rollback(connection)
        raise CompositionAdmissionError("control_operation_unknown") from None
    finally:
        close(cursor)
        close(connection)


def read_attempt_occurrence(
    ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity
) -> CompositionActivationOccurrence:
    """Resolve the parent only from protected canonical request identity."""
    ledger.cursor.execute(
        f"SELECT LOWER(CONVERT(char(36), activation_id)) FROM {ledger.table('activations')} WITH (HOLDLOCK) "
        "WHERE request_sha256 = ?;",
        attempt.activation_request_sha256,
    )
    records = tuple(ledger.cursor.fetchall())
    if len(records) != 1:
        raise CompositionAdmissionError("attempt_parent")
    occurrence = ledger.read(records[0][0])
    if occurrence is None:
        raise CompositionAdmissionError("attempt_parent")
    require_composition_attempt_scope(occurrence, attempt)
    return occurrence


def _receipt(ledger: CompositionMssqlLedger, record: tuple[Any, ...]) -> CompositionAttemptReceipt:
    digest, parent, epochs, document, state, closed, quiescent, outcome, activation_id = record
    attempt = decode_attempt_identity(document, digest)
    if (parent, epochs) != (attempt.activation_request_sha256, composition_attempt_epoch_subject(attempt)):
        raise CompositionAdmissionError("attempt_identity")
    ledger.cursor.execute(
        f"SELECT request_sha256 FROM {ledger.table('activations')} WITH (HOLDLOCK) WHERE activation_id=?;",
        activation_id,
    )
    if row(ledger.cursor) != (parent,):
        raise CompositionAdmissionError("attempt_parent_identity")
    ledger.cursor.execute(
        f"SELECT guard_id, fencing_epoch FROM {ledger.table('attempt_domains')} WITH (HOLDLOCK) "
        "WHERE attempt_sha256 = ? ORDER BY guard_id;",
        attempt.attempt_sha256,
    )
    if tuple(tuple(value) for value in ledger.cursor.fetchall()) != attempt.guard_epochs:
        raise CompositionAdmissionError("attempt_partition")
    return CompositionAttemptReceipt(attempt, state, closed, quiescent, outcome)


def read_attempt(ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt:
    """Read exact attempt bytes and every reserved guard; no new admission."""
    ledger.cursor.execute(
        "SELECT attempt_sha256, activation_request_sha256, guard_epochs_sha256, attempt_document, state, "
        "closed_gates_sha256, quiescence_sha256, outcome_evidence_sha256, LOWER(CONVERT(char(36), activation_id)) "
        f"FROM {ledger.table('attempts')} "
        "WITH (HOLDLOCK) WHERE attempt_sha256 = ?;",
        attempt.attempt_sha256,
    )
    record = row(ledger.cursor)
    if record is None:
        raise CompositionAdmissionError("attempt_missing")
    receipt = _receipt(ledger, record)
    if receipt.attempt != attempt:
        raise CompositionAdmissionError("attempt_identity")
    return receipt


def require_terminal_attempt(
    ledger: CompositionMssqlLedger,
    occurrence: CompositionActivationOccurrence,
    receipt: CompositionAttemptReceipt,
) -> None:
    """A terminal label cannot free a writer scope without its protected proofs."""
    guards = require_composition_attempt_scope(occurrence, receipt.attempt)
    closed, quiescent, outcome = receipt.closed_gates_sha256, receipt.quiescence_sha256, receipt.outcome_evidence_sha256
    if closed is None or quiescent is None or outcome is None:
        raise CompositionAdmissionError("terminal_evidence")
    services = {
        (resource.connector, resource.service_id)
        for resource in occurrence.request.resources
        if resource.guard_id in guards
    }
    ledger.require_proofs(receipt.attempt, services, (closed, quiescent, outcome), expected_outcome_state=receipt.state)


def reserve_composition_attempt(ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity) -> None:
    """Insert RUNNING and every guard inside the caller's protected transaction.

    The caller must hold the existing ledger lock and commit all related budget
    reservations atomically. This function commits nothing and grants no
    executor; only a fresh acknowledged operation plus independent exact
    readback may admit execution. Replays follow the existing rejection policy.
    """
    attempt.__post_init__()
    occurrence = read_attempt_occurrence(ledger, attempt)
    ledger.cursor.execute(
        "SELECT attempt_sha256, activation_request_sha256, guard_epochs_sha256, attempt_document, state, "
        "closed_gates_sha256, quiescence_sha256, outcome_evidence_sha256, LOWER(CONVERT(char(36), activation_id)) "
        f"FROM {ledger.table('attempts')} "
        "WITH (UPDLOCK, HOLDLOCK) ORDER BY attempt_sha256;",
    )
    records = tuple(tuple(value) for value in ledger.cursor.fetchall())
    existing = tuple(_receipt(ledger, record) for record in records)
    require_composition_attempt_admission(occurrence, attempt, existing)
    requested_guards = {guard for guard, _ in attempt.guard_epochs}
    for receipt in existing:
        if receipt.state in {"SUCCEEDED", "FAILED"} and requested_guards.intersection(
            guard for guard, _ in receipt.attempt.guard_epochs
        ):
            prior = (
                occurrence
                if receipt.attempt.activation_request_sha256 == attempt.activation_request_sha256
                else read_attempt_occurrence(ledger, receipt.attempt)
            )
            require_terminal_attempt(ledger, prior, receipt)
    ledger.cursor.execute(
        f"INSERT INTO {ledger.table('attempts')} (attempt_sha256, activation_id, activation_request_sha256, "
        "guard_epochs_sha256, attempt_document, state) VALUES (?, ?, ?, ?, ?, 'RUNNING');",
        attempt.attempt_sha256,
        occurrence.request.activation_id,
        attempt.activation_request_sha256,
        composition_attempt_epoch_subject(attempt),
        encode_attempt_identity(attempt),
    )
    for guard, epoch in attempt.guard_epochs:
        ledger.cursor.execute(
            f"INSERT INTO {ledger.table('attempt_domains')} (attempt_sha256, guard_id, fencing_epoch) "
            "VALUES (?, ?, ?);",
            attempt.attempt_sha256,
            guard,
            epoch,
        )


class MssqlCompositionAttemptStore:
    """Persist one RUNNING reservation and audit trusted terminal proof triplets."""

    def __init__(
        self, connection_factory: ConnectionFactory, *, expected_service_id: str, control_schema: str = "dpone_control"
    ) -> None:
        if str(UUID(expected_service_id)) != expected_service_id:
            raise CompositionAdmissionError("control_service")
        self._factory = connection_factory
        self._service_id = expected_service_id
        self._schema = require_control_schema(control_schema)

    def admit_once(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt:
        """Reserve all selected guards atomically before any credential issuance."""
        attempt.__post_init__()
        with composition_control_transaction(self._factory, self._schema, self._service_id) as ledger:
            reserve_composition_attempt(ledger, attempt)
        receipt = self.read_exact(attempt)
        if receipt.state != "RUNNING":
            raise CompositionAdmissionError("attempt_admission_readback")
        return receipt

    def read_exact(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt:
        """Fresh durable audit; RUNNING readback does not authorize replay."""
        attempt.__post_init__()
        with composition_control_transaction(self._factory, self._schema, self._service_id) as ledger:
            occurrence = read_attempt_occurrence(ledger, attempt)
            receipt = read_attempt(ledger, attempt)
            if receipt.state in {"SUCCEEDED", "FAILED"}:
                require_terminal_attempt(ledger, occurrence, receipt)
            return receipt

    def finalize(
        self, attempt: CompositionAttemptIdentity, *, state: str, outcome_evidence_sha256: str
    ) -> CompositionAttemptReceipt:
        """Select protected outcome proof by hash, never accept an unbacked digest.

        All three selected proofs must cover the complete immutable issuance
        journal, including ClickHouse. SQL gate closure cannot produce an outcome
        or authorize missing cross-backend proof. Unknown commits remain blocking.
        """
        if state not in {"SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}:
            raise CompositionAdmissionError("attempt_terminal_state")
        return self._finish(attempt, state=state, outcome=outcome_evidence_sha256, previous_state="RUNNING")

    def reconcile_unknown(
        self,
        attempt: CompositionAttemptIdentity,
        *,
        state: str,
        outcome_evidence_sha256: str,
    ) -> CompositionAttemptReceipt:
        """Explicitly resolve a protected unknown using a new trusted outcome.

        ``outcome_evidence_sha256`` selects the protected OUTCOME proof digest;
        its inner ``evidence_sha256`` identifies actual producer evidence. Both
        original unknown and new terminal proof triplets are independently
        audited. This method grants no executor, resets no password and changes
        no previously successful/failed attempt.
        """
        if state not in {"SUCCEEDED", "FAILED"}:
            raise CompositionAdmissionError("attempt_reconciliation_state")
        return self._finish(attempt, state=state, outcome=outcome_evidence_sha256, previous_state="COMMIT_UNKNOWN")

    def _finish(
        self, attempt: CompositionAttemptIdentity, *, state: str, outcome: str, previous_state: str
    ) -> CompositionAttemptReceipt:
        with composition_control_transaction(self._factory, self._schema, self._service_id) as ledger:
            occurrence = read_attempt_occurrence(ledger, attempt)
            current = read_attempt(ledger, attempt)
            if previous_state == "COMMIT_UNKNOWN":
                if current.state != "COMMIT_UNKNOWN":
                    raise CompositionAdmissionError("attempt_unknown_required")
                require_terminal_attempt(ledger, occurrence, current)
            hashes = self._terminal_hashes(ledger, attempt, outcome)
            guards = {guard for guard, _ in attempt.guard_epochs}
            services = {
                (resource.connector, resource.service_id)
                for resource in occurrence.request.resources
                if resource.guard_id in guards
            }
            ledger.require_proofs(attempt, services, hashes, expected_outcome_state=state)
            result = CompositionAttemptReceipt(attempt, state, *hashes)
            if current.state != previous_state:
                if current != result:
                    raise CompositionAdmissionError("attempt_terminal_replay")
            else:
                ledger.cursor.execute(
                    f"UPDATE {ledger.table('attempts')} SET state = ?, closed_gates_sha256 = ?, quiescence_sha256 = ?, "
                    "outcome_evidence_sha256 = ? OUTPUT inserted.state WHERE attempt_sha256 = ? AND state = ?;",
                    state,
                    *hashes,
                    attempt.attempt_sha256,
                    previous_state,
                )
                if row(ledger.cursor) != (state,):
                    raise CompositionAdmissionError("attempt_terminal_transition")
        observed = self.read_exact(attempt)
        if observed != result:
            raise CompositionAdmissionError("attempt_terminal_readback")
        return observed

    @staticmethod
    def _terminal_hashes(
        ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity, outcome: str
    ) -> tuple[str, str, str]:
        from dpone.contracts.composition_control import CompositionProofAuthority

        ledger.cursor.execute(
            "SELECT connector, LOWER(CONVERT(char(36), service_id)), principal_id "
            f"FROM {ledger.table('issued_authorities')} WITH (HOLDLOCK) WHERE attempt_sha256 = ?;",
            attempt.attempt_sha256,
        )
        issued = tuple(sorted(CompositionProofAuthority(*value) for value in ledger.cursor.fetchall()))
        selected = []
        for kind in ("CLOSED_GATES", "QUIESCENCE", "OUTCOME"):
            ledger.cursor.execute(
                f"SELECT proof_sha256, proof_document FROM {ledger.table('proofs')} WITH (HOLDLOCK) "
                "WHERE attempt_sha256 = ? AND kind = ?;",
                attempt.attempt_sha256,
                kind,
            )
            matches = []
            for digest, document in ledger.cursor.fetchall():
                proof = decode_attempt_proof(document, digest).require_attempt(attempt)
                if proof.kind != kind:
                    raise CompositionAdmissionError("attempt_proof_kind")
                if proof.authorities == issued and (kind != "OUTCOME" or digest == outcome):
                    matches.append(digest)
            if len(matches) != 1:
                raise CompositionAdmissionError("attempt_terminal_proof")
            selected.append(matches[0])
        return selected[0], selected[1], selected[2]
