"""SQL Server attempt admission against the complete protected composition ledger.

The transaction lock serializes short control operations across both backends;
execution never holds it. A RUNNING replay is reconciliation input, never a new
executor permit. These adapters require a trusted, separately provisioned control
connection and do not create schema, enrollment or business credentials.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from uuid import UUID

from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_operations import require_execution_attempt_in
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.composition_mssql_terminal import (
    require_execution_terminal_in,
    select_terminal_hashes_in,
)
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
    encode_attempt_identity,
)
from dpone.contracts.composition_ownership import CompositionOwnerReference
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
        transaction = ledger.begin(service_id)
        yield ledger
        ledger.require_transaction(transaction)
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
    """Inspect an already reserved operation; this cannot admit a fresh attempt."""
    occurrence, _ = require_existing_execution_in(
        ledger, attempt, expected_service_id=ledger.expected_service_id, terminal_validator=ledger.terminal_validator
    )
    return occurrence


def read_attempt(ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt:
    """Preserve exact existing receipt readback, including complete shared history."""
    _, receipt = require_existing_execution_in(
        ledger, attempt, expected_service_id=ledger.expected_service_id, terminal_validator=ledger.terminal_validator
    )
    return receipt


def require_terminal_attempt(
    ledger: CompositionMssqlLedger, occurrence: CompositionActivationOccurrence, receipt: CompositionAttemptReceipt
) -> None:
    """Keep the old helper while binding its configured control identity explicitly."""
    require_execution_terminal_in(ledger, occurrence, receipt, expected_service_id=ledger.expected_service_id)


def reserve_composition_attempt(ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity) -> None:
    """Reserve a fresh operation and complete partition in the caller transaction.

    Neither the insert nor its exact same-transaction readback is a durable ACK.
    The owning boundary commits once and independently reconciles the result.
    """
    occurrence = require_execution_attempt_in(
        ledger, attempt, expected_service_id=ledger.expected_service_id, terminal_validator=ledger.terminal_validator
    )
    owner = CompositionOwnerReference("execution", occurrence.request.activation_id)
    ledger.cursor.execute(
        f"INSERT INTO {ledger.table('operations')} "
        "(operation_key, operation_family, owner_key, owner_subject_sha256, replay_key, operation_document, state) "
        "VALUES (?, 'execution', ?, ?, ?, ?, 'RUNNING');",
        attempt.attempt_sha256,
        owner.owner_key,
        attempt.activation_request_sha256,
        attempt.attempt_sha256,
        encode_attempt_identity(attempt),
    )
    for guard, epoch in attempt.guard_epochs:
        ledger.cursor.execute(
            f"INSERT INTO {ledger.table('operation_domains')} (operation_key, owner_key, guard_id, fencing_epoch) "
            "VALUES (?, ?, ?, ?);",
            attempt.attempt_sha256,
            owner.owner_key,
            guard,
            epoch,
        )
    observed, receipt = require_existing_execution_in(
        ledger, attempt, expected_service_id=ledger.expected_service_id, terminal_validator=ledger.terminal_validator
    )
    if observed != occurrence or receipt != CompositionAttemptReceipt(attempt, "RUNNING"):
        raise CompositionAdmissionError("attempt_admission_readback")


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
            occurrence, receipt = require_existing_execution_in(
                ledger, attempt, expected_service_id=self._service_id, terminal_validator=ledger.terminal_validator
            )
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
            occurrence, current = require_existing_execution_in(
                ledger, attempt, expected_service_id=self._service_id, terminal_validator=ledger.terminal_validator
            )
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
                    "DECLARE @changed TABLE (state varchar(16)); "
                    f"UPDATE {ledger.table('operations')} SET state = ?, closed_gates_sha256 = ?, quiescence_sha256 = ?, "
                    "outcome_evidence_sha256 = ? OUTPUT inserted.state INTO @changed "
                    "WHERE operation_key = ? AND operation_family = 'execution' AND state = ?; SELECT state FROM @changed;",
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
        return select_terminal_hashes_in(ledger, attempt, outcome, expected_service_id=ledger.expected_service_id)
