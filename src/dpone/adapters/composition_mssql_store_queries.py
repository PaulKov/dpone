"""SQL Server composition ledger statements and exact protected row readback.

The caller supplies a cursor inside the fixed, transaction-owned ledger lock.
No helper acquires writer-session authority or modifies enrollment identities.
"""

from __future__ import annotations

from dataclasses import asdict

from dpone.adapters.composition_mssql_schema import (
    COMPOSITION_MSSQL_LEDGER_LOCK,
    COMPOSITION_MSSQL_SCHEMA_VERSION,
    require_control_schema,
)
from dpone.adapters.dbapi_lifecycle import row
from dpone.contracts.composition_control import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionAttemptReceipt,
    CompositionPhysicalResource,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
    decode_activation_request,
    decode_attempt_identity,
    decode_attempt_proof,
    encode_activation_request,
    require_composition_attempt_scope,
)
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.ports.sql_connection import SqlControlCursor


def resource_document(resource: CompositionPhysicalResource) -> bytes:
    """Keep the original physical observation and exact write partition as bytes."""
    return canonical_json_bytes(asdict(resource))


class CompositionMssqlLedger:
    """Bound cursor for one serialized transaction against an enrolled ledger."""

    def __init__(self, cursor: SqlControlCursor, control_schema: str) -> None:
        self.cursor = cursor
        self.schema = require_control_schema(control_schema)

    def table(self, name: str) -> str:
        return f"[{self.schema}].[composition_{name}]"

    def begin(self, expected_service_id: str) -> None:
        """Require the fixed ledger lock and provisioned version/service identity."""
        self.cursor.execute(
            "SET XACT_ABORT ON; SET NOCOUNT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; "
            "IF @@TRANCOUNT = 0 BEGIN TRANSACTION;"
        )
        self.cursor.execute(
            "DECLARE @result int; EXEC @result = sys.sp_getapplock "
            "@Resource = ?, @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 0; "
            "SELECT @result;",
            COMPOSITION_MSSQL_LEDGER_LOCK,
        )
        result = row(self.cursor)
        if result is None or len(result) != 1 or type(result[0]) is not int or result[0] < 0:
            raise CompositionAdmissionError("ledger_lock")
        self.cursor.execute(
            f"SELECT singleton, schema_version, LOWER(CONVERT(char(36), service_id)) "
            f"FROM {self.table('authority')} WITH (HOLDLOCK);"
        )
        if tuple(tuple(item) for item in self.cursor.fetchall()) != (
            (1, COMPOSITION_MSSQL_SCHEMA_VERSION, expected_service_id),
        ):
            raise CompositionAdmissionError("control_authority")
        self.cursor.execute(
            "SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID(?) "
            "AND name IN ('composition_authority', 'composition_domains', 'composition_activations', "
            "'composition_activation_domains', 'composition_attempts', 'composition_attempt_domains', "
            "'composition_proofs', 'composition_issued_authorities');",
            self.schema,
        )
        if row(self.cursor) != (8,):
            raise CompositionAdmissionError("control_schema")

    def read(self, activation_id: str) -> CompositionActivationOccurrence | None:
        """Read historical closure separately from current enrolled domain rows."""
        self.cursor.execute(
            f"SELECT request_sha256, request_document, state FROM {self.table('activations')} WITH (HOLDLOCK) "
            "WHERE activation_id = ?;",
            activation_id,
        )
        record = row(self.cursor)
        if record is None:
            return None
        digest, document, state = record
        request = decode_activation_request(document, digest)
        if request.activation_id != activation_id:
            raise CompositionAdmissionError("occurrence_identity")
        self.cursor.execute(
            f"SELECT guard_id, resource_document, fencing_epoch FROM {self.table('activation_domains')} "
            "WITH (HOLDLOCK) WHERE activation_id = ? ORDER BY guard_id;",
            activation_id,
        )
        records = tuple(tuple(item) for item in self.cursor.fetchall())
        if len(records) != len(request.resources):
            raise CompositionAdmissionError("guard_partition")
        epochs = []
        for resource, record in zip(request.resources, records, strict=True):
            guard, document, epoch = record
            if (
                (guard, document) != (resource.guard_id, resource_document(resource))
                or type(epoch) is not int
                or epoch < 1
            ):
                raise CompositionAdmissionError("guard_readback")
            current_epoch, owner = self.domain(resource)
            if state == "RETIRED":
                if current_epoch < epoch or owner == activation_id or (current_epoch == epoch and owner is not None):
                    raise CompositionAdmissionError("guard_readback")
            elif (current_epoch, owner) != (epoch, activation_id):
                raise CompositionAdmissionError("guard_readback")
            epochs.append((guard, epoch))
        occurrence = CompositionActivationOccurrence(
            request, CompositionActivationReceipt(digest, state, tuple(epochs))
        )
        if state == "RETIRED":
            self.require_terminal(occurrence)
        return occurrence

    def domain(self, resource: CompositionPhysicalResource) -> tuple[int, str | None]:
        """Require an already enrolled, exact physical identity; never upsert it."""
        self.cursor.execute(
            "SELECT connector, LOWER(CONVERT(char(36), service_id)), physical_subject_sha256, fencing_epoch, "
            f"LOWER(CONVERT(char(36), owner_activation_id)) FROM {self.table('domains')} WITH (UPDLOCK, HOLDLOCK) "
            "WHERE guard_id = ?;",
            resource.guard_id,
        )
        record = row(self.cursor)
        if record is None or record[:3] != (resource.connector, resource.service_id, resource.physical_subject_sha256):
            raise CompositionAdmissionError("domain_enrollment")
        epoch, owner = record[3:]
        if type(epoch) is not int or epoch < 0 or epoch > 9223372036854775807 or (owner is not None and epoch == 0):
            raise CompositionAdmissionError("guard_epoch")
        return epoch, owner

    def prepare(self, request: CompositionActivationRequest) -> None:
        """Reserve only unowned enrolled domains, recording the complete union."""
        domains = [(resource, *self.domain(resource)) for resource in request.resources]
        if any(owner is not None or epoch == 9223372036854775807 for _, epoch, owner in domains):
            raise CompositionAdmissionError("guard_conflict")
        self.require_unblocked(request)
        self.cursor.execute(
            f"INSERT INTO {self.table('activations')} (activation_id, request_sha256, request_document, state) "
            "VALUES (?, ?, ?, 'PREPARED');",
            request.activation_id,
            request.request_sha256,
            encode_activation_request(request),
        )
        for resource, epoch, _ in domains:
            self.cursor.execute(
                f"UPDATE {self.table('domains')} SET fencing_epoch = ?, owner_activation_id = ? "
                "OUTPUT inserted.guard_id WHERE guard_id = ? AND fencing_epoch = ? AND owner_activation_id IS NULL;",
                epoch + 1,
                request.activation_id,
                resource.guard_id,
                epoch,
            )
            if row(self.cursor) != (resource.guard_id,):
                raise CompositionAdmissionError("guard_conflict")
            self.cursor.execute(
                f"INSERT INTO {self.table('activation_domains')} "
                "(activation_id, guard_id, resource_document, fencing_epoch) VALUES (?, ?, ?, ?);",
                request.activation_id,
                resource.guard_id,
                resource_document(resource),
                epoch + 1,
            )

    def transition(self, occurrence: CompositionActivationOccurrence, state: str) -> None:
        """Compare the entire immutable request plus previous state before update."""
        request = occurrence.request
        document = encode_activation_request(request)
        self.cursor.execute(
            f"UPDATE {self.table('activations')} SET state = ? OUTPUT inserted.state "
            "WHERE activation_id = ? AND request_sha256 = ? AND request_document = ? "
            "AND DATALENGTH(request_document) = ? AND state = ?;",
            state,
            request.activation_id,
            request.request_sha256,
            document,
            len(document),
            occurrence.receipt.state,
        )
        if row(self.cursor) != (state,):
            raise CompositionAdmissionError("occurrence_transition")

    def release(self, occurrence: CompositionActivationOccurrence) -> None:
        """Release exact owned epochs without resetting their monotonic history."""
        for guard, epoch in occurrence.receipt.guard_epochs:
            self.cursor.execute(
                f"UPDATE {self.table('domains')} SET owner_activation_id = NULL OUTPUT inserted.guard_id "
                "WHERE guard_id = ? AND fencing_epoch = ? AND owner_activation_id = ?;",
                guard,
                epoch,
                occurrence.request.activation_id,
            )
            if row(self.cursor) != (guard,):
                raise CompositionAdmissionError("guard_release")

    def require_terminal(self, occurrence: CompositionActivationOccurrence) -> None:
        """Audit all durable attempts, exact scopes and the selected proof triplets."""
        self.cursor.execute(
            "SELECT attempt_sha256, LOWER(CONVERT(char(36), activation_id)), activation_request_sha256, "
            "guard_epochs_sha256, attempt_document, state, closed_gates_sha256, quiescence_sha256, "
            f"outcome_evidence_sha256 FROM {self.table('attempts')} WITH (UPDLOCK, HOLDLOCK) "
            "WHERE activation_id = ? OR activation_request_sha256 = ? ORDER BY attempt_sha256;",
            occurrence.request.activation_id,
            occurrence.request.request_sha256,
        )
        attempts = tuple(tuple(item) for item in self.cursor.fetchall())
        for digest, activation_id, parent, epochs, document, state, closed, quiescent, outcome in attempts:
            attempt = decode_attempt_identity(document, digest)
            if (activation_id, parent, epochs) != (
                occurrence.request.activation_id,
                occurrence.request.request_sha256,
                composition_attempt_epoch_subject(attempt),
            ):
                raise CompositionAdmissionError("terminal_attempt_identity")
            guards = require_composition_attempt_scope(occurrence, attempt)
            receipt = CompositionAttemptReceipt(attempt, state, closed, quiescent, outcome)
            if receipt.state not in {"SUCCEEDED", "FAILED"}:
                raise CompositionAdmissionError("unresolved_attempt")
            self.cursor.execute(
                f"SELECT guard_id, fencing_epoch FROM {self.table('attempt_domains')} WITH (HOLDLOCK) "
                "WHERE attempt_sha256 = ? ORDER BY guard_id;",
                digest,
            )
            if tuple(tuple(item) for item in self.cursor.fetchall()) != attempt.guard_epochs:
                raise CompositionAdmissionError("terminal_attempt_partition")
            services = {
                (row.connector, row.service_id) for row in occurrence.request.resources if row.guard_id in guards
            }
            self.require_proofs(attempt, services, (closed, quiescent, outcome), expected_outcome_state=state)

    def require_unblocked(self, request: CompositionActivationRequest) -> None:
        """Audit previous parents before trusting available domain ownership.

        Historical activation partitions remain authoritative when a damaged
        attempt partition disappears. Terminal labels and proof digests alone
        cannot acknowledge complete writer closure.
        """
        parents: set[str] = set()
        for resource in request.resources:
            self.cursor.execute(
                f"SELECT LOWER(CONVERT(char(36), activation_id)) FROM {self.table('activation_domains')} "
                "WITH (HOLDLOCK) WHERE guard_id = ?;",
                resource.guard_id,
            )
            parents.update(item[0] for item in self.cursor.fetchall())
        parents.discard(request.activation_id)
        for activation_id in sorted(parents):
            occurrence = self.read(activation_id)
            if occurrence is None:
                raise CompositionAdmissionError("historical_occurrence_missing")
            if occurrence.receipt.state != "RETIRED":
                raise CompositionAdmissionError("historical_occurrence_unresolved")
        # Retain an independent rejection path for orphaned attempt journals.
        for resource in request.resources:
            self.cursor.execute(
                f"SELECT TOP (1) a.attempt_sha256 FROM {self.table('attempts')} a WITH (UPDLOCK, HOLDLOCK) "
                f"JOIN {self.table('attempt_domains')} d WITH (HOLDLOCK) ON d.attempt_sha256 = a.attempt_sha256 "
                "WHERE d.guard_id = ? AND (a.state NOT IN ('SUCCEEDED', 'FAILED') OR a.closed_gates_sha256 IS NULL "
                "OR a.quiescence_sha256 IS NULL OR a.outcome_evidence_sha256 IS NULL);",
                resource.guard_id,
            )
            if row(self.cursor) is not None:
                raise CompositionAdmissionError("unresolved_attempt")

    def require_proofs(
        self,
        attempt: CompositionAttemptIdentity,
        services: set[tuple[str, str]],
        proof_hashes: tuple[str, str, str],
        *,
        expected_outcome_state: str,
    ) -> None:
        """Require exact protected issuance and proof scope inside the ledger lock.

        ``services`` comes from independently read parent resources selected by
        the verified attempt scope. Hashes select the durable terminal triplet;
        they do not themselves prove gate closure, quiescence or an outcome.
        """
        self.cursor.execute(
            "SELECT connector, LOWER(CONVERT(char(36), service_id)), principal_id "
            f"FROM {self.table('issued_authorities')} WITH (HOLDLOCK) WHERE attempt_sha256 = ?;",
            attempt.attempt_sha256,
        )
        issued = tuple(sorted(CompositionProofAuthority(*item) for item in self.cursor.fetchall()))
        if (
            not issued
            or len(set(issued)) != len(issued)
            or {(row.connector, row.service_id) for row in issued} != services
        ):
            raise CompositionAdmissionError("terminal_issued_authorities")
        for kind, digest in zip(("CLOSED_GATES", "QUIESCENCE", "OUTCOME"), proof_hashes, strict=True):
            self.cursor.execute(
                "SELECT activation_request_sha256, guard_epochs_sha256, proof_document "
                f"FROM {self.table('proofs')} WITH (HOLDLOCK) WHERE attempt_sha256 = ? AND kind = ? AND proof_sha256 = ?;",
                attempt.attempt_sha256,
                kind,
                digest,
            )
            records = tuple(tuple(item) for item in self.cursor.fetchall())
            if len(records) != 1 or records[0][:2] != (
                attempt.activation_request_sha256,
                composition_attempt_epoch_subject(attempt),
            ):
                raise CompositionAdmissionError("terminal_proof_identity")
            proof = decode_attempt_proof(records[0][2], digest).require_attempt(attempt)
            if proof.kind != kind or proof.authorities != issued:
                raise CompositionAdmissionError("terminal_proof_authorities")
            if kind == "OUTCOME" and proof.outcome_state != expected_outcome_state:
                raise CompositionAdmissionError("terminal_outcome_state")
