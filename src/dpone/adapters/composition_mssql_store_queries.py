"""Existing execution API over the single protected shared SQL ownership journal.

The ledger carries a configured service pin, never cached lock authority. Every
shared read reobserves its actual transaction. Mutations follow complete original
history checks, preserve legacy documents and use exact epoch/state comparisons.
"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from dpone.adapters.composition_mssql_catalog import require_composition_mssql_schema
from dpone.adapters.composition_mssql_existing_operation import require_retired_execution_in
from dpone.adapters.composition_mssql_operations import require_execution_history_in
from dpone.adapters.composition_mssql_ownership import read_shared_domain_in, read_shared_owner_in
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_LEDGER_LOCK, require_control_schema
from dpone.adapters.composition_mssql_terminal import require_execution_terminal_in, require_proofs_in
from dpone.adapters.composition_mssql_transaction import require_shared_transaction_in
from dpone.adapters.dbapi_lifecycle import row
from dpone.contracts.composition_control import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionPhysicalResource,
    encode_activation_request,
)
from dpone.contracts.composition_ownership import CompositionOwnerReference
from dpone.contracts.composition_persistence import encode_physical_resource as resource_document

if TYPE_CHECKING:
    from dpone.ports.composition_sql import ExecutionTerminalValidator
    from dpone.ports.sql_connection import SqlControlCursor


class CompositionMssqlLedger:
    """One caller-owned cursor in the common transaction and protected namespace."""

    def __init__(self, cursor: SqlControlCursor, control_schema: str) -> None:
        self.cursor = cursor
        self.schema = require_control_schema(control_schema)
        self._expected_service_id: str | None = None

    @property
    def expected_service_id(self) -> str:
        """Return configuration only; readers still independently inspect SQL."""
        if self._expected_service_id is None:
            raise CompositionAdmissionError("control_service_id")
        return self._expected_service_id

    @property
    def terminal_validator(self) -> ExecutionTerminalValidator:
        """Bind the configured pin at this composition root, not from observed rows."""
        return partial(require_execution_terminal_in, expected_service_id=self.expected_service_id)

    def table(self, name: str) -> str:
        return f"[{self.schema}].[composition_{name}]"

    def begin(self, expected_service_id: str) -> int:
        """Acquire the unchanged global lock, then audit actual identity/catalog."""
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
        transaction = require_shared_transaction_in(self, expected_service_id=expected_service_id)
        require_composition_mssql_schema(self.cursor, self.schema)
        require_shared_transaction_in(self, expected_service_id=expected_service_id, transaction_id=transaction)
        self._expected_service_id = expected_service_id
        return transaction

    def require_transaction(self, transaction_id: int | None = None) -> int:
        """Observe actual authority again, optionally proving boundary continuity."""
        return require_shared_transaction_in(
            self,
            expected_service_id=self.expected_service_id,
            transaction_id=transaction_id,
        )

    def read(self, activation_id: str) -> CompositionActivationOccurrence | None:
        """Project the exact existing execution document from its observed owner."""
        owner = read_shared_owner_in(
            self, CompositionOwnerReference("execution", activation_id), expected_service_id=self.expected_service_id
        )
        if owner is None:
            return None
        if type(owner.subject) is not CompositionActivationRequest:
            raise CompositionAdmissionError("occurrence_identity")
        occurrence = CompositionActivationOccurrence(
            owner.subject, CompositionActivationReceipt(owner.subject_sha256, owner.state, owner.guard_epochs)
        )
        if owner.state == "RETIRED":
            require_retired_execution_in(
                self,
                occurrence,
                expected_service_id=self.expected_service_id,
                terminal_validator=self.terminal_validator,
            )
        return occurrence

    def domain(self, resource: CompositionPhysicalResource) -> tuple[int, CompositionOwnerReference | None]:
        """Read one original physical identity and its discriminated current owner."""
        domain = read_shared_domain_in(self, resource, expected_service_id=self.expected_service_id)
        return domain.fencing_epoch, domain.owner

    def prepare(self, request: CompositionActivationRequest) -> None:
        """Acquire the complete original scope in one protected transaction."""
        domains = [(resource, *self.domain(resource)) for resource in request.resources]
        if any(owner is not None or epoch == 9223372036854775807 for _, epoch, owner in domains):
            raise CompositionAdmissionError("guard_conflict")
        self.require_unblocked(request)
        owner = CompositionOwnerReference("execution", request.activation_id)
        self.cursor.execute(
            f"INSERT INTO {self.table('owners')} "
            "(owner_key, owner_kind, owner_id, subject_sha256, subject_document, state) "
            "VALUES (?, 'execution', ?, ?, ?, 'PREPARED');",
            owner.owner_key,
            request.activation_id,
            request.request_sha256,
            encode_activation_request(request),
        )
        for resource, epoch, _ in domains:
            self.cursor.execute(
                "DECLARE @changed TABLE (guard_id varchar(71)); "
                f"UPDATE {self.table('domains')} SET fencing_epoch = ?, owner_key = ? "
                "OUTPUT inserted.guard_id INTO @changed "
                "WHERE guard_id = ? AND fencing_epoch = ? AND owner_key IS NULL; SELECT guard_id FROM @changed;",
                epoch + 1,
                owner.owner_key,
                resource.guard_id,
                epoch,
            )
            if row(self.cursor) != (resource.guard_id,):
                raise CompositionAdmissionError("guard_conflict")
            self.cursor.execute(
                f"INSERT INTO {self.table('owner_domains')} "
                "(owner_key, guard_id, claim_document, fencing_epoch) VALUES (?, ?, ?, ?);",
                owner.owner_key,
                resource.guard_id,
                resource_document(resource),
                epoch + 1,
            )

    def transition(self, occurrence: CompositionActivationOccurrence, state: str) -> None:
        """Compare all immutable parent bytes and the previous lifecycle state."""
        require_shared_transaction_in(self, expected_service_id=self.expected_service_id)
        request = occurrence.request
        document = encode_activation_request(request)
        self.cursor.execute(
            "DECLARE @changed TABLE (state varchar(16)); "
            f"UPDATE {self.table('owners')} SET state = ? OUTPUT inserted.state INTO @changed "
            "WHERE owner_key = ? AND owner_kind = 'execution' AND owner_id = ? AND subject_sha256 = ? "
            "AND subject_document = ? AND DATALENGTH(subject_document) = ? AND state = ?; SELECT state FROM @changed;",
            state,
            CompositionOwnerReference("execution", request.activation_id).owner_key,
            request.activation_id,
            request.request_sha256,
            document,
            len(document),
            occurrence.receipt.state,
        )
        if row(self.cursor) != (state,):
            raise CompositionAdmissionError("occurrence_transition")

    def release(self, occurrence: CompositionActivationOccurrence) -> None:
        """Release only the observed owner/epochs; historical partitions remain."""
        require_shared_transaction_in(self, expected_service_id=self.expected_service_id)
        owner = CompositionOwnerReference("execution", occurrence.request.activation_id)
        for guard, epoch in occurrence.receipt.guard_epochs:
            self.cursor.execute(
                "DECLARE @changed TABLE (guard_id varchar(71)); "
                f"UPDATE {self.table('domains')} SET owner_key = NULL OUTPUT inserted.guard_id INTO @changed "
                "WHERE guard_id = ? AND fencing_epoch = ? AND owner_key = ?; SELECT guard_id FROM @changed;",
                guard,
                epoch,
                owner.owner_key,
            )
            if row(self.cursor) != (guard,):
                raise CompositionAdmissionError("guard_release")

    def require_terminal(self, occurrence: CompositionActivationOccurrence) -> None:
        """Retirement requires complete shared history and every selected proof."""
        self.require_unblocked(occurrence.request)

    def require_unblocked(self, request: CompositionActivationRequest) -> None:
        """Audit all original owners/operations before considering scope overlap."""
        require_execution_history_in(
            self,
            request,
            expected_service_id=self.expected_service_id,
            terminal_validator=self.terminal_validator,
        )

    def require_proofs(
        self,
        attempt: CompositionAttemptIdentity,
        services: set[tuple[str, str]],
        proof_hashes: tuple[str, str, str],
        *,
        expected_outcome_state: str,
    ) -> None:
        """Retain the existing helper path with the execution-only proof owner."""
        require_proofs_in(
            self,
            attempt,
            services,
            proof_hashes,
            expected_outcome_state=expected_outcome_state,
            expected_service_id=self.expected_service_id,
        )
