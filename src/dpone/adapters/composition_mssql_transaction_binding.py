"""Controller-only registration of exact source-verified transfer originals.

Registration commits before any business write. The callback reopens the real
worker plan and verifies invocation, physical target and mutation-plan mapping;
structural caller values are never accepted as producer evidence on their own.
"""

from collections.abc import Callable
from typing import Protocol

from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_transaction_fence_schema import require_transaction_fence_schema
from dpone.adapters.composition_mssql_transfer_outcome import resolve_transfer_principal
from dpone.contracts.composition_control import (
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionExecutionPlan,
    DbtRelationWrite,
    dbt_relation_write_subject,
    encode_activation_request,
    encode_attempt_identity,
)
from dpone.contracts.composition_mssql_binding import CompositionMssqlOperationBinding
from dpone.contracts.composition_ownership import CompositionOwnerReference
from dpone.contracts.mssql_transaction_governance import MssqlTransactionOperation
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.ports.sql_connection import SqlControlConnection


class VerifyTransferOperation(Protocol):
    """Trusted producer boundary; raise if any generic execution field differs."""

    def __call__(
        self,
        attempt: CompositionAttemptIdentity,
        operation: MssqlTransactionOperation,
        write: DbtRelationWrite,
        mutation_plan_sha256: bytes,
        plan: CompositionExecutionPlan,
    ) -> None:
        """Reopen the sealed worker plan and verify actual physical/mutation identity."""
        ...


class MssqlCompositionTransactionBindings:
    """Retain an immutable binding after complete parent and retained-history audit."""

    def __init__(
        self,
        connection_factory: Callable[[], SqlControlConnection],
        *,
        expected_service_id: str,
        control_database: str,
        read_plan: Callable[[CompositionAttemptIdentity], CompositionExecutionPlan],
        verify_operation: VerifyTransferOperation,
        control_schema: str = "dpone_control",
    ) -> None:
        self._factory = connection_factory
        self._service = expected_service_id
        self._database = control_database
        self._schema = control_schema
        self._read_plan = read_plan
        self._verify_operation = verify_operation

    def bind(
        self,
        attempt: CompositionAttemptIdentity,
        operation: MssqlTransactionOperation,
        write: DbtRelationWrite,
        mutation_plan_sha256: bytes,
        *,
        preplan_document_sha256: str | None = None,
    ) -> CompositionMssqlOperationBinding:
        """Verify source membership and current issued identity, then commit once.

        An uncertain commit raises; retry rereads the complete original. No
        worker login, permissions, execution attempt or schema is created here.
        """
        plan = self._read_plan(attempt)
        if type(plan) is not CompositionExecutionPlan:
            raise CompositionAdmissionError("transfer_source_plan")
        plan.__post_init__()
        self._verify_operation(attempt, operation, write, mutation_plan_sha256, plan)
        with composition_control_transaction(self._factory, self._schema, self._service) as ledger:
            cursor = ledger.cursor
            cursor.execute("SELECT DB_NAME();")
            if tuple(tuple(row) for row in cursor.fetchall()) != ((self._database,),):
                raise CompositionAdmissionError("control_database")
            require_transaction_fence_schema(cursor, self._schema)
            occurrence, receipt = require_existing_execution_in(
                ledger, attempt, expected_service_id=self._service, terminal_validator=ledger.terminal_validator
            )
            occurrence.require_state("ACTIVE")
            if receipt.state != "RUNNING":
                raise CompositionAdmissionError("attempt_not_running")
            subject = dbt_relation_write_subject(write)
            workloads = [row for row in occurrence.request.workloads if row.workload_id == attempt.workload_id]
            if (
                plan.sources.subject_sha256 != occurrence.request.source_subject_sha256
                or plan.workloads != occurrence.request.workloads
                or write not in plan.writes
                or len(workloads) != 1
                or workloads[0].pack_sha256 != attempt.pack_sha256
                or subject not in workloads[0].write_subjects
                or workloads[0].execution_cell != "postgres_mssql_full_refresh_v1"
            ):
                raise CompositionAdmissionError("transfer_workload_membership")
            cursor.execute(
                "SELECT TOP (2) g.login_sid FROM "
                + ledger.table("login_gates")
                + " g WITH (HOLDLOCK) JOIN "
                + ledger.table("issued_authorities")
                + " a WITH (HOLDLOCK) ON a.operation_key=g.operation_key "
                "WHERE g.operation_key=? AND g.operation_family='execution' AND g.gate_state='READY' "
                "AND g.disabled_evidence_sha256 IS NULL AND a.connector='mssql' AND a.service_id=? "
                "AND a.principal_id='mssql-sid:'+LOWER(CONVERT(varchar(32),g.login_sid,2));",
                attempt.attempt_sha256,
                self._service,
            )
            rows = tuple(tuple(row) for row in cursor.fetchall())
            if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not bytes or len(rows[0][0]) != 16:
                raise CompositionAdmissionError("transfer_issued_sid")
            binding = CompositionMssqlOperationBinding(
                attempt,
                operation,
                write,
                mutation_plan_sha256,
                rows[0][0],
                self._service,
                self._database,
                encode_activation_request(occurrence.request),
                preplan_document_sha256,
            )
            owner_key = CompositionOwnerReference("execution", occurrence.request.activation_id).owner_key
            request = operation.attempt.request
            values = (
                binding.digest,
                binding.document,
                attempt.attempt_sha256,
                owner_key,
                binding.parent_document,
                encode_attempt_identity(attempt),
                self._service,
                binding.issued_sid,
                request.target_database,
                request.target_schema,
                request.target_table,
                canonical_json_bytes(attempt.guard_epochs).decode("utf-8"),
            )
            table = ledger.table("transfer_bindings")
            cursor.execute(
                f"SELECT TOP (2) binding_document FROM {table} WITH (UPDLOCK,HOLDLOCK) WHERE binding_sha256=?;",
                binding.digest,
            )
            existing = tuple(tuple(row) for row in cursor.fetchall())
            if existing and existing != ((binding.document,),):
                raise CompositionAdmissionError("transfer_binding_original")
            if not existing:
                cursor.execute(
                    f"INSERT INTO {table} (binding_sha256,binding_document,operation_key,owner_key,"
                    "owner_document,attempt_document,service_id,login_sid,target_database,target_schema,target_table,guard_epochs) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);",
                    *values,
                )
            cursor.execute(
                f"SELECT TOP (2) binding_sha256,binding_document,operation_key,owner_key,owner_document,"
                "attempt_document,LOWER(CONVERT(char(36),service_id)),login_sid,target_database,target_schema,"
                f"target_table,guard_epochs FROM {table} WITH (HOLDLOCK) WHERE binding_sha256=?;",
                binding.digest,
            )
            if tuple(tuple(row) for row in cursor.fetchall()) != (values,):
                raise CompositionAdmissionError("transfer_binding_original")
            return binding

    def read_closed(self, attempt: CompositionAttemptIdentity) -> CompositionMssqlOperationBinding:
        """Reopen protected originals and closed principal evidence on one transaction."""
        plan = self._read_plan(attempt)
        with composition_control_transaction(self._factory, self._schema, self._service) as ledger:
            occurrence, receipt = require_existing_execution_in(
                ledger,
                attempt,
                expected_service_id=self._service,
                terminal_validator=ledger.terminal_validator,
            )
            if occurrence.receipt.state not in {"ACTIVE", "RETIRING"} or receipt.state not in {
                "RUNNING",
                "COMMIT_UNKNOWN",
            }:
                raise CompositionAdmissionError("transfer_observation_attempt")
            ledger.cursor.execute("SELECT DB_NAME();")
            if tuple(tuple(row) for row in ledger.cursor.fetchall()) != ((self._database,),):
                raise CompositionAdmissionError("control_database")
            principal = resolve_transfer_principal(ledger, attempt, self._service)
            ledger.cursor.execute(
                f"SELECT TOP (2) binding_sha256,binding_document FROM {ledger.table('transfer_bindings')} WITH (HOLDLOCK) WHERE operation_key=?;",
                attempt.attempt_sha256,
            )
            rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
            if len(rows) != 1 or len(rows[0]) != 2 or type(rows[0][1]) is not bytes:
                raise CompositionAdmissionError("transfer_binding_original")
            bound = CompositionMssqlOperationBinding.from_bytes(rows[0][1], attempt)
            if (
                bound.digest != rows[0][0]
                or bound.service_id != self._service
                or bound.control_database != self._database
                or "mssql-sid:" + bound.issued_sid.hex() != principal
            ):
                raise CompositionAdmissionError("transfer_binding_original")
            if (
                plan.sources.subject_sha256 != occurrence.request.source_subject_sha256
                or plan.workloads != occurrence.request.workloads
                or bound.write not in plan.writes
            ):
                raise CompositionAdmissionError("transfer_source_plan")
            self._verify_operation(attempt, bound.operation, bound.write, bound.mutation_plan_sha256, plan)
            return bound
