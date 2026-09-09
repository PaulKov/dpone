"""Facade for generic SQL Server attempts, operation fences, and receipts."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from dpone.contracts.mssql_transaction_governance import (
    MssqlAttemptRequest,
    MssqlGenericCommitReceipt,
    MssqlOperationClaimRejected,
    MssqlOperationRequest,
    MssqlPayloadCommitEvidence,
    MssqlReceiptMetrics,
    MssqlSourceLifecycleEvidence,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)
from dpone.runtime.state.mssql_fresh_session import MssqlFreshSessionFactory
from dpone.runtime.state.mssql_generic_attempt_state import (
    MssqlAttemptAllocationOutcomeUnknown,
    MssqlGenericAttemptState,
)
from dpone.runtime.state.mssql_generic_operation_history import (
    MssqlAttemptOperationHistory,
    MssqlGenericOperationHistoryReader,
)
from dpone.runtime.state.mssql_generic_operation_state import (
    MssqlGenericOperationState,
    MssqlOperationClaimOutcomeUnknown,
)
from dpone.runtime.state.mssql_generic_transaction_contract import require_generic_transaction_catalog
from dpone.runtime.state.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    FENCE_TABLE,
    OPERATION_TABLE,
    RECEIPT_TABLE,
)


class MssqlGenericTransactionState:
    """Coordinate the external catalog without owning business transactions."""

    def __init__(
        self,
        connector: Any,
        *,
        database: str,
        schema: str,
        fresh_session_factory: MssqlFreshSessionFactory | None = None,
    ) -> None:
        factory = fresh_session_factory or MssqlFreshSessionFactory()
        self.connector = connector
        self.database = database
        self.schema = schema
        self.attempts = MssqlGenericAttemptState(
            connector,
            database=database,
            schema=schema,
            fresh_session_factory=factory,
        )
        self.operations = MssqlGenericOperationState(
            connector,
            database=database,
            schema=schema,
            fresh_session_factory=factory,
        )

    @classmethod
    def from_state_storage(
        cls,
        state_storage: Any,
        *,
        fresh_session_factory: MssqlFreshSessionFactory | None = None,
    ) -> MssqlGenericTransactionState:
        connector = getattr(state_storage, "connector", None)
        database = str(getattr(state_storage, "database", "") or "").strip()
        schema = str(getattr(state_storage, "schema", "") or "").strip()
        if connector is None:
            raise ValueError("mssql_transaction.state_connector_missing")
        return cls(
            connector,
            database=database,
            schema=schema,
            fresh_session_factory=fresh_session_factory,
        )

    def preflight(self, target_connector: Any) -> None:
        """Verify exact catalog plus target-session cross-database capability."""

        require_generic_transaction_catalog(self.connector, database=self.database, schema=self.schema)
        self._permission_probe(target_connector)

    def admit(
        self,
        attempt_request: MssqlAttemptRequest,
        operation_request: MssqlOperationRequest,
    ) -> MssqlTransactionAdmission:
        """Suppress exact replay or claim current generation/owner before source I/O."""

        attempt = self._allocate_on_fresh_session(attempt_request)
        if not attempt.is_current_generation:
            receipt = self._receipt_on_fresh_session(attempt, operation_request)
            if receipt is None:
                raise RuntimeError("mssql_transaction.stale_attempt_generation_pre_source")
            self._validate_replay(receipt, attempt_request, operation_request)
            return MssqlTransactionAdmission(replay_receipt=receipt)
        outcome = self._claim_on_fresh_session(attempt, operation_request)
        if isinstance(outcome, MssqlGenericCommitReceipt):
            self._validate_replay(outcome, attempt_request, operation_request)
            return MssqlTransactionAdmission(replay_receipt=outcome)
        return MssqlTransactionAdmission(operation=outcome)

    def replay_if_committed(
        self,
        attempt_request: MssqlAttemptRequest,
        operation_request: MssqlOperationRequest,
    ) -> MssqlTransactionAdmission | None:
        """Probe the deterministic receipt key without allocating or claiming state."""

        receipt = self._receipt_by_key_on_fresh_session(operation_request.operation_key(attempt_request))
        if receipt is None:
            return None
        self._validate_replay(receipt, attempt_request, operation_request)
        return MssqlTransactionAdmission(replay_receipt=receipt)

    def operation_history(
        self,
        attempt_request: MssqlAttemptRequest,
    ) -> MssqlAttemptOperationHistory | None:
        """Read one exact attempt's operation scopes on a fresh session."""

        with self.operations.fresh_sessions.open(self.connector) as session:
            reader = MssqlGenericOperationHistoryReader(
                session,
                database=self.database,
                schema=self.schema,
                operation_key_projector=lambda request, scope_hash: MssqlOperationRequest(
                    scope_hash,
                    bytes(32),
                ).operation_key(request),
                fresh_session_factory=self.operations.fresh_sessions,
            )
            return reader.read(attempt_request)

    def _allocate_on_fresh_session(self, request: MssqlAttemptRequest) -> MssqlTransactionAttempt:
        with self.attempts.fresh_sessions.open(self.connector) as session:
            state = MssqlGenericAttemptState(
                session,
                database=self.database,
                schema=self.schema,
                fresh_session_factory=self.attempts.fresh_sessions,
            )
            return state.allocate(request)

    def _receipt_on_fresh_session(
        self,
        attempt: MssqlTransactionAttempt,
        request: MssqlOperationRequest,
    ) -> MssqlGenericCommitReceipt | None:
        with self.operations.fresh_sessions.open(self.connector) as session:
            state = MssqlGenericOperationState(
                session,
                database=self.database,
                schema=self.schema,
                fresh_session_factory=self.operations.fresh_sessions,
            )
            return state.receipt_by_key(request.operation_key(attempt), connector=session)

    def _receipt_by_key_on_fresh_session(self, operation_key: bytes) -> MssqlGenericCommitReceipt | None:
        with self.operations.fresh_sessions.open(self.connector) as session:
            state = MssqlGenericOperationState(
                session,
                database=self.database,
                schema=self.schema,
                fresh_session_factory=self.operations.fresh_sessions,
            )
            return state.receipt_by_key(operation_key, connector=session)

    def _claim_on_fresh_session(
        self,
        attempt: MssqlTransactionAttempt,
        request: MssqlOperationRequest,
    ) -> MssqlTransactionOperation | MssqlGenericCommitReceipt:
        with self.operations.fresh_sessions.open(self.connector) as session:
            state = MssqlGenericOperationState(
                session,
                database=self.database,
                schema=self.schema,
                fresh_session_factory=self.operations.fresh_sessions,
            )
            return state.claim_or_replay(attempt, request)

    def assert_current(
        self,
        executor: Any,
        operation: MssqlTransactionOperation,
        *,
        require_unexpired_lease: bool = True,
    ) -> None:
        self.operations.assert_current(
            executor,
            operation,
            require_unexpired_lease=require_unexpired_lease,
        )

    def renew_operation_lease(
        self,
        operation: MssqlTransactionOperation,
        *,
        lease_expires_at_utc: datetime,
    ) -> bool:
        """Heartbeat a still-current owner; return false after an exact commit receipt."""

        request = MssqlOperationRequest(
            scope_hash=operation.scope_hash,
            owner_digest=operation.owner_digest,
            lease_expires_at_utc=lease_expires_at_utc,
        )
        outcome = self._claim_on_fresh_session(operation.attempt, request)
        if isinstance(outcome, MssqlGenericCommitReceipt):
            self._validate_replay(outcome, operation.attempt.request, request)
            return False
        if outcome.epoch != operation.epoch or outcome.owner_digest != operation.owner_digest:
            raise RuntimeError("mssql_transaction.operation_heartbeat_epoch_changed")
        return True

    def insert_receipt(
        self,
        executor: Any,
        operation: MssqlTransactionOperation,
        *,
        load_id: str,
        payload_evidence: MssqlPayloadCommitEvidence,
        source_lifecycle: MssqlSourceLifecycleEvidence,
        mutation_plan_sha256: bytes,
        target_before_sha256: bytes,
        target_after_sha256: bytes,
        loaded_at_utc: datetime,
        metrics: MssqlReceiptMetrics,
    ) -> MssqlGenericCommitReceipt:
        return self.operations.insert_receipt(
            executor,
            operation,
            load_id=load_id,
            payload_evidence=payload_evidence,
            source_lifecycle=source_lifecycle,
            mutation_plan_sha256=mutation_plan_sha256,
            target_before_sha256=target_before_sha256,
            target_after_sha256=target_after_sha256,
            loaded_at_utc=loaded_at_utc,
            metrics=metrics,
        )

    def probe_receipt(
        self,
        operation: MssqlTransactionOperation,
        *,
        connector: Any | None = None,
        require_owner: bool = False,
    ) -> MssqlGenericCommitReceipt | None:
        return self.operations.probe_receipt(
            operation,
            connector=connector,
            require_owner=require_owner,
        )

    def probe_receipt_fresh(
        self,
        operation: MssqlTransactionOperation,
    ) -> MssqlGenericCommitReceipt | None:
        return self.operations.probe_receipt_fresh(operation)

    @staticmethod
    def _validate_replay(
        receipt: MssqlGenericCommitReceipt,
        attempt: MssqlAttemptRequest,
        operation: MssqlOperationRequest,
    ) -> None:
        exact = (
            receipt.receipt_id == f"mssql-generic-v1:{operation.operation_key(attempt).hex()}"
            and receipt.operation_key == operation.operation_key(attempt)
            and receipt.attempt_key == attempt.attempt_key
            and receipt.target_identity == attempt.target_identity
            and receipt.scope_hash == operation.scope_hash
            and receipt.route_fingerprint == attempt.route_fingerprint
            and receipt.strategy == attempt.strategy
        )
        if not exact:
            raise RuntimeError("mssql_transaction.receipt_mismatch")

    def _permission_probe(self, connector: Any) -> None:
        statements = ["SET NOCOUNT ON;"]
        for table, column, mode in (
            (self.operations.qualified(FENCE_TABLE), "target_identity", "write"),
            (self.operations.qualified(ATTEMPT_TABLE), "attempt_key", "insert"),
            (self.operations.qualified(OPERATION_TABLE), "operation_key", "write"),
            (self.operations.qualified(RECEIPT_TABLE), "receipt_id", "insert"),
        ):
            quoted = connector.quote_identifier(column)
            statements.append(f"SELECT TOP (0) {quoted} FROM {table};")
            if mode == "write":
                statements.append(f"UPDATE {table} SET {quoted} = {quoted} WHERE 1 = 0;")
            statements.append(f"INSERT INTO {table} ({quoted}) SELECT {quoted} FROM {table} WHERE 1 = 0;")
        started = False
        try:
            connector.begin()
            started = True
            connector.execute_query("\n".join(statements))
        except Exception as exc:
            raise RuntimeError("mssql_transaction.cross_database_permission_denied") from exc
        finally:
            if started:
                try:
                    connector.rollback()
                except Exception as exc:
                    raise RuntimeError("mssql_transaction.permission_probe_rollback_failed") from exc


__all__ = [
    "MssqlAttemptAllocationOutcomeUnknown",
    "MssqlGenericTransactionState",
    "MssqlOperationClaimOutcomeUnknown",
    "MssqlOperationClaimRejected",
]
