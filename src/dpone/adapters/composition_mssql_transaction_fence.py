"""Protected composition ownership inside the actual MSSQL target transaction.

The app binds one admitted attempt and its co-located control database. This is
an additional fence, never an attempt issuer or a replacement for target/state
identity validation. The shared ledger lock intentionally serializes composition
control and target commits until a finer-grained protected protocol is approved.
"""

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.mssql_transaction_governance import MssqlTransactionOperation


@dataclass(frozen=True)
class MssqlCompositionTransactionFence:
    """Immutable app binding; every check reads SQL rather than cached authority.

    ``target_database`` is the independently verified target/control database;
    cross-database authority is unsupported. ``transaction_id`` returned by the
    first check must be supplied again immediately before receipt insertion.
    Connector begin/commit/rollback remain exclusively owned by the finalizer.
    """

    attempt: CompositionAttemptIdentity
    expected_service_id: str
    target_database: str
    control_schema: str = "dpone_control"

    def __post_init__(self) -> None:
        if type(self.attempt) is not CompositionAttemptIdentity:
            raise CompositionAdmissionError("attempt_identity")
        self.attempt.__post_init__()
        try:
            valid = str(UUID(self.expected_service_id)) == self.expected_service_id
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            raise CompositionAdmissionError("control_service_id")
        require_control_schema(self.control_schema)
        if type(self.target_database) is not str or not self.target_database.strip():
            raise CompositionAdmissionError("target_database")

    def require_current(
        self,
        connector: Any,
        operation: MssqlTransactionOperation | None = None,
        *,
        transaction_id: int | None = None,
    ) -> int:
        """Lock and audit exact parent, operation, partitions and retained history.

        This method opens no independent connection and never commits. Even an
        existing receipt replay must pass ACTIVE/RUNNING on a read transaction.
        An unknown/terminal attempt cannot authorize a second executor.
        """
        if operation is not None and operation.attempt.request.target_database != self.target_database:
            raise CompositionAdmissionError("target_database")
        connection = connector.connection
        if connection.autocommit is not False:
            raise CompositionAdmissionError("shared_transaction")
        cursor = connection.cursor()
        try:
            self._require_database(cursor, transaction_id)
            ledger = CompositionMssqlLedger(cursor, self.control_schema)
            observed = ledger.begin(self.expected_service_id)
            if transaction_id is not None and observed != transaction_id:
                raise CompositionAdmissionError("shared_transaction_identity")
            occurrence, receipt = require_existing_execution_in(
                ledger,
                self.attempt,
                expected_service_id=self.expected_service_id,
                terminal_validator=ledger.terminal_validator,
            )
            occurrence.require_state("ACTIVE")
            if receipt.state != "RUNNING":
                raise CompositionAdmissionError("attempt_not_running")
            self._require_database(cursor, observed)
            ledger.require_transaction(observed)
            return observed
        finally:
            cursor.close()

    def _require_database(self, cursor: Any, transaction_id: int | None) -> None:
        cursor.execute("SELECT DB_NAME(), CURRENT_TRANSACTION_ID();")
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if len(rows) != 1 or len(rows[0]) != 2 or rows[0][0] != self.target_database:
            raise CompositionAdmissionError("target_database")
        if transaction_id is not None and rows[0][1] != transaction_id:
            raise CompositionAdmissionError("shared_transaction_identity")
