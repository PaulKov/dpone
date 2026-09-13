"""Narrow issued-worker control authority on the actual target transaction.

Only the external owner-executed procedure touches control rows. The worker
receives EXECUTE on that single module, not controller credentials or table
privileges. This boundary never starts, commits or rolls back a transaction.
"""

from dataclasses import dataclass
from typing import Any

from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_transaction_fence_schema import TRANSFER_PROCEDURE
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_mssql_binding import CompositionMssqlOperationBinding
from dpone.contracts.mssql_transaction_governance import MssqlGenericCommitReceipt, MssqlTransactionOperation


@dataclass(frozen=True)
class MssqlCompositionTransactionFence:
    """Exactly one pre-registered binding; live checks reuse no cached admission."""

    binding: CompositionMssqlOperationBinding
    control_schema: str = "dpone_control"

    def __post_init__(self) -> None:
        if type(self.binding) is not CompositionMssqlOperationBinding:
            raise CompositionAdmissionError("transfer_binding")
        self.binding.__post_init__()
        require_control_schema(self.control_schema)
        require_control_schema(self.binding.control_database)

    def require_current(
        self,
        connector: Any,
        operation: MssqlTransactionOperation | None = None,
        *,
        transaction_id: int | None = None,
        receipt: MssqlGenericCommitReceipt | None = None,
        mutation_plan_sha256: bytes | None = None,
    ) -> int:
        """Reject foreign operations/replays before SQL; retain the exact target txn.

        Normal writes must supply both operation and the verified mutation plan
        digest. Receipt replay instead supplies the complete immutable receipt.
        The returned transaction ID is required again immediately before receipt
        insertion; only the generic finalizer may commit or roll back.
        """
        self.__post_init__()
        if receipt is not None:
            self.binding.require_receipt(receipt)
        if operation is not None:
            if mutation_plan_sha256 is None:
                raise CompositionAdmissionError("transfer_mutation_plan")
            self.binding.require_operation(operation, mutation_plan_sha256)
        elif receipt is None:
            raise CompositionAdmissionError("transfer_operation")
        if transaction_id is not None and (type(transaction_id) is not int or not 0 < transaction_id < 2**63):
            raise CompositionAdmissionError("shared_transaction_identity")
        connection = connector.connection
        if connection.autocommit is not False:
            raise CompositionAdmissionError("shared_transaction")
        cursor = connection.cursor()
        try:
            observed = self._target_transaction(cursor, transaction_id)
            request = self.binding.operation.attempt.request
            cursor.execute(
                f"EXEC [{self.binding.control_database}].[{self.control_schema}].[{TRANSFER_PROCEDURE}] "
                "@binding_sha256=?,@binding_document=?,@transaction_id=?,@target_database=?,"
                "@target_schema=?,@target_table=?;",
                self.binding.digest,
                self.binding.document,
                observed,
                request.target_database,
                request.target_schema,
                request.target_table,
            )
            rows = tuple(tuple(row) for row in cursor.fetchall())
            if rows != ((observed, self.binding.digest),) or type(rows[0][0]) is not int:
                raise CompositionAdmissionError("transfer_module_result")
            self._target_transaction(cursor, observed)
            return observed
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("transfer_fence_unavailable") from None
        finally:
            cursor.close()

    def _target_transaction(self, cursor: Any, expected: int | None) -> int:
        cursor.execute(
            "SELECT DB_NAME(),@@TRANCOUNT,XACT_STATE(),CURRENT_TRANSACTION_ID(),SUSER_SID(ORIGINAL_LOGIN());"
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if (
            len(rows) != 1
            or len(rows[0]) != 5
            or rows[0][0] != self.binding.operation.attempt.request.target_database
            or type(rows[0][1]) is not int
            or rows[0][1] < 1
            or type(rows[0][2]) is not int
            or rows[0][2] != 1
            or type(rows[0][3]) is not int
            or not 0 < rows[0][3] < 2**63
            or rows[0][4] != self.binding.issued_sid
        ):
            raise CompositionAdmissionError("shared_target_transaction")
        if expected is not None and rows[0][3] != expected:
            raise CompositionAdmissionError("shared_transaction_identity")
        return rows[0][3]
