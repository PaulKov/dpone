"""Independent receipt and typed content readback for the bounded transfer cell.

The supervisor reopens exact retained extraction bytes, reconstructs their
consumed-payload manifest, and compares it to the committed generic receipt.
Actual target rows are read under a fresh stable transaction. Executor results,
row counts alone and later live source contents cannot authorize success.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpone.adapters.composition_mssql_transfer_outcome import CompositionTransferObservation
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_mssql_binding import CompositionMssqlOperationBinding
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.composition_transfer_payload import RetainedTransferPayload


class CompositionTransferCommittedObserver:
    """Observe one protected binding through independently reopened originals.

    ``read_binding`` must reopen the protected registered binding and verify the
    sealed source plan. ``transaction`` yields a fresh target DB-API connection;
    ``require_target`` checks its physical service/database authority on that
    same connection. ``read_receipt`` reads the generic receipt there, joining
    its state attempt row; it must never return a cached executor receipt.
    """

    def __init__(
        self,
        *,
        read_binding: Callable[..., Any],
        transaction: Callable[..., Any],
        read_receipt: Callable[..., Any],
        require_target: Callable[..., Any],
        read_payload: Callable[..., RetainedTransferPayload] | None,
        verify_retained_commit: Callable[..., None] | None = None,
        max_rows: int = 1_000_000,
        max_row_bytes: int = 1024 * 1024,
    ) -> None:
        self._binding, self._transaction, self._receipt = read_binding, transaction, read_receipt
        self._target, self._payload = require_target, read_payload
        self._verify_preplan = verify_retained_commit
        if type(max_rows) is not int or max_rows <= 0 or type(max_row_bytes) is not int or max_row_bytes <= 0:
            raise CompositionAdmissionError("transfer_observation_budget")
        self._max_rows, self._max_row_bytes = max_rows, max_row_bytes

    @property
    def proves_outcome(self) -> bool:
        return all(
            callable(value) for value in (self._binding, self._transaction, self._receipt, self._target, self._payload)
        )

    def __call__(self, attempt: Any) -> CompositionTransferObservation:
        if not self.proves_outcome or self._payload is None:
            raise CompositionAdmissionError("transfer_payload_unavailable")
        bound = self._binding(attempt)
        if type(bound) is not CompositionMssqlOperationBinding or bound.attempt != attempt:
            raise CompositionAdmissionError("transfer_observation_binding")
        bound.__post_init__()
        payload = self._payload(attempt)
        if type(payload) is not RetainedTransferPayload:
            raise CompositionAdmissionError("transfer_payload_unavailable")
        with self._transaction() as connection:
            cursor = connection.cursor()
            try:
                connection.autocommit = False
                cursor.execute(
                    "SET XACT_ABORT ON; SET LOCK_TIMEOUT 10000; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;"
                )
                # A real catalog/marker read opens the driver's implicit
                # transaction. Never nest it with an explicit SQL BEGIN.
                self._target(connection, bound)
                state = _transaction_state(cursor)
                receipt = self._receipt(connection, bound)
                if receipt is None:
                    return CompositionTransferObservation(False, False, False, False, False)
                bound.require_receipt(receipt)
                if self._verify_preplan is not None:
                    self._verify_preplan(connection, bound, receipt, payload)
                columns = _columns(cursor, bound)
                names = ",".join(_quote(column["target_name"]) for column in columns)
                target = ".".join(
                    _quote(value) for value in (bound.write.database, bound.write.schema, bound.write.relation)
                )
                cursor.execute(f"SELECT {names} FROM {target} WITH (HOLDLOCK);")
                reconciled = payload.reconcile_native(
                    columns, iter(cursor.fetchone, None), max_rows=self._max_rows, max_row_bytes=self._max_row_bytes
                )
                complete = reconciled.evidence
                native = complete.native_contract_sha256
                if (
                    native is None
                    or bytes.fromhex(complete.manifest_sha256) != receipt.payload_evidence.manifest_sha256
                    or bytes.fromhex(native) != receipt.payload_evidence.native_contract_sha256
                ):
                    raise CompositionAdmissionError("transfer_payload_receipt")
                source_rows, target_rows = reconciled.source_rows, reconciled.target_rows
                source_digest, target_digest = reconciled.source_digest, reconciled.target_digest
                self._target(connection, bound)
                if self._verify_preplan is not None:
                    self._verify_preplan(connection, bound, receipt, payload)
                if (
                    connection.autocommit is not False
                    or _transaction_state(cursor) != state
                    or self._receipt(connection, bound) != receipt
                ):
                    raise CompositionAdmissionError("transfer_observation_changed")
                if self._binding(attempt) != bound:
                    raise CompositionAdmissionError("transfer_observation_binding")
                counts = (
                    receipt.payload_evidence.declared_rows,
                    receipt.payload_evidence.actual_raw_rows,
                    receipt.payload_evidence.actual_native_rows,
                    receipt.metrics.inserted_rows,
                    receipt.metrics.total_rows,
                )
                rows_match = all(value == source_rows == target_rows for value in counts)
                document = canonical_json_bytes(
                    {
                        "attempt_sha256": attempt.attempt_sha256,
                        "binding_sha256": bound.digest.hex(),
                        "receipt_id": receipt.receipt_id,
                        "payload_manifest_sha256": complete.manifest_sha256,
                        "source_rows": source_rows,
                        "target_rows": target_rows,
                        "source_content_sha256": source_digest,
                        "target_content_sha256": target_digest,
                        "columns": columns,
                        "transaction_id": state[0],
                    }
                )
                return CompositionTransferObservation(
                    True, rows_match, source_digest == target_digest, False, False, document
                )
            finally:
                try:
                    connection.rollback()
                finally:
                    cursor.close()


def _quote(value: str | None) -> str:
    if type(value) is not str or not value or len(value) > 128 or "\x00" in value:
        raise CompositionAdmissionError("transfer_observation_identifier")
    return "[" + value.replace("]", "]]") + "]"


def _transaction_state(cursor: Any) -> tuple[Any, ...]:
    cursor.execute("SELECT CURRENT_TRANSACTION_ID(),@@TRANCOUNT,XACT_STATE();")
    rows = tuple(tuple(row) for row in cursor.fetchall())
    if (
        len(rows) != 1
        or len(rows[0]) != 3
        or any(type(value) is not int for value in rows[0])
        or rows[0][0] <= 0
        or rows[0][1:] != (1, 1)
    ):
        raise CompositionAdmissionError("transfer_observation_transaction")
    return rows[0]


def _columns(cursor: Any, bound: CompositionMssqlOperationBinding) -> list[dict[str, Any]]:
    from dpone.adapters.composition_mssql_dbt_materialization import _actual_type

    database = _quote(bound.write.database)
    cursor.execute(
        f"SELECT c.name,t.name,c.max_length,c.precision,c.scale,CONVERT(int,c.is_nullable),c.collation_name "
        f"FROM {database}.sys.columns c JOIN {database}.sys.types t ON t.user_type_id=c.user_type_id "
        f"JOIN {database}.sys.tables o ON o.object_id=c.object_id JOIN {database}.sys.schemas s ON s.schema_id=o.schema_id "
        "WHERE s.name=? AND o.name=? AND c.is_computed=0 AND c.is_hidden=0 AND c.generated_always_type=0 "
        "AND c.encryption_type IS NULL AND t.is_user_defined=0 ORDER BY c.column_id;",
        bound.write.schema,
        bound.write.relation,
    )
    rows = tuple(tuple(row) for row in cursor.fetchall())
    if not 1 <= len(rows) <= 1024 or any(len(row) != 7 for row in rows):
        raise CompositionAdmissionError("transfer_observation_columns")
    return [
        {
            "wire_name": row[0],
            "target_name": row[0],
            "target_type": _actual_type(
                {
                    "system_type": row[1],
                    "max_length": row[2],
                    "precision": row[3],
                    "scale": row[4],
                    "user_defined": False,
                    "encryption_type": None,
                }
            ),
            "nullable": bool(row[5]),
            "collation": row[6],
            "generation_contract": None,
        }
        for row in rows
    ]
