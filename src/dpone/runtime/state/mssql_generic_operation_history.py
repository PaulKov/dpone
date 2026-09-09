"""Read-only historical operation proof for one immutable MSSQL attempt."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dpone.runtime.state.mssql_generic_state_base import MssqlGenericStateBase
from dpone.runtime.state.mssql_generic_transaction_contract import (
    require_generic_transaction_catalog,
)
from dpone.runtime.state.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    OPERATION_TABLE,
    RECEIPT_TABLE,
)
from dpone.runtime.state.mssql_generic_transaction_rows import (
    attempt_from_rows,
    binary,
)

_PAGE_SIZE = 1000


@dataclass(frozen=True, slots=True)
class MssqlAttemptOperationHistory:
    """Stable scope hashes observed under one exact persisted attempt."""

    attempt_key: bytes
    scope_hashes: frozenset[bytes]
    receipt_scope_hashes: frozenset[bytes] = frozenset()

    def __post_init__(self) -> None:
        digests = self.scope_hashes | self.receipt_scope_hashes
        if len(self.attempt_key) != 32 or any(len(value) != 32 for value in digests):
            raise RuntimeError("mssql_transaction.operation_history_digest_invalid")
        if not self.receipt_scope_hashes.issubset(self.scope_hashes):
            raise RuntimeError("mssql_transaction.receipt_history_without_operation")


class MssqlGenericOperationHistoryReader(MssqlGenericStateBase):
    """Page immutable operations under a serializable, rollback-only read."""

    def __init__(
        self,
        connector: Any,
        *,
        database: str,
        schema: str,
        operation_key_projector: Callable[[Any, bytes], bytes],
        fresh_session_factory: Any | None = None,
    ) -> None:
        super().__init__(
            connector,
            database=database,
            schema=schema,
            fresh_session_factory=fresh_session_factory,
        )
        self._operation_key_projector = operation_key_projector

    def read(self, request: Any) -> MssqlAttemptOperationHistory | None:
        """Return exact attempt history without allocating or mutating state."""

        started = False
        primary: BaseException | None = None
        try:
            self.connector.begin()
            started = True
            self.connector.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            require_generic_transaction_catalog(
                self.connector,
                database=self.database,
                schema=self.schema,
            )
            attempt_rows = self.connector.get_records(
                self._attempt_sql(),
                (request.attempt_key,),
                as_dict=True,
            )
            if not attempt_rows:
                return None
            attempt_from_rows(request, attempt_rows)
            scopes, receipts = self._read_scope_pages(request)
            return MssqlAttemptOperationHistory(
                request.attempt_key,
                frozenset(scopes),
                frozenset(receipts),
            )
        except BaseException as exc:
            primary = exc
            raise
        finally:
            if started:
                try:
                    self.connector.rollback()
                except Exception as rollback_error:
                    if primary is None:
                        raise RuntimeError("mssql_transaction.operation_history_rollback_failed") from rollback_error

    def _read_scope_pages(self, request: Any) -> tuple[set[bytes], set[bytes]]:
        scopes: set[bytes] = set()
        receipts: set[bytes] = set()
        cursor: bytes | None = None
        while True:
            rows = self.connector.get_records(
                self._operation_page_sql(after_cursor=cursor is not None),
                (request.attempt_key,) if cursor is None else (request.attempt_key, cursor),
                as_dict=True,
            )
            if not rows:
                return scopes, receipts
            for row in rows:
                scope_hash = binary(row.get("scope_hash"))
                operation_key = binary(row.get("operation_key"))
                expected_key = self._operation_key_projector(request, scope_hash)
                if operation_key != expected_key or scope_hash in scopes:
                    raise RuntimeError("mssql_transaction.operation_history_identity_collision")
                if cursor is not None and scope_hash <= cursor:
                    raise RuntimeError("mssql_transaction.operation_history_order_invalid")
                scopes.add(scope_hash)
                cursor = scope_hash
                if row.get("receipt_operation_key") is not None:
                    receipt_key = binary(row.get("receipt_operation_key"))
                    receipt_scope = binary(row.get("receipt_scope_hash"))
                    if receipt_key != operation_key or receipt_scope != scope_hash:
                        raise RuntimeError("mssql_transaction.receipt_history_identity_collision")
                    receipts.add(receipt_scope)
            if len(rows) < _PAGE_SIZE:
                return scopes, receipts

    def _attempt_sql(self) -> str:
        return f"""
            SELECT a.attempt_key, a.target_identity, a.generation,
                   a.invocation_digest, a.route_fingerprint,
                   a.target_database, a.target_schema, a.target_table,
                   a.strategy
            FROM {self.attempt_table} AS a WITH (HOLDLOCK)
            WHERE a.attempt_key = ?
            """

    def _operation_page_sql(self, *, after_cursor: bool) -> str:
        cursor = "AND o.scope_hash > ?" if after_cursor else ""
        return f"""
            SELECT TOP ({_PAGE_SIZE}) o.operation_key, o.scope_hash,
                   r.operation_key AS receipt_operation_key,
                   r.scope_hash AS receipt_scope_hash
            FROM {self.operation_table} AS o WITH (HOLDLOCK)
            LEFT JOIN {self.receipt_table} AS r WITH (HOLDLOCK)
              ON r.operation_key = o.operation_key
            WHERE o.attempt_key = ? {cursor}
            ORDER BY o.scope_hash
            """

    @property
    def attempt_table(self) -> str:
        return self.qualified(ATTEMPT_TABLE)

    @property
    def operation_table(self) -> str:
        return self.qualified(OPERATION_TABLE)

    @property
    def receipt_table(self) -> str:
        return self.qualified(RECEIPT_TABLE)


__all__ = [
    "MssqlAttemptOperationHistory",
    "MssqlGenericOperationHistoryReader",
]
