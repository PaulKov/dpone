"""Parent ownership fence consumed by the generic target transaction finalizer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.contracts.mssql_transaction_governance import MssqlGenericCommitReceipt, MssqlTransactionOperation


class CompositionMssqlTransactionFence(Protocol):
    """Verify exact write/replay identity in the caller-owned target transaction.

    The implementation must not start, commit, replace or roll back a transaction.
    The returned identity must be compared again immediately before receipt insert.
    """

    def require_current(
        self,
        connector: Any,
        operation: MssqlTransactionOperation | None = None,
        *,
        transaction_id: int | None = None,
        receipt: MssqlGenericCommitReceipt | None = None,
        mutation_plan_sha256: bytes | None = None,
    ) -> int: ...
