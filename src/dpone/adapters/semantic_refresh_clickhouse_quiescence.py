"""ClickHouse process-list observer for same-DagRun continuation."""

from __future__ import annotations

from typing import Any, Protocol

from dpone.adapters import semantic_refresh_clickhouse_http_queries as queries
from dpone.adapters.semantic_refresh_clickhouse_connection import (
    ClickHouseConnectionAuthorityVerifier,
)
from dpone.ports.semantic_refresh_attempt_quiescence import (
    ClickHouseAttemptQuiescenceProof,
)


class _Client(Protocol):
    def execute(self, statement: str) -> Any: ...


class ClickHouseAttemptQuiescenceError(RuntimeError):
    """Raised when an old operation query is active or cannot be observed."""


class ClickHouseHttpAttemptQuiescenceObserver:
    """Require an empty exact operation query-id namespace."""

    def __init__(
        self,
        *,
        client: _Client,
        connection: ClickHouseConnectionAuthorityVerifier,
    ) -> None:
        self._client = client
        self._connection = connection

    def prove_quiescent(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
        original_attempt_binding_sha256: str,
        clickhouse_cluster_authority_id: str,
        observed_at: str,
    ) -> ClickHouseAttemptQuiescenceProof:
        """Fail unless the protected cluster reports no operation query IDs."""

        self._connection.assert_current(clickhouse_cluster_authority_id)
        prefix = queries.operation_query_id_prefix(
            workflow_execution_binding_sha256,
            operation_id,
        )
        try:
            rows = self._client.execute(queries.active_operation_query_ids_query(prefix))
        except Exception as exc:
            raise ClickHouseAttemptQuiescenceError("ClickHouse operation-query quiescence is unavailable") from exc
        if not isinstance(rows, list) or any(
            not isinstance(row, tuple) or len(row) != 1 or not isinstance(row[0], str) or not row[0] for row in rows
        ):
            raise ClickHouseAttemptQuiescenceError("ClickHouse operation-query observation is invalid")
        if rows:
            raise ClickHouseAttemptQuiescenceError("ClickHouse operation query is still active")
        return ClickHouseAttemptQuiescenceProof.build(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
            original_attempt_binding_sha256=original_attempt_binding_sha256,
            clickhouse_cluster_authority_id=clickhouse_cluster_authority_id,
            query_id_prefix=prefix,
            observed_at=observed_at,
        )


__all__ = [
    "ClickHouseAttemptQuiescenceError",
    "ClickHouseHttpAttemptQuiescenceObserver",
]
