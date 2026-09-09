"""Execute one protected ClickHouse operation under its deterministic query ID."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.adapters import semantic_refresh_clickhouse_http_queries as queries
from dpone.ports.semantic_refresh_clickhouse_connection import SemanticRefreshClickHouseHttpClientPort


class ClickHouseOperationExecutor:
    """Bind every mutation to the workflow/operation query-id namespace."""

    def __init__(self, client: SemanticRefreshClickHouseHttpClientPort) -> None:
        self._client = client

    def execute(self, statement: str, plan: Mapping[str, object], phase: str) -> None:
        """Execute one operation mutation or fail with the gateway error contract."""

        try:
            self._client.execute_operation(statement, query_id=self.query_id(plan, phase))
        except Exception as exc:
            raise queries.ClickHouseHttpGatewayError("ClickHouse operation command outcome is unavailable") from exc

    @staticmethod
    def query_id(plan: Mapping[str, object], phase: str) -> str:
        """Return the exact protected operation query ID for one phase."""

        return queries.operation_query_id(
            str(plan.get("workflow_execution_binding_sha256")),
            str(plan.get("operation_id")),
            phase,
        )


__all__ = ["ClickHouseOperationExecutor"]
