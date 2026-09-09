"""Verify concrete ClickHouse HTTP transport against protected topology authority."""

from __future__ import annotations

from typing import Any, Protocol

from dpone.adapters import semantic_refresh_clickhouse_http_queries as queries
from dpone.ports.semantic_refresh_clickhouse_connection import (
    ClickHouseClusterConnectionAuthority,
    assert_clickhouse_cluster_topology,
)


class ClickHouseEndpointClient(Protocol):
    """Minimum concrete transport surface required for physical authority checks."""

    @property
    def endpoint_authority_id(self) -> str:
        """Return the normalized non-secret endpoint used by every request."""

    def execute(self, statement: str) -> Any:
        """Execute one HTTP-backed ClickHouse query."""

    def execute_operation(self, statement: str, *, query_id: str) -> Any:
        """Execute one mutation with its protected operation query ID."""


class ClickHouseConnectionAuthorityVerifier:
    """Compare endpoint and current system.clusters rows before engine I/O."""

    def __init__(
        self,
        *,
        client: ClickHouseEndpointClient,
        authority: ClickHouseClusterConnectionAuthority,
    ) -> None:
        self._client = client
        self._authority = authority

    def assert_current(self, clickhouse_cluster_authority_id: str) -> tuple[int, int]:
        """Return certified counts only after exact endpoint/topology comparison."""

        authority = self._authority
        if authority.clickhouse_cluster_authority_id != clickhouse_cluster_authority_id:
            raise queries.ClickHouseHttpGatewayError("ClickHouse cluster connection authority differs")
        if self._client.endpoint_authority_id != authority.endpoint_authority_id:
            raise queries.ClickHouseHttpGatewayError("ClickHouse endpoint authority differs")
        try:
            raw = self._client.execute(queries.cluster_topology_query(authority.cluster_name))
        except Exception as exc:
            raise queries.ClickHouseHttpGatewayError("ClickHouse topology authority is unavailable") from exc
        if not isinstance(raw, list) or not raw or any(not isinstance(row, tuple) or len(row) != 4 for row in raw):
            raise queries.ClickHouseHttpGatewayError("ClickHouse topology authority response is invalid")
        rows = tuple(raw)
        try:
            assert_clickhouse_cluster_topology(authority, rows)
        except ValueError as exc:
            raise queries.ClickHouseHttpGatewayError("ClickHouse topology authority differs") from exc
        return 1, 1


__all__ = [
    "ClickHouseConnectionAuthorityVerifier",
    "ClickHouseEndpointClient",
]
