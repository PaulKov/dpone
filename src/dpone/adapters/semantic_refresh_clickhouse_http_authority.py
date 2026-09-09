"""HTTP-backed physical authority observation for one ClickHouse relation."""

from __future__ import annotations

from dpone.adapters import semantic_refresh_clickhouse_http_queries as queries
from dpone.adapters.semantic_refresh_clickhouse_connection import (
    ClickHouseConnectionAuthorityVerifier,
)
from dpone.adapters.semantic_refresh_clickhouse_http_response import tabular_rows
from dpone.ports.semantic_refresh_clickhouse_authority import (
    clickhouse_physical_authority_rows,
)
from dpone.ports.semantic_refresh_clickhouse_connection import (
    ClickHouseClusterConnectionAuthority,
    SemanticRefreshClickHouseHttpClientPort,
)


class ClickHouseHttpRelationAuthorityReader:
    """Read canonical schema, physical design, and protected topology evidence."""

    def __init__(
        self,
        *,
        client: SemanticRefreshClickHouseHttpClientPort,
        connection: ClickHouseConnectionAuthorityVerifier,
        connection_authority: ClickHouseClusterConnectionAuthority,
    ) -> None:
        self._client = client
        self._connection = connection
        self._connection_authority = connection_authority

    def load(self, *, database: str, table: str) -> dict[str, object]:
        """Return the exact authenticated relation authority projection."""

        shard_count, replica_count = self._connection.assert_current(
            self._connection_authority.clickhouse_cluster_authority_id
        )
        schema_rows = tabular_rows(self._client, queries.schema_query(database, table), columns=6)
        physical_rows = tabular_rows(self._client, queries.physical_query(database, table), columns=15)
        if len(physical_rows) != 1:
            raise queries.ClickHouseHttpGatewayError("ClickHouse physical authority is absent or ambiguous")
        if physical_rows[0][12] != 0:
            raise queries.ClickHouseHttpGatewayError(
                "DPONE_REFRESH_CLICKHOUSE_MATERIALIZED_WRITER_UNSUPPORTED: "
                "incoming MaterializedView writer targets the protected relation"
            )
        table_engine = physical_rows[0][0]
        if not isinstance(table_engine, str) or not table_engine:
            raise queries.ClickHouseHttpGatewayError("ClickHouse table engine observation is invalid")
        database_engine = self._database_engine(database)
        canonical_physical_rows = clickhouse_physical_authority_rows(
            database_engine=database_engine,
            shard_count=shard_count,
            replica_count=replica_count,
            table_observation=physical_rows[0],
        )
        return {
            "schema_sha256": queries.canonical_rows_sha256(schema_rows),
            "physical_sha256": queries.canonical_rows_sha256(canonical_physical_rows),
            "database_engine": database_engine,
            "table_engine": table_engine,
            "shard_count": shard_count,
            "replica_count": replica_count,
        }

    def _database_engine(self, database: str) -> str:
        rows = tabular_rows(
            self._client,
            f"SELECT engine FROM system.databases WHERE name = {queries.literal(database)}",
            columns=1,
        )
        if len(rows) != 1 or not isinstance(rows[0][0], str) or not rows[0][0]:
            raise queries.ClickHouseHttpGatewayError("ClickHouse database engine observation is invalid")
        return rows[0][0]


__all__ = ["ClickHouseHttpRelationAuthorityReader"]
