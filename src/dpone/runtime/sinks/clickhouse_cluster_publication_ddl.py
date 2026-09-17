"""One-shot distributed-DDL adapter for cluster publication."""

from __future__ import annotations

from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts

_DDL_SETTINGS = {
    "skip_unavailable_shards": 0,
    "distributed_ddl_output_mode": "throw",
    "distributed_ddl_task_timeout": 60,
}


class ClickHouseClusterPublicationDdl:
    """Execute each externally visible DDL effect exactly once per call."""

    def __init__(self, connector: Any, catalog: Any) -> None:
        self._connector = connector
        self._catalog = catalog

    def publication_query_digest(self, record: contracts.AuthorityRecord, *, cluster: str) -> str:
        return contracts.ddl_query_digest(_publication_sql(record, cluster))

    def cleanup_query_digest(self, record: contracts.AuthorityRecord, *, cluster: str) -> str:
        return contracts.ddl_query_digest(_cleanup_sql(record, cluster))

    def dispatch_publication(
        self, record: contracts.AuthorityRecord, permit: contracts.DispatchPermit, *, cluster: str
    ) -> None:
        self._require_permit(record, permit)
        if not record.ddl_correlation_token:
            raise ValueError("cluster publication correlation token is missing")
        self._execute_once(_publication_sql(record, cluster), record.ddl_correlation_token, permit)

    def drop_predecessor(
        self, record: contracts.AuthorityRecord, permit: contracts.DispatchPermit, *, cluster: str
    ) -> None:
        self._require_permit(record, permit)
        if not record.cleanup_correlation_token:
            raise ValueError("cluster cleanup correlation token is missing")
        self._execute_once(_cleanup_sql(record, cluster), record.cleanup_correlation_token, permit)

    def find_entries(self, cluster: str, correlation_token: str) -> tuple[contracts.QueueEntry, ...]:
        return self._catalog.find_entries(cluster, correlation_token)

    def read_entry(self, cluster: str, entry: str) -> contracts.QueueEntry | None:
        return self._catalog.read_entry(cluster, entry)

    def _execute_once(self, sql: str, token: str, permit: contracts.DispatchPermit) -> None:
        settings = {**_DDL_SETTINGS, "log_comment": token}
        query_id = f"dpone-cluster-ddl-{permit.operation_id[:20]}-{permit.dispatch_epoch}"
        self._connector.connection.execute(sql, settings=settings, query_id=query_id)

    @staticmethod
    def _require_permit(record: contracts.AuthorityRecord, permit: contracts.DispatchPermit) -> None:
        if (
            permit.target_key != record.target_key
            or permit.operation_id != record.operation_id
            or permit.fence_token != record.fence_token
            or permit.dispatch_epoch != record.dispatch_epoch
        ):
            raise ValueError("cluster publication dispatch permit does not match authority")


def _quote(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _qualified(database: str, table: str) -> str:
    return f"{_quote(database)}.{_quote(table)}"


def _publication_sql(record: contracts.AuthorityRecord, cluster: str) -> str:
    target = _qualified(record.database, record.target)
    candidate = _qualified(record.database, record.candidate)
    if record.predecessor is None:
        return f"RENAME TABLE {candidate} TO {target} ON CLUSTER {_quote(cluster)}"
    return f"EXCHANGE TABLES {target} AND {candidate} ON CLUSTER {_quote(cluster)}"


def _cleanup_sql(record: contracts.AuthorityRecord, cluster: str) -> str:
    return f"DROP TABLE IF EXISTS {_qualified(record.database, record.candidate)} ON CLUSTER {_quote(cluster)}"
