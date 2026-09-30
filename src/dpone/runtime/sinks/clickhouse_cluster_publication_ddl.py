"""One-shot distributed-DDL adapter for cluster publication."""

from __future__ import annotations

from datetime import datetime
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

    def prove_no_prior_publication(
        self, record: contracts.AuthorityRecord, *, cluster: str, operation_started_at: datetime
    ) -> bool:
        """Require complete logged history plus absence of the exact DDL identity.

        Query-log absence alone is never accepted: every replica must retain
        query events older than the original operation and have logging enabled.
        An unavailable observation fails closed.
        """
        if not record.authority_write_id:
            return False
        hosts = set(self._catalog.inventory(cluster).hosts)
        params = {
            "cluster": cluster,
            "started_at": operation_started_at,
            "query_id_pattern": f"dpone-cluster-ddl-{record.operation_id[:20]}-%",
            "publication_query": _publication_sql(record, cluster),
        }
        try:
            settings = self._connector.get_records(
                "SELECT hostName(), value FROM clusterAllReplicas(%(cluster)s, system.settings) "
                "WHERE name = 'log_queries'",
                params,
            )
            if {str(host) for host, value in settings if str(value) == "1"} != hosts or len(settings) != len(hosts):
                return False
            coverage = self._connector.get_records(
                "SELECT hostName(), min(event_time) FROM clusterAllReplicas(%(cluster)s, system.query_log) "
                "WHERE event_time <= %(started_at)s GROUP BY hostName()",
                params,
            )
            if {str(host) for host, _ in coverage} != hosts or len(coverage) != len(hosts):
                return False
            if any(start is None or start > operation_started_at for _, start in coverage):
                return False
            prior = self._connector.get_records(
                "SELECT hostName(), count() FROM clusterAllReplicas(%(cluster)s, system.query_log) "
                "WHERE query_id LIKE %(query_id_pattern)s GROUP BY hostName()",
                params,
            )
            active = self._connector.get_records(
                "SELECT hostName(), count() FROM clusterAllReplicas(%(cluster)s, system.processes) "
                "WHERE query_id LIKE %(query_id_pattern)s GROUP BY hostName()",
                params,
            )
            queued = self._connector.get_records(
                "SELECT count() FROM clusterAllReplicas(%(cluster)s, system.distributed_ddl_queue) "
                "WHERE query = %(publication_query)s",
                params,
            )
            return not prior and not active and len(queued) == 1 and int(queued[0][0]) == 0
        except Exception:
            return False

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
