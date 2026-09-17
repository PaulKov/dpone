"""Strict bootstrap for the fixed KeeperMap publication facade."""

from __future__ import annotations

import secrets
from collections import Counter
from collections.abc import Sequence
from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts

_KEEPER_PATH = "/dpone_cluster_publication_authority"
_COLUMNS = (
    "target_key String PRIMARY KEY, operation_id String, fence_token String, phase String, "
    "dispatch_epoch UInt64, payload String, payload_sha256 FixedString(64)"
)


class ClickHouseClusterAuthorityBootstrap:
    """Create an absent facade once and require exact replicas afterward."""

    def __init__(self, connector: Any, catalog: Any) -> None:
        self._connector = connector
        self._catalog = catalog

    def ensure(self, cluster: str, database: str, hosts: Sequence[str]) -> None:
        observed = self._facades(cluster, database)
        if observed and not self._valid_existing(observed, hosts, allow_missing=True):
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_INVALID", "KeeperMap facade differs across replicas"
            )
        if Counter(host for host, _ in observed) == Counter(hosts):
            return
        token = f"dpone-v1-bootstrap-{secrets.token_hex(16)}"
        sql = (
            f"CREATE TABLE IF NOT EXISTS {_qualified(database, contracts.AUTHORITY_TABLE)} "
            f"ON CLUSTER {_quote(cluster)} ({_COLUMNS}) ENGINE=KeeperMap('{_KEEPER_PATH}')"
        )
        settings = {
            "skip_unavailable_shards": 0,
            "distributed_ddl_output_mode": "throw",
            "distributed_ddl_task_timeout": 60,
            "log_comment": token,
        }
        try:
            self._connector.connection.execute(
                sql,
                settings=settings,
                query_id=f"dpone-authority-bootstrap-{token[-16:]}",
            )
        except Exception:
            pass
        entries = self._catalog.find_entries(cluster, token)
        if len(entries) != 1 or entries[0].state_for(hosts) not in {
            contracts.QueueState.TERMINAL_SUCCESS,
            contracts.QueueState.TERMINAL_FAILURE,
        }:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_BOOTSTRAP_UNKNOWN",
                "exact terminal bootstrap queue entry is required",
            )
        complete = self._facades(cluster, database)
        if not self._valid_existing(complete, hosts, allow_missing=False):
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_BOOTSTRAP_UNKNOWN", "facade is not complete"
            )

    def _facades(self, cluster: str, database: str) -> tuple[tuple[str, str], ...]:
        rows = self._connector.get_records(
            "SELECT hostName(), engine_full FROM clusterAllReplicas(%(cluster)s, system.tables) "
            "WHERE database = %(database)s AND name = %(table)s ORDER BY hostName()",
            {"cluster": cluster, "database": database, "table": contracts.AUTHORITY_TABLE},
        )
        return tuple((str(host), " ".join(str(engine).split())) for host, engine in rows)

    @staticmethod
    def _valid_existing(rows: Sequence[tuple[str, str]], hosts: Sequence[str], *, allow_missing: bool) -> bool:
        actual = [host for host, _ in rows]
        if len(actual) != len(set(actual)) or not set(actual) <= set(hosts):
            return False
        if not allow_missing and Counter(actual) != Counter(hosts):
            return False
        engines = {engine for _, engine in rows}
        return len(engines) <= 1 and all(
            engine.startswith("KeeperMap(") and _KEEPER_PATH in engine for engine in engines
        )


def _quote(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _qualified(database: str, table: str) -> str:
    return f"{_quote(database)}.{_quote(table)}"
