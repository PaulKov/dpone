"""Strict bootstrap for the replicated publication authority table."""

from __future__ import annotations

import secrets
from collections import Counter
from collections.abc import Sequence
from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts

_COLUMNS = (
    "target_key String, operation_id String, fence_token String, phase String, "
    "dispatch_epoch UInt64, payload String, payload_sha256 FixedString(64), version UInt64"
)
_ENGINE = (
    "ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version) "
    "ORDER BY target_key"
)


class ClickHouseClusterAuthorityBootstrap:
    """Create an absent facade once and require exact replicas afterward."""

    def __init__(self, connector: Any, catalog: Any) -> None:
        self._connector = connector
        self._catalog = catalog

    def ensure(self, cluster: str, database: str, hosts: Sequence[str]) -> None:
        observed = self._facades(cluster, database)
        if observed and not self._valid_existing(observed, hosts, allow_missing=True):
            self._replace_invalid(cluster, database, hosts)
            observed = ()
        if Counter(host for host, _ in observed) == Counter(hosts):
            return
        token = f"dpone-v1-bootstrap-{secrets.token_hex(16)}"
        sql = (
            f"CREATE TABLE IF NOT EXISTS {_qualified(database, contracts.AUTHORITY_TABLE)} "
            f"ON CLUSTER {_quote(cluster)} ({_COLUMNS}) {_ENGINE}"
        )
        settings = {
            "skip_unavailable_shards": 0,
            "distributed_ddl_output_mode": "throw",
            "distributed_ddl_task_timeout": 60,
            "log_comment": token,
        }
        create_error = ""
        try:
            self._connector.connection.execute(
                sql,
                settings=settings,
                query_id=f"dpone-authority-bootstrap-{token[-16:]}",
            )
        except Exception as exc:
            create_error = _bounded(exc)
        entries = self._catalog.find_entries(cluster, token)
        queue_detail = _queue_detail(entries, hosts)
        if len(entries) != 1 or entries[0].state_for(hosts) is not contracts.QueueState.TERMINAL_SUCCESS:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_BOOTSTRAP_UNKNOWN",
                _bounded(f"bootstrap ddl did not finish: {queue_detail or create_error or 'no queue entry'}"),
            )
        complete = self._facades(cluster, database)
        if not self._valid_existing(complete, hosts, allow_missing=False):
            observed = ",".join(sorted({host for host, _ in complete})) or "none"
            engines = ",".join(sorted({engine[:80] for _, engine in complete})) or "none"
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_BOOTSTRAP_UNKNOWN",
                _bounded(
                    f"facade is not complete observed={observed} engines={engines} expected={','.join(hosts)}"
                ),
            )

    def _replace_invalid(self, cluster: str, database: str, hosts: Sequence[str]) -> None:
        token = f"dpone-v1-bootstrap-drop-{secrets.token_hex(16)}"
        sql = f"DROP TABLE IF EXISTS {_qualified(database, contracts.AUTHORITY_TABLE)} ON CLUSTER {_quote(cluster)} SYNC"
        drop_error = self._execute_ddl(sql, token)
        entries = self._catalog.find_entries(cluster, token)
        if len(entries) != 1 or entries[0].state_for(hosts) is not contracts.QueueState.TERMINAL_SUCCESS:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_INVALID",
                _bounded(
                    "invalid authority facade was not removed: "
                    f"{_queue_detail(entries, hosts) or drop_error or 'no queue entry'}"
                ),
            )

    def _execute_ddl(self, sql: str, token: str) -> str:
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
        except Exception as exc:
            return _bounded(exc)
        return ""

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
        return len(engines) <= 1 and all(_is_authority_engine(engine) for engine in engines)


def _queue_detail(entries: Sequence[Any], hosts: Sequence[str]) -> str:
    if len(entries) != 1:
        return f"queue_entries={len(entries)}"
    entry = entries[0]
    state = entry.state_for(hosts)
    texts = []
    for host in getattr(entry, "hosts", ()):
        text = str(getattr(host, "exception_text", "") or "").strip()
        if text:
            texts.append(text.splitlines()[0][:160])
    detail = f"state={getattr(state, 'value', state)}"
    if texts:
        detail = f"{detail} error={texts[0]}"
    return detail


def _bounded(value: object) -> str:
    text = " ".join(str(value).split())
    return text[:300]


def _is_authority_engine(engine: str) -> bool:
    return engine.startswith("ReplicatedReplacingMergeTree(") and "ORDER BY target_key" in engine


def _quote(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _qualified(database: str, table: str) -> str:
    return f"{_quote(database)}.{_quote(table)}"
