"""Strict system-table adapter for ClickHouse cluster publication."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Sequence
from typing import Any

from dpone.contracts import clickhouse_cluster_publication as contracts


class ClickHouseClusterPublicationCatalog:
    """Read complete cluster facts; never use partial fan-out settings."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def inventory(self, cluster: str) -> contracts.ClusterInventory:
        rows = self._rows(
            "SELECT host_name, host_address, port, shard_num, replica_num, internal_replication "
            "FROM system.clusters WHERE cluster = %(cluster)s ORDER BY shard_num, replica_num",
            {"cluster": cluster},
        )
        inventory = contracts.ClusterInventory(
            cluster=cluster,
            replicas=tuple(
                contracts.ClusterReplica(
                    host=str(row[0]),
                    address=str(row[1]),
                    native_port=int(row[2]),
                    shard_num=int(row[3]),
                    replica_num=int(row[4]),
                    internal_replication=bool(row[5]),
                )
                for row in rows
            ),
        )
        inventory.validate()
        return inventory

    def require_atomic_database(self, cluster: str, database: str, hosts: Sequence[str]) -> None:
        rows = self._rows(
            "SELECT hostName(), engine FROM clusterAllReplicas(%(cluster)s, system.databases) "
            "WHERE name = %(database)s ORDER BY hostName()",
            {"cluster": cluster, "database": database},
        )
        facts = [(str(row[0]), str(row[1])) for row in rows]
        self._require_exact_hosts(facts, hosts, fact="database")
        if any(engine != "Atomic" for _, engine in facts):
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_DATABASE_UNSUPPORTED", "Atomic database required on every replica"
            )

    def generations(
        self, cluster: str, database: str, target: str, candidate: str, hosts: Sequence[str]
    ) -> tuple[contracts.ReplicaGeneration, ...]:
        names = (target, candidate)
        rows = self._rows(
            "SELECT hostName(), name, toString(uuid), engine_full, total_rows "
            "FROM clusterAllReplicas(%(cluster)s, system.tables) "
            "WHERE database = %(database)s AND name IN %(names)s ORDER BY hostName(), name",
            {"cluster": cluster, "database": database, "names": names},
        )
        schema = self._schema_digests(cluster, database, names)
        replication = self._replication_facts(cluster, database, names, hosts)
        by_host: dict[str, dict[str, contracts.GenerationIdentity]] = {host: {} for host in hosts}
        health: dict[tuple[str, str], bool] = {}
        counts: dict[tuple[str, str], int | None] = {}
        for row in rows:
            host, name = str(row[0]), str(row[1])
            if host not in by_host or name not in names or name in by_host[host]:
                raise contracts.ClusterPublicationError(
                    "DPONE_CLICKHOUSE_CLUSTER_GENERATION_UNKNOWN", "unexpected or duplicate replica fact"
                )
            replica = replication.get((host, name))
            if replica is None:
                raise contracts.ClusterPublicationError(
                    "DPONE_CLICKHOUSE_CLUSTER_GENERATION_UNKNOWN", "replication identity is missing"
                )
            identity = contracts.GenerationIdentity(
                uuid=str(row[2]),
                engine_full=_normalize_engine(str(row[3])),
                schema_digest=schema.get((host, name), ""),
                keeper_name=replica[0],
                keeper_path=replica[1],
            )
            identity.validate()
            by_host[host][name] = identity
            health[(host, name)] = replica[2]
            counts[(host, name)] = None if row[4] is None else int(row[4])
        unknown_hosts = set(by_host) - {str(row[0]) for row in rows}
        if unknown_hosts:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_INVENTORY_INCOMPLETE", "one or more replicas were not observed"
            )
        result = tuple(
            contracts.ReplicaGeneration(
                host=host,
                target=values.get(target),
                candidate=values.get(candidate),
                healthy=health.get((host, candidate), health.get((host, target), False)),
                row_count=counts.get((host, candidate)),
            )
            for host, values in sorted(by_host.items())
        )
        _require_consistent_generation(result, role="target")
        _require_consistent_generation(result, role="candidate")
        return result

    def _replication_facts(
        self, cluster: str, database: str, names: tuple[str, str], hosts: Sequence[str]
    ) -> dict[tuple[str, str], tuple[str, str, bool]]:
        rows = self._rows(
            "SELECT hostName(), table, ifNull(zookeeper_name, ''), zookeeper_path, is_readonly, "
            "is_session_expired, queue_size, active_replicas, total_replicas, "
            "last_queue_update_exception, zookeeper_exception "
            "FROM clusterAllReplicas(%(cluster)s, system.replicas) "
            "WHERE database = %(database)s AND table IN %(names)s",
            {"cluster": cluster, "database": database, "names": names},
        )
        return {
            (str(row[0]), str(row[1])): (
                str(row[2]),
                str(row[3]),
                int(row[4]) == 0
                and int(row[5]) == 0
                and int(row[6]) == 0
                and int(row[7]) == int(row[8]) == len(hosts)
                and not str(row[9] or "")
                and not str(row[10] or ""),
            )
            for row in rows
        }

    def find_entries(self, cluster: str, token: str) -> tuple[contracts.QueueEntry, ...]:
        rows = self._rows(
            "SELECT entry, query, settings['log_comment'], host, status, exception_code, exception_text "
            "FROM system.distributed_ddl_queue WHERE cluster = %(cluster)s "
            "AND mapContains(settings, 'log_comment') AND settings['log_comment'] = %(token)s "
            "ORDER BY entry, host",
            {"cluster": cluster, "token": token},
        )
        return _queue_entries(rows)

    def read_entry(self, cluster: str, entry: str) -> contracts.QueueEntry | None:
        rows = self._rows(
            "SELECT entry, query, settings['log_comment'], host, status, exception_code, exception_text "
            "FROM system.distributed_ddl_queue WHERE cluster = %(cluster)s AND entry = %(entry)s ORDER BY host",
            {"cluster": cluster, "entry": entry},
        )
        entries = _queue_entries(rows)
        if not entries:
            return None
        if len(entries) != 1:
            raise contracts.ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN", "queue entry is ambiguous")
        return entries[0]

    def _schema_digests(self, cluster: str, database: str, names: tuple[str, str]) -> dict[tuple[str, str], str]:
        rows = self._rows(
            "SELECT hostName(), table, groupArray((name, type, default_kind, default_expression, position)) "
            "FROM clusterAllReplicas(%(cluster)s, system.columns) "
            "WHERE database = %(database)s AND table IN %(names)s GROUP BY hostName(), table",
            {"cluster": cluster, "database": database, "names": names},
        )
        return {
            (str(host), str(table)): hashlib.sha256(repr(tuple(columns)).encode()).hexdigest()
            for host, table, columns in rows
        }

    def _rows(self, query: str, params: dict[str, Any]) -> list[Any]:
        return self._connector.get_records(query, params)

    @staticmethod
    def _require_exact_hosts(facts: Sequence[tuple[str, Any]], hosts: Sequence[str], *, fact: str) -> None:
        actual = [host for host, _ in facts]
        if Counter(actual) != Counter(hosts):
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_INVENTORY_INCOMPLETE", f"{fact} facts do not cover exact inventory"
            )


def _normalize_engine(value: str) -> str:
    return " ".join(value.split())


def _require_consistent_generation(rows: Sequence[contracts.ReplicaGeneration], *, role: str) -> None:
    values = [getattr(row, role) for row in rows]
    present = [value for value in values if value is not None]
    if present and (len(present) != len(values) or len(set(present)) != 1):
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_GENERATION_DIVERGED", f"{role} identity differs across replicas"
        )


def _queue_entries(rows: Sequence[Any]) -> tuple[contracts.QueueEntry, ...]:
    grouped: dict[str, list[Any]] = {}
    for row in rows:
        grouped.setdefault(str(row[0]), []).append(row)
    result: list[contracts.QueueEntry] = []
    for entry, items in sorted(grouped.items()):
        queries = {str(item[1]) for item in items}
        tokens = {str(item[2]) for item in items}
        if len(queries) != 1 or len(tokens) != 1:
            raise contracts.ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN", "queue identity changed")
        query = next(iter(queries))
        result.append(
            contracts.QueueEntry(
                entry=entry,
                query_digest=hashlib.sha256(" ".join(query.split()).encode()).hexdigest(),
                correlation_token=next(iter(tokens)),
                hosts=tuple(
                    contracts.QueueHostResult(
                        host=str(item[3]),
                        status=None if item[4] is None else str(item[4]),
                        exception_code=item[5],
                        exception_text=item[6],
                    )
                    for item in items
                ),
            )
        )
    return tuple(result)
