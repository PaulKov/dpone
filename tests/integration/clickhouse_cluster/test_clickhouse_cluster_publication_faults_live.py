"""Deterministic fault matrix for replicated ClickHouse publication."""

from __future__ import annotations

import os
import secrets
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from typing import Any

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    ClusterPublicationError,
    QueueHostResult,
    QueueState,
    ddl_query_digest,
)
from tests.integration.clickhouse_cluster.evidence import record_scenario
from tests.integration.clickhouse_cluster.test_clickhouse_cluster_publication_live import (
    _CLUSTER,
    _configs,
    _create_database,
    _create_replicated_table,
    _database_name,
    _execute,
    _execute_at,
    _service,
    _wait_for_counts,
)

pytestmark = pytest.mark.integration_live

_RUN_LIVE = os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") == "1"


@pytest.mark.skipif(not _RUN_LIVE, reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1")
def test_partial_in_progress_waits_for_original_entry_then_converges() -> None:
    database = _database_name("publication_partial_active")
    _create_database(database)
    _create_replicated_table(database, "target")
    _create_replicated_table(database, "candidate")
    _execute(
        f"CREATE TABLE {database}.delay_probe ON CLUSTER {_CLUSTER} (id UInt64, value UInt64) "
        f"ENGINE=ReplicatedMergeTree('/{database}/{{uuid}}/{{shard}}', '{{replica}}') ORDER BY id"
    )
    _execute(f"INSERT INTO {database}.target VALUES (1)")
    _execute(f"INSERT INTO {database}.candidate VALUES (10),(20)")
    _wait_for_counts(database, "candidate", expected=2)
    _execute(f"INSERT INTO {database}.delay_probe VALUES (1,1)")
    _wait_for_counts(database, "delay_probe", expected=1)
    _execute_at(28123, f"SYSTEM STOP MERGES {database}.delay_probe")

    connector, catalog, _unused = _service(database)
    ddl = _PartialInProgressDdl(connector, catalog, database)
    service = _service(database, ddl=ddl)[2]
    config, candidate = _configs(database, scheduler_identity="docker-partial-in-progress")

    try:
        with pytest.raises(ClusterPublicationError, match="PUBLICATION_IN_PROGRESS"):
            service.publish(config, candidate, staged_rows=2)
        assert ddl.dispatches == 1
        assert ddl.entry is not None
        assert ddl.entry.state_for(("node1", "node2")) is QueueState.IN_PROGRESS

        _execute_at(28123, f"SYSTEM START MERGES {database}.delay_probe")
        ddl.wait_until_terminal()
        _execute_at(28123, f"EXCHANGE TABLES {database}.target AND {database}.candidate")

        replay = service.prepare_admission(config)
        assert ddl.dispatches == 1
        assert replay.options.get("__dpone_clickhouse_full_refresh_replay_v1") is not None
        assert _execute(
            f"SELECT count() FROM clusterAllReplicas('{_CLUSTER}', system.tables) "
            f"WHERE database='{database}' AND name='candidate'"
        ) == [("0",)]
    finally:
        _execute_at(28123, f"SYSTEM START MERGES {database}.delay_probe")
        ddl.close()

    _record_fault_scenario("partial_in_progress_then_converged", "passed_live")


@pytest.mark.skipif(not _RUN_LIVE, reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1")
def test_terminal_partial_fails_closed_and_retains_both_generations() -> None:
    database = _database_name("publication_partial_terminal")
    _create_database(database)
    _create_replicated_table(database, "target")
    _create_replicated_table(database, "candidate")
    _execute(f"INSERT INTO {database}.target VALUES (1)")
    _execute(f"INSERT INTO {database}.candidate VALUES (10),(20)")
    _wait_for_counts(database, "candidate", expected=2)

    connector, catalog, _unused = _service(database)
    ddl = _TerminalPartialDdl(connector, catalog, database)
    service = _service(database, ddl=ddl)[2]
    config, candidate = _configs(database, scheduler_identity="docker-partial-terminal")

    with pytest.raises(ClusterPublicationError, match="PUBLICATION_PARTIAL_TERMINAL"):
        service.publish(config, candidate, staged_rows=2)
    with pytest.raises(ClusterPublicationError, match="PUBLICATION_PARTIAL_TERMINAL"):
        service.prepare_admission(config)

    assert ddl.dispatches == 1
    assert ddl.entry is not None
    assert ddl.entry.state_for(("node1", "node2")) is QueueState.TERMINAL_FAILURE
    presence = _execute(
        f"SELECT hostName(), groupArray(name) FROM clusterAllReplicas('{_CLUSTER}', system.tables) "
        f"WHERE database='{database}' AND name IN ('target','candidate') GROUP BY hostName() ORDER BY hostName()"
    )
    assert presence == [("node1", "['candidate','target']"), ("node2", "['candidate','target']")]
    _record_fault_scenario("terminal_partial", "passed_live")


@pytest.mark.skipif(not _RUN_LIVE, reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1")
def test_complete_queue_status_and_host_matrix_fails_closed() -> None:
    from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog

    database = _database_name("publication_queue_matrix")
    _create_database(database)
    connector, catalog, _unused = _service(database)
    token = f"dpone-v1-queue-matrix-{secrets.token_hex(16)}"
    _execute(
        f"DROP TABLE IF EXISTS {database}.absent_probe ON CLUSTER {_CLUSTER}",
        log_comment=token,
        distributed_ddl_output_mode="throw",
        skip_unavailable_shards=0,
    )
    entries = ClickHouseClusterPublicationCatalog(connector).find_entries(_CLUSTER, token)
    assert len(entries) == 1
    live = entries[0]
    assert live.state_for(("node1", "node2")) is QueueState.TERMINAL_SUCCESS

    host_cases = (
        (("Active", None, None), QueueState.IN_PROGRESS),
        (("Inactive", None, None), QueueState.IN_PROGRESS),
        (("Finished", 0, ""), QueueState.TERMINAL_SUCCESS),
        (("Finished", 57, "failure"), QueueState.TERMINAL_FAILURE),
        (("Finished", None, None), QueueState.UNKNOWN),
        (("Finished", -1, "failure"), QueueState.UNKNOWN),
        (("Finished", "bad", "failure"), QueueState.UNKNOWN),
        (("Removing", 0, ""), QueueState.UNKNOWN),
        (("Unknown", 0, ""), QueueState.UNKNOWN),
        ((None, None, None), QueueState.UNKNOWN),
        (("FutureStatus", None, None), QueueState.UNKNOWN),
        (("Active", 0, ""), QueueState.UNKNOWN),
    )
    for (status, code, text), expected in host_cases:
        hosts = tuple(QueueHostResult(item.host, status, code, text) for item in live.hosts)
        assert replace(live, hosts=hosts).state_for(("node1", "node2")) is expected

    first, second = live.hosts
    assert replace(live, hosts=(first,)).state_for(("node1", "node2")) is QueueState.UNKNOWN
    assert replace(live, hosts=(first, second, second)).state_for(("node1", "node2")) is QueueState.UNKNOWN
    extra = QueueHostResult("node3", "Finished", 0, "")
    assert replace(live, hosts=(first, second, extra)).state_for(("node1", "node2")) is QueueState.UNKNOWN
    mixed = (replace(first, status="Finished", exception_code=57, exception_text="failure"), second)
    assert replace(live, hosts=mixed).state_for(("node1", "node2")) is QueueState.TERMINAL_FAILURE
    unknown_dominates = (replace(first, status="Active", exception_code=None, exception_text=None), extra)
    assert replace(live, hosts=unknown_dominates).state_for(("node1", "node2")) is QueueState.UNKNOWN
    _record_fault_scenario("queue_status_and_host_matrix", "passed_fault_injection")


class _QueueBackedFaultDdl:
    def __init__(self, connector: Any, catalog: Any, database: str) -> None:
        from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl

        self.connector = connector
        self.catalog = catalog
        self.database = database
        self.dispatches = 0
        self.entry: Any = None
        self._delegate = ClickHouseClusterPublicationDdl(connector, catalog)

    def cleanup_query_digest(self, record: Any, *, cluster: str) -> str:
        return self._delegate.cleanup_query_digest(record, cluster=cluster)

    def find_entries(self, cluster: str, correlation_token: str) -> tuple[Any, ...]:
        entries = self.catalog.find_entries(cluster, correlation_token)
        if entries:
            self.entry = entries[0]
        return entries

    def read_entry(self, cluster: str, entry: str) -> Any:
        observed = self.catalog.read_entry(cluster, entry)
        if observed is not None:
            self.entry = observed
        return observed

    def drop_predecessor(self, record: Any, permit: Any, *, cluster: str) -> None:
        self._delegate.drop_predecessor(record, permit, cluster=cluster)


class _PartialInProgressDdl(_QueueBackedFaultDdl):
    def __init__(self, connector: Any, catalog: Any, database: str) -> None:
        super().__init__(connector, catalog, database)
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._future: Future[Any] | None = None

    def _sql(self) -> str:
        return f"ALTER TABLE `{self.database}`.`delay_probe` ON CLUSTER `{_CLUSTER}` UPDATE value = value + 1 WHERE 1"

    def publication_query_digest(self, record: Any, *, cluster: str) -> str:
        return ddl_query_digest(self._sql())

    def dispatch_publication(self, record: Any, permit: Any, *, cluster: str) -> None:
        self.dispatches += 1
        _execute_at(18123, f"EXCHANGE TABLES {self.database}.target AND {self.database}.candidate")
        settings = {
            "skip_unavailable_shards": 0,
            "distributed_ddl_output_mode": "throw",
            "distributed_ddl_task_timeout": 30,
            "mutations_sync": 1,
            "log_comment": record.ddl_correlation_token,
        }
        settings["query_id"] = f"fault-active-{secrets.token_hex(8)}"
        self._future = self._executor.submit(_execute, self._sql(), **settings)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            entries = self.catalog.find_entries(cluster, record.ddl_correlation_token)
            if len(entries) == 1 and entries[0].state_for(("node1", "node2")) is QueueState.IN_PROGRESS:
                self.entry = entries[0]
                return
            time.sleep(0.1)
        raise AssertionError("distributed DDL did not expose an in-progress exact-host entry")

    def wait_until_terminal(self) -> None:
        assert self._future is not None
        self._future.result(timeout=30)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            entries = self.catalog.find_entries(_CLUSTER, self.entry.correlation_token)
            if len(entries) == 1 and entries[0].state_for(("node1", "node2")) is QueueState.TERMINAL_SUCCESS:
                self.entry = entries[0]
                return
            time.sleep(0.1)
        raise AssertionError("original distributed DDL entry did not converge")

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)


class _TerminalPartialDdl(_QueueBackedFaultDdl):
    def _sql(self) -> str:
        return f"DROP TABLE `{self.database}`.`failure_probe` ON CLUSTER `{_CLUSTER}`"

    def publication_query_digest(self, record: Any, *, cluster: str) -> str:
        return ddl_query_digest(self._sql())

    def dispatch_publication(self, record: Any, permit: Any, *, cluster: str) -> None:
        self.dispatches += 1
        _execute_at(18123, f"EXCHANGE TABLES {self.database}.target AND {self.database}.candidate")
        _execute_at(18123, f"CREATE TABLE {self.database}.failure_probe (id UInt8) ENGINE=Log")
        with pytest.raises(Exception):
            self.connector.connection.execute(
                self._sql(),
                settings={
                    "skip_unavailable_shards": 0,
                    "distributed_ddl_output_mode": "throw",
                    "distributed_ddl_task_timeout": 30,
                    "log_comment": record.ddl_correlation_token,
                },
                query_id=f"fault-terminal-{secrets.token_hex(8)}",
            )


def _record_fault_scenario(name: str, result: str) -> None:
    record_scenario(
        name,
        result,
        server_version=_execute("SELECT version()")[0][0],
    )
