from __future__ import annotations

from typing import Any

import pytest

from dpone.config import LoadConfig
from dpone.runtime.decision_audit import RuntimeDecisionContext
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.clickhouse_cluster_topology import ClickHouseClusterTopologyProbe
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink


class _DecisionPublisher:
    def __init__(self) -> None:
        self.decisions: list[Any] = []

    def publish(self, decision: Any) -> None:
        self.decisions.append(decision)


class _ClusterConnector:
    host = "127.0.0.1"
    port = 9000
    database = "default"
    user = "default"
    password = ""
    application_name = "test"
    secure = False
    compression = True
    connect_timeout = 10
    send_receive_timeout = 3600
    settings: dict[str, object] = {}

    def __init__(
        self,
        *,
        actual_hosts: tuple[str, ...] = ("dc3",),
        uuids: tuple[str, ...] = ("target-uuid",),
        engines: tuple[str, ...] = ("ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",),
    ) -> None:
        self.actual_hosts = actual_hosts
        self.uuids = uuids
        self.engines = engines
        self.queries: list[str] = []

    def get_records(self, query: str) -> list[tuple[Any, ...]]:
        self.queries.append(query)
        if "system.clusters" in query:
            return [("dc1",), ("dc2",), ("dc3",)]
        if "clusterAllReplicas" in query:
            return [
                (
                    host,
                    self.uuids[index % len(self.uuids)],
                    self.engines[index % len(self.engines)],
                )
                for index, host in enumerate(self.actual_hosts)
            ]
        return []

    def execute_query(self, query: str, params: object | None = None) -> int:
        self.queries.append(query)
        return 0


class _IsolatedDc1Connector(_ClusterConnector):
    """dc1 is intentionally isolated: clusterAllReplicas fails, remote(dc1) fails."""

    def __init__(self, *, fail_cluster_all: bool = True) -> None:
        super().__init__(actual_hosts=("dc2", "dc3"))
        self.fail_cluster_all = fail_cluster_all

    def get_records(self, query: str) -> list[tuple[Any, ...]]:
        self.queries.append(query)
        if "system.clusters" in query:
            return [("dc1",), ("dc2",), ("dc3",)]
        if "clusterAllReplicas" in query:
            if self.fail_cluster_all:
                raise RuntimeError(
                    "ClickHouse get_records error: Code: 279. All connection tries failed. "
                    "internal-host.example.test:9000 While executing Remote (HedgedConnections)"
                )
            return [(host, self.uuids[0], self.engines[0]) for host in self.actual_hosts]
        if "remote(" in query:
            if "remote('dc1'" in query:
                raise RuntimeError(
                    "ClickHouse get_records error: Code: 279. All connection tries failed. "
                    "internal-host.example.test:9000"
                )
            for index, host in enumerate(self.actual_hosts):
                if f"remote('{host}'" in query:
                    return [(host, self.uuids[index % len(self.uuids)], self.engines[index % len(self.engines)])]
            return []
        return []


def test_clickhouse_cluster_topology_blocks_missing_target_replicas() -> None:
    evidence = ClickHouseClusterTopologyProbe(_ClusterConnector(actual_hosts=("dc3",))).inspect(_clustered_config())

    assert evidence.passed is False
    assert evidence.blockers == ("clickhouse_cluster_target_missing_replicas",)
    assert evidence.missing_hosts == ("dc1", "dc2")
    assert evidence.to_dict()["schema_version"] == "dpone.clickhouse.cluster_topology.v1"


def test_clickhouse_cluster_topology_passes_when_target_absent_for_create_path() -> None:
    evidence = ClickHouseClusterTopologyProbe(_ClusterConnector(actual_hosts=())).inspect(_clustered_config())

    assert evidence.passed is True
    assert evidence.status == "target_absent"


def test_clickhouse_cluster_topology_blocks_uuid_mismatch() -> None:
    evidence = ClickHouseClusterTopologyProbe(
        _ClusterConnector(actual_hosts=("dc1", "dc2", "dc3"), uuids=("uuid-a", "uuid-b"))
    ).inspect(_clustered_config())

    assert evidence.passed is False
    assert evidence.blockers == ("clickhouse_cluster_target_uuid_mismatch",)
    assert evidence.uuids == ("uuid-a", "uuid-b")


def test_clickhouse_cluster_topology_skips_unreachable_hosts_after_cluster_all_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DPONE_CLICKHOUSE_SKIP_UNAVAILABLE_SHARDS", "1")
    evidence = ClickHouseClusterTopologyProbe(_IsolatedDc1Connector(fail_cluster_all=True)).inspect(_clustered_config())

    assert evidence.passed is True
    assert evidence.status == "passed_partial"
    assert evidence.actual_hosts == ("dc2", "dc3")
    assert evidence.unreachable_hosts == ("dc1",)
    assert evidence.missing_hosts == ("dc1",)
    assert "clickhouse_cluster_unreachable_replicas_skipped" in evidence.warnings


def test_clickhouse_cluster_topology_skips_unreachable_when_cluster_all_returns_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DPONE_CLICKHOUSE_SKIP_UNAVAILABLE_SHARDS", "1")
    evidence = ClickHouseClusterTopologyProbe(_IsolatedDc1Connector(fail_cluster_all=False)).inspect(
        _clustered_config()
    )

    assert evidence.passed is True
    assert evidence.status == "passed_partial"
    assert evidence.actual_hosts == ("dc2", "dc3")
    assert evidence.unreachable_hosts == ("dc1",)


def test_clickhouse_cluster_topology_propagates_unavailable_when_skip_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DPONE_CLICKHOUSE_SKIP_UNAVAILABLE_SHARDS", "0")
    with pytest.raises(RuntimeError, match="Code: 279"):
        ClickHouseClusterTopologyProbe(_IsolatedDc1Connector(fail_cluster_all=True)).inspect(_clustered_config())


def test_clickhouse_sink_publishes_topology_blocker_before_extract() -> None:
    publisher = _DecisionPublisher()
    sink = ClickHouseSink(_ClusterConnector(actual_hosts=("dc3",)))

    with RuntimeDecisionContext.activate(publisher), pytest.raises(RuntimeError, match="missing_hosts=dc1, dc2"):
        sink.preflight_before_extract(load_config=_clustered_config())

    decision = publisher.decisions[-1]
    assert decision.decision_id == "clickhouse.cluster_target_topology"
    assert decision.release_gate == "blocked"
    assert decision.blockers == ("clickhouse_cluster_target_missing_replicas",)
    assert decision.details["missing_hosts"] == ["dc1", "dc2"]


def test_etl_processor_runs_preflight_before_source_extract() -> None:
    source = _Source()
    sink = _PreflightSink()
    processor = ETLProcessor(source, sink)

    with pytest.raises(RuntimeError, match="preflight failed"):
        processor.run(_clustered_config())

    assert sink.preflight_called is True
    assert source.extract_called is False


class _Source:
    extract_called = False

    def extract(self, load_config: Any, state: Any) -> Any:
        del load_config, state
        self.extract_called = True
        raise AssertionError("extract should not run after failed preflight")


class _PreflightSink:
    preflight_called = False

    def preflight_before_extract(self, *, load_config: Any, load_record: Any | None = None) -> None:
        del load_config, load_record
        self.preflight_called = True
        raise RuntimeError("preflight failed")


def _clustered_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="Example_Datamarts",
        target_table="orders",
        options={"physical_design": {"storage": {"clickhouse": {"cluster": "dwh"}}}},
    )
