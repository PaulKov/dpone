"""Pinned live acceptance for external-replication publication adapters."""

from __future__ import annotations

import os
import secrets
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest

from tests.integration.clickhouse_cluster.evidence import record_scenario

pytestmark = pytest.mark.integration_live

_CLUSTER = "external_publication_cluster"


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_external_replication_stages_each_member_and_fresh_service_cleans_exact_predecessors() -> None:
    from dpone.config.load_config import LoadConfig
    from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
    from dpone.runtime.artifacts import InMemoryRowsArtifact
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector
    from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION
    from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
    from dpone.runtime.sinks.load_payload import LoadPayload

    database = f"external_publication_{secrets.token_hex(4)}"
    _execute(18123, f"CREATE DATABASE {database} ON CLUSTER `{_CLUSTER}` ENGINE=Atomic")
    for port in (18123, 28123):
        _execute(port, f"CREATE TABLE {database}.target (id Int64) ENGINE=MergeTree ORDER BY id")
        _execute(port, f"INSERT INTO {database}.target VALUES (1)")
    connector = ClickHouseConnector(
        host="127.0.0.1",
        port=18123,
        database=database,
        user="default",
        password="",
        driver="http",
        external_member_endpoint_resolver=_docker_member_endpoint,
    )
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="source",
        source_table="source_table",
        target_schema=database,
        target_table="target",
        load_strategy=LoadStrategy.FULL_REFRESH,
        staging_schema=database,
        options={
            SCHEDULER_IDENTITY_OPTION: "docker-external-publication",
            SOURCE_BYTE_BUDGET_OPTION: 1024 * 1024,
            "lineage": False,
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "engine": "MergeTree",
                        "order_by": ["id"],
                        "cluster": {
                            "name": _CLUSTER,
                            "ddl_scope": "cluster",
                            "replication_mode": "external",
                            "external_content_row_budget": 100,
                        },
                    }
                }
            },
        },
    )
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 10}, {"id": 20}]),
        schema=(("id", "bigint"),),
    )
    sink = ClickHouseSink(connector)
    admitted = sink._full_refresh_publication.prepare_admission(config)
    result = sink.load(admitted, payload)
    receipt = (result.reconciliation_metrics or {})["clickhouse_cluster_external_full_refresh"]

    assert result.total_rows == 2
    assert receipt["phase"] == "COMMITTED"
    assert len(receipt["member_ids"]) == 2
    for port in (18123, 28123):
        assert _execute(port, f"SELECT groupArray(id) FROM {database}.target") == [("[10,20]",)]
        assert _execute(
            port,
            f"SELECT count() FROM system.tables WHERE database='{database}' AND name LIKE 'target__dpone_ext_%'",
        ) == [("0",)]
    record_scenario(
        "external_replication_fresh_cleanup",
        "passed_live",
        server_version=_execute(18123, "SELECT version()")[0][0],
        details={
            "operation_id": result.commit_receipt_id,
            "member_count": len(receipt["member_ids"]),
            "evidence_scope": "local_docker_live",
            "production_composition": True,
        },
    )


def _docker_member_endpoint(host: str, address: str, port: int) -> tuple[str, str, int]:
    del address, port
    published_port = {"node1": 19000, "node2": 29000}.get(host)
    if published_port is None:
        raise ValueError("unexpected Docker member")
    return "127.0.0.1", "127.0.0.1", published_port


def _execute(port: int, sql: str) -> list[tuple[str, ...]]:
    statement = sql + " FORMAT TabSeparated" if sql.lstrip().upper().startswith("SELECT") else sql
    request = Request(
        f"http://127.0.0.1:{port}/?" + urlencode({"distributed_ddl_output_mode": "throw"}),
        data=statement.encode(),
        method="POST",
    )
    with urlopen(request, timeout=60) as response:  # noqa: S310 - fixed local Docker endpoint
        body = response.read().decode().strip()
    return [tuple(line.split("\t")) for line in body.splitlines()] if body else []
