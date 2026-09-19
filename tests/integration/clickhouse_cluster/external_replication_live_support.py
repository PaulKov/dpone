"""Shared production-composed helpers for the external publication Docker profile."""

from __future__ import annotations

import secrets
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.connectors.clickhouse import ClickHouseConnector
from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sinks.load_payload import LoadPayload

CLUSTER = "external_publication_cluster"


def publication_case(
    label: str,
    *,
    database: str | None = None,
    initialize: bool = True,
    rows: list[dict[str, int]] | None = None,
    source_byte_budget: int = 1024 * 1024,
    content_row_budget: int = 100,
) -> tuple[ClickHouseSink, LoadConfig, LoadPayload, str]:
    """Create one isolated real-sink case against the pinned two-member fixture."""

    database = database or f"external_publication_{label}_{secrets.token_hex(4)}"
    if initialize:
        execute(18123, f"CREATE DATABASE {database} ON CLUSTER `{CLUSTER}` ENGINE=Atomic")
        for port in (18123, 28123):
            execute(port, f"CREATE TABLE {database}.target (id Int64) ENGINE=MergeTree ORDER BY id")
            execute(port, f"INSERT INTO {database}.target VALUES (1)")
    connector = ClickHouseConnector(
        host="127.0.0.1",
        port=18123,
        database=database,
        user="default",
        password="",
        driver="http",
        external_member_endpoint_resolver=docker_member_endpoint,
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
            SCHEDULER_IDENTITY_OPTION: f"docker-external-{label}",
            SOURCE_BYTE_BUDGET_OPTION: source_byte_budget,
            "external_artifact_store_path": "/tmp/dpone-external-artifacts-docker",
            "lineage": False,
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "engine": "MergeTree",
                        "order_by": ["id"],
                        "cluster": {
                            "name": CLUSTER,
                            "ddl_scope": "cluster",
                            "replication_mode": "external",
                            "external_content_row_budget": content_row_budget,
                        },
                    }
                }
            },
        },
    )
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 10}, {"id": 20}] if rows is None else rows),
        schema=(("id", "bigint"),),
    )
    return ClickHouseSink(connector), config, payload, database


def docker_member_endpoint(host: str, address: str, port: int) -> tuple[str, str, int]:
    """Resolve only known fixture member identities to their published native ports."""

    del address, port
    published_port = {"node1": 19000, "node2": 29000}.get(host)
    if published_port is None:
        raise ValueError("unexpected Docker member")
    return "127.0.0.1", "127.0.0.1", published_port


def execute(port: int, sql: str) -> list[tuple[str, ...]]:
    """Execute SQL against a fixed local fixture endpoint."""

    statement = sql + " FORMAT TabSeparated" if sql.lstrip().upper().startswith("SELECT") else sql
    request = Request(
        f"http://127.0.0.1:{port}/?" + urlencode({"distributed_ddl_output_mode": "throw"}),
        data=statement.encode(),
        method="POST",
    )
    with urlopen(request, timeout=60) as response:  # noqa: S310 - fixed local Docker endpoint
        body = response.read().decode().strip()
    return [tuple(line.split("\t")) for line in body.splitlines()] if body else []


__all__ = ["CLUSTER", "docker_member_endpoint", "execute", "publication_case"]
