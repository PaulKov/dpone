"""Opt-in real-row bounded reader checks, not durable-publication certification.

Set ``DPONE_RUN_CLICKHOUSE_TARGET_READER=1`` only for an approved disposable
cluster where this Python process and its children can reach every advertised
native replica endpoint. The existing Compose fixture advertises node1/node2
on port 9000; its localhost port mappings alone do not satisfy that condition.

Optional DPONE_TARGET_READER_HOST/PORT/USER/PASSWORD/CLUSTER values configure
the fixture connection in memory. Defaults select node1:9000, default user and
publication_cluster. No connection descriptor or credential is persisted.

Each case creates and drops only a randomly named Atomic database and a plain
ReplicatedMergeTree candidate. Its request binding comes from real replica
metadata, not a KeeperMap authority. Passing this test establishes native reader
behavior for these rows only. Durable publication, guard CAS/retirement, ACK loss
and CLI/Airflow live completion remain UNVERIFIED and need a separate profile.
"""

from __future__ import annotations

import os
import re
import secrets
import time
from collections.abc import Iterator
from typing import Any

import pytest

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.getenv("DPONE_RUN_CLICKHOUSE_TARGET_READER") != "1",
        reason="set DPONE_RUN_CLICKHOUSE_TARGET_READER=1 only for an approved reachable disposable cluster",
    ),
]

_SCHEMA = (("id", "UInt64"), ("value", "Nullable(String)"))


@pytest.fixture
def native_reader_candidate() -> Iterator[tuple[Any, dict[str, Any], str, str]]:
    """Own one isolated database; never modify the shared authority or receipts."""
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector

    cluster = os.getenv("DPONE_TARGET_READER_CLUSTER", "publication_cluster")
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cluster) is None:
        pytest.fail("DPONE_TARGET_READER_CLUSTER must be a simple identifier")
    database = "dpone_target_reader_" + secrets.token_hex(12)
    connection = {
        "host": os.getenv("DPONE_TARGET_READER_HOST", "node1"),
        "port": int(os.getenv("DPONE_TARGET_READER_PORT", "9000")),
        "database": "default",
        "user": os.getenv("DPONE_TARGET_READER_USER", "default"),
        "password": os.getenv("DPONE_TARGET_READER_PASSWORD", ""),
        "driver": "native",
        "compression": False,
        "connect_timeout": 10,
        "send_receive_timeout": 30,
        "settings": {"distributed_ddl_output_mode": "throw", "skip_unavailable_shards": 0},
    }
    connector = ClickHouseConnector(**connection)
    try:
        connector.execute_query(f"CREATE DATABASE `{database}` ON CLUSTER `{cluster}` ENGINE=Atomic")
        connector.execute_query(
            f"CREATE TABLE `{database}`.candidate ON CLUSTER `{cluster}` "
            "(id UInt64, value Nullable(String)) "
            f"ENGINE=ReplicatedMergeTree('/{database}/{{uuid}}/{{shard}}', '{{replica}}') ORDER BY id"
        )
        yield connector, connection, cluster, database
    finally:
        try:
            connector.execute_query(f"DROP DATABASE IF EXISTS `{database}` ON CLUSTER `{cluster}` SYNC")
        finally:
            connector.close()


def _synchronize(connector: Any, connection: dict[str, Any], cluster: str, database: str) -> Any:
    """Wait for each real replica before the immutable observation begins."""
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector
    from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog

    inventory = ClickHouseClusterPublicationCatalog(connector).inventory(cluster)
    for replica in inventory.replicas:
        member = ClickHouseConnector(**{**connection, "host": replica.host, "port": replica.native_port})
        try:
            member.execute_query(f"SYSTEM SYNC REPLICA `{database}`.candidate")
        finally:
            member.close()
    return inventory


@pytest.mark.parametrize("with_rows", [False, True], ids=["empty-zero-aggregate", "nullable-duplicate-values"])
def test_real_native_target_reader_exact_metrics(native_reader_candidate: Any, with_rows: bool) -> None:
    """Read actual nullable rows once from one replica; never sum replicas."""
    from dpone.adapters.target_acceptance.native import inspect_replicas
    from dpone.adapters.target_acceptance.reader import BoundedClickHouseTargetAcceptanceReader
    from dpone.contracts.quality_replay import quality_digest
    from dpone.contracts.target_acceptance import TargetAcceptanceRequest, validate_target_observation
    from dpone.runtime.governance.quality_replay_identity import schema_digest

    connector, connection, cluster, database = native_reader_candidate
    if with_rows:
        connector.execute_query(f"INSERT INTO `{database}`.candidate VALUES (1, NULL), (2, 'a'), (3, 'a')")
    inventory = _synchronize(connector, connection, cluster, database)
    reader = BoundedClickHouseTargetAcceptanceReader.from_connector(connector)
    reader.require_ready(cluster=cluster, database=database, table="candidate")
    # Use the same inert descriptor fields the production worker reconstructs.
    descriptor = {
        name: getattr(connector, name)
        for name in (
            "host",
            "port",
            "driver",
            "database",
            "user",
            "password",
            "secure",
            "compression",
            "connect_timeout",
            "send_receive_timeout",
            "ca_cert",
            "settings",
        )
    }
    observed_inventory, facts = inspect_replicas(descriptor, cluster, database, "candidate", time.monotonic() + 60)
    assert observed_inventory == inventory
    assert set(facts) == set(inventory.hosts)
    desired = {key: value for key, value in facts[inventory.hosts[0]].items() if key != "columns"}
    assert all({key: value for key, value in fact.items() if key != "columns"} == desired for fact in facts.values())
    binding = {"inventory_digest": inventory.digest, "desired": desired}
    selection = {"row_count": True, "null_columns": ["value"], "distinct_columns": ["id", "value"]}
    request = TargetAcceptanceRequest(
        cluster=cluster,
        database=database,
        table="candidate",
        dataset=f"{database}.candidate",
        columns=_SCHEMA,
        selection_digest=quality_digest(selection),
        schema_digest=schema_digest(_SCHEMA),
        binding=binding,
        reader_token=secrets.token_hex(16),
        core_digest=quality_digest({"binding": binding, "selection": selection, "schema": _SCHEMA}),
        row_count=True,
        null_columns=("value",),
        distinct_columns=("id", "value"),
    )
    reader.validate_plan(request)
    deadline = time.monotonic() + 60
    reader.verify_generation(request, deadline=deadline)
    observation = reader.collect(request, deadline=deadline)
    reader.verify_generation(request, deadline=deadline)
    assert time.monotonic() < deadline
    validate_target_observation(request, observation)
    assert observation["replica"] == sorted(inventory.hosts)[0]
    assert observation["row_count"] == (3 if with_rows else 0)
    assert observation["null_counts"] == {"value": 1 if with_rows else 0}
    assert observation["distinct_counts"] == {"id": 3 if with_rows else 0, "value": 1 if with_rows else 0}
    assert observation["warnings"] == []
    assert observation["binding"] == binding
    assert observation["capture_boundary"] == "committed_generation_before_governance_complete"
    assert "captured_before_cleanup" not in observation
    # No serialized guard/reader capability leaks into the public observation.
    assert request.reader_token not in str(observation)
