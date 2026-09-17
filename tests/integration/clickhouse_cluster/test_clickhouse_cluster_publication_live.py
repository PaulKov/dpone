"""Pinned two-replica ClickHouse/Keeper protocol acceptance."""

from __future__ import annotations

import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest

from tests.integration.clickhouse_cluster.evidence import record_scenario

pytestmark = pytest.mark.integration_live

_CLUSTER = "publication_cluster"


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_keeper_cas_and_distributed_ddl_correlation() -> None:
    cluster = _CLUSTER
    _execute(f"DROP DATABASE IF EXISTS publication_acceptance ON CLUSTER {cluster} SYNC")
    _execute(f"CREATE DATABASE publication_acceptance ON CLUSTER {cluster} ENGINE=Atomic")
    _execute(
        f"CREATE TABLE publication_acceptance.authority ON CLUSTER {cluster} "
        "(target_key String PRIMARY KEY, operation_id String, fence_token String, phase String, "
        "dispatch_epoch UInt64, payload String, payload_sha256 FixedString(64)) "
        "ENGINE=KeeperMap('/publication_acceptance')"
    )
    _execute(
        "INSERT INTO publication_acceptance.authority VALUES "
        "('target','operation','fence','PREPARED',0,'{}','" + "0" * 64 + "')",
        keeper_map_strict_mode=1,
        insert_keeper_max_retries=0,
        query_id="publication-authority-create",
    )
    before = int(_execute("SELECT _version FROM publication_acceptance.authority WHERE target_key='target'")[0][0])
    _execute(
        "ALTER TABLE publication_acceptance.authority UPDATE phase='DISPATCHING', dispatch_epoch=1 "
        f"WHERE target_key='target' AND _version={before} AND operation_id='operation' "
        "AND fence_token='fence' AND phase='PREPARED' SETTINGS keeper_map_strict_mode=1, insert_keeper_max_retries=0",
        query_id="publication-authority-cas",
    )
    raw_after = _execute(
        "SELECT phase, dispatch_epoch, _version FROM publication_acceptance.authority WHERE target_key='target'"
    )[0]
    after = (raw_after[0], int(raw_after[1]), int(raw_after[2]))
    token = f"dpone-v1-synthetic-publish-1-{secrets.token_hex(16)}"
    leading_comment = f"leading-comment-{secrets.token_hex(8)}"
    _execute(
        f"/* {leading_comment} */ CREATE TABLE publication_acceptance.probe ON CLUSTER {cluster} (id UInt8) ENGINE=Log",
        log_comment=token,
        skip_unavailable_shards=0,
        distributed_ddl_output_mode="throw",
        query_id="publication-ddl-probe",
    )
    raw_entries = _execute(
        "SELECT uniqExact(entry), uniqExact(settings['log_comment']) FROM system.distributed_ddl_queue "
        f"WHERE cluster='{cluster}' AND settings['log_comment']='{token}'"
    )[0]
    entries = (int(raw_entries[0]), int(raw_entries[1]))
    stored_queries = _execute(
        "SELECT query FROM system.distributed_ddl_queue "
        f"WHERE cluster='{cluster}' AND settings['log_comment']='{token}' GROUP BY query"
    )
    assert after == ("DISPATCHING", 1, before + 1)
    assert entries == (1, 1)
    assert stored_queries and all(leading_comment not in query for (query,) in stored_queries)
    _record_scenario(
        "keeper_cas_and_log_comment",
        "passed_live",
        keeper_version_before=before,
        keeper_version_after=after[2],
        correlated_entries=entries[0],
    )


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_service_publishes_and_cleans_exact_replicated_generations() -> None:
    cluster, database = _CLUSTER, _database_name("publication_service")
    _create_database(database)
    _create_replicated_table(database, "target")
    _create_replicated_table(database, "candidate")
    _execute(f"INSERT INTO {database}.target VALUES (1)")
    _execute(f"INSERT INTO {database}.candidate VALUES (10),(20)")
    _wait_for_counts(database, "target", expected=1)
    _wait_for_counts(database, "candidate", expected=2)
    _connector, _catalog, service = _service(database)
    config, candidate = _configs(database, scheduler_identity="docker-existing-target")
    receipt = service.publish(config, candidate, staged_rows=2)
    assert receipt.authority.ddl_entry
    _wait_for_counts(database, "target", expected=2)
    service.cleanup(receipt)
    assert _execute(
        f"SELECT count() FROM clusterAllReplicas('{cluster}', system.tables) "
        f"WHERE database='{database}' AND name='candidate'"
    ) == [("0",)]
    absent_database = _database_name("publication_absent")
    _create_database(absent_database)
    _create_replicated_table(absent_database, "candidate")
    _execute(f"INSERT INTO {absent_database}.candidate VALUES (30),(40)")
    _wait_for_counts(absent_database, "candidate", expected=2)
    _connector, _catalog, absent_service = _service(absent_database)
    absent_config, absent_candidate = _configs(absent_database, scheduler_identity="docker-absent-target")
    absent_receipt = absent_service.publish(absent_config, absent_candidate, staged_rows=2)
    absent_service.cleanup(absent_receipt)
    _wait_for_counts(absent_database, "target", expected=2)
    assert _execute(
        "SELECT count() FROM clusterAllReplicas('publication_cluster', system.tables) "
        f"WHERE database='{absent_database}' AND name='candidate'"
    ) == [("0",)]
    _record_scenario(
        "normal_existing_and_absent_target",
        "passed_live",
        normal_publication_entry=receipt.authority.ddl_entry,
        absent_publication_entry=absent_receipt.authority.ddl_entry,
    )


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_lost_publication_and_cleanup_responses_reconcile_without_redispatch() -> None:
    from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl

    database = _database_name("publication_lost_response")
    _create_database(database)
    _create_replicated_table(database, "target")
    _create_replicated_table(database, "candidate")
    _execute(f"INSERT INTO {database}.target VALUES (1)")
    _execute(f"INSERT INTO {database}.candidate VALUES (10),(20)")
    _wait_for_counts(database, "candidate", expected=2)
    connector, catalog, _service_unused = _service(database)
    delegate = ClickHouseClusterPublicationDdl(connector, catalog)

    class LostResponseDdl:
        def __init__(self) -> None:
            self.publication_calls = 0
            self.cleanup_calls = 0

        def publication_query_digest(self, record, *, cluster):
            return delegate.publication_query_digest(record, cluster=cluster)

        def cleanup_query_digest(self, record, *, cluster):
            return delegate.cleanup_query_digest(record, cluster=cluster)

        def dispatch_publication(self, record, permit, *, cluster):
            self.publication_calls += 1
            delegate.dispatch_publication(record, permit, cluster=cluster)
            raise TimeoutError("synthetic response loss after publication commit")

        def drop_predecessor(self, record, permit, *, cluster):
            self.cleanup_calls += 1
            delegate.drop_predecessor(record, permit, cluster=cluster)
            raise TimeoutError("synthetic response loss after cleanup commit")

        def find_entries(self, cluster, correlation_token):
            return delegate.find_entries(cluster, correlation_token)

        def read_entry(self, cluster, entry):
            return delegate.read_entry(cluster, entry)

    ddl = LostResponseDdl()
    service = _service(database, ddl=ddl)[2]
    config, candidate = _configs(database, scheduler_identity="docker-lost-response")
    receipt = service.publish(config, candidate, staged_rows=2)
    assert ddl.publication_calls == 1
    assert len(delegate.find_entries(_CLUSTER, receipt.authority.ddl_correlation_token or "")) == 1
    service.cleanup(receipt)
    assert ddl.cleanup_calls == 1
    assert _execute(
        f"SELECT count() FROM clusterAllReplicas('{_CLUSTER}', system.tables) "
        f"WHERE database='{database}' AND name='candidate'"
    ) == [("0",)]
    _record_scenario("lost_publication_response", "passed_live")
    _record_scenario("lost_cleanup_response", "passed_live")


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_real_keeper_worker_race_and_lost_cas_response_fail_closed() -> None:
    from dpone.contracts.clickhouse_cluster_publication import (
        AuthorityMutationStatus,
        AuthorityPhase,
        AuthorityRecord,
        GenerationIdentity,
    )
    from dpone.runtime.sinks.clickhouse_cluster_publication_authority import ClickHouseKeeperMapAuthority

    database = "publication_authority_faults"
    _create_database(database)
    connector, _catalog, _service_unused = _service(database)
    # Bootstrap through the real service composition, then use the raw authority.
    inventory = _catalog.inventory(_CLUSTER)
    from dpone.runtime.sinks.clickhouse_cluster_publication_bootstrap import ClickHouseClusterAuthorityBootstrap

    ClickHouseClusterAuthorityBootstrap(connector, _catalog).ensure(_CLUSTER, database, inventory.hosts)
    identity = GenerationIdentity(
        "00000000-0000-0000-0000-000000000001",
        "ReplicatedMergeTree('/synthetic/{uuid}/{shard}', '{replica}')",
        "schema",
        "default",
        "/synthetic/desired",
    )

    def record(target_key: str, operation: str) -> AuthorityRecord:
        return AuthorityRecord(
            target_key=target_key,
            operation_id=operation,
            fence_token=f"fence-{operation}",
            phase=AuthorityPhase.PREPARED,
            dispatch_epoch=0,
            inventory_digest="inventory",
            plan_digest="plan",
            database=database,
            target="target",
            candidate="candidate",
            desired=identity,
            predecessor=None,
            staged_rows=0,
        )

    authority = ClickHouseKeeperMapAuthority(connector, database)
    nonce = secrets.token_hex(8)
    race_record = record(f"race-target-{nonce}", f"race-operation-{nonce}")
    assert authority.create_if_absent(race_record).status is AuthorityMutationStatus.VERIFIED
    prior = authority.read_versioned(race_record.target_key)
    assert prior is not None
    candidates = (
        replace(race_record, phase=AuthorityPhase.DISPATCHING, dispatch_epoch=1, ddl_correlation_token="race-a"),
        replace(race_record, phase=AuthorityPhase.DISPATCHING, dispatch_epoch=1, ddl_correlation_token="race-b"),
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda desired: authority.compare_and_swap(prior, desired), candidates))
    assert sum(item.status.value == "verified" for item in results) == 1
    assert sum(item.permit is not None for item in results) == 1
    assert all(item.permit is None for item in results if item.status.value != "verified")

    lost_record = record(f"lost-cas-target-{nonce}", f"lost-cas-operation-{nonce}")
    assert authority.create_if_absent(lost_record).status is AuthorityMutationStatus.VERIFIED
    lost_prior = authority.read_versioned(lost_record.target_key)
    assert lost_prior is not None
    desired = replace(
        lost_record,
        phase=AuthorityPhase.DISPATCHING,
        dispatch_epoch=1,
        ddl_correlation_token="lost-cas",
    )

    class ExecuteThenLose:
        def __init__(self, connection: Any) -> None:
            self._connection = connection
            self.calls = 0

        def execute(self, *args: Any, **kwargs: Any) -> Any:
            self.calls += 1
            self._connection.execute(*args, **kwargs)
            raise TimeoutError("synthetic response loss after committed CAS")

    class ConnectorProxy:
        def __init__(self, base: Any) -> None:
            self.connection = ExecuteThenLose(base.connection)

        def get_records(self, *args: Any, **kwargs: Any) -> Any:
            return connector.get_records(*args, **kwargs)

    proxy = ConnectorProxy(connector)
    lost_result = ClickHouseKeeperMapAuthority(proxy, database).compare_and_swap(lost_prior, desired)
    assert lost_result.status is AuthorityMutationStatus.OUTCOME_UNKNOWN
    assert lost_result.permit is None
    assert proxy.connection.calls == 1
    independently_observed = authority.read_versioned(lost_record.target_key)
    assert independently_observed is not None and independently_observed.record == desired
    _record_scenario("worker_race", "passed_live")
    _record_scenario("lost_cas_response", "passed_live")


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_duplicate_correlation_and_catalog_drift_are_blocking() -> None:
    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError
    from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog

    database = "publication_blockers"
    _create_database(database)
    connector, catalog, service = _service(database)
    token = f"dpone-v1-duplicate-{secrets.token_hex(16)}"
    for suffix in ("a", "b"):
        _execute(
            f"CREATE TABLE {database}.probe_{suffix} ON CLUSTER {_CLUSTER} (id UInt8) ENGINE=Log",
            log_comment=token,
            skip_unavailable_shards=0,
            distributed_ddl_output_mode="throw",
        )
    assert len(ClickHouseClusterPublicationCatalog(connector).find_entries(_CLUSTER, token)) == 2

    _create_replicated_table(database, "target")
    _create_replicated_table(database, "candidate")
    _execute(f"INSERT INTO {database}.candidate VALUES (1)")
    _wait_for_counts(database, "candidate", expected=1)
    _execute_at(28123, f"DROP TABLE {database}.candidate SYNC")
    _execute_at(
        28123,
        f"CREATE TABLE {database}.candidate (id UInt64) "
        f"ENGINE=ReplicatedMergeTree('/{database}/drift/{{shard}}', '{{replica}}') ORDER BY id",
    )
    config, candidate = _configs(database, scheduler_identity="docker-identity-drift")
    with pytest.raises(ClusterPublicationError, match="GENERATION_DIVERGED"):
        service.publish(config, candidate, staged_rows=1)
    _record_scenario("duplicate_correlation_token", "passed_live")
    _record_scenario("identity_and_membership_drift", "partially_passed_schema_drift_live")


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_partial_authority_bootstrap_repairs_absence_and_blocks_mismatch() -> None:
    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError
    from dpone.runtime.sinks.clickhouse_cluster_publication_authority import AUTHORITY_TABLE
    from dpone.runtime.sinks.clickhouse_cluster_publication_bootstrap import ClickHouseClusterAuthorityBootstrap

    database = "publication_partial_bootstrap"
    _create_database(database)
    connector, catalog, _service_unused = _service(database)
    hosts = catalog.inventory(_CLUSTER).hosts
    _execute(
        f"CREATE TABLE {database}.{AUTHORITY_TABLE} "
        "(target_key String PRIMARY KEY, operation_id String, fence_token String, phase String, "
        "dispatch_epoch UInt64, payload String, payload_sha256 FixedString(64)) "
        "ENGINE=KeeperMap('/dpone_cluster_publication_authority')"
    )
    ClickHouseClusterAuthorityBootstrap(connector, catalog).ensure(_CLUSTER, database, hosts)
    facades = _execute(
        f"SELECT count() FROM clusterAllReplicas('{_CLUSTER}', system.tables) "
        f"WHERE database='{database}' AND name='{AUTHORITY_TABLE}'"
    )
    assert facades == [("2",)]

    mismatch = "publication_mismatched_bootstrap"
    _create_database(mismatch)
    mismatch_connector, mismatch_catalog, _unused = _service(mismatch)
    _execute(
        f"CREATE TABLE {mismatch}.{AUTHORITY_TABLE} "
        "(target_key String PRIMARY KEY, operation_id String, fence_token String, phase String, "
        "dispatch_epoch UInt64, payload String, payload_sha256 FixedString(64)) "
        "ENGINE=KeeperMap('/wrong_publication_authority')"
    )
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_INVALID"):
        ClickHouseClusterAuthorityBootstrap(mismatch_connector, mismatch_catalog).ensure(_CLUSTER, mismatch, hosts)
    _record_scenario("partial_authority_bootstrap", "passed_live")


def _wait_for_counts(database: str, table: str, *, expected: int) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        rows = _execute(
            "SELECT groupArray(rows) FROM ("
            f"SELECT hostName(), count() AS rows FROM clusterAllReplicas('{_CLUSTER}', {database}.{table}) "
            "GROUP BY hostName() ORDER BY hostName())"
        )
        if rows and rows[0][0] == f"[{expected},{expected}]":
            return
        time.sleep(0.2)
    raise AssertionError(f"replicated count did not converge for {database}.{table}")


def _execute(sql: str, **settings: object) -> list[tuple[str, ...]]:
    return _execute_at(18123, sql, **settings)


def _execute_at(port: int, sql: str, **settings: object) -> list[tuple[str, ...]]:
    params = {key: str(value) for key, value in settings.items()}
    statement = sql + " FORMAT TabSeparated" if sql.lstrip().upper().startswith("SELECT") else sql
    request = Request(
        f"http://127.0.0.1:{port}/?" + urlencode(params),
        data=statement.encode(),
        method="POST",
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed local Docker endpoint
        body = response.read().decode().strip()
    return [tuple(line.split("\t")) for line in body.splitlines()] if body else []


def _create_database(database: str) -> None:
    _execute(f"DROP DATABASE IF EXISTS {database} ON CLUSTER {_CLUSTER} SYNC")
    _execute(f"CREATE DATABASE {database} ON CLUSTER {_CLUSTER} ENGINE=Atomic")


def _database_name(prefix: str) -> str:
    """Keep live reruns isolated from intentionally retained Keeper slots."""

    return f"{prefix}_{secrets.token_hex(4)}"


def _create_replicated_table(database: str, table: str) -> None:
    _execute(
        f"CREATE TABLE {database}.{table} ON CLUSTER {_CLUSTER} (id UInt64) "
        f"ENGINE=ReplicatedMergeTree('/{database}/{{uuid}}/{{shard}}', '{{replica}}') ORDER BY id"
    )


def _service(database: str, *, ddl: Any = None) -> tuple[Any, Any, Any]:
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector
    from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
        ClickHouseClusterFullRefreshPublicationService,
    )
    from dpone.runtime.sinks.clickhouse_cluster_publication_authority import ClickHouseKeeperMapAuthority
    from dpone.runtime.sinks.clickhouse_cluster_publication_bootstrap import ClickHouseClusterAuthorityBootstrap
    from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog
    from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl

    connector = ClickHouseConnector(
        host="127.0.0.1",
        port=18123,
        database=database,
        user="default",
        password="",
        driver="http",
    )
    catalog = ClickHouseClusterPublicationCatalog(connector)
    publication_ddl = ddl or ClickHouseClusterPublicationDdl(connector, catalog)
    service = ClickHouseClusterFullRefreshPublicationService(
        catalog,
        lambda selected: ClickHouseKeeperMapAuthority(connector, selected),
        publication_ddl,
        ClickHouseClusterAuthorityBootstrap(connector, catalog),
    )
    return connector, catalog, service


def _configs(database: str, *, scheduler_identity: str) -> tuple[Any, Any]:
    from dpone.config.load_config import LoadConfig
    from dpone.config.load_strategy import LoadStrategy
    from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION

    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="source",
        source_table="source",
        target_schema=database,
        target_table="target",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            SCHEDULER_IDENTITY_OPTION: scheduler_identity,
            "physical_design": {"storage": {"clickhouse": {"cluster": _CLUSTER}}},
        },
    )
    return config, LoadConfig(**{**config.__dict__, "target_table": "candidate"})


def _record_scenario(name: str, result: str, **details: Any) -> None:
    record_scenario(
        name,
        result,
        server_version=_execute("SELECT version()")[0][0],
        details=details,
    )
