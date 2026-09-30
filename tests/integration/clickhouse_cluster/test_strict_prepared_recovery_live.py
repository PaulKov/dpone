"""Opt-in real KeeperMap and three-replica recovery; never a PROD certificate."""

from __future__ import annotations

import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.ports.clickhouse_cluster_publication import contracts, require_verified_mutation
from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog
from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl
from dpone.runtime.sinks.clickhouse_prepared_recovery import PreparedRecoveryService, plan_prepared_recovery
from dpone.runtime.sinks.clickhouse_quality_authority import ClickHouseQualityKeeperMapAuthority
from tests.integration.clickhouse_cluster.test_clickhouse_cluster_publication_live import _configs

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(os.getenv("DPONE_RUN_STRICT_PREPARED_RECOVERY") != "1", reason="opt-in local strict fixture"),
]
CLUSTER = "publication_cluster"
HOSTS = ("node1", "node2", "node3")


class NativeConnector:
    def __init__(self, port=39100):
        from clickhouse_driver import Client

        self.connection = Client(host="127.0.0.1", port=port, send_receive_timeout=30)

    def get_records(self, sql, params=None):
        return self.connection.execute(sql, params or {})


@pytest.fixture
def strict_cluster():
    connector = NativeConnector()
    database = "strict_recovery_" + secrets.token_hex(6)
    deadline = time.monotonic() + 45
    while True:
        try:
            assert connector.get_records(
                "SELECT count() FROM clusterAllReplicas('publication_cluster', system.one)"
            ) == [(3,)]
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)
    connector.get_records(f"CREATE DATABASE {database} ON CLUSTER {CLUSTER} ENGINE=Atomic")
    connector.get_records(
        f"CREATE TABLE {database}.{contracts.AUTHORITY_TABLE} ON CLUSTER {CLUSTER} "
        "(target_key String, operation_id String, fence_token String, phase String, "
        "dispatch_epoch UInt64, payload String, payload_sha256 FixedString(64)) "
        f"ENGINE=KeeperMap('/strict-recovery/{database}') PRIMARY KEY target_key"
    )
    try:
        yield connector, database
    finally:
        connector.get_records(f"DROP DATABASE {database} ON CLUSTER {CLUSTER} SYNC")
        connector.connection.disconnect()


def _authority(connector, database):
    authority = ClickHouseQualityKeeperMapAuthority(connector, database)
    authority.require_ready(CLUSTER, database, HOSTS)
    return authority


def _table(connector, database, name, values):
    connector.get_records(
        f"CREATE TABLE {database}.{name} ON CLUSTER {CLUSTER} (id UInt64) "
        f"ENGINE=ReplicatedMergeTree('/strict-recovery/{database}/{{uuid}}', '{{replica}}') ORDER BY id"
    )
    connector.get_records(f"INSERT INTO {database}.{name} VALUES " + ",".join(f"({value})" for value in values))
    deadline = time.monotonic() + 30
    while True:
        rows = connector.get_records(
            f"SELECT hostName(), count() FROM clusterAllReplicas('{CLUSTER}', {database}.{name}) "
            "GROUP BY hostName() ORDER BY hostName()"
        )
        if rows == [(host, len(values)) for host in HOSTS]:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"replication did not settle: {rows}")
        time.sleep(0.2)


def test_real_three_replica_reused_slot_recovers_original_operation(strict_cluster):
    connector, database = strict_cluster
    catalog = ClickHouseClusterPublicationCatalog(connector)
    authority = _authority(connector, database)
    ddl = ClickHouseClusterPublicationDdl(connector, catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: authority, ddl, authority)
    _table(connector, database, "target", [1])
    for generation in (1, 2, 3):
        _table(connector, database, "candidate", [generation * 10, generation * 10 + 1])
        config, candidate = _configs(database, scheduler_identity=f"strict-generation-{generation}")

        def interrupted_cas(current, desired):
            if desired.phase is contracts.AuthorityPhase.DISPATCHING:
                raise KeyboardInterrupt("injected crash before dispatch CAS")
            return authority.compare_and_swap(current, desired)

        acquisition = SimpleNamespace(
            read_versioned=authority.read_versioned,
            create_if_absent=authority.create_if_absent,
            compare_and_swap=interrupted_cas,
        )
        interrupted = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: acquisition, ddl, authority)
        with pytest.raises(KeyboardInterrupt):
            interrupted.publish(config, candidate, staged_rows=2)
        key = contracts.digest_payload({"cluster": CLUSTER, "database": database, "target": "target"})
        prepared = authority.read_versioned(key)
        assert prepared is not None and prepared.record.phase is contracts.AuthorityPhase.PREPARED
        assert prepared.record.prepared_origin
        for port in (39100, 49100, 59100):
            probe = NativeConnector(port)
            probe.get_records("SYSTEM FLUSH LOGS")
            probe.connection.disconnect()
        plan = plan_prepared_recovery(
            catalog,
            authority,
            ddl,
            cluster=CLUSTER,
            database=database,
            target="target",
            operation_id=prepared.record.operation_id,
            expected_version=prepared.version,
            operation_started_at=datetime.now(UTC),
        )
        recovery = PreparedRecoveryService(
            catalog,
            authority,
            ddl,
            lambda current, cluster: service._reconcile_existing(authority, current, cluster),
            service.cleanup,
        )
        receipt = recovery.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
        assert receipt.authority.phase is contracts.AuthorityPhase.COMPLETED
        replay = recovery.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
        assert replay.authority == receipt.authority
        assert len(ddl.find_entries(CLUSTER, plan.token)) == 1
        rows = connector.get_records(
            f"SELECT hostName(), sum(id), count() FROM clusterAllReplicas('{CLUSTER}', {database}.target) "
            "GROUP BY hostName() ORDER BY hostName()"
        )
        assert rows == [(host, generation * 20 + 1, 2) for host in HOSTS]


def test_real_competing_keeper_cas_grants_exactly_one_permit(strict_cluster):
    from tests.test_clickhouse_quality_authority import record

    connector, database = strict_cluster
    authority = _authority(connector, database)
    prepared = require_verified_mutation(authority.create_if_absent(record()), permit=False)
    desired = prepared.record.dispatching(token="same", query_digest="same")

    def compete(port):
        peer = NativeConnector(port)
        try:
            return _authority(peer, database).compare_and_swap(prepared, desired)
        finally:
            peer.connection.disconnect()

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(compete, (39100, 49100, 59100)))
    assert sum(result.permit is not None for result in results) == 1


def test_real_keeper_lost_ack_and_stale_retry_never_grant_permit(strict_cluster):
    from tests.test_clickhouse_quality_authority import record

    connector, database = strict_cluster
    authority = _authority(connector, database)
    prepared = require_verified_mutation(authority.create_if_absent(record()), permit=False)
    desired = prepared.record.dispatching(token="one-intent", query_digest="query")
    delegate = connector.connection

    def lose_ack(sql, *args, **kwargs):
        result = delegate.execute(sql, *args, **kwargs)
        if sql.startswith("ALTER TABLE"):
            raise TimeoutError("injected lost acknowledgement after real Keeper mutation")
        return result

    connector.connection = SimpleNamespace(execute=lose_ack)
    try:
        unknown = authority.compare_and_swap(prepared, desired)
    finally:
        connector.connection = delegate
    assert unknown.status is contracts.AuthorityMutationStatus.OUTCOME_UNKNOWN
    assert unknown.permit is None
    observed = authority.read_versioned(prepared.record.target_key)
    assert observed is not None and observed.record.phase is contracts.AuthorityPhase.DISPATCHING
    assert observed.version == prepared.version + 1
    retried = authority.compare_and_swap(prepared, desired)
    assert retried.permit is None
    assert retried.status is not contracts.AuthorityMutationStatus.VERIFIED
