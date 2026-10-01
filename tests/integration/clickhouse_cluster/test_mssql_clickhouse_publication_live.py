"""Real SQL authority plus two-replica ClickHouse publication fault checks.

This opt-in local profile retains unique synthetic databases. It certifies the
service boundary, not a deployment freeze, legacy history, or source transport.
Faults wrap an actual I/O boundary; physical generations and SQL events remain
real. Nothing here admits retirement or supplies synthetic safety observations.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from uuid import uuid4

import pytest

from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase, ClusterPublicationError, digest_payload
from dpone.runtime.connectors.clickhouse import ClickHouseConnector
from dpone.runtime.publication_authority_composition import BoundPublicationAuthorityProvider
from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog
from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl
from dpone.runtime.sinks.clickhouse_full_refresh_publication import REPLAY_OPTION
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from tests.integration.clickhouse_cluster.test_clickhouse_cluster_publication_live import _configs
from tests.integration.clickhouse_cluster.test_mssql_publication_authority_live import authority, connect
from tests.integration.clickhouse_cluster.test_mssql_publication_authority_live import catalog as catalog

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_MSSQL_CLICKHOUSE_PUBLICATION_LIVE") != "1",
        reason="explicit combined local SQL/ClickHouse opt-in required",
    ),
]
CLUSTER = "publication_cluster"


class WorkerStopped(BaseException):
    """Abrupt control-flow loss: bypass the service's Exception reconciliation."""


class ObservedDdl:
    """Count attempted effects; optionally lose their real committed response."""

    def __init__(self, delegate, fault=None):
        self.delegate = delegate
        self.fault = fault
        self.publications = 0
        self.cleanups = 0

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def dispatch_publication(self, record, permit, *, cluster):
        self.publications += 1
        if self.fault == "before_publication":
            raise TimeoutError("synthetic interruption before actual DDL dispatch")
        self.delegate.dispatch_publication(record, permit, cluster=cluster)
        if self.fault == "stop_after_publication":
            raise WorkerStopped()
        if self.fault == "publication_response":
            raise TimeoutError("synthetic lost response after actual DDL")

    def drop_predecessor(self, record, permit, *, cluster):
        self.cleanups += 1
        self.delegate.drop_predecessor(record, permit, cluster=cluster)
        if self.fault == "stop_after_cleanup":
            raise WorkerStopped()
        if self.fault == "cleanup_response":
            raise TimeoutError("synthetic lost response after actual cleanup")


@dataclass
class LivePublication:
    connector: object
    physical: object
    provider: object
    database: str
    config: object
    candidate: object
    before: tuple
    sql_catalog: tuple

    @property
    def key(self):
        return digest_payload({"cluster": CLUSTER, "database": self.database, "target": "target"})

    def generations(self):
        hosts = self.physical.inventory(CLUSTER).hosts
        return self.physical.generations(CLUSTER, self.database, "target", "candidate", hosts)

    def service(self, fault=None):
        ddl = ObservedDdl(ClickHouseClusterPublicationDdl(self.connector, self.physical), fault)
        return (
            ClickHouseClusterFullRefreshPublicationService(
                self.physical, self.provider.for_database, ddl, self.provider
            ),
            ddl,
        )

    def current(self):
        return self.provider.for_database(self.database).read_versioned(self.key)


@pytest.fixture
def live_publication(catalog, request):
    host = os.environ.get("DPONE_IT_PUBLICATION_CH_HOST", "127.0.0.1")
    if host not in {"127.0.0.1", "localhost", "host.docker.internal"}:
        raise ValueError("combined publication tests require the local Docker fixture")
    database = "mssql_publication_" + uuid4().hex[:16]
    connector = ClickHouseConnector(
        host=host, port=18123, database="default", user="default", password="", driver="http"
    )
    try:
        inventory = ClickHouseClusterPublicationCatalog(connector).inventory(CLUSTER)
        assert inventory.hosts == ("node1", "node2")
        connector.execute_query(f"CREATE DATABASE {database} ON CLUSTER {CLUSTER} ENGINE=Atomic")
        tables = ("candidate",) if getattr(request, "param", "existing") == "absent" else ("target", "candidate")
        for table in tables:
            connector.execute_query(
                f"CREATE TABLE {database}.{table} ON CLUSTER {CLUSTER} (id UInt64) "
                f"ENGINE=ReplicatedMergeTree('/{database}/{{uuid}}/{{shard}}', '{{replica}}') ORDER BY id"
            )
            rows = "(10),(20)" if table == "candidate" else "(1)"
            connector.execute_query(f"INSERT INTO {database}.{table} VALUES {rows}")
        physical = ClickHouseClusterPublicationCatalog(connector)
        deadline = time.monotonic() + 30
        while True:
            counts = physical.candidate_counts(CLUSTER, database, "candidate")
            before = physical.generations(CLUSTER, database, "target", "candidate", inventory.hosts)
            if dict(counts) == {"node1": 2, "node2": 2} and all(
                item.candidate_healthy and item.target_healthy and item.row_count == 2 for item in before
            ):
                break
            if time.monotonic() >= deadline:
                raise AssertionError("fixture candidate did not replicate")
            time.sleep(0.2)
        config, candidate = _configs(database, scheduler_identity="local-" + uuid4().hex)
        provider = BoundPublicationAuthorityProvider(lambda: authority(catalog))
        yield LivePublication(connector, physical, provider, database, config, candidate, before, catalog)
    finally:
        connector.close()


def assert_published(live):
    facts = live.generations()
    assert tuple(item.host for item in facts) == ("node1", "node2")
    assert all(
        item.target == old.candidate and item.target_healthy for item, old in zip(facts, live.before, strict=True)
    )
    assert live.connector.get_records(
        f"SELECT hostName(),arraySort(groupArray(id)) FROM clusterAllReplicas('{CLUSTER}', {live.database}.target) "
        "GROUP BY hostName() ORDER BY hostName()"
    ) == [("node1", [10, 20]), ("node2", [10, 20])]


def assert_replay(live, service):
    admitted = service.prepare_admission(live.config)
    replay = admitted.options[REPLAY_OPTION]
    assert isinstance(replay, LoadResult)
    assert (replay.inserted_rows, replay.updated_rows, replay.total_rows, replay.staging_rows) == (2, 0, 2, 2)
    assert replay.commit_receipt_id == live.current().record.operation_id
    assert replay.commit_outcome is AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE
    record = live.current().record
    assert len(live.physical.find_entries(CLUSTER, record.ddl_correlation_token)) == 1
    if record.cleanup_correlation_token is not None:
        assert len(live.physical.find_entries(CLUSTER, record.cleanup_correlation_token)) == 1


@pytest.mark.parametrize("live_publication", ["existing", "absent"], indirect=True)
def test_sql_authority_publishes_real_generations_and_replays_without_ddl(live_publication):
    live = live_publication
    service, ddl = live.service()
    receipt = service.publish(live.config, live.candidate, staged_rows=2)
    assert_published(live)
    assert live.current().record.phase is AuthorityPhase.COMMITTED
    assert len(ddl.find_entries(CLUSTER, receipt.authority.ddl_correlation_token)) == 1
    service.cleanup(receipt)
    assert live.current().record.phase is AuthorityPhase.COMPLETED
    assert all(item.candidate is None for item in live.generations())
    # A new service/authority instance must reconcile durable SQL, not redispatch.
    resumed, fresh_ddl = live.service()
    assert_replay(live, resumed)
    assert (fresh_ddl.publications, fresh_ddl.cleanups) == (0, 0)
    assert ddl.publications == 1
    assert ddl.cleanups == (1 if live.before[0].target is not None else 0)
    assert_published(live)


@pytest.mark.parametrize("fault", ["publication_response", "cleanup_response"])
def test_lost_real_clickhouse_response_reconciles_sql_without_redispatch(live_publication, fault):
    live = live_publication
    service, ddl = live.service(fault)
    receipt = service.publish(live.config, live.candidate, staged_rows=2)
    service.cleanup(receipt)
    assert live.current().record.phase is AuthorityPhase.COMPLETED
    assert (ddl.publications, ddl.cleanups) == (1, 1)
    assert len(ddl.find_entries(CLUSTER, receipt.authority.ddl_correlation_token)) == 1
    assert_published(live)
    resumed, fresh_ddl = live.service()
    assert_replay(live, resumed)
    assert (fresh_ddl.publications, fresh_ddl.cleanups) == (0, 0)


def test_sql_committed_intent_without_ddl_stays_blocked_and_preserves_both_generations(live_publication):
    live = live_publication
    service, interrupted = live.service("before_publication")
    with pytest.raises(ClusterPublicationError):
        service.publish(live.config, live.candidate, staged_rows=2)
    current = live.current()
    assert current.record.phase is AuthorityPhase.DISPATCHING
    assert interrupted.publications == 1
    assert interrupted.find_entries(CLUSTER, current.record.ddl_correlation_token) == ()
    assert live.generations() == live.before
    resumed, fresh_ddl = live.service()
    with pytest.raises(ClusterPublicationError):
        resumed.prepare_admission(live.config)
    assert (fresh_ddl.publications, fresh_ddl.cleanups) == (0, 0)
    assert live.current() == current
    assert live.generations() == live.before


@pytest.mark.parametrize("fault", ["stop_after_publication", "stop_after_cleanup"])
def test_fresh_service_reconciles_durable_intermediate_phase_after_real_ddl(live_publication, fault):
    live = live_publication
    service, stopped_ddl = live.service(fault)
    with pytest.raises(WorkerStopped):
        receipt = service.publish(live.config, live.candidate, staged_rows=2)
        service.cleanup(receipt)
    interrupted = live.current().record
    assert interrupted.phase is (
        AuthorityPhase.DISPATCHING if fault == "stop_after_publication" else AuthorityPhase.CLEANUP_DISPATCHING
    )
    assert_published(live)
    live.provider = BoundPublicationAuthorityProvider(lambda: authority(live.sql_catalog))
    resumed, fresh_ddl = live.service()
    assert_replay(live, resumed)
    assert live.current().record.phase is AuthorityPhase.COMPLETED
    assert stopped_ddl.publications == 1 and fresh_ddl.publications == 0
    assert stopped_ddl.cleanups + fresh_ddl.cleanups == 1
    assert all(item.candidate is None for item in live.generations())
    assert_published(live)


class LostSqlCommitAck:
    """Lose only the commit response from a real dedicated SQL session."""

    def __init__(self, database):
        self.connection = connect(database).connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    @property
    def autocommit(self):
        return self.connection.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.connection.autocommit = value

    def commit(self):
        self.connection.commit()
        raise TimeoutError("synthetic lost acknowledgement of real SQL commit")


@pytest.mark.parametrize("fault", ["lost_commit_ack", "session_unavailable"])
def test_sql_dispatch_uncertainty_never_reaches_clickhouse(live_publication, fault):
    live = live_publication
    normal = authority(live.sql_catalog)

    def faulty_session():
        if fault == "session_unavailable":
            raise ConnectionError("synthetic SQL session outage at the CAS boundary")
        return LostSqlCommitAck(live.sql_catalog[1].database)

    uncertain = authority(live.sql_catalog, factory=faulty_session)

    class DispatchFault:
        def __getattr__(self, name):
            return getattr(normal, name)

        def compare_and_swap(self, current, desired):
            assert desired.phase is AuthorityPhase.DISPATCHING
            return uncertain.compare_and_swap(current, desired)

    live.provider = BoundPublicationAuthorityProvider(DispatchFault)
    service, ddl = live.service()
    with pytest.raises(ClusterPublicationError):
        service.publish(live.config, live.candidate, staged_rows=2)
    current = normal.read_versioned(live.key)
    assert current.record.phase is (
        AuthorityPhase.DISPATCHING if fault == "lost_commit_ack" else AuthorityPhase.PREPARED
    )
    assert (ddl.publications, ddl.cleanups) == (0, 0)
    assert live.generations() == live.before
    live.provider = BoundPublicationAuthorityProvider(lambda: authority(live.sql_catalog))
    resumed, fresh_ddl = live.service()
    with pytest.raises(ClusterPublicationError):
        resumed.prepare_admission(live.config)
    assert (fresh_ddl.publications, fresh_ddl.cleanups) == (0, 0)
    assert live.current() == current
    assert live.generations() == live.before
