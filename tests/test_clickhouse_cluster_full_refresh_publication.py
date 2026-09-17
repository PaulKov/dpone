from __future__ import annotations

from dataclasses import dataclass

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityMutationResult,
    AuthorityMutationStatus,
    ClusterInventory,
    ClusterPublicationError,
    ClusterReplica,
    GenerationIdentity,
    QueueEntry,
    QueueHostResult,
    ReplicaGeneration,
    VersionedAuthorityRecord,
)
from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION


def _identity(uuid: str) -> GenerationIdentity:
    return GenerationIdentity(
        uuid, "ReplicatedMergeTree('/tables/x', '{replica}')", "schema", "default", f"/tables/{uuid}"
    )


class _Authority:
    def __init__(self) -> None:
        self.current: VersionedAuthorityRecord | None = None

    def read_versioned(self, target_key):
        return self.current

    def create_if_absent(self, record):
        self.current = VersionedAuthorityRecord(record, 0)
        return AuthorityMutationResult(AuthorityMutationStatus.VERIFIED, self.current)

    def compare_and_swap(self, current, desired):
        assert self.current == current
        self.current = VersionedAuthorityRecord(desired, current.version + 1)
        from dpone.contracts.clickhouse_cluster_publication import DispatchPermit

        permit = None
        if desired.phase.value.endswith("DISPATCHING"):
            permit = DispatchPermit(
                desired.target_key, desired.operation_id, desired.fence_token, desired.dispatch_epoch
            )
        return AuthorityMutationResult(AuthorityMutationStatus.VERIFIED, self.current, permit)


class _Catalog:
    def __init__(self) -> None:
        self.old, self.new = _identity("old"), _identity("new")
        self.committed = False

    def inventory(self, cluster):
        return ClusterInventory(
            cluster,
            (
                ClusterReplica("node-1", "127.0.0.1", 9000, 1, 1, True),
                ClusterReplica("node-2", "127.0.0.2", 9000, 1, 2, True),
            ),
        )

    def require_atomic_database(self, cluster, database, hosts):
        return None

    def generations(self, cluster, database, target, candidate, hosts):
        target_identity, candidate_identity = (self.new, self.old) if self.committed else (self.old, self.new)
        return tuple(ReplicaGeneration(host, target_identity, candidate_identity, True, 2) for host in hosts)


class _Ddl:
    def __init__(self, catalog: _Catalog, *, duplicate=False) -> None:
        self.catalog = catalog
        self.duplicate = duplicate
        self.dispatches = 0

    def dispatch_publication(self, record, permit, *, cluster):
        self.dispatches += 1
        self.catalog.committed = True

    def find_entries(self, cluster, token):
        entry = QueueEntry(
            "query-1",
            "digest",
            token,
            (
                QueueHostResult("node-1", "Finished", 0, ""),
                QueueHostResult("node-2", "Finished", 0, ""),
            ),
        )
        return (entry, entry) if self.duplicate else (entry,)

    def read_entry(self, cluster, entry):
        return None

    def drop_predecessor(self, record, permit, *, cluster):
        return None


class _Bootstrap:
    def ensure(self, cluster, database, hosts):
        return None


@dataclass
class _Candidate:
    target_schema: str = "analytics"
    target_table: str = "candidate"


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="source",
        source_table="source",
        target_schema="analytics",
        target_table="target",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            SCHEDULER_IDENTITY_OPTION: "run",
            "physical_design": {"storage": {"clickhouse": {"cluster": "one_shard"}}},
        },
    )


def test_cluster_publish_dispatches_once_and_binds_terminal_entry() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    receipt = service.publish(_config(), _Candidate(), staged_rows=2)
    assert ddl.dispatches == 1
    assert receipt.authority.ddl_entry == "query-1"
    assert receipt.authority.phase.value == "COMMITTED"


def test_duplicate_queue_correlation_fails_closed() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog, duplicate=True)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN"):
        service.publish(_config(), _Candidate(), staged_rows=2)
    assert ddl.dispatches == 1
