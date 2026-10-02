from __future__ import annotations

from dataclasses import dataclass, replace

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

    def read_for_operation(self, target_key, operation_id):
        return self.read_versioned(target_key)

    def create_if_absent(self, record):
        self.current = VersionedAuthorityRecord(record, 0)
        return AuthorityMutationResult(AuthorityMutationStatus.VERIFIED, self.current)

    def compare_and_swap(self, current, desired):
        assert self.current == current
        self.current = VersionedAuthorityRecord(desired, current.version + 1)
        from dpone.contracts.clickhouse_cluster_publication import DispatchPermit

        permit = None
        if desired.phase.value.endswith("DISPATCHING"):
            permit = DispatchPermit.for_record(desired)
        return AuthorityMutationResult(AuthorityMutationStatus.VERIFIED, self.current, permit)


class _Catalog:
    def __init__(self) -> None:
        self.old, self.new = _identity("old"), _identity("new")
        self.committed = False
        self.candidate_removed = False
        self.target_healthy = True
        self.target_missing = False
        self.inventory_address = "127.0.0.1"

    def inventory(self, cluster):
        return ClusterInventory(
            cluster,
            (
                ClusterReplica("node-1", self.inventory_address, 9000, 1, 1, True),
                ClusterReplica("node-2", "127.0.0.2", 9000, 1, 2, True),
            ),
        )

    def require_atomic_database(self, cluster, database, hosts):
        return None

    def candidate_counts(self, cluster, database, candidate):
        del cluster, database, candidate
        return {"node-1": 2, "node-2": 2}

    def generations(self, cluster, database, target, candidate, hosts):
        target_identity, candidate_identity = (self.new, self.old) if self.committed else (self.old, self.new)
        if self.committed and self.candidate_removed:
            candidate_identity = None
        if self.committed and self.target_missing:
            target_identity = None
        return tuple(
            ReplicaGeneration(
                host,
                target_identity,
                candidate_identity,
                target_healthy=self.target_healthy,
                candidate_healthy=True,
                row_count=2,
            )
            for host in hosts
        )


class _Ddl:
    def __init__(
        self,
        catalog: _Catalog,
        *,
        duplicate=False,
        publication_entry_digest="digest",
        drift_after_dispatch=False,
        unhealthy_target_after_dispatch=False,
        publication_in_progress=False,
    ) -> None:
        self.catalog = catalog
        self.duplicate = duplicate
        self.dispatches = 0
        self.publication_entry_digest = publication_entry_digest
        self.drift_after_dispatch = drift_after_dispatch
        self.unhealthy_target_after_dispatch = unhealthy_target_after_dispatch
        self.publication_in_progress = publication_in_progress
        self.last_token = None
        self.cleanup_dispatches = 0

    def publication_query_digest(self, record, *, cluster):
        return "digest"

    def cleanup_query_digest(self, record, *, cluster):
        return "cleanup-digest"

    def dispatch_publication(self, record, permit, *, cluster):
        self.dispatches += 1
        self.catalog.committed = True
        if self.unhealthy_target_after_dispatch:
            self.catalog.target_healthy = False
        if self.drift_after_dispatch:
            self.catalog.inventory_address = "127.0.0.9"

    def find_entries(self, cluster, token):
        self.last_token = token
        query_digest = "cleanup-digest" if "-cleanup-" in token else self.publication_entry_digest
        entry = QueueEntry(
            "query-1",
            query_digest,
            token,
            (
                QueueHostResult(
                    "node-1",
                    "Active" if self.publication_in_progress else "Finished",
                    None if self.publication_in_progress else 0,
                    None if self.publication_in_progress else "",
                ),
                QueueHostResult(
                    "node-2",
                    "Active" if self.publication_in_progress else "Finished",
                    None if self.publication_in_progress else 0,
                    None if self.publication_in_progress else "",
                ),
            ),
        )
        return (entry, entry) if self.duplicate else (entry,)

    def read_entry(self, cluster, entry):
        assert self.last_token is not None
        return self.find_entries(cluster, self.last_token)[0]

    def drop_predecessor(self, record, permit, *, cluster):
        self.cleanup_dispatches += 1
        self.catalog.candidate_removed = True


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


def test_first_run_bootstraps_authority_before_first_authority_read() -> None:
    events: list[str] = []

    class Catalog(_Catalog):
        def inventory(self, cluster):
            events.append("inventory")
            return super().inventory(cluster)

        def require_atomic_database(self, cluster, database, hosts):
            events.append("atomic_database")

    class Bootstrap(_Bootstrap):
        def ensure(self, cluster, database, hosts):
            events.append("bootstrap")

    class Authority(_Authority):
        def read_versioned(self, target_key):
            events.append("authority_read")
            return super().read_versioned(target_key)

    catalog, authority = Catalog(), Authority()
    service = ClickHouseClusterFullRefreshPublicationService(
        catalog,
        lambda database: authority,
        _Ddl(catalog),
        Bootstrap(),
    )

    assert service.prepare_admission(_config()) == _config()
    assert events == ["inventory", "atomic_database", "bootstrap", "authority_read"]


def test_duplicate_queue_correlation_fails_closed() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog, duplicate=True)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN"):
        service.publish(_config(), _Candidate(), staged_rows=2)
    assert ddl.dispatches == 1


def test_unhealthy_desired_target_blocks_commit_and_cleanup() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog, unhealthy_target_after_dispatch=True)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_UNKNOWN"):
        service.publish(_config(), _Candidate(), staged_rows=2)
    assert authority.current is not None
    assert authority.current.record.phase.value == "DISPATCHING"


def test_wrong_query_with_matching_token_is_not_commit_evidence() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog, publication_entry_digest="wrong-query-digest")
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN"):
        service.publish(_config(), _Candidate(), staged_rows=2)


def test_inventory_drift_after_dispatch_fences_reconciliation() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog, drift_after_dispatch=True)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_INVENTORY_DRIFT"):
        service.publish(_config(), _Candidate(), staged_rows=2)


def test_cleanup_revalidates_inventory_and_bound_publication_digest() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    receipt = service.publish(_config(), _Candidate(), staged_rows=2)

    catalog.inventory_address = "127.0.0.9"
    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_INVENTORY_DRIFT"):
        service.cleanup(receipt)

    catalog.inventory_address = "127.0.0.1"
    ddl.publication_entry_digest = "changed-query-digest"
    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN"):
        service.cleanup(receipt)


def test_cleanup_completes_when_exact_predecessor_is_already_absent_on_every_replica() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    receipt = service.publish(_config(), _Candidate(), staged_rows=2)
    catalog.candidate_removed = True

    service.cleanup(receipt)

    assert authority.current is not None
    assert authority.current.record.phase.value == "COMPLETED"
    assert ddl.cleanup_dispatches == 0


def test_next_operation_reconciles_prior_commit_before_source_io() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    service.publish(_config(), _Candidate(), staged_rows=2)
    catalog.candidate_removed = True
    next_config = replace(
        _config(),
        options={**_config().options, SCHEDULER_IDENTITY_OPTION: "next-run"},
    )

    admitted = service.prepare_admission(next_config)

    assert admitted is next_config
    assert authority.current is not None
    assert authority.current.record.phase.value == "COMPLETED"
    assert ddl.cleanup_dispatches == 0


def test_next_operation_cleans_exact_predecessor_before_source_io() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    service.publish(_config(), _Candidate(), staged_rows=2)
    next_config = replace(
        _config(),
        options={**_config().options, SCHEDULER_IDENTITY_OPTION: "next-run"},
    )

    assert service.prepare_admission(next_config) is next_config
    assert authority.current is not None
    assert authority.current.record.phase.value == "COMPLETED"
    assert ddl.cleanup_dispatches == 1


def test_next_operation_stays_fenced_when_prior_target_is_unhealthy() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    service.publish(_config(), _Candidate(), staged_rows=2)
    catalog.target_healthy = False
    next_config = replace(
        _config(),
        options={**_config().options, SCHEDULER_IDENTITY_OPTION: "next-run"},
    )

    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNSAFE"):
        service.prepare_admission(next_config)

    assert authority.current is not None
    assert authority.current.record.phase.value == "COMMITTED"
    assert ddl.cleanup_dispatches == 0


@pytest.mark.parametrize("target_problem", ["target_missing", "target_healthy"])
def test_first_publication_recovery_fences_invalid_target_before_new_source_io(target_problem: str) -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    catalog.old = None
    service.publish(_config(), _Candidate(), staged_rows=2)
    setattr(catalog, target_problem, target_problem != "target_healthy")
    next_config = replace(_config(), options={**_config().options, SCHEDULER_IDENTITY_OPTION: "next-run"})

    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNSAFE"):
        service.prepare_admission(next_config)

    assert authority.current is not None
    assert authority.current.record.phase.value == "COMMITTED"


def test_first_publication_recovery_requires_terminal_ddl_before_new_source_io() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    catalog.old = None
    service.publish(_config(), _Candidate(), staged_rows=2)
    ddl.publication_in_progress = True
    next_config = replace(_config(), options={**_config().options, SCHEDULER_IDENTITY_OPTION: "next-run"})

    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNSAFE"):
        service.prepare_admission(next_config)

    assert authority.current is not None
    assert authority.current.record.phase.value == "COMMITTED"


def test_tampered_mapping_receipt_cannot_authorize_predecessor_drop() -> None:
    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    receipt = service.publish(_config(), _Candidate(), staged_rows=2)
    tampered = receipt.to_dict()
    tampered["authority"]["candidate"] = "unrelated_table"

    with pytest.raises(ClusterPublicationError, match="DPONE_CLICKHOUSE_CLUSTER_RECEIPT_INVALID"):
        service.cleanup(tampered)

    assert ddl.cleanup_dispatches == 0
    assert authority.current is not None
    assert authority.current.record.phase.value == "COMMITTED"


def test_replicated_publication_accepts_a_cluster_without_distributed_internal_replication() -> None:
    inventory = ClusterInventory(
        "dwh",
        (
            ClusterReplica("node-1", "127.0.0.1", 9000, 1, 1, False),
            ClusterReplica("node-2", "127.0.0.2", 9000, 1, 2, False),
        ),
    )

    inventory.validate()

    assert inventory.hosts == ("node-1", "node-2")


def test_new_operation_admitted_after_completed_publication_without_claiming_slot() -> None:
    from dataclasses import replace

    from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase
    from dpone.runtime.sinks.clickhouse_full_refresh_publication import REPLAY_OPTION

    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    service.publish(_config(), _Candidate(), staged_rows=2)
    authority.current = replace(
        authority.current, record=replace(authority.current.record, phase=AuthorityPhase.COMPLETED)
    )
    previous = authority.current
    config = replace(_config(), options={**_config().options, SCHEDULER_IDENTITY_OPTION: "next-run"})

    assert service.prepare_admission(config) is config
    assert REPLAY_OPTION not in config.options
    assert authority.current == previous
    assert ddl.dispatches == 1

    # A different contender can win after admission; publication must still fence
    # the stale attempt, not treat admission as a reservation or mutate its slot.
    authority.current = replace(
        previous, record=replace(previous.record, phase=AuthorityPhase.PREPARED, operation_id="contender")
    )
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_CONFLICT"):
        service.publish(config, _Candidate(), staged_rows=2)
    assert ddl.dispatches == 1


@pytest.mark.parametrize(
    ("phase", "expected_error"),
    [
        ("PREPARED", "AUTHORITY_CONFLICT"),
        ("DISPATCHING", "PUBLICATION_UNKNOWN"),
        ("COMMITTED", "CLEANUP_UNSAFE"),
        ("CLEANUP_DISPATCHING", "CLEANUP_UNKNOWN"),
    ],
)
def test_new_operation_cannot_bypass_unresolved_publication(phase: str, expected_error: str) -> None:
    from dataclasses import replace

    from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase

    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    service.publish(_config(), _Candidate(), staged_rows=2)
    authority.current = replace(
        authority.current, record=replace(authority.current.record, phase=AuthorityPhase(phase))
    )
    if phase == "DISPATCHING":
        ddl.publication_in_progress = True
    if phase == "COMMITTED":
        catalog.target_healthy = False
    previous = authority.current
    config = replace(_config(), options={**_config().options, SCHEDULER_IDENTITY_OPTION: "next-run"})
    with pytest.raises(ClusterPublicationError, match=expected_error):
        service.prepare_admission(config)
    assert authority.current == previous
    assert ddl.dispatches == 1


def test_same_completed_operation_replays_without_source_scan_or_redispatch() -> None:
    from dataclasses import replace

    from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase
    from dpone.runtime.sinks.clickhouse_full_refresh_publication import REPLAY_OPTION

    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    service.publish(_config(), _Candidate(), staged_rows=2)
    authority.current = replace(
        authority.current, record=replace(authority.current.record, phase=AuthorityPhase.COMPLETED)
    )
    admitted = service.prepare_admission(_config())
    assert admitted.options[REPLAY_OPTION].total_rows == 2
    assert ddl.dispatches == 1


def test_completed_slot_reuse_fences_stale_cleanup_and_preserves_new_inventory() -> None:
    from dataclasses import replace

    from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase

    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    old_receipt = service.publish(_config(), _Candidate(), staged_rows=2)
    authority.current = replace(
        authority.current, record=replace(authority.current.record, phase=AuthorityPhase.COMPLETED)
    )
    previous = authority.current
    catalog.inventory_address = "127.0.0.9"
    with pytest.raises(ClusterPublicationError, match="INVENTORY_DRIFT"):
        service.prepare_admission(_config())
    config = replace(_config(), options={**_config().options, SCHEDULER_IDENTITY_OPTION: "next-run"})
    assert service.prepare_admission(config) is config
    # Model the current target and a distinct new candidate after prior cleanup.
    catalog.old, catalog.new, catalog.committed = catalog.new, _identity("next"), False
    new_receipt = service.publish(config, _Candidate(), staged_rows=2)
    assert new_receipt.authority.operation_id != old_receipt.authority.operation_id
    assert new_receipt.authority.fence_token != old_receipt.authority.fence_token
    assert new_receipt.authority.dispatch_epoch > previous.record.dispatch_epoch
    assert new_receipt.authority.inventory_digest == catalog.inventory("one_shard").digest
    assert ddl.dispatches == 2
    with pytest.raises(ClusterPublicationError, match="RECEIPT_INVALID"):
        service.cleanup(old_receipt)
    assert ddl.cleanup_dispatches == 0
