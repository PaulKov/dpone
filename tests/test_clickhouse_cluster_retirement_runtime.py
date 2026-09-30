"""Pre-source retirement admission and fresh publication, using synthetic replicas."""

from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityMutationResult,
    AuthorityMutationStatus,
    ClusterPublicationError,
    VersionedAuthorityRecord,
    digest_payload,
)
from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase as Phase
from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_identity import operation_id
from dpone.runtime.sinks.clickhouse_full_refresh_publication import REPLAY_OPTION, SCHEDULER_IDENTITY_OPTION
from dpone.runtime.state.mssql_publication_envelope import require_transition
from tests.test_clickhouse_cluster_full_refresh_publication import (
    _Authority,
    _Bootstrap,
    _Candidate,
    _Catalog,
    _config,
    _Ddl,
    _identity,
)
from tests.test_mssql_publication_authority import record


class RetainedAuthority(_Authority):
    """SQL policy double, not a claim of SQL concurrency or deployment admission."""

    def __init__(self, catalog):
        super().__init__()
        original = replace(
            record(),
            target_key=digest_payload({"cluster": "one_shard", "database": "analytics", "target": "target"}),
            database="analytics",
            target="target",
            operation_id=operation_id(_config()),
            inventory_digest=catalog.inventory("one_shard").digest,
            phase=Phase.RETIRED_UNPUBLISHED,
            candidate="retired_candidate",
            desired=_identity("retired-generation"),
            predecessor=catalog.old,
        )
        self.current = VersionedAuthorityRecord(original, 1)
        self.retired_id = original.operation_id
        self.operation_reads = []
        self.writes = []

    def read_for_operation(self, target_key, requested_id):
        self.operation_reads.append((target_key, requested_id))
        if requested_id == self.retired_id:
            raise ClusterPublicationError("DPONE_MSSQL_PUBLICATION_RETIRED_OPERATION", "retired ID")
        return self.current

    def compare_and_swap(self, current, desired):
        require_transition(current, desired)
        self.writes.append(desired)
        return super().compare_and_swap(current, desired)


def setup():
    catalog = _Catalog()
    authority = RetainedAuthority(catalog)
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    config = replace(_config(), options={**_config().options, SCHEDULER_IDENTITY_OPTION: "fresh-run"})
    return service, config, catalog, authority, ddl


def test_retired_id_is_rejected_at_pre_source_admission():
    service, _, _, authority, ddl = setup()
    with pytest.raises(ClusterPublicationError, match="RETIRED_OPERATION"):
        service.prepare_admission(_config())
    assert len(authority.operation_reads) == 1
    assert authority.writes == []
    assert ddl.dispatches == ddl.cleanup_dispatches == 0


def test_new_id_gets_ordinary_admission_not_replay_or_cleanup():
    service, config, _, authority, ddl = setup()
    admitted = service.prepare_admission(config)
    assert admitted == config
    assert REPLAY_OPTION not in admitted.options
    assert len(authority.operation_reads) == 1
    assert authority.writes == []
    assert ddl.dispatches == ddl.cleanup_dispatches == 0


@pytest.mark.parametrize("drift", ["inventory", "generation", "health", "missing_replica", "duplicate_replica"])
def test_retirement_admission_requires_exact_healthy_predecessor_everywhere(drift):
    service, config, catalog, authority, ddl = setup()
    if drift == "inventory":
        catalog.inventory_address = "127.0.0.9"
    elif drift == "generation":
        catalog.old = _identity("unexpected-generation")
    elif drift == "health":
        catalog.target_healthy = False
    else:
        original = catalog.generations
        catalog.generations = lambda *args: (
            original(*args)[:1] if drift == "missing_replica" else (original(*args)[0],) * 2
        )
    with pytest.raises(ClusterPublicationError, match="INVENTORY_DRIFT|GENERATION_DIVERGED"):
        service.prepare_admission(config)
    assert authority.writes == []
    assert ddl.dispatches == ddl.cleanup_dispatches == 0


def test_fresh_generation_publishes_normally_without_reusing_retired_candidate():
    service, config, _, authority, ddl = setup()
    original = authority.current
    service.prepare_admission(config)
    receipt = service.publish(config, _Candidate(), staged_rows=2)
    assert receipt.authority.phase is Phase.COMMITTED
    assert receipt.authority.operation_id == operation_id(config)
    assert receipt.authority.desired.uuid == "new"
    assert receipt.authority.predecessor == original.record.predecessor
    assert authority.writes[0].phase is Phase.PREPARED
    assert authority.writes[0].dispatch_epoch == 1
    assert authority.writes[0].operation_id != original.record.operation_id
    assert len(authority.operation_reads) == 2
    assert ddl.dispatches == 1
    assert ddl.cleanup_dispatches == 0


def test_target_drift_after_admission_cannot_become_a_new_baseline_at_publish():
    service, config, catalog, authority, ddl = setup()
    service.prepare_admission(config)
    catalog.old = _identity("changed-after-admission")
    with pytest.raises(ClusterPublicationError, match="GENERATION_DIVERGED"):
        service.publish(config, _Candidate(), staged_rows=2)
    assert authority.writes == []
    assert ddl.dispatches == 0


def test_fresh_cas_loser_does_not_reconcile_or_dispatch_the_winners_operation():
    service, config, _, authority, ddl = setup()
    authority.compare_and_swap = lambda current, desired: AuthorityMutationResult(
        AuthorityMutationStatus.CONFLICT, observed=current
    )
    with pytest.raises(ClusterPublicationError, match="CAS_CONFLICT"):
        service.publish(config, _Candidate(), staged_rows=2)
    assert ddl.dispatches == ddl.cleanup_dispatches == 0


def test_retired_id_is_also_rejected_if_publish_is_called_without_admission():
    service, _, _, authority, ddl = setup()
    with pytest.raises(ClusterPublicationError, match="RETIRED_OPERATION"):
        service.publish(_config(), _Candidate(), staged_rows=2)
    assert authority.writes == []
    assert ddl.dispatches == ddl.cleanup_dispatches == 0


def test_first_publication_retirement_preserves_verified_target_absence():
    service, config, catalog, authority, ddl = setup()
    catalog.old = None
    authority.current = replace(authority.current, record=replace(authority.current.record, predecessor=None))
    assert service.prepare_admission(config) == config
    receipt = service.publish(config, _Candidate(), staged_rows=2)
    assert receipt.authority.phase is Phase.COMMITTED
    assert receipt.authority.predecessor is None
    assert ddl.dispatches == 1
