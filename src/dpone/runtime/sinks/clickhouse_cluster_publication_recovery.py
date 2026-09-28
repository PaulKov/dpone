"""Fail-closed evidence checks for a retained cluster-publication slot."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityPhase,
    AuthorityRecord,
    ClusterInventory,
    ClusterPublicationError,
    QueueEntry,
    QueueState,
    ReplicaPublicationState,
    VersionedAuthorityRecord,
    classify_replica,
    one_generation_identity,
    optional_generation_identity,
    require_inventory,
)
from dpone.ports.clickhouse_cluster_publication import (
    ClusterPublicationAuthorityPort,
    ClusterPublicationCatalogPort,
    require_verified_mutation,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt


def settle_prior_publication(
    authority: ClusterPublicationAuthorityPort,
    current: VersionedAuthorityRecord,
    *,
    cluster: str,
    inventory: ClusterInventory,
    reconcile: Callable[[VersionedAuthorityRecord], ClusterFullRefreshReceipt],
    cleanup: Callable[[ClusterFullRefreshReceipt], None],
) -> None:
    """Finish only a provable prior operation before new source I/O."""

    require_inventory(current.record, inventory)
    if current.record.phase is AuthorityPhase.DISPATCHING:
        receipt = reconcile(current)
    elif current.record.phase in {AuthorityPhase.COMMITTED, AuthorityPhase.CLEANUP_DISPATCHING}:
        receipt = ClusterFullRefreshReceipt.from_authority(current, cluster)
    else:
        raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "another operation owns target")
    cleanup(receipt)
    settled = authority.read_versioned(current.record.target_key)
    if settled is None or settled.record.phase is not AuthorityPhase.COMPLETED:
        raise ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "prior operation remains unresolved"
        )


def require_first_publication_complete(
    catalog: ClusterPublicationCatalogPort,
    current: VersionedAuthorityRecord,
    *,
    cluster: str,
    inventory: ClusterInventory,
    publication_entry: QueueEntry,
) -> None:
    """Verify exact healthy target and terminal DDL on every replica."""

    record = current.record
    facts = catalog.generations(cluster, record.database, record.target, record.candidate, inventory.hosts)
    states = tuple(classify_replica(fact, desired=record.desired, predecessor=None) for fact in facts)
    if set(states) != {ReplicaPublicationState.COMMITTED} or publication_entry.state_for(inventory.hosts) not in {
        QueueState.TERMINAL_SUCCESS,
        QueueState.TERMINAL_FAILURE,
    }:
        raise ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNSAFE", "first publication is not proven complete"
        )


def require_pre_dispatch_generation(
    catalog: ClusterPublicationCatalogPort, cluster: str, record: AuthorityRecord
) -> None:
    """Recheck the immutable generation identity and staged row count."""

    inventory = catalog.inventory(cluster)
    require_inventory(record, inventory)
    catalog.require_atomic_database(cluster, record.database, inventory.hosts)
    facts = catalog.generations(cluster, record.database, record.target, record.candidate, inventory.hosts)
    if (
        one_generation_identity(facts, "candidate") != record.desired
        or optional_generation_identity(facts, "target") != record.predecessor
        or any(not fact.candidate_healthy or fact.row_count != record.staged_rows for fact in facts)
    ):
        raise ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_GENERATION_DIVERGED", "generation changed before publication dispatch"
        )


def complete_authority(authority: ClusterPublicationAuthorityPort, current: VersionedAuthorityRecord) -> None:
    """Seal a reconciled publication with a versioned transition."""

    if current.record.phase is AuthorityPhase.COMPLETED:
        return
    completed = replace(current.record, phase=AuthorityPhase.COMPLETED)
    require_verified_mutation(authority.compare_and_swap(current, completed), permit=False)
