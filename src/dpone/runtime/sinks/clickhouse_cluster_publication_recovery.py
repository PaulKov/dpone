"""Fail-closed candidate and authority checks for cluster publication."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import TypeVar

from dpone.ports.clickhouse_cluster_publication import (
    ClusterPublicationAuthorityPort,
    ClusterPublicationCatalogPort,
    contracts,
    require_verified_mutation,
)
from dpone.runtime.sinks.clickhouse_cluster_candidate_readiness import require_candidate_rows as require_candidate_rows

_Receipt = TypeVar("_Receipt")


def settle_prior_publication(
    authority: ClusterPublicationAuthorityPort,
    current: contracts.VersionedAuthorityRecord,
    *,
    cluster: str,
    inventory: contracts.ClusterInventory,
    reconcile: Callable[[contracts.VersionedAuthorityRecord], _Receipt],
    receipt_factory: Callable[[contracts.VersionedAuthorityRecord, str], _Receipt],
    cleanup: Callable[[_Receipt], None],
) -> None:
    """Finish only a provable prior operation before new source I/O."""

    contracts.require_inventory(current.record, inventory)
    if current.record.phase is contracts.AuthorityPhase.DISPATCHING:
        receipt = reconcile(current)
    elif current.record.phase in {contracts.AuthorityPhase.COMMITTED, contracts.AuthorityPhase.CLEANUP_DISPATCHING}:
        receipt = receipt_factory(current, cluster)
    else:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "another operation owns target"
        )
    cleanup(receipt)
    settled = authority.read_versioned(current.record.target_key)
    if settled is None or settled.record.phase is not contracts.AuthorityPhase.COMPLETED:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "prior operation remains unresolved"
        )


def require_first_publication_complete(
    catalog: ClusterPublicationCatalogPort,
    current: contracts.VersionedAuthorityRecord,
    *,
    cluster: str,
    inventory: contracts.ClusterInventory,
    publication_entry: contracts.QueueEntry,
) -> None:
    """Verify exact healthy target and terminal DDL on every replica."""

    record = current.record
    facts = catalog.generations(cluster, record.database, record.target, record.candidate, inventory.hosts)
    states = tuple(contracts.classify_replica(fact, desired=record.desired, predecessor=None) for fact in facts)
    if set(states) != {contracts.ReplicaPublicationState.COMMITTED} or publication_entry.state_for(
        inventory.hosts
    ) not in {
        contracts.QueueState.TERMINAL_SUCCESS,
        contracts.QueueState.TERMINAL_FAILURE,
    }:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNSAFE", "first publication is not proven complete"
        )


def require_pre_dispatch_generation(
    catalog: ClusterPublicationCatalogPort, cluster: str, record: contracts.AuthorityRecord
) -> None:
    """Recheck the immutable generation identity and staged row count."""

    inventory = catalog.inventory(cluster)
    contracts.require_inventory(record, inventory)
    catalog.require_atomic_database(cluster, record.database, inventory.hosts)
    facts = catalog.generations(cluster, record.database, record.target, record.candidate, inventory.hosts)
    if (
        contracts.one_generation_identity(facts, "candidate") != record.desired
        or contracts.optional_generation_identity(facts, "target") != record.predecessor
        or any(not fact.candidate_healthy or fact.row_count != record.staged_rows for fact in facts)
    ):
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_GENERATION_DIVERGED", "generation changed before publication dispatch"
        )


def complete_authority(authority: ClusterPublicationAuthorityPort, current: contracts.VersionedAuthorityRecord) -> None:
    """Seal a reconciled publication with a versioned transition."""

    if current.record.phase is contracts.AuthorityPhase.COMPLETED:
        return
    completed = replace(current.record, phase=contracts.AuthorityPhase.COMPLETED)
    require_verified_mutation(authority.compare_and_swap(current, completed), permit=False)
