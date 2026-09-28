"""Fail-closed candidate and authority checks for cluster publication."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import replace
from typing import TypeVar

from dpone.ports.clickhouse_cluster_publication import (
    ClusterPublicationAuthorityPort,
    ClusterPublicationCatalogPort,
    ClusterPublicationDdlPort,
    contracts,
    require_exact_ddl_entry,
    require_verified_mutation,
)
from dpone.runtime.sinks.clickhouse_cluster_candidate_readiness import DEFAULT_WAIT_SECONDS
from dpone.runtime.sinks.clickhouse_cluster_candidate_readiness import require_candidate_rows as require_candidate_rows
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt

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


def candidate_readiness_deadline() -> float:
    """Start one monotonic budget shared by pre-authority and pre-DDL checks."""
    return time.monotonic() + DEFAULT_WAIT_SECONDS


def require_pre_dispatch_generation(
    catalog: ClusterPublicationCatalogPort,
    cluster: str,
    record: contracts.AuthorityRecord,
    *,
    deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Wait for rows/health, but reject immutable generation drift immediately."""

    inventory = catalog.inventory(cluster)
    contracts.require_inventory(record, inventory)
    require_candidate_rows(
        contracts.ClusterPublicationError,
        catalog.candidate_counts,
        cluster,
        record.database,
        record.candidate,
        inventory.hosts,
        record.staged_rows,
        deadline=deadline,
        additional_readiness=lambda: _generation_readiness(catalog, cluster, record),
        monotonic=monotonic,
        sleep=sleep,
    )


def _generation_readiness(
    catalog: ClusterPublicationCatalogPort, cluster: str, record: contracts.AuthorityRecord
) -> str | None:
    """Keep hard identity checks distinct from transient replication readiness."""

    inventory = catalog.inventory(cluster)
    contracts.require_inventory(record, inventory)
    catalog.require_atomic_database(cluster, record.database, inventory.hosts)
    facts = catalog.generations(cluster, record.database, record.target, record.candidate, inventory.hosts)
    if (
        contracts.one_generation_identity(facts, "candidate") != record.desired
        or contracts.optional_generation_identity(facts, "target") != record.predecessor
    ):
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_GENERATION_DIVERGED", "generation changed before publication dispatch"
        )
    pending = ",".join(
        fact.host for fact in facts if not fact.candidate_healthy or fact.row_count != record.staged_rows
    )
    return f"candidate replica health or metadata count is not ready on hosts={pending}" if pending else None


def complete_authority(authority: ClusterPublicationAuthorityPort, current: contracts.VersionedAuthorityRecord) -> None:
    """Seal a reconciled publication with a versioned transition."""

    if current.record.phase is contracts.AuthorityPhase.COMPLETED:
        return
    completed = replace(current.record, phase=contracts.AuthorityPhase.COMPLETED)
    require_verified_mutation(authority.compare_and_swap(current, completed), permit=False)


def require_same_operation(current: contracts.AuthorityRecord, proposed: contracts.AuthorityRecord) -> None:
    """Fence attempts to reuse an authority slot for a different plan."""

    if current.operation_id != proposed.operation_id or current.plan_digest != proposed.plan_digest:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "another operation owns target"
        )


def reconcile_existing(
    catalog: ClusterPublicationCatalogPort,
    ddl: ClusterPublicationDdlPort,
    authority: ClusterPublicationAuthorityPort,
    current: contracts.VersionedAuthorityRecord,
    cluster: str,
) -> ClusterFullRefreshReceipt:
    """Reconcile the retained one-shot DDL against exact per-replica truth."""
    record = current.record
    inventory = catalog.inventory(cluster)
    contracts.require_inventory(record, inventory)
    hosts = inventory.hosts
    entry = require_exact_ddl_entry(
        ddl,
        cluster,
        entry_id=record.ddl_entry,
        token=record.ddl_correlation_token,
        query_digest=record.ddl_query_digest,
        error_code="DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN",
    )
    queue_state = entry.state_for(hosts)
    observed = catalog.generations(cluster, record.database, record.target, record.candidate, hosts)
    states = tuple(
        contracts.classify_replica(item, desired=record.desired, predecessor=record.predecessor) for item in observed
    )
    aggregate = contracts.classify_aggregate(states, queue_state)
    if aggregate is contracts.AggregatePublicationState.PARTIAL_IN_PROGRESS:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_IN_PROGRESS", "original DDL is active"
        )
    if aggregate is contracts.AggregatePublicationState.PARTIAL_TERMINAL:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_PARTIAL_TERMINAL", "manual repair required"
        )
    if aggregate is not contracts.AggregatePublicationState.COMMITTED:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_UNKNOWN", "completion is not proven"
        )
    committed = replace(
        record,
        phase=contracts.AuthorityPhase.COMMITTED,
        ddl_entry=entry.entry,
    )
    if current.record != committed:
        result = authority.compare_and_swap(current, committed)
        current = require_verified_mutation(result, permit=False)
    return ClusterFullRefreshReceipt.from_authority(current, cluster)
