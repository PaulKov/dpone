"""Recoverable bounded full-refresh publication for one replicated shard."""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from dataclasses import asdict, replace
from typing import Any

from dpone.ports.clickhouse_cluster_publication import (
    ClusterPublicationAuthorityPort,
    ClusterPublicationBootstrapPort,
    ClusterPublicationCatalogPort,
    ClusterPublicationDdlPort,
    contracts,
    require_exact_ddl_entry,
)
from dpone.ports.clickhouse_cluster_publication import (
    require_verified_mutation as _require_verified,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt
from dpone.runtime.sinks.clickhouse_full_refresh_contract import (
    publication_invocation_id,
)
from dpone.runtime.sinks.clickhouse_full_refresh_publication import REPLAY_OPTION, SCHEDULER_IDENTITY_OPTION
from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDesign
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult

AggregatePublicationState = contracts.AggregatePublicationState
AuthorityMutationStatus = contracts.AuthorityMutationStatus
AuthorityPhase = contracts.AuthorityPhase
AuthorityRecord = contracts.AuthorityRecord
ClusterPublicationError = contracts.ClusterPublicationError
QueueState = contracts.QueueState
ReplicaPublicationState = contracts.ReplicaPublicationState
VersionedAuthorityRecord = contracts.VersionedAuthorityRecord
classify_aggregate = contracts.classify_aggregate
classify_replica = contracts.classify_replica
digest_payload = contracts.digest_payload
_one_identity = contracts.one_generation_identity
_optional_one_identity = contracts.optional_generation_identity
_require_inventory = contracts.require_inventory


class ClickHouseClusterFullRefreshPublicationService:
    """Fence publication in Keeper and reconcile exact per-replica truth."""

    def __init__(
        self,
        catalog: ClusterPublicationCatalogPort,
        authority_factory: Any,
        ddl: ClusterPublicationDdlPort,
        bootstrap: ClusterPublicationBootstrapPort,
    ) -> None:
        self._catalog = catalog
        self._authority_factory = authority_factory
        self._ddl = ddl
        self._bootstrap = bootstrap

    @staticmethod
    def is_enabled(load_config: Any) -> bool:
        return ClickHouseTableDesign.from_options(getattr(load_config, "options", {}) or {}).cluster.on_cluster

    def publish(self, load_config: Any, candidate_config: Any, *, staged_rows: int) -> ClusterFullRefreshReceipt:
        cluster = _cluster(load_config)
        database, target = str(load_config.target_schema), str(load_config.target_table)
        candidate = str(candidate_config.target_table)
        if str(candidate_config.target_schema) != database:
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_DATABASE_MISMATCH", "candidate must be target-local"
            )
        inventory = self._catalog.inventory(cluster)
        self._catalog.require_atomic_database(cluster, database, inventory.hosts)
        self._bootstrap.ensure(cluster, database, inventory.hosts)
        facts = self._catalog.generations(cluster, database, target, candidate, inventory.hosts)
        desired = _one_identity(facts, "candidate")
        predecessor = _optional_one_identity(facts, "target")
        if predecessor == desired:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_GENERATION_INVALID", "generations must differ")
        if any(fact.row_count != staged_rows for fact in facts):
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY", "row count differs by replica"
            )
        operation_id = _operation_id(load_config)
        target_key = digest_payload({"cluster": cluster, "database": database, "target": target})
        record = AuthorityRecord(
            target_key=target_key,
            operation_id=operation_id,
            fence_token=secrets.token_hex(16),
            phase=AuthorityPhase.PREPARED,
            dispatch_epoch=0,
            inventory_digest=inventory.digest,
            plan_digest=digest_payload(
                {
                    "inventory": inventory.digest,
                    "desired": asdict(desired),
                    "predecessor": asdict(predecessor) if predecessor else None,
                }
            ),
            database=database,
            target=target,
            candidate=candidate,
            desired=desired,
            predecessor=predecessor,
            staged_rows=staged_rows,
        )
        authority = self._authority_factory(database)
        current = authority.read_versioned(target_key)
        if current is None:
            created = authority.create_if_absent(record)
            current = _require_verified(created, permit=False)
        elif current.record.phase is AuthorityPhase.COMPLETED and current.record.operation_id != record.operation_id:
            # The fixed target slot is intentionally retained. Reuse it through
            # one versioned transition so stale workers cannot resurrect the
            # prior completed operation or create unbounded Keeper rows.
            record = replace(record, dispatch_epoch=current.record.dispatch_epoch + 1)
            current = _require_verified(authority.compare_and_swap(current, record), permit=False)
        else:
            self._require_same_operation(current.record, record)
            record = current.record
            if record.phase is not AuthorityPhase.PREPARED:
                _require_inventory(record, inventory)
                return self._reconcile_existing(authority, current, cluster)
        self._revalidate_pre_dispatch(cluster, record)
        token = _correlation_token(operation_id, "publish", record.dispatch_epoch + 1)
        dispatching = record.dispatching(
            token=token,
            query_digest=self._ddl.publication_query_digest(record, cluster=cluster),
        )
        won = authority.compare_and_swap(current, dispatching)
        verified = _require_verified(won, permit=True)
        assert won.permit is not None
        try:
            self._ddl.dispatch_publication(dispatching, won.permit, cluster=cluster)
        except Exception:
            return self._reconcile_existing(authority, verified, cluster)
        return self._reconcile_existing(authority, verified, cluster)

    def prepare_admission(self, load_config: Any) -> Any:
        """Resume an owned operation before source I/O; never redispatch publication."""

        cluster = _cluster(load_config)
        database, target = str(load_config.target_schema), str(load_config.target_table)
        target_key = digest_payload({"cluster": cluster, "database": database, "target": target})
        authority = self._authority_factory(database)
        current = authority.read_versioned(target_key)
        if current is None:
            return load_config
        if current.record.operation_id != _operation_id(load_config):
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "another operation owns target"
            )
        inventory = self._catalog.inventory(cluster)
        _require_inventory(current.record, inventory)
        self._catalog.require_atomic_database(cluster, database, inventory.hosts)
        self._bootstrap.ensure(cluster, database, inventory.hosts)
        if current.record.phase is AuthorityPhase.COMPLETED:
            receipt = ClusterFullRefreshReceipt.from_authority(current, cluster)
        elif current.record.phase is AuthorityPhase.DISPATCHING:
            receipt = self._reconcile_existing(authority, current, cluster)
            self.cleanup(receipt)
        elif current.record.phase in {AuthorityPhase.COMMITTED, AuthorityPhase.CLEANUP_DISPATCHING}:
            receipt = ClusterFullRefreshReceipt.from_authority(current, cluster)
            self.cleanup(receipt)
        else:
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_UNRESOLVED", "owned publication cannot be resumed safely"
            )
        options = dict(getattr(load_config, "options", {}) or {})
        options[REPLAY_OPTION] = LoadResult(
            inserted_rows=receipt.authority.staged_rows,
            updated_rows=0,
            total_rows=receipt.authority.staged_rows,
            staging_rows=receipt.authority.staged_rows,
            commit_receipt_id=receipt.authority.operation_id,
            commit_outcome=AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE,
            reconciliation_metrics={"clickhouse_cluster_full_refresh": receipt.to_dict()},
        )
        return replace(load_config, options=options)

    def cleanup(self, receipt: ClusterFullRefreshReceipt | Mapping[str, Any]) -> None:
        resolved = (
            receipt
            if isinstance(receipt, ClusterFullRefreshReceipt)
            else ClusterFullRefreshReceipt.from_mapping(receipt)
        )
        record = resolved.authority
        authority = self._authority_factory(record.database)
        current = authority.read_versioned(record.target_key)
        if current is None or current.record.operation_id != record.operation_id:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "authority changed")
        if current.record.phase is AuthorityPhase.COMPLETED:
            return
        inventory = self._catalog.inventory(resolved.cluster)
        _require_inventory(current.record, inventory)
        require_exact_ddl_entry(
            self._ddl,
            resolved.cluster,
            entry_id=current.record.ddl_entry,
            token=current.record.ddl_correlation_token,
            query_digest=current.record.ddl_query_digest,
            error_code="DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN",
        )
        if current.record.predecessor is None:
            if current.record.phase is not AuthorityPhase.COMMITTED:
                raise ClusterPublicationError(
                    "DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNSAFE", "authority is not ready for completion"
                )
            self._complete(authority, current)
            return
        facts = self._catalog.generations(
            resolved.cluster, record.database, record.target, record.candidate, inventory.hosts
        )
        states = tuple(classify_replica(fact, desired=record.desired, predecessor=record.predecessor) for fact in facts)
        if current.record.phase is AuthorityPhase.CLEANUP_DISPATCHING:
            self._finish_dispatched_cleanup(authority, current, resolved.cluster, inventory.hosts, states)
            return
        if current.record.phase is not AuthorityPhase.COMMITTED:
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNSAFE", "authority is not ready for cleanup"
            )
        if set(states) != {ReplicaPublicationState.COMMITTED}:
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNSAFE", "predecessor identity is not exact"
            )
        cleanup = replace(
            current.record,
            phase=AuthorityPhase.CLEANUP_DISPATCHING,
            dispatch_epoch=current.record.dispatch_epoch + 1,
            cleanup_correlation_token=_correlation_token(
                record.operation_id, "cleanup", current.record.dispatch_epoch + 1
            ),
        )
        cleanup = replace(
            cleanup,
            cleanup_query_digest=self._ddl.cleanup_query_digest(cleanup, cluster=resolved.cluster),
        )
        mutation = authority.compare_and_swap(current, cleanup)
        verified = _require_verified(mutation, permit=True)
        assert mutation.permit is not None
        try:
            self._ddl.drop_predecessor(cleanup, mutation.permit, cluster=resolved.cluster)
        except Exception:
            pass
        after = self._catalog.generations(
            resolved.cluster, record.database, record.target, record.candidate, inventory.hosts
        )
        after_states = tuple(
            classify_replica(fact, desired=record.desired, predecessor=record.predecessor) for fact in after
        )
        self._finish_dispatched_cleanup(authority, verified, resolved.cluster, inventory.hosts, after_states)

    def _finish_dispatched_cleanup(
        self,
        authority: ClusterPublicationAuthorityPort,
        current: VersionedAuthorityRecord,
        cluster: str,
        hosts: tuple[str, ...],
        states: tuple[ReplicaPublicationState, ...],
    ) -> None:
        entry = require_exact_ddl_entry(
            self._ddl,
            cluster,
            entry_id=current.record.cleanup_entry,
            token=current.record.cleanup_correlation_token,
            query_digest=current.record.cleanup_query_digest,
            error_code="DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNKNOWN",
        )
        if entry.state_for(hosts) not in {
            QueueState.TERMINAL_SUCCESS,
            QueueState.TERMINAL_FAILURE,
        }:
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNKNOWN", "exact terminal cleanup entry is required"
            )
        if set(states) != {ReplicaPublicationState.CLEANUP_PENDING}:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNKNOWN", "cleanup not proven everywhere")
        completed = replace(
            current.record,
            phase=AuthorityPhase.COMPLETED,
            cleanup_entry=entry.entry,
        )
        _require_verified(authority.compare_and_swap(current, completed), permit=False)

    def _reconcile_existing(
        self,
        authority: ClusterPublicationAuthorityPort,
        current: VersionedAuthorityRecord,
        cluster: str,
    ) -> ClusterFullRefreshReceipt:
        record = current.record
        inventory = self._catalog.inventory(cluster)
        _require_inventory(record, inventory)
        hosts = inventory.hosts
        entry = require_exact_ddl_entry(
            self._ddl,
            cluster,
            entry_id=record.ddl_entry,
            token=record.ddl_correlation_token,
            query_digest=record.ddl_query_digest,
            error_code="DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN",
        )
        queue_state = entry.state_for(hosts)
        observed = self._catalog.generations(cluster, record.database, record.target, record.candidate, hosts)
        states = tuple(
            classify_replica(item, desired=record.desired, predecessor=record.predecessor) for item in observed
        )
        aggregate = classify_aggregate(states, queue_state)
        if aggregate is AggregatePublicationState.PARTIAL_IN_PROGRESS:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_IN_PROGRESS", "original DDL is active")
        if aggregate is AggregatePublicationState.PARTIAL_TERMINAL:
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_PARTIAL_TERMINAL", "manual repair required"
            )
        if aggregate is not AggregatePublicationState.COMMITTED:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_UNKNOWN", "completion is not proven")
        committed = replace(
            record,
            phase=AuthorityPhase.COMMITTED,
            ddl_entry=entry.entry,
        )
        if current.record != committed:
            result = authority.compare_and_swap(current, committed)
            current = _require_verified(result, permit=False)
        return ClusterFullRefreshReceipt.from_authority(current, cluster)

    def _revalidate_pre_dispatch(self, cluster: str, record: AuthorityRecord) -> None:
        inventory = self._catalog.inventory(cluster)
        _require_inventory(record, inventory)
        self._catalog.require_atomic_database(cluster, record.database, inventory.hosts)
        facts = self._catalog.generations(cluster, record.database, record.target, record.candidate, inventory.hosts)
        if (
            _one_identity(facts, "candidate") != record.desired
            or _optional_one_identity(facts, "target") != record.predecessor
            or any(not fact.candidate_healthy or fact.row_count != record.staged_rows for fact in facts)
        ):
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_GENERATION_DIVERGED",
                "generation changed before publication dispatch",
            )

    @staticmethod
    def _require_same_operation(current: AuthorityRecord, proposed: AuthorityRecord) -> None:
        if current.operation_id != proposed.operation_id or current.plan_digest != proposed.plan_digest:
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "another operation owns target"
            )

    @staticmethod
    def _complete(authority: ClusterPublicationAuthorityPort, current: VersionedAuthorityRecord) -> None:
        if current.record.phase is AuthorityPhase.COMPLETED:
            return
        completed = replace(current.record, phase=AuthorityPhase.COMPLETED)
        _require_verified(authority.compare_and_swap(current, completed), permit=False)


def _operation_id(load_config: Any) -> str:
    options = getattr(load_config, "options", {}) or {}
    stable = str(options.get(SCHEDULER_IDENTITY_OPTION) or "")
    if not stable:
        stable = publication_invocation_id(scheduler_run_id="direct", process_id=str(load_config.target_table))
    return digest_payload({"stable": stable, "database": load_config.target_schema, "target": load_config.target_table})


def _cluster(load_config: Any) -> str:
    name = ClickHouseTableDesign.from_options(getattr(load_config, "options", {}) or {}).cluster.name
    if not name:
        raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_TOPOLOGY_UNSUPPORTED", "cluster name is missing")
    return name


def _correlation_token(operation_id: str, action: str, epoch: int) -> str:
    return f"dpone-v1-{operation_id[:20]}-{action}-{epoch}-{secrets.token_hex(16)}"
