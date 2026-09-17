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
)
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt
from dpone.runtime.sinks.clickhouse_full_refresh_contract import (
    FullRefreshPublicationMarker,
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
                return self._reconcile_existing(authority, current, cluster, inventory.hosts, facts)
        token = _correlation_token(operation_id, "publish", record.dispatch_epoch + 1)
        dispatching = record.dispatching(token=token)
        won = authority.compare_and_swap(current, dispatching)
        verified = _require_verified(won, permit=True)
        assert won.permit is not None
        try:
            self._ddl.dispatch_publication(dispatching, won.permit, cluster=cluster)
        except Exception:
            return self._reconcile_existing(authority, verified, cluster, inventory.hosts)
        return self._reconcile_existing(authority, verified, cluster, inventory.hosts)

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
        self._catalog.require_atomic_database(cluster, database, inventory.hosts)
        self._bootstrap.ensure(cluster, database, inventory.hosts)
        if current.record.phase is AuthorityPhase.COMPLETED:
            receipt = self._receipt(current, cluster)
        elif current.record.phase is AuthorityPhase.DISPATCHING:
            receipt = self._reconcile_existing(authority, current, cluster, inventory.hosts)
            self.cleanup(receipt)
        elif current.record.phase in {AuthorityPhase.COMMITTED, AuthorityPhase.CLEANUP_DISPATCHING}:
            receipt = self._receipt(current, cluster)
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
        if current.record.predecessor is None:
            self._complete(authority, current)
            return
        inventory = self._catalog.inventory(resolved.cluster)
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
        token = current.record.cleanup_correlation_token
        if not token:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_CLEANUP_UNKNOWN", "cleanup token is missing")
        entries = self._ddl.find_entries(cluster, token)
        if len(entries) != 1 or entries[0].state_for(hosts) not in {
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
            cleanup_entry=entries[0].entry,
        )
        _require_verified(authority.compare_and_swap(current, completed), permit=False)

    def _reconcile_existing(
        self,
        authority: ClusterPublicationAuthorityPort,
        current: VersionedAuthorityRecord,
        cluster: str,
        hosts: tuple[str, ...],
        facts: Any = None,
    ) -> ClusterFullRefreshReceipt:
        record = current.record
        if not record.ddl_correlation_token:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN", "dispatch token is missing")
        entries = self._ddl.find_entries(cluster, record.ddl_correlation_token)
        if len(entries) != 1:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN", "exactly one queue entry required")
        entry = entries[0]
        queue_state = entry.state_for(hosts)
        observed = facts or self._catalog.generations(cluster, record.database, record.target, record.candidate, hosts)
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
            ddl_query_digest=entry.query_digest,
        )
        if current.record != committed:
            result = authority.compare_and_swap(current, committed)
            current = _require_verified(result, permit=False)
        marker = FullRefreshPublicationMarker.create(
            operation_id=record.operation_id,
            database=record.database,
            target=record.target,
            candidate=record.candidate,
            predecessor_uuid=record.predecessor.uuid if record.predecessor else None,
            desired_uuid=record.desired.uuid,
            staged_rows=record.staged_rows,
        )
        return ClusterFullRefreshReceipt(
            marker=marker, authority=current.record, authority_version=current.version, cluster=cluster
        )

    @staticmethod
    def _receipt(current: VersionedAuthorityRecord, cluster: str) -> ClusterFullRefreshReceipt:
        record = current.record
        marker = FullRefreshPublicationMarker.create(
            operation_id=record.operation_id,
            database=record.database,
            target=record.target,
            candidate=record.candidate,
            predecessor_uuid=record.predecessor.uuid if record.predecessor else None,
            desired_uuid=record.desired.uuid,
            staged_rows=record.staged_rows,
        )
        return ClusterFullRefreshReceipt(
            marker=marker, authority=record, authority_version=current.version, cluster=cluster
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


def _require_verified(result: Any, *, permit: bool) -> VersionedAuthorityRecord:
    if result.status is not AuthorityMutationStatus.VERIFIED or result.observed is None:
        code = "DPONE_CLICKHOUSE_CLUSTER_CAS_UNKNOWN"
        if result.status is AuthorityMutationStatus.CONFLICT:
            code = "DPONE_CLICKHOUSE_CLUSTER_CAS_CONFLICT"
        raise ClusterPublicationError(code, "KeeperMap mutation was not acknowledged and verified")
    if permit and result.permit is None:
        raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_CAS_UNKNOWN", "dispatch permit was not issued")
    return result.observed


def _one_identity(facts: Any, name: str) -> Any:
    value = _optional_one_identity(facts, name)
    if value is None:
        raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_GENERATION_UNKNOWN", f"{name} is absent")
    return value


def _optional_one_identity(facts: Any, name: str) -> Any:
    values = {getattr(item, name) for item in facts}
    if len(values) != 1:
        raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_GENERATION_DIVERGED", f"{name} differs")
    return next(iter(values))


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
