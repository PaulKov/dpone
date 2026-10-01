"""Source-free native PREPARED recovery using the normal publication lifecycle.

This is an internal, injected service, not a deployment admission mechanism.
Composition must supply an authenticated selected authority and a real held
observer. Unsupported original quality/empty-load policies fail closed.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from dpone.contracts.prepared_recovery import PreparedRecoveryPlan, require_unpublished_safety
from dpone.ports.clickhouse_cluster_publication import (
    ClusterPublicationCatalogPort,
    ClusterPublicationDdlPort,
    require_verified_mutation,
)
from dpone.ports.clickhouse_cluster_publication import (
    contracts as c,
)
from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt
from dpone.runtime.sinks.clickhouse_cluster_publication_recovery import (
    reconcile_existing,
    require_completed_publication,
    require_pre_dispatch_generation,
)
from dpone.runtime.sinks.clickhouse_recovery_hold import HeldRecoveryAuthority, HeldRecoveryDdl

if TYPE_CHECKING:
    from dpone.contracts.publication_preparation import NativePublicationPreparation
    from dpone.ports.prepared_recovery import (
        HeldPreparedRecoverySafety,
        NativePublicationAuthorityPort,
        NativePublicationAuthorityProvider,
        PreparedRecoverySafetyObserver,
    )


class PreparedRecoveryService:
    """Plan read-only; consume only a winning CAS permit under held exclusion."""

    def __init__(
        self,
        *,
        catalog: ClusterPublicationCatalogPort,
        provider: NativePublicationAuthorityProvider,
        ddl: ClusterPublicationDdlPort,
        safety: PreparedRecoverySafetyObserver,
        binding_digest: str,
        cluster: str,
        database: str,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if re.fullmatch(r"[0-9a-f]{64}", binding_digest) is None or not cluster or not database:
            raise ValueError("exact recovery binding and destination required")
        self._catalog, self._provider, self._ddl, self._safety = catalog, provider, ddl, safety
        self._binding, self._cluster, self._database = binding_digest, cluster, database
        self._clock = clock if clock is not None else lambda: int(time.time())

    def plan(self, *, target: str, operation_id: str, expected_version: int) -> PreparedRecoveryPlan:
        """Inspect the original preparation without creating state or issuing DDL."""
        if not target or not operation_id or type(expected_version) is not int or expected_version < 1:
            raise ValueError("exact target, operation and preparation revision required")
        target_key = c.digest_payload({"cluster": self._cluster, "database": self._database, "target": target})
        authority = self._admit_provider()
        preparation = authority.read_native_preparation(target_key, operation_id)
        self._require_origin(preparation, target_key, operation_id)
        if preparation.prepared.version != expected_version or preparation.current != preparation.prepared:
            raise ValueError("planning requires the exact current native PREPARED revision")
        with self._safety.hold(cluster=self._cluster, preparation=preparation) as held:
            held.require_held()
            if authority.read_native_preparation(target_key, operation_id) != preparation:
                raise ValueError("native preparation changed while acquiring recovery hold")
            self._require_physical_prepared(preparation)
            observed = held.observe_unpublished(preparation)
            require_unpublished_safety(observed, preparation, now=self._clock())
            held.require_held()
            return PreparedRecoveryPlan(
                preparation,
                observed,
                self._token(preparation),
                self._ddl.publication_query_digest(preparation.prepared.record, cluster=self._cluster),
            )

    def execute(self, plan: PreparedRecoveryPlan, *, confirmation_digest: str) -> ClusterFullRefreshReceipt:
        """Re-observe; never rebuild a dispatch permit from a read or lost ACK."""
        if not isinstance(plan, PreparedRecoveryPlan) or confirmation_digest != plan.digest:
            raise ValueError("exact recovery plan confirmation required")
        original = plan.preparation.prepared.record
        self._require_origin(plan.preparation, original.target_key, original.operation_id)
        if plan.preparation.current != plan.preparation.prepared or plan.dispatch_token != self._token(
            plan.preparation
        ):
            raise ValueError("recovery plan preparation or intent changed")
        # The original observation is part of confirmation, not fresh admission.
        boundary = max(item.history_through for item in plan.safety.histories)
        require_unpublished_safety(plan.safety, plan.preparation, now=boundary)
        authority = self._admit_provider()
        preparation = self._read_same_origin(authority, plan)
        if plan.dispatch_query_digest != self._ddl.publication_query_digest(original, cluster=self._cluster):
            raise ValueError("recovery publication query changed")
        with self._safety.hold(cluster=self._cluster, preparation=preparation) as held:
            held.require_held()
            preparation = self._read_same_origin(authority, plan)
            guarded = HeldRecoveryAuthority(authority, held, lambda: self._read_same_origin(authority, plan).current)
            ddl = HeldRecoveryDdl(self._ddl, held)
            current = preparation.current
            if current.record.phase is c.AuthorityPhase.PREPARED:
                self._require_physical_prepared(preparation)
                observed = held.observe_unpublished(preparation)
                require_unpublished_safety(observed, preparation, now=self._clock())
                desired = current.record.dispatching(token=plan.dispatch_token, query_digest=plan.dispatch_query_digest)
                mutation = guarded.compare_and_swap(current, desired)
                current = require_verified_mutation(mutation, permit=True)
                assert mutation.permit is not None
                try:
                    ddl.dispatch_publication(desired, mutation.permit, cluster=self._cluster)
                except Exception:
                    # Unknown transport outcome is observed, never redispatched.
                    held.require_held()
            if current.record.phase is c.AuthorityPhase.DISPATCHING:
                receipt = reconcile_existing(self._catalog, ddl, guarded, current, self._cluster)
            else:
                receipt = ClusterFullRefreshReceipt.from_authority(current, self._cluster)
            held.require_held()
            if receipt.authority.phase is not c.AuthorityPhase.COMPLETED:

                def scoped_authority(database: str) -> HeldRecoveryAuthority:
                    if database != self._database:
                        raise ValueError("recovery cleanup database changed")
                    return guarded

                publication = ClickHouseClusterFullRefreshPublicationService(
                    self._catalog,
                    scoped_authority,
                    ddl,
                    self._provider,
                )
                publication.cleanup(receipt)
            return self._completed(authority, ddl, held, plan)

    def _completed(
        self,
        authority: NativePublicationAuthorityPort,
        ddl: ClusterPublicationDdlPort,
        held: HeldPreparedRecoverySafety,
        plan: PreparedRecoveryPlan,
    ) -> ClusterFullRefreshReceipt:
        held.require_held()
        current = self._read_same_origin(authority, plan).current
        require_completed_publication(self._catalog, ddl, current, cluster=self._cluster)
        if self._read_same_origin(authority, plan).current != current:
            raise ValueError("authority changed during completed recovery observation")
        held.require_held()
        return ClusterFullRefreshReceipt.from_authority(current, self._cluster)

    def _admit_provider(self) -> NativePublicationAuthorityPort:
        inventory = self._catalog.inventory(self._cluster)
        inventory.validate()
        self._catalog.require_atomic_database(self._cluster, self._database, inventory.hosts)
        self._provider.ensure(self._cluster, self._database, inventory.hosts)
        return self._provider.for_database(self._database)

    def _read_same_origin(
        self,
        authority: NativePublicationAuthorityPort,
        plan: PreparedRecoveryPlan,
    ) -> NativePublicationPreparation:
        original = plan.preparation
        record = original.prepared.record
        observed = authority.read_native_preparation(record.target_key, record.operation_id)
        self._require_origin(observed, record.target_key, record.operation_id)
        if (observed.prepared, observed.prepared_at) != (original.prepared, original.prepared_at):
            raise ValueError("native recovery preparation changed")
        return observed

    def _require_origin(self, value: NativePublicationPreparation, target_key: str, operation_id: str) -> None:
        record = value.prepared.record
        if (
            value.binding_digest != self._binding
            or record.database != self._database
            or (record.target_key, record.operation_id) != (target_key, operation_id)
            or target_key
            != c.digest_payload({"cluster": self._cluster, "database": self._database, "target": record.target})
        ):
            raise ValueError("native recovery scope or binding differs")
        if record.phase is not c.AuthorityPhase.PREPARED or value.prepared.version < 1:
            raise ValueError("native preparation required")
        if type(record.staged_rows) is not int or record.staged_rows <= 0:
            raise ValueError("empty recovery requires original authenticated empty-load policy")
        if any(
            item is not None
            for item in (
                record.quality_evidence,
                record.quality_reader,
                value.current.record.quality_evidence,
                value.current.record.quality_reader,
            )
        ):
            raise ValueError("quality recovery requires original authenticated governance")
        if value.current.record.phase is c.AuthorityPhase.RETIRED_UNPUBLISHED:
            raise ValueError("retired operation cannot be recovered")
        c.require_inventory(record, self._catalog.inventory(self._cluster))

    def _require_physical_prepared(self, preparation: NativePublicationPreparation) -> None:
        if preparation.current != preparation.prepared:
            raise ValueError("current preparation changed")
        record = preparation.prepared.record
        inventory = self._catalog.inventory(self._cluster)
        facts = self._catalog.generations(
            self._cluster, self._database, record.target, record.candidate, inventory.hosts
        )
        if tuple(sorted(item.host for item in facts)) != inventory.hosts or any(
            item.target is not None and item.target_healthy is not True for item in facts
        ):
            raise ValueError("recovery requires exact healthy replica inventory")
        require_pre_dispatch_generation(self._catalog, self._cluster, record)

    def _token(self, preparation: NativePublicationPreparation) -> str:
        record = preparation.prepared.record
        suffix = c.digest_payload(
            {
                "binding": self._binding,
                "preparation": record.payload_sha256,
                "version": preparation.prepared.version,
            }
        )
        return f"dpone-recovery-v1-{record.operation_id[:20]}-publish-{record.dispatch_epoch + 1}-{suffix}"
