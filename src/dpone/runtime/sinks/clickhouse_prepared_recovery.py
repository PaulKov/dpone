"""Bounded recovery planning for an existing cluster publication."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from dpone.ports.clickhouse_cluster_publication import (
    ClusterPublicationAuthorityPort,
    ClusterPublicationCatalogPort,
    ClusterPublicationDdlPort,
    contracts,
    require_exact_ddl_entry,
    require_verified_mutation,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt
from dpone.runtime.sinks.clickhouse_cluster_publication_recovery import require_pre_dispatch_generation


@dataclass(frozen=True, slots=True)
class PreparedRecoveryPlan:
    """Immutable evidence bound to one original operation and authority version."""

    cluster: str
    record: contracts.AuthorityRecord
    authority_version: int
    operation_started_at: datetime
    token: str
    query_digest: str
    plan_digest: str

    @property
    def operation_id(self) -> str:
        return self.record.operation_id

    @property
    def candidate_identity(self) -> contracts.GenerationIdentity:
        return self.record.desired

    @property
    def predecessor_identity(self) -> contracts.GenerationIdentity | None:
        return self.record.predecessor

    def to_public_dict(self) -> dict[str, str | int]:
        """Expose proof digests, not physical table names or identifiers."""
        return {
            "status": "ready",
            "operation_digest": contracts.digest_payload(self.operation_id),
            "authority_version": self.authority_version,
            "record_digest": self.record.payload_sha256,
            "inventory_digest": self.record.inventory_digest,
            "candidate_digest": contracts.digest_payload(asdict(self.candidate_identity)),
            "predecessor_digest": contracts.digest_payload(asdict(self.predecessor_identity))
            if self.predecessor_identity
            else "none",
            "query_digest": self.query_digest,
            "plan_digest": self.plan_digest,
        }


class PreparedRecoveryService:
    """Execute only a preflighted original operation."""

    def __init__(
        self,
        catalog: ClusterPublicationCatalogPort,
        authority: ClusterPublicationAuthorityPort,
        ddl: ClusterPublicationDdlPort,
        reconcile: Callable[[contracts.VersionedAuthorityRecord, str], Any],
        cleanup: Callable[[Any], None],
    ) -> None:
        self.catalog = catalog
        self.authority = authority
        self.ddl = ddl
        self.reconcile = reconcile
        self.cleanup = cleanup

    def execute_prepared_recovery(
        self, plan: PreparedRecoveryPlan, *, confirmation_digest: str
    ) -> ClusterFullRefreshReceipt:
        """Revalidate and dispatch only with one acknowledged strict permit."""
        if confirmation_digest != plan.plan_digest:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_RECOVERY_PLAN_CHANGED", "confirmation digest differs"
            )
        if not self.authority.supports_linearizable_dispatch_permit():
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_UNSAFE", "strict dispatch authority is not admitted"
            )
        existing = self.authority.read_versioned(plan.record.target_key)
        if existing is not None and existing.record.phase is contracts.AuthorityPhase.COMPLETED:
            return self._completed_receipt(plan, existing)
        if existing is not None and existing.record.phase in {
            contracts.AuthorityPhase.DISPATCHING,
            contracts.AuthorityPhase.COMMITTED,
            contracts.AuthorityPhase.CLEANUP_DISPATCHING,
        }:
            return self._resume_existing(plan, existing)
        fresh = plan_prepared_recovery(
            self.catalog,
            self.authority,
            self.ddl,
            cluster=plan.cluster,
            database=plan.record.database,
            target=plan.record.target,
            operation_id=plan.operation_id,
            expected_version=plan.authority_version,
            operation_started_at=plan.operation_started_at,
        )
        if fresh.plan_digest != plan.plan_digest:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_RECOVERY_PLAN_CHANGED", "physical or authority evidence changed"
            )
        current = contracts.VersionedAuthorityRecord(fresh.record, fresh.authority_version)
        dispatching = fresh.record.dispatching(token=fresh.token, query_digest=fresh.query_digest)
        mutation = self.authority.compare_and_swap(current, dispatching)
        verified = require_verified_mutation(mutation, permit=True)
        assert mutation.permit is not None
        try:
            self.ddl.dispatch_publication(verified.record, mutation.permit, cluster=plan.cluster)
        except Exception:
            pass  # Exact reconciliation decides the outcome; a second dispatch is forbidden.
        receipt = self.reconcile(verified, plan.cluster)
        self.cleanup(receipt)
        completed = self.authority.read_versioned(fresh.record.target_key)
        if completed is None or completed.record.phase is not contracts.AuthorityPhase.COMPLETED:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_UNRESOLVED", "cleanup has not completed"
            )
        receipt.validate_for_authority(completed)
        return ClusterFullRefreshReceipt.from_authority(completed, plan.cluster)

    def _resume_existing(
        self, plan: PreparedRecoveryPlan, current: contracts.VersionedAuthorityRecord
    ) -> ClusterFullRefreshReceipt:
        record = current.record
        original_receipt = ClusterFullRefreshReceipt.from_authority(
            contracts.VersionedAuthorityRecord(plan.record, plan.authority_version), plan.cluster
        )
        original_receipt.validate_for_authority(current)
        if (
            current.version <= plan.authority_version
            or record.ddl_correlation_token != plan.token
            or record.ddl_query_digest != plan.query_digest
            or record.dispatch_epoch != plan.record.dispatch_epoch + 1
        ):
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "resumed dispatch identity differs"
            )
        receipt = (
            self.reconcile(current, plan.cluster)
            if record.phase is contracts.AuthorityPhase.DISPATCHING
            else ClusterFullRefreshReceipt.from_authority(current, plan.cluster)
        )
        self.cleanup(receipt)
        completed = self.authority.read_versioned(plan.record.target_key)
        if completed is None or completed.record.phase is not contracts.AuthorityPhase.COMPLETED:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_UNRESOLVED", "reconciliation has not completed"
            )
        return self._completed_receipt(plan, completed)

    def _completed_receipt(
        self, plan: PreparedRecoveryPlan, current: contracts.VersionedAuthorityRecord
    ) -> ClusterFullRefreshReceipt:
        record = current.record
        if (
            record.operation_id != plan.operation_id
            or record.target_key != plan.record.target_key
            or record.fence_token != plan.record.fence_token
            or record.plan_digest != plan.record.plan_digest
            or record.desired != plan.candidate_identity
            or record.predecessor != plan.predecessor_identity
            or record.staged_rows != plan.record.staged_rows
        ):
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "completed authority belongs to another plan"
            )
        inventory = self.catalog.inventory(plan.cluster)
        contracts.require_inventory(record, inventory)
        entry = require_exact_ddl_entry(
            self.ddl,
            plan.cluster,
            entry_id=record.ddl_entry,
            token=record.ddl_correlation_token,
            query_digest=record.ddl_query_digest,
            error_code="DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN",
        )
        facts = self.catalog.generations(
            plan.cluster, record.database, record.target, record.candidate, inventory.hosts
        )
        observed_hosts = [fact.host for fact in facts]
        if (
            entry.state_for(inventory.hosts) is not contracts.QueueState.TERMINAL_SUCCESS
            or len(observed_hosts) != len(inventory.hosts)
            or set(observed_hosts) != set(inventory.hosts)
            or any(fact.target != record.desired or not fact.target_healthy for fact in facts)
        ):
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_UNKNOWN", "completed publication has no physical proof"
            )
        return ClusterFullRefreshReceipt.from_authority(current, plan.cluster)


def plan_prepared_recovery(
    catalog: ClusterPublicationCatalogPort,
    authority: ClusterPublicationAuthorityPort,
    ddl: ClusterPublicationDdlPort,
    *,
    cluster: str,
    database: str,
    target: str,
    operation_id: str,
    expected_version: int,
    operation_started_at: datetime,
) -> PreparedRecoveryPlan:
    """Plan recovery without performing any mutation."""
    if (
        not cluster
        or not database
        or not target
        or not operation_id
        or type(expected_version) is not int
        or expected_version < 0
        or operation_started_at.tzinfo is None
    ):
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_RECOVERY_INPUT_INVALID", "recovery identity or start time is invalid"
        )
    target_key = contracts.digest_payload({"cluster": cluster, "database": database, "target": target})
    current = authority.read_versioned(target_key)
    if current is None or current.record.target_key != target_key:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "original authority is absent"
        )
    record = current.record
    if current.version != expected_version or record.operation_id != operation_id:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_CONFLICT", "original authority identity changed"
        )
    if (
        record.phase is not contracts.AuthorityPhase.PREPARED
        or record.dispatch_epoch != 0
        or record.database != database
        or record.target != target
        or record.ddl_entry
        or record.ddl_correlation_token
        or record.ddl_query_digest
        or record.cleanup_entry
        or record.cleanup_correlation_token
    ):
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_PUBLICATION_UNRESOLVED", "original operation is not undispatched PREPARED"
        )
    if not record.authority_write_id:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_UNSAFE", "prepared authority has no strict-origin write identity"
        )
    if record.staged_rows == 0:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_EMPTY_CANDIDATE", "zero-row recovery needs authenticated authored policy"
        )
    if record.quality_reader is not None or record.quality_evidence is not None:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_QUALITY_UNVERIFIED", "original quality receipt needs separate verification"
        )
    inventory = catalog.inventory(cluster)
    contracts.require_inventory(record, inventory)
    require_pre_dispatch_generation(catalog, cluster, record, deadline=time.monotonic() + 0.5)
    if not ddl.prove_no_prior_publication(
        record, cluster=cluster, operation_started_at=operation_started_at.astimezone(UTC)
    ):
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN", "absence of prior publication is not proven"
        )
    token = _recovery_token(record)
    query_digest = ddl.publication_query_digest(record, cluster=cluster)
    plan_digest = contracts.digest_payload(
        {
            "record": record.payload_sha256,
            "version": current.version,
            "inventory": inventory.digest,
            "started_at": operation_started_at.astimezone(UTC).isoformat(),
            "token": token,
            "query_digest": query_digest,
        }
    )
    return PreparedRecoveryPlan(
        cluster=cluster,
        record=record,
        authority_version=current.version,
        operation_started_at=operation_started_at,
        token=token,
        query_digest=query_digest,
        plan_digest=plan_digest,
    )


def _recovery_token(record: contracts.AuthorityRecord) -> str:
    """Bind a stable one-shot DDL identity to the original publication fence."""
    epoch = record.dispatch_epoch + 1
    suffix = contracts.digest_payload(
        {
            "target_key": record.target_key,
            "operation_id": record.operation_id,
            "fence_token": record.fence_token,
            "plan_digest": record.plan_digest,
            "epoch": epoch,
        }
    )[:32]
    return f"dpone-recovery-v1-{record.operation_id[:20]}-publish-{epoch}-{suffix}"
