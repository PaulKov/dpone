"""Bounded recovery planning for an existing cluster publication."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from dpone.ports.clickhouse_cluster_publication import (
    ClusterPublicationAuthorityPort,
    ClusterPublicationCatalogPort,
    ClusterPublicationDdlPort,
    contracts,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_identity import correlation_token
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
    token = correlation_token(operation_id, "publish", record.dispatch_epoch + 1)
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
