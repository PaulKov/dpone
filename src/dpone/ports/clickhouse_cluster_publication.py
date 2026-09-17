"""Capability-oriented ports for ClickHouse cluster publication."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from dpone.contracts import clickhouse_cluster_publication as contracts
from dpone.contracts.clickhouse_cluster_admission import (
    ClickHouseClusterAdmissionError as ClickHouseClusterAdmissionError,
)
from dpone.contracts.clickhouse_cluster_admission import (
    clickhouse_cluster_admission_input as clickhouse_cluster_admission_input,
)
from dpone.contracts.clickhouse_cluster_admission import (
    evaluate_clickhouse_cluster_admission as evaluate_clickhouse_cluster_admission,
)


class ClusterPublicationCatalogPort(Protocol):
    def inventory(self, cluster: str) -> contracts.ClusterInventory: ...
    def require_atomic_database(self, cluster: str, database: str, hosts: Sequence[str]) -> None: ...
    def generations(
        self, cluster: str, database: str, target: str, candidate: str, hosts: Sequence[str]
    ) -> tuple[contracts.ReplicaGeneration, ...]: ...


class ClusterPublicationAuthorityPort(Protocol):
    def read_versioned(self, target_key: str) -> contracts.VersionedAuthorityRecord | None: ...
    def create_if_absent(self, record: contracts.AuthorityRecord) -> contracts.AuthorityMutationResult: ...
    def compare_and_swap(
        self, current: contracts.VersionedAuthorityRecord, desired: contracts.AuthorityRecord
    ) -> contracts.AuthorityMutationResult: ...


class ClusterPublicationBootstrapPort(Protocol):
    def ensure(self, cluster: str, database: str, hosts: Sequence[str]) -> None: ...


class ClusterPublicationDdlPort(Protocol):
    def publication_query_digest(self, record: contracts.AuthorityRecord, *, cluster: str) -> str: ...
    def cleanup_query_digest(self, record: contracts.AuthorityRecord, *, cluster: str) -> str: ...
    def dispatch_publication(
        self, record: contracts.AuthorityRecord, permit: contracts.DispatchPermit, *, cluster: str
    ) -> None: ...
    def find_entries(self, cluster: str, correlation_token: str) -> tuple[contracts.QueueEntry, ...]: ...
    def read_entry(self, cluster: str, entry: str) -> contracts.QueueEntry | None: ...
    def drop_predecessor(
        self, record: contracts.AuthorityRecord, permit: contracts.DispatchPermit, *, cluster: str
    ) -> None: ...


def require_exact_ddl_entry(
    ddl: ClusterPublicationDdlPort,
    cluster: str,
    *,
    entry_id: str | None,
    token: str | None,
    query_digest: str | None,
    error_code: str,
) -> contracts.QueueEntry:
    """Resolve evidence only when queue entry, token, and query are identical."""

    if not token or not query_digest:
        raise contracts.ClusterPublicationError(error_code, "DDL identity is incomplete")
    entries: tuple[contracts.QueueEntry, ...]
    if entry_id:
        entry = ddl.read_entry(cluster, entry_id)
        entries = () if entry is None else (entry,)
    else:
        entries = ddl.find_entries(cluster, token)
    if len(entries) != 1:
        raise contracts.ClusterPublicationError(error_code, "exactly one queue entry required")
    entry = entries[0]
    if entry.correlation_token != token or entry.query_digest != query_digest:
        raise contracts.ClusterPublicationError(
            error_code,
            "queue entry does not match the fenced DDL identity",
        )
    return entry


def require_verified_mutation(result: Any, *, permit: bool) -> contracts.VersionedAuthorityRecord:
    """Reject ambiguous KeeperMap outcomes and missing dispatch permits."""

    if result.status is not contracts.AuthorityMutationStatus.VERIFIED or result.observed is None:
        code = "DPONE_CLICKHOUSE_CLUSTER_CAS_UNKNOWN"
        if result.status is contracts.AuthorityMutationStatus.CONFLICT:
            code = "DPONE_CLICKHOUSE_CLUSTER_CAS_CONFLICT"
        raise contracts.ClusterPublicationError(code, "KeeperMap mutation was not acknowledged and verified")
    if permit and result.permit is None:
        raise contracts.ClusterPublicationError(
            "DPONE_CLICKHOUSE_CLUSTER_CAS_UNKNOWN", "dispatch permit was not issued"
        )
    return result.observed
