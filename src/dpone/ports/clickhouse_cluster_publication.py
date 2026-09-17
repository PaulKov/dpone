"""Capability-oriented ports for ClickHouse cluster publication."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from dpone.contracts import clickhouse_cluster_publication as contracts


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
    def dispatch_publication(
        self, record: contracts.AuthorityRecord, permit: contracts.DispatchPermit, *, cluster: str
    ) -> None: ...
    def find_entries(self, cluster: str, correlation_token: str) -> tuple[contracts.QueueEntry, ...]: ...
    def read_entry(self, cluster: str, entry: str) -> contracts.QueueEntry | None: ...
    def drop_predecessor(
        self, record: contracts.AuthorityRecord, permit: contracts.DispatchPermit, *, cluster: str
    ) -> None: ...
