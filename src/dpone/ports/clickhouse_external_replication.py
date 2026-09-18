"""Ports for fenced ClickHouse external-replication publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from dpone._compat import StrEnum
from dpone.contracts.clickhouse_cluster_publication import QueueEntry
from dpone.contracts.clickhouse_external_replication import (
    ArtifactIdentity,
    ExternalAuthorityRecord,
    ExternalTopology,
    MemberGenerationObservation,
    PhysicalGeneration,
)


class ExternalAuthorityMutationStatus(StrEnum):
    VERIFIED = "verified"
    CONFLICT = "conflict"
    OUTCOME_UNKNOWN = "outcome_unknown"


@dataclass(frozen=True, slots=True)
class VersionedExternalAuthorityRecord:
    record: ExternalAuthorityRecord
    version: int


@dataclass(frozen=True, slots=True)
class ExternalDispatchPermit:
    target_key: str
    operation_id: str
    fence_token: str
    dispatch_epoch: int


@dataclass(frozen=True, slots=True)
class ExternalAuthorityMutationResult:
    status: ExternalAuthorityMutationStatus
    observed: VersionedExternalAuthorityRecord | None = None
    permit: ExternalDispatchPermit | None = None


class ExternalTopologyCatalogPort(Protocol):
    """Return one complete, normalized external-replication inventory."""

    def inventory(self, cluster: str) -> ExternalTopology: ...


class ExternalAuthorityPort(Protocol):
    """Persist the V2 target slot through acknowledged one-shot CAS calls."""

    def read_versioned(self, target_key: str) -> VersionedExternalAuthorityRecord | None: ...
    def create_if_absent(self, record: ExternalAuthorityRecord) -> ExternalAuthorityMutationResult: ...
    def compare_and_swap(
        self,
        current: VersionedExternalAuthorityRecord,
        desired: ExternalAuthorityRecord,
    ) -> ExternalAuthorityMutationResult: ...


class ReplicaConnectionProvider(Protocol):
    """Resolve a direct member connection from already-authorized credentials."""

    def connection_for(self, member_id: str) -> Any: ...


class ExternalArtifactPort(Protocol):
    """Reopen and verify the immutable artifact immediately before use."""

    def revalidate(self, artifact: ArtifactIdentity) -> None: ...


class ExternalReplicaStagingPort(Protocol):
    """Perform direct local candidate operations for exactly one opaque member."""

    def observe(self, member_id: str, record: ExternalAuthorityRecord) -> MemberGenerationObservation: ...
    def create_candidate(self, member_id: str, record: ExternalAuthorityRecord) -> PhysicalGeneration: ...
    def load_candidate(self, member_id: str, record: ExternalAuthorityRecord) -> None: ...
    def drop_candidate(
        self,
        member_id: str,
        record: ExternalAuthorityRecord,
        expected: PhysicalGeneration,
    ) -> None: ...


class ExternalClusterDdlPort(Protocol):
    """Dispatch and observe the single correlated cluster publication effects."""

    def publication_query_digest(self, record: ExternalAuthorityRecord, *, cluster: str) -> str: ...
    def cleanup_query_digest(self, record: ExternalAuthorityRecord, *, cluster: str) -> str: ...
    def dispatch_publication(
        self,
        record: ExternalAuthorityRecord,
        permit: ExternalDispatchPermit,
        *,
        cluster: str,
    ) -> None: ...
    def find_entries(self, cluster: str, correlation_token: str) -> tuple[QueueEntry, ...]: ...
    def read_entry(self, cluster: str, entry: str) -> QueueEntry | None: ...
    def drop_predecessor(
        self,
        record: ExternalAuthorityRecord,
        permit: ExternalDispatchPermit,
        *,
        cluster: str,
    ) -> None: ...


__all__ = [
    "ExternalArtifactPort",
    "ExternalAuthorityMutationResult",
    "ExternalAuthorityMutationStatus",
    "ExternalAuthorityPort",
    "ExternalClusterDdlPort",
    "ExternalDispatchPermit",
    "ExternalReplicaStagingPort",
    "ExternalTopologyCatalogPort",
    "ReplicaConnectionProvider",
    "VersionedExternalAuthorityRecord",
]
