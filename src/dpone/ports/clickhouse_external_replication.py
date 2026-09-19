"""Ports for fenced ClickHouse external-replication publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from dpone._compat import StrEnum
from dpone.contracts import clickhouse_cluster_publication as cluster_contract
from dpone.contracts.clickhouse_cluster_publication import QueueEntry, QueueState
from dpone.contracts.clickhouse_external_replication import (
    EXTERNAL_AUTHORITY_SCHEMA_VERSION,
    EXTERNAL_RECEIPT_SCHEMA_VERSION,
    INTERNAL_AUTHORITY_SCHEMA_VERSION,
    ArtifactIdentity,
    ExternalArtifactReceipt,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalContractError,
    ExternalMember,
    ExternalMemberRecord,
    ExternalMemberStageState,
    ExternalPublicationError,
    ExternalPublicationRequest,
    ExternalTopology,
    MemberGenerationObservation,
    MemberPublicationState,
    PhysicalGeneration,
    ReplicationMode,
    canonical_json,
    classify_member_publication,
    derive_generation_id,
    derive_operation_id,
    derive_target_key,
    digest_payload,
)


class ExternalArtifactSourcePort(Protocol):
    """Invocation-scoped access to one sealed replayable artifact."""

    @property
    def binding_id(self) -> str: ...

    @property
    def identity(self) -> ArtifactIdentity: ...

    def revalidate(self, expected: ArtifactIdentity) -> None: ...

    def open_replay(self) -> Any: ...


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


@dataclass(frozen=True, slots=True)
class ExternalReplicationReceipt:
    """Serializable proof that excludes names, endpoints, paths, and row data."""

    target_key: str
    operation_id: str
    generation_id: str
    inventory_digest: str
    plan_digest: str
    artifact_sha256: str
    member_ids: tuple[str, ...]
    authority_version: int
    phase: str = "COMPLETED"
    replication_mode: str = "external"
    evidence_status: str = "UNVERIFIED"
    evidence_scope: str = "runtime"
    schema_version: str = EXTERNAL_RECEIPT_SCHEMA_VERSION

    @classmethod
    def from_authority(cls, record: ExternalAuthorityRecord, *, version: int) -> ExternalReplicationReceipt:
        """Project only immutable, opaque authority identity into public evidence."""

        if record.artifact is None or record.generation_id is None:
            raise ValueError("external publication authority is incomplete")
        return cls(
            target_key=record.target_key,
            operation_id=record.operation_id,
            generation_id=record.generation_id,
            inventory_digest=record.inventory_digest,
            plan_digest=record.plan_digest,
            artifact_sha256=record.artifact.sha256,
            member_ids=tuple(sorted(member.member_id for member in record.members)),
            authority_version=version,
            phase=record.phase.value,
        )

    @classmethod
    def from_state(
        cls,
        state: dict[str, Any],
        *,
        evidence_scope: str,
        evidence_status: str,
    ) -> ExternalReplicationReceipt:
        """Create a receipt from the high-level durable runtime state."""

        return cls(
            target_key=str(state["target_key"]),
            operation_id=str(state["operation_id"]),
            generation_id=str(state["generation_id"]),
            inventory_digest=str(state["inventory_digest"]),
            plan_digest=str(state["plan_digest"]),
            artifact_sha256=str(state["artifact_sha256"]),
            member_ids=tuple(sorted(str(value) for value in state["member_ids"])),
            authority_version=int(state["version"]),
            phase=str(state["phase"]),
            evidence_scope=evidence_scope,
            evidence_status=evidence_status,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the stable redacted evidence representation."""

        return {
            "schema_version": self.schema_version,
            "replication_mode": self.replication_mode,
            "phase": self.phase,
            "target_key": self.target_key,
            "operation_id": self.operation_id,
            "generation_id": self.generation_id,
            "inventory_digest": self.inventory_digest,
            "plan_digest": self.plan_digest,
            "artifact_sha256": self.artifact_sha256,
            "member_ids": list(self.member_ids),
            "authority_version": self.authority_version,
            "evidence_status": self.evidence_status,
            "evidence_scope": self.evidence_scope,
        }


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


class ExternalReplicaStagingPort(Protocol):
    """Perform direct local candidate operations for exactly one opaque member."""

    def observe(self, member_id: str, record: ExternalAuthorityRecord) -> MemberGenerationObservation: ...
    def create_candidate(
        self, member_id: str, record: ExternalAuthorityRecord, *, expected_uuid: str
    ) -> PhysicalGeneration: ...
    def load_candidate(
        self,
        member_id: str,
        record: ExternalAuthorityRecord,
        source: ExternalArtifactSourcePort,
    ) -> None: ...
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
    "EXTERNAL_AUTHORITY_SCHEMA_VERSION",
    "EXTERNAL_RECEIPT_SCHEMA_VERSION",
    "INTERNAL_AUTHORITY_SCHEMA_VERSION",
    "ArtifactIdentity",
    "ExternalArtifactReceipt",
    "ExternalArtifactSourcePort",
    "ExternalAuthorityPhase",
    "ExternalAuthorityRecord",
    "ExternalAuthorityMutationResult",
    "ExternalAuthorityMutationStatus",
    "ExternalAuthorityPort",
    "ExternalClusterDdlPort",
    "ExternalContractError",
    "ExternalDispatchPermit",
    "ExternalMember",
    "ExternalMemberRecord",
    "ExternalMemberStageState",
    "ExternalPublicationError",
    "ExternalPublicationRequest",
    "ExternalReplicationReceipt",
    "ExternalReplicaStagingPort",
    "ExternalTopology",
    "ExternalTopologyCatalogPort",
    "MemberGenerationObservation",
    "MemberPublicationState",
    "PhysicalGeneration",
    "QueueEntry",
    "QueueState",
    "ReplicationMode",
    "ReplicaConnectionProvider",
    "VersionedExternalAuthorityRecord",
    "canonical_json",
    "classify_member_publication",
    "cluster_contract",
    "derive_generation_id",
    "derive_operation_id",
    "derive_target_key",
    "digest_payload",
]
