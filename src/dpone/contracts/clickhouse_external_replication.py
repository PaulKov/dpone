"""Pure contracts for ClickHouse external-replication publication."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from typing import Any

from dpone._compat import StrEnum
from dpone.contracts.clickhouse_cluster_publication import canonical_json, digest_payload

EXTERNAL_AUTHORITY_SCHEMA_VERSION = "dpone.clickhouse.cluster-external-full-refresh.v1"
EXTERNAL_RECEIPT_SCHEMA_VERSION = "dpone.clickhouse.cluster-external-full-refresh-receipt.v1"
INTERNAL_AUTHORITY_SCHEMA_VERSION = "dpone.clickhouse.cluster-full-refresh.v1"


class ExternalContractError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(f"DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_{code}:{detail}")


class ReplicationMode(StrEnum):
    INTERNAL = "internal"
    EXTERNAL = "external"


class ExternalAuthorityPhase(StrEnum):
    LOCKED = "LOCKED"
    STAGING = "STAGING"
    STAGED = "STAGED"
    PUBLICATION_DISPATCHING = "PUBLICATION_DISPATCHING"
    COMMITTED = "COMMITTED"
    CLEANUP_DISPATCHING = "CLEANUP_DISPATCHING"
    COMPLETED = "COMPLETED"
    ABORTED = "ABORTED"


class ExternalMemberStageState(StrEnum):
    PENDING = "pending"
    CANDIDATE_BOUND = "candidate_bound"
    READY = "ready"


class MemberPublicationState(StrEnum):
    PENDING = "pending"
    COMMITTED = "committed"
    CLEANUP_PENDING = "cleanup_pending"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True, order=True)
class ExternalMember:
    member_id: str
    shard_num: int
    replica_num: int
    internal_replication: bool

    @classmethod
    def create(cls, *, shard_num: int, replica_num: int, internal_replication: bool) -> ExternalMember:
        return cls(
            member_id=derive_member_id(shard_num, replica_num),
            shard_num=shard_num,
            replica_num=replica_num,
            internal_replication=internal_replication,
        )

    def validate(self) -> None:
        if self.shard_num <= 0 or self.replica_num <= 0:
            raise ExternalContractError("INVENTORY_INVALID", "member coordinates must be positive")
        if self.member_id != derive_member_id(self.shard_num, self.replica_num):
            raise ExternalContractError("INVENTORY_INVALID", "member identity does not match coordinates")


@dataclass(frozen=True, slots=True)
class ExternalTopology:
    cluster: str
    replication_mode: ReplicationMode
    members: tuple[ExternalMember, ...]

    def validate(self) -> None:
        if not self.cluster or len(self.members) < 2:
            raise ExternalContractError("INVENTORY_INVALID", "one shard with at least two members is required")
        for member in self.members:
            member.validate()
        coordinates = [(item.shard_num, item.replica_num) for item in self.members]
        if {item.shard_num for item in self.members} != {1} or len(coordinates) != len(set(coordinates)):
            raise ExternalContractError("INVENTORY_INVALID", "members must be unique on shard one")
        if self.replication_mode is not ReplicationMode.EXTERNAL or any(
            item.internal_replication for item in self.members
        ):
            raise ExternalContractError("MODE_MISMATCH", "external mode requires uniform internal_replication=false")

    @property
    def ordered_members(self) -> tuple[ExternalMember, ...]:
        return tuple(sorted(self.members, key=lambda item: item.member_id))

    @property
    def digest(self) -> str:
        self.validate()
        return digest_payload(
            {
                "cluster": self.cluster,
                "replication_mode": self.replication_mode,
                "members": [asdict(item) for item in self.ordered_members],
            }
        )


@dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    sha256: str
    byte_size: int
    row_count: int
    schema_digest: str
    wire_digest: str

    def validate(self) -> None:
        _require_digest(self.sha256, "artifact")
        _require_digest(self.schema_digest, "schema")
        _require_digest(self.wire_digest, "wire")
        if any(isinstance(value, bool) or value < 0 for value in (self.byte_size, self.row_count)):
            raise ExternalContractError("ARTIFACT_INVALID", "byte size and row count must be non-negative")


@dataclass(frozen=True, slots=True)
class ExternalArtifactReceipt:
    artifact_id: str
    sha256: str
    byte_size: int
    row_count: int
    schema_sha256: str
    content_sha256: str
    replayable: bool

    def validate(self) -> None:
        if not self.artifact_id or not self.replayable:
            raise ExternalContractError("ARTIFACT_INVALID", "a replayable artifact identity is required")
        self.identity.validate()
        _require_digest(self.content_sha256, "content")

    @property
    def identity(self) -> ArtifactIdentity:
        return ArtifactIdentity(
            sha256=self.sha256,
            byte_size=self.byte_size,
            row_count=self.row_count,
            schema_digest=self.schema_sha256,
            wire_digest=self.content_sha256,
        )


@dataclass(frozen=True, slots=True)
class ExternalPublicationRequest:
    cluster: str
    database: str
    target: str
    scheduler_invocation: str
    plan_sha256: str
    artifact: ExternalArtifactReceipt

    def validate(self) -> None:
        if not all((self.cluster, self.database, self.target, self.scheduler_invocation)):
            raise ExternalContractError("REQUEST_INVALID", "publication identity is incomplete")
        _require_digest(self.plan_sha256, "plan")
        self.artifact.validate()

    @property
    def target_key(self) -> str:
        return derive_target_key(self.cluster, self.database, self.target)

    @property
    def operation_id(self) -> str:
        return derive_operation_id(
            scheduler_invocation=self.scheduler_invocation,
            target_key=self.target_key,
            normalized_plan_digest=self.plan_sha256,
        )

    @property
    def generation_id(self) -> str:
        return derive_generation_id(
            operation_id=self.operation_id,
            artifact_sha256=self.artifact.sha256,
            schema_digest=self.artifact.schema_sha256,
            row_count=self.artifact.row_count,
        )


class ExternalPublicationError(RuntimeError):
    def __init__(self, code: str, *, evidence: Mapping[str, object] | None = None) -> None:
        self.code = code
        self.evidence = dict(evidence or {})
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class PhysicalGeneration:
    uuid: str
    engine_full: str
    schema_digest: str
    content_digest: str
    row_count: int

    def validate(self) -> None:
        if not self.uuid or not self.engine_full:
            raise ExternalContractError("GENERATION_INVALID", "physical identity is incomplete")
        replicated = self.engine_full.lstrip().startswith("Replicated")
        if replicated or "MergeTree" not in self.engine_full:
            raise ExternalContractError("ENGINE_UNSUPPORTED", "external mode requires non-replicated MergeTree")
        _require_digest(self.schema_digest, "schema")
        _require_digest(self.content_digest, "content")
        if isinstance(self.row_count, bool) or self.row_count < 0:
            raise ExternalContractError("GENERATION_INVALID", "row count must be non-negative")


@dataclass(frozen=True, slots=True)
class MemberGenerationObservation:
    member_id: str
    target: PhysicalGeneration | None
    candidate: PhysicalGeneration | None


@dataclass(frozen=True, slots=True)
class ExternalMemberRecord:
    member_id: str
    stage_state: ExternalMemberStageState = ExternalMemberStageState.PENDING
    publication_state: MemberPublicationState = MemberPublicationState.UNKNOWN
    cleanup_complete: bool = False
    predecessor: PhysicalGeneration | None = None
    candidate: PhysicalGeneration | None = None

    def validate(self) -> None:
        if not self.member_id:
            raise ExternalContractError("MEMBER_INVALID", "member identity is missing")
        for generation in (self.predecessor, self.candidate):
            if generation is not None:
                generation.validate()
        if self.stage_state is ExternalMemberStageState.READY and self.candidate is None:
            raise ExternalContractError("MEMBER_INVALID", "ready member has no bound candidate")


@dataclass(frozen=True, slots=True)
class ExternalAuthorityRecord:
    target_key: str
    operation_id: str
    fence_token: str
    phase: ExternalAuthorityPhase
    dispatch_epoch: int
    inventory_digest: str
    plan_digest: str
    database: str
    target: str
    candidate: str
    members: tuple[ExternalMemberRecord, ...]
    artifact: ArtifactIdentity | None = None
    artifact_binding_id: str | None = None
    generation_id: str | None = None
    publication_correlation_token: str | None = None
    publication_entry: str | None = None
    publication_query_digest: str | None = None
    cleanup_correlation_token: str | None = None
    cleanup_entry: str | None = None
    cleanup_query_digest: str | None = None
    error_code: str | None = None
    replication_mode: ReplicationMode = ReplicationMode.EXTERNAL
    schema_version: str = EXTERNAL_AUTHORITY_SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != EXTERNAL_AUTHORITY_SCHEMA_VERSION:
            raise ExternalContractError("SCHEMA_UNSUPPORTED", "authority schema is not external V1")
        if self.replication_mode is not ReplicationMode.EXTERNAL:
            raise ExternalContractError("MODE_MISMATCH", "authority must remain in external mode")
        if not all((self.target_key, self.operation_id, self.fence_token, self.database, self.target, self.candidate)):
            raise ExternalContractError("AUTHORITY_INVALID", "required authority identity is missing")
        _require_digest(self.target_key, "target key")
        _require_digest(self.operation_id, "operation")
        _require_digest(self.inventory_digest, "inventory")
        _require_digest(self.plan_digest, "plan")
        if isinstance(self.dispatch_epoch, bool) or self.dispatch_epoch < 0:
            raise ExternalContractError("AUTHORITY_INVALID", "dispatch epoch must be non-negative")
        ordered = tuple(sorted(self.members, key=lambda item: item.member_id))
        if len(ordered) < 2 or len({item.member_id for item in ordered}) != len(ordered):
            raise ExternalContractError("AUTHORITY_INVALID", "exact unique member set is required")
        for member in ordered:
            member.validate()
        if self.artifact is None:
            if self.generation_id is not None or self.artifact_binding_id is not None:
                raise ExternalContractError("AUTHORITY_INVALID", "generation requires artifact identity")
        else:
            self.artifact.validate()
            if not self.artifact_binding_id:
                raise ExternalContractError("AUTHORITY_INVALID", "artifact binding identity is missing")
            expected = derive_generation_id(
                operation_id=self.operation_id,
                artifact_sha256=self.artifact.sha256,
                schema_digest=self.artifact.schema_digest,
                row_count=self.artifact.row_count,
            )
            if self.generation_id != expected:
                raise ExternalContractError("GENERATION_INVALID", "logical generation identity differs")
        for value, label in (
            (self.publication_query_digest, "publication query"),
            (self.cleanup_query_digest, "cleanup query"),
        ):
            if value is not None:
                _require_digest(value, label)

    @property
    def payload(self) -> str:
        self.validate()
        value = asdict(self)
        value["members"] = [asdict(item) for item in sorted(self.members, key=lambda item: item.member_id)]
        return canonical_json(value)

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.payload.encode()).hexdigest()

    @classmethod
    def from_payload(cls, raw: str) -> ExternalAuthorityRecord:
        try:
            value = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ExternalContractError("AUTHORITY_INVALID", "authority payload is not valid JSON") from exc
        if not isinstance(value, dict) or value.get("schema_version") != EXTERNAL_AUTHORITY_SCHEMA_VERSION:
            raise ExternalContractError("SCHEMA_UNSUPPORTED", "authority schema is not external V1")
        _require_exact_fields(value, cls)
        artifact = value["artifact"]
        value["artifact"] = None if artifact is None else _decode_dataclass(ArtifactIdentity, artifact)
        value["members"] = tuple(_decode_member(item) for item in value["members"])
        value["phase"] = ExternalAuthorityPhase(value["phase"])
        value["replication_mode"] = ReplicationMode(value["replication_mode"])
        record = cls(**value)
        record.validate()
        return record

    def to_evidence(self) -> dict[str, Any]:
        """Project audit evidence without object names, endpoints, secrets, or paths."""

        self.validate()
        artifact = self.artifact
        return {
            "schema_version": self.schema_version,
            "replication_mode": self.replication_mode.value,
            "target_key": self.target_key,
            "operation_id": self.operation_id,
            "phase": self.phase.value,
            "dispatch_epoch": self.dispatch_epoch,
            "inventory_digest": self.inventory_digest,
            "plan_digest": self.plan_digest,
            "generation_id": self.generation_id,
            "artifact": (
                None
                if artifact is None
                else {
                    "sha256": artifact.sha256,
                    "byte_size": artifact.byte_size,
                    "row_count": artifact.row_count,
                    "schema_digest": artifact.schema_digest,
                    "wire_digest": artifact.wire_digest,
                }
            ),
            "members": [_member_evidence(item) for item in sorted(self.members, key=lambda item: item.member_id)],
            "error_code": self.error_code,
        }


def classify_member_publication(
    observation: MemberGenerationObservation,
    *,
    desired: PhysicalGeneration,
    predecessor: PhysicalGeneration | None,
) -> MemberPublicationState:
    if observation.target == desired:
        if predecessor is None and observation.candidate is None:
            return MemberPublicationState.COMMITTED
        if predecessor is not None and observation.candidate == predecessor:
            return MemberPublicationState.COMMITTED
        if predecessor is not None and observation.candidate is None:
            return MemberPublicationState.CLEANUP_PENDING
    if observation.target == predecessor and observation.candidate == desired:
        return MemberPublicationState.PENDING
    return MemberPublicationState.UNKNOWN


def derive_target_key(cluster: str, database: str, target: str) -> str:
    return digest_payload({"cluster": cluster, "database": database, "target": target})


def derive_operation_id(*, scheduler_invocation: str, target_key: str, normalized_plan_digest: str) -> str:
    if not scheduler_invocation:
        raise ExternalContractError("IDENTITY_INVALID", "scheduler invocation is required")
    _require_digest(target_key, "target key")
    _require_digest(normalized_plan_digest, "plan")
    return digest_payload(
        {
            "protocol_version": 1,
            "scheduler_invocation": scheduler_invocation,
            "target_key": target_key,
            "normalized_plan_digest": normalized_plan_digest,
        }
    )


def derive_member_id(shard_num: int, replica_num: int) -> str:
    if shard_num <= 0 or replica_num <= 0:
        raise ExternalContractError("IDENTITY_INVALID", "member coordinates must be positive")
    return digest_payload({"shard_num": shard_num, "replica_num": replica_num})


def derive_generation_id(*, operation_id: str, artifact_sha256: str, schema_digest: str, row_count: int) -> str:
    _require_digest(operation_id, "operation")
    _require_digest(artifact_sha256, "artifact")
    _require_digest(schema_digest, "schema")
    if isinstance(row_count, bool) or row_count < 0:
        raise ExternalContractError("IDENTITY_INVALID", "generation row count must be non-negative")
    return digest_payload(
        {
            "operation_id": operation_id,
            "artifact_sha256": artifact_sha256,
            "schema_digest": schema_digest,
            "row_count": row_count,
        }
    )


def _decode_member(value: Any) -> ExternalMemberRecord:
    _require_exact_fields(value, ExternalMemberRecord)
    value = dict(value)
    value["stage_state"] = ExternalMemberStageState(value["stage_state"])
    value["publication_state"] = MemberPublicationState(value["publication_state"])
    for name in ("predecessor", "candidate"):
        value[name] = None if value[name] is None else _decode_dataclass(PhysicalGeneration, value[name])
    return ExternalMemberRecord(**value)


def _decode_dataclass(kind: type[Any], value: Any) -> Any:
    _require_exact_fields(value, kind)
    return kind(**value)


def _require_exact_fields(value: Any, kind: type[Any]) -> None:
    if not isinstance(value, dict) or set(value) != {item.name for item in fields(kind)}:
        raise ExternalContractError("AUTHORITY_INVALID", f"{kind.__name__} fields differ")


def _member_evidence(member: ExternalMemberRecord) -> dict[str, Any]:
    def generation(value: PhysicalGeneration | None) -> dict[str, Any] | None:
        if value is None:
            return None
        return {
            "uuid": value.uuid,
            "schema_digest": value.schema_digest,
            "content_digest": value.content_digest,
            "row_count": value.row_count,
        }

    return {
        "member_id": member.member_id,
        "stage_state": member.stage_state.value,
        "publication_state": member.publication_state.value,
        "cleanup_complete": member.cleanup_complete,
        "predecessor": generation(member.predecessor),
        "candidate": generation(member.candidate),
    }


def _require_digest(value: str, label: str) -> None:
    if not isinstance(value, str) or len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise ExternalContractError("IDENTITY_INVALID", f"{label} digest must be lowercase SHA-256")
