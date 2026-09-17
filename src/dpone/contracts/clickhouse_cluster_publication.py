"""Pure contracts for recoverable ClickHouse cluster publication."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from enum import Enum
from typing import Any

from dpone._compat import StrEnum

SCHEMA_VERSION = "dpone.clickhouse.cluster-full-refresh.v1"
AUTHORITY_TABLE = "__dpone_cluster_publication_authority"


class ClusterPublicationError(RuntimeError):
    """A cluster publication cannot continue without risking data integrity."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}:{detail}")


class AuthorityPhase(StrEnum):
    PREPARED = "PREPARED"
    DISPATCHING = "DISPATCHING"
    COMMITTED = "COMMITTED"
    CLEANUP_DISPATCHING = "CLEANUP_DISPATCHING"
    COMPLETED = "COMPLETED"


class AuthorityMutationStatus(StrEnum):
    VERIFIED = "verified"
    CONFLICT = "conflict"
    OUTCOME_UNKNOWN = "outcome_unknown"


class ReplicaPublicationState(StrEnum):
    PENDING = "pending"
    COMMITTED = "committed"
    CLEANUP_PENDING = "cleanup_pending"
    UNKNOWN = "unknown"


class QueueHostState(StrEnum):
    IN_PROGRESS = "in_progress"
    TERMINAL_SUCCESS = "terminal_success"
    TERMINAL_FAILURE = "terminal_failure"
    UNKNOWN = "unknown"


class QueueState(StrEnum):
    IN_PROGRESS = "in_progress"
    TERMINAL_SUCCESS = "terminal_success"
    TERMINAL_FAILURE = "terminal_failure"
    UNKNOWN = "unknown"


class AggregatePublicationState(StrEnum):
    PENDING = "pending"
    COMMITTED = "committed"
    CLEANUP_PENDING = "cleanup_pending"
    PARTIAL_IN_PROGRESS = "partial_in_progress"
    PARTIAL_TERMINAL = "partial_terminal"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ClusterReplica:
    host: str
    address: str
    native_port: int
    shard_num: int
    replica_num: int
    internal_replication: bool


@dataclass(frozen=True, slots=True)
class ClusterInventory:
    cluster: str
    replicas: tuple[ClusterReplica, ...]

    def validate(self) -> None:
        if not self.cluster or len(self.replicas) < 2:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_TOPOLOGY_UNSUPPORTED", "two replicas required")
        if {item.shard_num for item in self.replicas} != {1}:
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_TOPOLOGY_UNSUPPORTED", "one shard required")
        hosts = [item.host for item in self.replicas]
        numbers = [item.replica_num for item in self.replicas]
        if len(hosts) != len(set(hosts)) or len(numbers) != len(set(numbers)):
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_INVENTORY_INVALID", "replicas must be unique")
        if not all(item.internal_replication for item in self.replicas):
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_TOPOLOGY_UNSUPPORTED", "internal replication required"
            )

    @property
    def hosts(self) -> tuple[str, ...]:
        return tuple(sorted(item.host for item in self.replicas))

    @property
    def digest(self) -> str:
        return digest_payload([asdict(item) for item in sorted(self.replicas, key=lambda item: item.host)])


@dataclass(frozen=True, slots=True)
class GenerationIdentity:
    uuid: str
    engine_full: str
    schema_digest: str
    keeper_name: str
    keeper_path: str

    def validate(self) -> None:
        required = (self.uuid, self.engine_full, self.schema_digest, self.keeper_name, self.keeper_path)
        if not all(isinstance(value, str) and value for value in required):
            raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_GENERATION_INVALID", "identity is incomplete")
        if not self.engine_full.startswith("Replicated") or "MergeTree" not in self.engine_full:
            raise ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_ENGINE_UNSUPPORTED", "Replicated*MergeTree required"
            )


@dataclass(frozen=True, slots=True)
class ReplicaGeneration:
    host: str
    target: GenerationIdentity | None
    candidate: GenerationIdentity | None
    healthy: bool = True
    row_count: int | None = None


@dataclass(frozen=True, slots=True)
class QueueHostResult:
    host: str
    status: str | None
    exception_code: Any = None
    exception_text: Any = None

    @property
    def normalized(self) -> QueueHostState:
        status = self.status
        code, text = self.exception_code, self.exception_text
        if status in {"Inactive", "Active"}:
            return QueueHostState.IN_PROGRESS if code is None and text is None else QueueHostState.UNKNOWN
        if status != "Finished" or isinstance(code, bool):
            return QueueHostState.UNKNOWN
        try:
            number = int(code)
        except (TypeError, ValueError):
            return QueueHostState.UNKNOWN
        clean_text = text.strip() if isinstance(text, str) else None
        if number == 0 and clean_text == "":
            return QueueHostState.TERMINAL_SUCCESS
        if number > 0 and bool(clean_text):
            return QueueHostState.TERMINAL_FAILURE
        return QueueHostState.UNKNOWN


@dataclass(frozen=True, slots=True)
class QueueEntry:
    entry: str
    query_digest: str
    correlation_token: str
    hosts: tuple[QueueHostResult, ...]

    def state_for(self, expected_hosts: Sequence[str]) -> QueueState:
        expected = tuple(sorted(expected_hosts))
        actual = tuple(sorted(item.host for item in self.hosts))
        if actual != expected or len(actual) != len(set(actual)):
            return QueueState.UNKNOWN
        states = tuple(item.normalized for item in self.hosts)
        if QueueHostState.UNKNOWN in states:
            return QueueState.UNKNOWN
        if QueueHostState.IN_PROGRESS in states:
            return QueueState.IN_PROGRESS
        if all(item is QueueHostState.TERMINAL_SUCCESS for item in states):
            return QueueState.TERMINAL_SUCCESS
        return QueueState.TERMINAL_FAILURE


@dataclass(frozen=True, slots=True)
class AuthorityRecord:
    target_key: str
    operation_id: str
    fence_token: str
    phase: AuthorityPhase
    dispatch_epoch: int
    inventory_digest: str
    plan_digest: str
    database: str
    target: str
    candidate: str
    desired: GenerationIdentity
    predecessor: GenerationIdentity | None
    staged_rows: int
    ddl_correlation_token: str | None = None
    ddl_entry: str | None = None
    ddl_query_digest: str | None = None
    cleanup_correlation_token: str | None = None
    cleanup_entry: str | None = None
    schema_version: str = SCHEMA_VERSION

    @property
    def payload(self) -> str:
        return canonical_json(asdict(self))

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.payload.encode()).hexdigest()

    def dispatching(self, *, token: str) -> AuthorityRecord:
        return replace(
            self,
            phase=AuthorityPhase.DISPATCHING,
            dispatch_epoch=self.dispatch_epoch + 1,
            ddl_correlation_token=token,
        )


@dataclass(frozen=True, slots=True)
class VersionedAuthorityRecord:
    record: AuthorityRecord
    version: int


@dataclass(frozen=True, slots=True)
class DispatchPermit:
    """In-memory proof that this call won an acknowledged, verified CAS."""

    target_key: str
    operation_id: str
    fence_token: str
    dispatch_epoch: int


@dataclass(frozen=True, slots=True)
class AuthorityMutationResult:
    status: AuthorityMutationStatus
    observed: VersionedAuthorityRecord | None = None
    permit: DispatchPermit | None = None


def classify_replica(
    fact: ReplicaGeneration,
    *,
    desired: GenerationIdentity,
    predecessor: GenerationIdentity | None,
) -> ReplicaPublicationState:
    if not fact.healthy:
        return ReplicaPublicationState.UNKNOWN
    if fact.target == desired:
        if predecessor is None and fact.candidate is None:
            return ReplicaPublicationState.COMMITTED
        if predecessor is not None and fact.candidate == predecessor:
            return ReplicaPublicationState.COMMITTED
        if predecessor is not None and fact.candidate is None:
            return ReplicaPublicationState.CLEANUP_PENDING
    if fact.candidate == desired and fact.target == predecessor:
        return ReplicaPublicationState.PENDING
    return ReplicaPublicationState.UNKNOWN


def classify_aggregate(
    replicas: Sequence[ReplicaPublicationState], queue: QueueState | None
) -> AggregatePublicationState:
    if not replicas or ReplicaPublicationState.UNKNOWN in replicas:
        return AggregatePublicationState.UNKNOWN
    states = set(replicas)
    if states == {ReplicaPublicationState.PENDING}:
        return AggregatePublicationState.PENDING if queue is None else AggregatePublicationState.UNKNOWN
    if states == {ReplicaPublicationState.COMMITTED}:
        if queue in {QueueState.TERMINAL_SUCCESS, QueueState.TERMINAL_FAILURE}:
            return AggregatePublicationState.COMMITTED
        return AggregatePublicationState.UNKNOWN
    if states == {ReplicaPublicationState.CLEANUP_PENDING}:
        return AggregatePublicationState.CLEANUP_PENDING
    if states <= {ReplicaPublicationState.PENDING, ReplicaPublicationState.COMMITTED}:
        if queue is QueueState.IN_PROGRESS:
            return AggregatePublicationState.PARTIAL_IN_PROGRESS
        if queue in {QueueState.TERMINAL_SUCCESS, QueueState.TERMINAL_FAILURE}:
            return AggregatePublicationState.PARTIAL_TERMINAL
    return AggregatePublicationState.UNKNOWN


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_value)


def digest_payload(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")
