# Runtime-local identities and state machines for fenced full refresh attempts.
# These models are independent from manifests and wire schemas: they describe
# only durable runtime facts used by the journal and effect-fencing port.

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Final
from uuid import UUID

_OPAQUE_REFERENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,255}\Z")


class AttemptState(StrEnum):
    # Durable whole-attempt states for the bounded full-refresh lifecycle.

    PLANNED = "planned"
    FROZEN_SOURCE = "frozen_source"
    EXPORTED_OBSERVED = "exported_observed"
    SLICE_INSERT_STARTED = "slice_insert_started"
    SLICE_CONFIRMED = "slice_confirmed"
    SLICE_QUARANTINED = "slice_quarantined"
    ASSEMBLY_BUILDING = "assembly_building"
    ASSEMBLY_QUARANTINED = "assembly_quarantined"
    SOURCE_STAGING_PROVED = "source_staging_proved"
    PUBLICATION_PREPARED = "publication_prepared"
    COMMAND_SENT = "command_sent"
    SERVER_CONFIRMED_OLD = "server_confirmed_old"
    SERVER_CONFIRMED_NEW = "server_confirmed_new"
    COMMIT_UNKNOWN = "commit_unknown"
    TARGET_QUALITY_CONFIRMED = "target_quality_confirmed"
    PUBLISHED_QUALITY_FAILED = "published_quality_failed"
    BACKUP_CLEANED = "backup_cleaned"
    COMPLETE = "complete"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        # Automatic execution must not advance terminal states.

        return self in {self.COMMIT_UNKNOWN, self.PUBLISHED_QUALITY_FAILED, self.COMPLETE, self.FAILED}


class ResourceKind(StrEnum):
    # Kinds of external objects tracked by exact server identity.

    MSSQL_WORK_TABLE = "mssql_work_table"
    CLICKHOUSE_SLICE_TABLE = "clickhouse_slice_table"
    CLICKHOUSE_ASSEMBLY_TABLE = "clickhouse_assembly_table"
    BACKUP_ALIAS = "backup_alias"
    QUARANTINE_ALIAS = "quarantine_alias"


class ResourceState(StrEnum):
    # Resource lifecycle; identity is absent until BOUND.

    CREATE_PLANNED = "create_planned"
    CREATE_GRANTED = "create_granted"
    CREATE_DISPATCHED = "create_dispatched"
    CREATE_OBSERVED = "create_observed"
    CREATE_NOT_APPLIED = "create_not_applied"
    CREATE_FOREIGN_IDENTITY = "create_foreign_identity"
    CREATE_UNKNOWN = "create_unknown"
    RENAME_PLANNED = "rename_planned"
    RENAME_GRANTED = "rename_granted"
    RENAME_DISPATCHED = "rename_dispatched"
    RENAME_CONFIRMED = "rename_confirmed"
    RENAME_NOT_APPLIED = "rename_not_applied"
    RENAME_FOREIGN_IDENTITY = "rename_foreign_identity"
    RENAME_UNKNOWN = "rename_unknown"
    BOUND = "bound"
    CLEANED = "cleaned"


class EffectKind(StrEnum):
    # External effects that require one epoch-scoped grant.

    CREATE = "create"
    BCP_EXPORT = "bcp_export"
    INSERT = "insert"
    RENAME = "rename"
    CLEANUP = "cleanup"


class EffectState(StrEnum):
    # Durable grant and dispatch lifecycle.

    PLANNED = "planned"
    GRANTED = "granted"
    DISPATCHED = "dispatched"
    TERMINATED = "terminated"
    APPLIED = "applied"
    NOT_APPLIED = "not_applied"
    UNKNOWN = "unknown"
    REVOKED = "revoked"


_ATTEMPT_EDGES: Final = frozenset(
    {
        (AttemptState.PLANNED, AttemptState.FROZEN_SOURCE),
        (AttemptState.FROZEN_SOURCE, AttemptState.EXPORTED_OBSERVED),
        (AttemptState.EXPORTED_OBSERVED, AttemptState.SLICE_INSERT_STARTED),
        (AttemptState.SLICE_INSERT_STARTED, AttemptState.SLICE_CONFIRMED),
        (AttemptState.SLICE_INSERT_STARTED, AttemptState.SLICE_QUARANTINED),
        (AttemptState.SLICE_QUARANTINED, AttemptState.SLICE_INSERT_STARTED),
        (AttemptState.SLICE_CONFIRMED, AttemptState.SLICE_CONFIRMED),
        (AttemptState.SLICE_CONFIRMED, AttemptState.ASSEMBLY_BUILDING),
        (AttemptState.ASSEMBLY_BUILDING, AttemptState.ASSEMBLY_BUILDING),
        (AttemptState.ASSEMBLY_BUILDING, AttemptState.ASSEMBLY_QUARANTINED),
        (AttemptState.ASSEMBLY_BUILDING, AttemptState.SOURCE_STAGING_PROVED),
        (AttemptState.ASSEMBLY_QUARANTINED, AttemptState.ASSEMBLY_BUILDING),
        (AttemptState.SOURCE_STAGING_PROVED, AttemptState.PUBLICATION_PREPARED),
        (AttemptState.PUBLICATION_PREPARED, AttemptState.COMMAND_SENT),
        (AttemptState.COMMAND_SENT, AttemptState.SERVER_CONFIRMED_OLD),
        (AttemptState.COMMAND_SENT, AttemptState.SERVER_CONFIRMED_NEW),
        (AttemptState.COMMAND_SENT, AttemptState.COMMIT_UNKNOWN),
        (AttemptState.SERVER_CONFIRMED_OLD, AttemptState.PUBLICATION_PREPARED),
        (AttemptState.SERVER_CONFIRMED_NEW, AttemptState.TARGET_QUALITY_CONFIRMED),
        (AttemptState.SERVER_CONFIRMED_NEW, AttemptState.PUBLISHED_QUALITY_FAILED),
        (AttemptState.TARGET_QUALITY_CONFIRMED, AttemptState.BACKUP_CLEANED),
        (AttemptState.BACKUP_CLEANED, AttemptState.COMPLETE),
        *(
            (AttemptState[name], AttemptState.FAILED)
            for name in "PLANNED FROZEN_SOURCE EXPORTED_OBSERVED SLICE_INSERT_STARTED SLICE_QUARANTINED SLICE_CONFIRMED ASSEMBLY_QUARANTINED SOURCE_STAGING_PROVED PUBLICATION_PREPARED SERVER_CONFIRMED_OLD".split()
        ),
    }
)
_RESOURCE_EDGES: Final = frozenset(
    {
        (ResourceState.CREATE_PLANNED, ResourceState.CREATE_GRANTED),
        (ResourceState.CREATE_GRANTED, ResourceState.CREATE_DISPATCHED),
        (ResourceState.CREATE_GRANTED, ResourceState.CREATE_NOT_APPLIED),
        (ResourceState.CREATE_DISPATCHED, ResourceState.CREATE_DISPATCHED),
        *(
            (ResourceState.CREATE_DISPATCHED, state)
            for state in (
                ResourceState.CREATE_OBSERVED,
                ResourceState.CREATE_NOT_APPLIED,
                ResourceState.CREATE_FOREIGN_IDENTITY,
                ResourceState.CREATE_UNKNOWN,
            )
        ),
        (ResourceState.CREATE_OBSERVED, ResourceState.BOUND),
        (ResourceState.CREATE_NOT_APPLIED, ResourceState.CREATE_PLANNED),
        (ResourceState.RENAME_PLANNED, ResourceState.RENAME_GRANTED),
        (ResourceState.RENAME_GRANTED, ResourceState.RENAME_DISPATCHED),
        *(
            (ResourceState.RENAME_DISPATCHED, state)
            for state in (
                ResourceState.RENAME_CONFIRMED,
                ResourceState.RENAME_NOT_APPLIED,
                ResourceState.RENAME_FOREIGN_IDENTITY,
                ResourceState.RENAME_UNKNOWN,
            )
        ),
        (ResourceState.RENAME_CONFIRMED, ResourceState.BOUND),
        (ResourceState.RENAME_NOT_APPLIED, ResourceState.RENAME_PLANNED),
        (ResourceState.BOUND, ResourceState.CLEANED),
    }
)
_EFFECT_EDGES: Final = frozenset(
    {
        (EffectState.PLANNED, EffectState.GRANTED),
        (EffectState.GRANTED, EffectState.DISPATCHED),
        (EffectState.GRANTED, EffectState.REVOKED),
        (EffectState.DISPATCHED, EffectState.TERMINATED),
        *(
            (state, outcome)
            for state in (EffectState.DISPATCHED, EffectState.TERMINATED)
            for outcome in (EffectState.APPLIED, EffectState.NOT_APPLIED, EffectState.UNKNOWN)
        ),
    }
)


def _require_transition(kind: str, edges: frozenset[tuple[Any, Any]], previous: StrEnum, next_state: StrEnum) -> None:
    if (previous, next_state) not in edges:
        raise ValueError(f"invalid {kind} transition: {previous.value} -> {next_state.value}")


def assert_attempt_transition(previous: AttemptState, next_state: AttemptState) -> None:
    _require_transition("attempt", _ATTEMPT_EDGES, previous, next_state)


def assert_resource_transition(previous: ResourceState, next_state: ResourceState) -> None:
    _require_transition("resource", _RESOURCE_EDGES, previous, next_state)


def assert_effect_transition(previous: EffectState, next_state: EffectState) -> None:
    _require_transition("effect", _EFFECT_EDGES, previous, next_state)


def require_opaque_reference(value: str, *, field_name: str) -> None:
    # Reject credential material where only a logical/version reference is valid.

    if not _OPAQUE_REFERENCE.fullmatch(value):
        raise ValueError(f"{field_name} must be an opaque logical reference")


def require_aware_datetime(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def assert_journal_safe(value: Any) -> None:
    # Accept only strict JSON values and reject credential-bearing keys recursively.
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("journal object keys must be strings")
            segmented = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
            normalized = re.sub(r"[^a-z0-9]+", "_", segmented.lower()).strip("_")
            if any(
                part in {"password", "secret", "token", "credential", "credentials"} for part in normalized.split("_")
            ):
                raise ValueError("journal facts may contain opaque references only, never credentials")
            assert_journal_safe(item)
        return
    if type(value) is list:
        for item in value:
            assert_journal_safe(item)
        return
    if type(value) is float and not math.isfinite(value):
        raise ValueError("journal facts must contain finite JSON numbers")
    if value is None or type(value) in {bool, int, float, str}:
        return
    raise ValueError("journal facts must contain JSON-native values only")


@dataclass(frozen=True, slots=True)
class InvocationKey:
    # Stable scheduler identity available before any random value exists.

    release_id: str
    deployment_id: str
    workflow_id: str
    workload_id: str
    dag_id: str
    airflow_dag_run_id: str
    task_id: str
    task_instance_try_number: int
    map_index: int

    def __post_init__(self) -> None:
        for field in (
            "release_id",
            "deployment_id",
            "workflow_id",
            "workload_id",
            "dag_id",
            "airflow_dag_run_id",
            "task_id",
        ):
            if not str(getattr(self, field)).strip():
                raise ValueError(f"{field} must be non-empty")
        if self.task_instance_try_number < 1:
            raise ValueError("task_instance_try_number must be positive")
        if self.map_index < -1:
            raise ValueError("map_index must be -1 or non-negative")

    def canonical_fields(self) -> dict[str, str | int]:
        return {
            "airflow_dag_run_id": self.airflow_dag_run_id,
            "dag_id": self.dag_id,
            "deployment_id": self.deployment_id,
            "map_index": self.map_index,
            "release_id": self.release_id,
            "task_id": self.task_id,
            "task_instance_try_number": self.task_instance_try_number,
            "workflow_id": self.workflow_id,
            "workload_id": self.workload_id,
        }

    @property
    def digest(self) -> str:
        payload = json.dumps(self.canonical_fields(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class AttemptIdentity:
    invocation: InvocationKey
    transfer_run_id: UUID
    attempt_nonce: UUID

    def __post_init__(self) -> None:
        if self.transfer_run_id == self.attempt_nonce:
            raise ValueError("transfer_run_id and attempt_nonce must be distinct")

    @property
    def attempt_id(self) -> UUID:
        return self.transfer_run_id


@dataclass(frozen=True, slots=True)
class PlannedResource:
    resource_id: UUID
    authority: str
    planned_name: str
    resource_kind: ResourceKind
    schema_hash: str

    def __post_init__(self) -> None:
        if not self.authority or not self.planned_name:
            raise ValueError("resource authority and planned_name are required")
        if len(self.schema_hash) != 64:
            raise ValueError("schema_hash must be a SHA-256 hex digest")
        try:
            bytes.fromhex(self.schema_hash)
        except ValueError as exc:
            raise ValueError("schema_hash must be a SHA-256 hex digest") from exc


@dataclass(frozen=True, slots=True)
class AuthenticatedResourceReadback:
    # Server-observed identity tied to one authenticated CREATE effect.

    effect_id: UUID
    credential_version_ref: str
    authority: str
    planned_name: str
    schema_hash: str
    exact_identity: Mapping[str, Any]
    server_receipt_ref: str

    def __post_init__(self) -> None:
        require_opaque_reference(self.credential_version_ref, field_name="credential_version_ref")
        require_opaque_reference(self.server_receipt_ref, field_name="server_receipt_ref")

    def proves(self, resource: PlannedResource, *, effect_id: UUID, credential_version_ref: str) -> bool:
        return (
            self.effect_id == effect_id
            and self.credential_version_ref == credential_version_ref
            and self.authority == resource.authority
            and self.planned_name == resource.planned_name
            and self.schema_hash == resource.schema_hash
            and self._identity_matches(resource.resource_kind)
        )

    def _identity_matches(self, kind: ResourceKind) -> bool:
        if kind is ResourceKind.MSSQL_WORK_TABLE:
            return (
                set(self.exact_identity) == {"object_id"}
                and type(self.exact_identity["object_id"]) is int
                and self.exact_identity["object_id"] > 0
            )
        if kind in {ResourceKind.CLICKHOUSE_SLICE_TABLE, ResourceKind.CLICKHOUSE_ASSEMBLY_TABLE}:
            if set(self.exact_identity) != {"uuid"}:
                return False
            try:
                identity = UUID(str(self.exact_identity["uuid"]))
            except ValueError:
                return False
            return identity.int != 0
        return False


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    # Current durable attempt row plus its create-once identity.

    identity: AttemptIdentity
    state: AttemptState
    lease_epoch: int
    owner_id: str
    lease_until: datetime
    version: int
    facts: Mapping[str, Any]

    def with_version(self, version: int) -> AttemptRecord:
        return AttemptRecord(
            self.identity, self.state, self.lease_epoch, self.owner_id, self.lease_until, version, self.facts
        )

    @classmethod
    def from_mappings(
        cls,
        row: Mapping[str, Any],
        invocation: InvocationKey,
        invocation_row: Mapping[str, Any],
    ) -> AttemptRecord:
        facts = row["facts_json"]
        return cls(
            identity=AttemptIdentity(
                invocation=invocation,
                transfer_run_id=invocation_row["transfer_run_id"],
                attempt_nonce=invocation_row["attempt_nonce"],
            ),
            state=AttemptState(row["state"]),
            lease_epoch=int(row["lease_epoch"]),
            owner_id=str(row["owner_id"]),
            lease_until=row["lease_until"],
            version=int(row["version"]),
            facts=json.loads(facts) if isinstance(facts, str) else facts,
        )


@dataclass(frozen=True, slots=True)
class PlannedCreate:
    # Versioned CREATE resource/effect pair returned by journal CAS.

    resource: PlannedResource
    effect_id: UUID
    attempt: AttemptRecord
    fence_epoch: int
    resource_version: int = 0
    effect_version: int = 0
    credential_version_ref: str | None = None

    def advanced(self, credential_version_ref: str | None = None, *, effect: bool = True) -> PlannedCreate:
        return PlannedCreate(
            self.resource,
            self.effect_id,
            self.attempt,
            self.fence_epoch,
            self.resource_version + 1,
            self.effect_version + int(effect),
            credential_version_ref or self.credential_version_ref,
        )
