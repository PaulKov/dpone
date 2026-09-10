"""Immutable parent occurrence contracts, independent of native-v2 authority.

Hashes establish exact readback equality; construction never grants permission.
Protected adapters must enforce transitions and non-expiring resource ownership.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from uuid import UUID

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.composition_physical_identity import composition_physical_guard_id


class CompositionAdmissionError(ValueError):
    """Sanitized stable admission failure, without driver or credential text."""

    code = "DPONE_COMPOSITION_ADMISSION_UNAVAILABLE"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"{self.code}: {reason}")


def require_digest(value: object) -> None:
    """Require canonical SHA-256 identity, never arbitrary caller labels."""
    if not is_canonical_sha256_digest(value):
        raise CompositionAdmissionError("digest")


def require_text(value: object, *, maximum: int = 512) -> None:
    """Reject empty, unbounded or control-bearing identity strings."""
    if not isinstance(value, str) or not value or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise CompositionAdmissionError("identity")


def require_ordered_unique(values: tuple[str, ...]) -> None:
    """Canonical ordering prevents different encodings of one resource closure."""
    if not isinstance(values, tuple) or not values or values != tuple(sorted(set(values))):
        raise CompositionAdmissionError("closure")


@dataclass(frozen=True, slots=True)
class CompositionOccurrenceContext:
    """Exact parent coordinates from a verified sealed deployment projection."""

    activation_id: str
    environment: str
    release_id: str
    deployment_id: str
    previous_deployment_id: str | None
    runtime_context_sha256: str

    def __post_init__(self) -> None:
        try:
            parsed = UUID(self.activation_id)
            valid = str(parsed) == self.activation_id and parsed.version == 4
        except (ValueError, AttributeError, TypeError):
            valid = False
        if not valid:
            raise CompositionAdmissionError("activation_id")
        if not isinstance(self.environment, str) or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", self.environment) is None:
            raise CompositionAdmissionError("environment")
        for value in (self.release_id, self.deployment_id, self.runtime_context_sha256):
            require_digest(value)
        if self.previous_deployment_id is not None:
            require_digest(self.previous_deployment_id)


@dataclass(frozen=True, slots=True)
class CompositionWorkloadAdmission:
    """One verified executable workload and its complete physical writer scope."""

    workload_id: str
    constituent_id: str
    pack_sha256: str
    execution_cell: str
    write_subjects: tuple[str, ...]

    def __post_init__(self) -> None:
        require_text(self.workload_id)
        require_digest(self.pack_sha256)
        if self.constituent_id not in {"native", "standalone"}:
            raise CompositionAdmissionError("constituent")
        if self.execution_cell not in {
            "sqlserver_dbt_v1",
            "postgres_mssql_full_refresh_v1",
            "mssql_clickhouse_full_refresh_v1",
        }:
            raise CompositionAdmissionError("execution_capability")
        require_ordered_unique(self.write_subjects)
        for subject in self.write_subjects:
            require_digest(subject)


@dataclass(frozen=True, slots=True)
class CompositionPhysicalResource:
    """A protected physical collision domain shared across aliases and principals.

    ``guard_id`` derives from service incarnation and database/target continuity,
    never endpoint spelling, current credentials, engine version or catalog time.
    The observation digest retains original collision proof, not future state.
    """

    guard_id: str
    connector: str
    service_id: str
    physical_subject_sha256: str
    observation_sha256: str
    write_subjects: tuple[str, ...]

    def __post_init__(self) -> None:
        require_digest(self.guard_id)
        require_digest(self.physical_subject_sha256)
        require_digest(self.observation_sha256)
        require_text(self.service_id)
        if self.connector not in {"mssql", "clickhouse"}:
            raise CompositionAdmissionError("physical_capability")
        expected = composition_physical_guard_id(
            connector=self.connector,
            service_id=self.service_id,
            physical_subject_sha256=self.physical_subject_sha256,
        )
        if self.guard_id != expected:
            raise CompositionAdmissionError("physical_guard")
        require_ordered_unique(self.write_subjects)
        for subject in self.write_subjects:
            require_digest(subject)


@dataclass(frozen=True, slots=True)
class CompositionActivationRequest:
    """Immutable full-parent reservation; never a native-only request in disguise."""

    context: CompositionOccurrenceContext
    source_subject_sha256: str
    workloads: tuple[CompositionWorkloadAdmission, ...]
    resources: tuple[CompositionPhysicalResource, ...]

    def __post_init__(self) -> None:
        self.context.__post_init__()
        require_digest(self.source_subject_sha256)
        if not isinstance(self.workloads, tuple) or not isinstance(self.resources, tuple):
            raise CompositionAdmissionError("closure")
        if len(self.workloads) > 8192 or len(self.resources) > 8192:
            raise CompositionAdmissionError("budget")
        require_ordered_unique(tuple(row.workload_id for row in self.workloads))
        require_ordered_unique(tuple(row.guard_id for row in self.resources))
        if {row.constituent_id for row in self.workloads} != {"native", "standalone"}:
            raise CompositionAdmissionError("complete_parent_required")
        for workload in self.workloads:
            workload.__post_init__()
        for resource in self.resources:
            resource.__post_init__()
        workload_writes = sorted(subject for row in self.workloads for subject in row.write_subjects)
        physical_writes = sorted(subject for row in self.resources for subject in row.write_subjects)
        if (
            len(workload_writes) > 8192
            or workload_writes != physical_writes
            or len(set(workload_writes)) != len(workload_writes)
        ):
            raise CompositionAdmissionError("physical_partition")

    @property
    def request_sha256(self) -> str:
        return canonical_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        """Return detached, secret-free persistence/evidence coordinates."""
        return {"schema": "dpone.composition-activation-request.v1", **asdict(self)}

    @property
    def activation_id(self) -> str:
        return self.context.activation_id

    @property
    def environment(self) -> str:
        return self.context.environment

    @property
    def release_id(self) -> str:
        return self.context.release_id

    @property
    def deployment_id(self) -> str:
        return self.context.deployment_id

    @property
    def previous_deployment_id(self) -> str | None:
        return self.context.previous_deployment_id


@dataclass(frozen=True, slots=True)
class CompositionActivationReceipt:
    """Exact protected readback, retaining epochs after retirement for audit."""

    request_sha256: str
    state: str
    guard_epochs: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        require_digest(self.request_sha256)
        if self.state not in {"PREPARED", "ACTIVE", "RETIRING", "RETIRED"}:
            raise CompositionAdmissionError("occurrence_state")
        if not isinstance(self.guard_epochs, tuple) or any(
            type(pair) is not tuple or len(pair) != 2 for pair in self.guard_epochs
        ):
            raise CompositionAdmissionError("guard_epochs")
        require_ordered_unique(tuple(guard for guard, _ in self.guard_epochs))
        for guard, epoch in self.guard_epochs:
            require_digest(guard)
            if type(epoch) is not int or epoch <= 0:
                raise CompositionAdmissionError("guard_epoch")


@dataclass(frozen=True, slots=True)
class CompositionActivationOccurrence:
    """One exact request and complete receipt, with explicit expected-state checks."""

    request: CompositionActivationRequest
    receipt: CompositionActivationReceipt

    def __post_init__(self) -> None:
        self.request.__post_init__()
        self.receipt.__post_init__()
        if self.receipt.request_sha256 != self.request.request_sha256 or tuple(
            guard for guard, _ in self.receipt.guard_epochs
        ) != tuple(row.guard_id for row in self.request.resources):
            raise CompositionAdmissionError("occurrence_readback")

    def require_state(self, state: str) -> CompositionActivationOccurrence:
        self.__post_init__()
        if self.receipt.state != state:
            raise CompositionAdmissionError("occurrence_state")
        return self
