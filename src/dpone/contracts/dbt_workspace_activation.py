"""Immutable workspace activation occurrences; shapes are not activation authority."""

from __future__ import annotations

import re
from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.dbt_relation_writes import DbtRelationWrite

_TOKEN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
_CONNECTORS = frozenset({"mssql", "clickhouse", "postgres"})
_STATES = frozenset({"PREPARED", "ACTIVE", "RETIRING", "RETIRED"})
MAX_WORKSPACE_ACTIVATION_RESOURCES = 8192


class DbtWorkspaceActivationError(ValueError):
    """Fail closed without retaining adapter diagnostics or credentials."""

    code = "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"{self.code}: {reason}")


@dataclass(frozen=True, order=True, slots=True)
class DbtWorkspacePhysicalResource:
    """One platform-authorized physical writer collision coordinate."""

    guard_id: str
    connector: str
    service_authority_sha256: str
    target_authority_sha256: str
    observation_sha256: str
    write_subjects: tuple[str, ...]

    def __post_init__(self) -> None:
        _bounded_text(self.guard_id, "guard_id", maximum=512)
        if self.connector not in _CONNECTORS:
            raise DbtWorkspaceActivationError("connector")
        _digests(self.service_authority_sha256, self.target_authority_sha256, self.observation_sha256)
        if (
            not isinstance(self.write_subjects, tuple)
            or not self.write_subjects
            or tuple(sorted(self.write_subjects)) != self.write_subjects
            or len(set(self.write_subjects)) != len(self.write_subjects)
        ):
            raise DbtWorkspaceActivationError("write_closure")
        _digests(*self.write_subjects)

    def to_dict(self) -> dict[str, object]:
        return {
            "guard_id": self.guard_id,
            "connector": self.connector,
            "service_authority_sha256": self.service_authority_sha256,
            "target_authority_sha256": self.target_authority_sha256,
            "observation_sha256": self.observation_sha256,
            "write_subjects": list(self.write_subjects),
        }


@dataclass(frozen=True, order=True, slots=True)
class DbtWorkspaceGuardEpoch:
    """Exact epoch owned by one durable activation occurrence."""

    guard_id: str
    fencing_epoch: int

    def __post_init__(self) -> None:
        _bounded_text(self.guard_id, "guard_id", maximum=512)
        if type(self.fencing_epoch) is not int or self.fencing_epoch <= 0:
            raise DbtWorkspaceActivationError("fencing_epoch")

    def to_dict(self) -> dict[str, str | int]:
        return {"guard_id": self.guard_id, "fencing_epoch": self.fencing_epoch}


@dataclass(frozen=True, slots=True)
class DbtWorkspaceActivationRequest:
    """Complete immutable activation subject assembled from verified authorities."""

    activation_id: str
    environment: str
    release_id: str
    deployment_id: str
    previous_deployment_id: str | None
    source_inventory_sha256: str
    runtime_context_sha256: str
    write_subjects: tuple[str, ...]
    resources: tuple[DbtWorkspacePhysicalResource, ...]
    request_sha256: str

    def __post_init__(self) -> None:
        _uuid(self.activation_id)
        if _TOKEN.fullmatch(self.environment) is None:
            raise DbtWorkspaceActivationError("environment")
        _digests(
            self.release_id,
            self.deployment_id,
            self.source_inventory_sha256,
            self.runtime_context_sha256,
            self.request_sha256,
        )
        if self.previous_deployment_id is not None:
            _digests(self.previous_deployment_id)
        if (
            not isinstance(self.write_subjects, tuple)
            or not 1 <= len(self.write_subjects) <= MAX_WORKSPACE_ACTIVATION_RESOURCES
            or tuple(sorted(self.write_subjects)) != self.write_subjects
            or len(set(self.write_subjects)) != len(self.write_subjects)
        ):
            raise DbtWorkspaceActivationError("write_closure")
        _digests(*self.write_subjects)
        if (
            not isinstance(self.resources, tuple)
            or not 1 <= len(self.resources) <= MAX_WORKSPACE_ACTIVATION_RESOURCES
            or tuple(sorted(self.resources)) != self.resources
            or len({item.guard_id for item in self.resources}) != len(self.resources)
            or any(not isinstance(item, DbtWorkspacePhysicalResource) for item in self.resources)
        ):
            raise DbtWorkspaceActivationError("resource_closure")
        partition = tuple(sorted(subject for resource in self.resources for subject in resource.write_subjects))
        if partition != self.write_subjects:
            raise DbtWorkspaceActivationError("resource_partition")
        if self.request_sha256 != canonical_fingerprint(self._unsigned()):
            raise DbtWorkspaceActivationError("request_subject")

    @classmethod
    def build(
        cls,
        *,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
        source_inventory_sha256: str,
        runtime_context_sha256: str,
        write_subjects: tuple[str, ...],
        resources: tuple[DbtWorkspacePhysicalResource, ...],
    ) -> DbtWorkspaceActivationRequest:
        unsigned = _request_dict(
            activation_id=activation_id,
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
            previous_deployment_id=previous_deployment_id,
            source_inventory_sha256=source_inventory_sha256,
            runtime_context_sha256=runtime_context_sha256,
            write_subjects=write_subjects,
            resources=resources,
        )
        return cls(
            activation_id=activation_id,
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
            previous_deployment_id=previous_deployment_id,
            source_inventory_sha256=source_inventory_sha256,
            runtime_context_sha256=runtime_context_sha256,
            write_subjects=write_subjects,
            resources=resources,
            request_sha256=canonical_fingerprint(unsigned),
        )

    def _unsigned(self) -> dict[str, object]:
        return _request_dict(
            activation_id=self.activation_id,
            environment=self.environment,
            release_id=self.release_id,
            deployment_id=self.deployment_id,
            previous_deployment_id=self.previous_deployment_id,
            source_inventory_sha256=self.source_inventory_sha256,
            runtime_context_sha256=self.runtime_context_sha256,
            write_subjects=self.write_subjects,
            resources=self.resources,
        )


@dataclass(frozen=True, slots=True)
class DbtWorkspaceActivationReceipt:
    """Durable readback of one exact occurrence and its owned guard epochs."""

    activation_id: str
    request_sha256: str
    state: str
    guard_epochs: tuple[DbtWorkspaceGuardEpoch, ...]
    receipt_sha256: str

    def __post_init__(self) -> None:
        _uuid(self.activation_id)
        _digests(self.request_sha256, self.receipt_sha256)
        if self.state not in _STATES:
            raise DbtWorkspaceActivationError("state")
        if (
            not isinstance(self.guard_epochs, tuple)
            or not self.guard_epochs
            or tuple(sorted(self.guard_epochs)) != self.guard_epochs
            or len({item.guard_id for item in self.guard_epochs}) != len(self.guard_epochs)
            or any(not isinstance(item, DbtWorkspaceGuardEpoch) for item in self.guard_epochs)
        ):
            raise DbtWorkspaceActivationError("guard_epochs")
        if self.receipt_sha256 != canonical_fingerprint(self._unsigned()):
            raise DbtWorkspaceActivationError("receipt_subject")

    @classmethod
    def build(
        cls,
        *,
        activation_id: str,
        request_sha256: str,
        state: str,
        guard_epochs: tuple[DbtWorkspaceGuardEpoch, ...],
    ) -> DbtWorkspaceActivationReceipt:
        unsigned = _receipt_dict(
            activation_id=activation_id,
            request_sha256=request_sha256,
            state=state,
            guard_epochs=guard_epochs,
        )
        return cls(
            activation_id=activation_id,
            request_sha256=request_sha256,
            state=state,
            guard_epochs=guard_epochs,
            receipt_sha256=canonical_fingerprint(unsigned),
        )

    def _unsigned(self) -> dict[str, object]:
        return _receipt_dict(
            activation_id=self.activation_id,
            request_sha256=self.request_sha256,
            state=self.state,
            guard_epochs=self.guard_epochs,
        )


@dataclass(frozen=True, slots=True)
class DbtWorkspacePreparedActivation:
    """Exact request plus durable PREPARED readback returned by a coordinator."""

    request: DbtWorkspaceActivationRequest
    receipt: DbtWorkspaceActivationReceipt

    def __post_init__(self) -> None:
        if not isinstance(self.request, DbtWorkspaceActivationRequest) or not isinstance(
            self.receipt, DbtWorkspaceActivationReceipt
        ):
            raise DbtWorkspaceActivationError("prepared_occurrence")
        require_activation_receipt(self.receipt, self.request, state="PREPARED")


@dataclass(frozen=True, slots=True)
class DbtWorkspaceActiveActivation:
    """Exact request plus durable ACTIVE readback returned by a coordinator."""

    request: DbtWorkspaceActivationRequest
    receipt: DbtWorkspaceActivationReceipt

    def __post_init__(self) -> None:
        if not isinstance(self.request, DbtWorkspaceActivationRequest) or not isinstance(
            self.receipt, DbtWorkspaceActivationReceipt
        ):
            raise DbtWorkspaceActivationError("active_occurrence")
        require_activation_receipt(self.receipt, self.request, state="ACTIVE")


@dataclass(frozen=True, slots=True)
class DbtWorkspaceRetiringActivation:
    """Exact occurrence after new task admission has been durably closed."""

    request: DbtWorkspaceActivationRequest
    receipt: DbtWorkspaceActivationReceipt

    def __post_init__(self) -> None:
        require_activation_receipt(self.receipt, self.request, state="RETIRING")


@dataclass(frozen=True, slots=True)
class DbtWorkspaceRetiredActivation:
    """Exact occurrence after terminal quiescence and durable guard release."""

    request: DbtWorkspaceActivationRequest
    receipt: DbtWorkspaceActivationReceipt

    def __post_init__(self) -> None:
        require_activation_receipt(self.receipt, self.request, state="RETIRED")


def require_activation_receipt(
    receipt: DbtWorkspaceActivationReceipt,
    request: DbtWorkspaceActivationRequest,
    *,
    state: str,
) -> DbtWorkspaceActivationReceipt:
    """Reject stale, foreign, partial or differently fenced occurrence readback."""

    receipt.__post_init__()
    expected_guards = tuple(item.guard_id for item in request.resources)
    if (
        receipt.activation_id != request.activation_id
        or receipt.request_sha256 != request.request_sha256
        or receipt.state != state
        or tuple(item.guard_id for item in receipt.guard_epochs) != expected_guards
    ):
        raise DbtWorkspaceActivationError("occurrence_mismatch")
    return receipt


def dbt_relation_write_subject(write: DbtRelationWrite) -> str:
    """Bind one source-derived logical write without claiming physical identity."""

    if not isinstance(write, DbtRelationWrite):
        raise DbtWorkspaceActivationError("write")
    return canonical_fingerprint(
        {
            "schema": "dpone.dbt-workspace-relation-write.v1",
            "project_path": write.project_path,
            "workflow_id": write.workflow_id,
            "resource_id": write.resource_id,
            "kind": write.kind,
            "connector": write.connector,
            "connection_ref": write.connection_ref,
            "database": write.database,
            "relation_schema": write.schema,
            "relation": write.relation,
            "role": write.role,
        }
    )


def _request_dict(
    *,
    activation_id: str,
    environment: str,
    release_id: str,
    deployment_id: str,
    previous_deployment_id: str | None,
    source_inventory_sha256: str,
    runtime_context_sha256: str,
    write_subjects: tuple[str, ...],
    resources: tuple[DbtWorkspacePhysicalResource, ...],
) -> dict[str, object]:
    return {
        "schema": "dpone.dbt-workspace-activation-request.v1",
        "activation_id": activation_id,
        "environment": environment,
        "release_id": release_id,
        "deployment_id": deployment_id,
        "previous_deployment_id": previous_deployment_id,
        "source_inventory_sha256": source_inventory_sha256,
        "runtime_context_sha256": runtime_context_sha256,
        "write_subjects": list(write_subjects),
        "resources": [item.to_dict() for item in resources],
    }


def _receipt_dict(
    *,
    activation_id: str,
    request_sha256: str,
    state: str,
    guard_epochs: tuple[DbtWorkspaceGuardEpoch, ...],
) -> dict[str, object]:
    return {
        "schema": "dpone.dbt-workspace-activation-receipt.v1",
        "activation_id": activation_id,
        "request_sha256": request_sha256,
        "state": state,
        "guard_epochs": [item.to_dict() for item in guard_epochs],
    }


def _bounded_text(value: object, field: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise DbtWorkspaceActivationError(field)
    return value


def _digests(*values: object) -> None:
    if any(not is_canonical_sha256_digest(value) for value in values):
        raise DbtWorkspaceActivationError("digest")


def _uuid(value: object) -> str:
    try:
        parsed = UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        raise DbtWorkspaceActivationError("activation_id") from None
    if str(parsed) != value:
        raise DbtWorkspaceActivationError("activation_id")
    return str(parsed)


__all__ = [
    "DbtWorkspaceActivationError",
    "DbtWorkspaceActiveActivation",
    "DbtWorkspaceActivationReceipt",
    "DbtWorkspaceActivationRequest",
    "DbtWorkspaceGuardEpoch",
    "DbtWorkspacePhysicalResource",
    "DbtWorkspacePreparedActivation",
    "DbtWorkspaceRetiredActivation",
    "DbtWorkspaceRetiringActivation",
    "MAX_WORKSPACE_ACTIVATION_RESOURCES",
    "dbt_relation_write_subject",
    "require_activation_receipt",
]
