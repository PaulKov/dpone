"""Historical workspace ownership identity, independent of today's catalog."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from uuid import UUID

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.dbt_workspace_activation import (
    MAX_WORKSPACE_ACTIVATION_RESOURCES,
    DbtWorkspaceActivationError,
    DbtWorkspaceActivationRequest,
    DbtWorkspaceGuardEpoch,
    DbtWorkspacePhysicalResource,
)


@dataclass(frozen=True, slots=True)
class DbtWorkspaceLifecycleIdentity:
    """Expected immutable coordinates proven by a sealed source/runtime context."""

    activation_id: str
    environment: str
    release_id: str
    deployment_id: str
    previous_deployment_id: str | None
    source_inventory_sha256: str
    runtime_context_sha256: str
    write_subjects: tuple[str, ...]

    def __post_init__(self) -> None:
        try:
            valid_uuid = str(UUID(self.activation_id)) == self.activation_id
        except (ValueError, TypeError, AttributeError):
            valid_uuid = False
        if (
            not valid_uuid
            or not isinstance(self.environment, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", self.environment)
        ):
            raise DbtWorkspaceActivationError("lifecycle_identity")
        digests: tuple[str, ...] = (
            self.release_id,
            self.deployment_id,
            self.source_inventory_sha256,
            self.runtime_context_sha256,
        )
        if self.previous_deployment_id is not None:
            digests += (self.previous_deployment_id,)
        if not all(is_canonical_sha256_digest(item) for item in digests):
            raise DbtWorkspaceActivationError("lifecycle_identity")
        _subjects(self.write_subjects)

    @classmethod
    def from_request(cls, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceLifecycleIdentity:
        request.__post_init__()
        return cls(
            request.activation_id,
            request.environment,
            request.release_id,
            request.deployment_id,
            request.previous_deployment_id,
            request.source_inventory_sha256,
            request.runtime_context_sha256,
            request.write_subjects,
        )


@dataclass(frozen=True, order=True, slots=True)
class DbtWorkspaceHistoricalGuard:
    """Protected original resource subject and its exact occurrence-owned epoch."""

    guard_id: str
    resource_sha256: str
    fencing_epoch: int
    write_subjects: tuple[str, ...]

    def __post_init__(self) -> None:
        DbtWorkspaceGuardEpoch(self.guard_id, self.fencing_epoch)
        if not is_canonical_sha256_digest(self.resource_sha256):
            raise DbtWorkspaceActivationError("historical_resource")
        _subjects(self.write_subjects)


@dataclass(frozen=True, slots=True)
class DbtWorkspaceLifecycleReadback:
    """Complete protected historical occurrence; shape alone grants no authority."""

    identity: DbtWorkspaceLifecycleIdentity
    request_sha256: str
    state: str
    guards: tuple[DbtWorkspaceHistoricalGuard, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.identity, DbtWorkspaceLifecycleIdentity):
            raise DbtWorkspaceActivationError("lifecycle_identity")
        self.identity.__post_init__()
        if not is_canonical_sha256_digest(self.request_sha256) or self.state not in {
            "PREPARED",
            "ACTIVE",
            "RETIRING",
            "RETIRED",
        }:
            raise DbtWorkspaceActivationError("lifecycle_readback")
        if (
            not isinstance(self.guards, tuple)
            or not 1 <= len(self.guards) <= MAX_WORKSPACE_ACTIVATION_RESOURCES
            or any(not isinstance(item, DbtWorkspaceHistoricalGuard) for item in self.guards)
            or tuple(sorted(self.guards)) != self.guards
            or len({item.guard_id for item in self.guards}) != len(self.guards)
        ):
            raise DbtWorkspaceActivationError("historical_guards")
        for guard in self.guards:
            guard.__post_init__()
        if (
            tuple(sorted(subject for guard in self.guards for subject in guard.write_subjects))
            != self.identity.write_subjects
        ):
            raise DbtWorkspaceActivationError("historical_write_closure")

    def require_same_ownership(self, previous: DbtWorkspaceLifecycleReadback) -> None:
        """Allow lifecycle progress without permitting immutable authority drift."""
        self.__post_init__()
        previous.__post_init__()
        if (self.identity, self.request_sha256, self.guards) != (
            previous.identity,
            previous.request_sha256,
            previous.guards,
        ):
            raise DbtWorkspaceActivationError("historical_ownership_changed")

    def require_request(self, request: DbtWorkspaceActivationRequest) -> None:
        """Bind complete original request coordinates and physical/write partition."""
        self.__post_init__()
        if not isinstance(request, DbtWorkspaceActivationRequest):
            raise DbtWorkspaceActivationError("historical_request_changed")
        request.__post_init__()
        expected_resources = {
            item.guard_id: (canonical_fingerprint(item.to_dict()), item.write_subjects) for item in request.resources
        }
        actual_resources = {item.guard_id: (item.resource_sha256, item.write_subjects) for item in self.guards}
        if (
            self.identity != DbtWorkspaceLifecycleIdentity.from_request(request)
            or self.request_sha256 != request.request_sha256
            or actual_resources != expected_resources
        ):
            raise DbtWorkspaceActivationError("historical_request_changed")


def workspace_request_payload(request: DbtWorkspaceActivationRequest) -> dict[str, object]:
    """Encode the existing request shape for bounded local recovery storage."""
    request.__post_init__()
    return {
        "schema": "dpone.dbt-workspace-activation-request.v1",
        **asdict(request),
        "write_subjects": list(request.write_subjects),
        "resources": [resource.to_dict() for resource in request.resources],
    }


def parse_workspace_request(payload: object) -> DbtWorkspaceActivationRequest:
    """Reject unknown fields and reconstruct a fingerprint-validated request."""
    fields = set(DbtWorkspaceActivationRequest.__dataclass_fields__)
    if not isinstance(payload, dict) or set(payload) != fields | {"schema"}:
        raise DbtWorkspaceActivationError("stored_request")
    if payload["schema"] != "dpone.dbt-workspace-activation-request.v1":
        raise DbtWorkspaceActivationError("stored_request")
    try:
        if not isinstance(payload["resources"], list) or not isinstance(payload["write_subjects"], list):
            raise ValueError
        resources = []
        for value in payload["resources"]:
            if not isinstance(value, dict) or set(value) != set(DbtWorkspacePhysicalResource.__dataclass_fields__):
                raise ValueError
            if not isinstance(value["write_subjects"], list):
                raise ValueError
            resources.append(
                DbtWorkspacePhysicalResource(**{**value, "write_subjects": tuple(value["write_subjects"])})
            )
        values = {key: value for key, value in payload.items() if key != "schema"}
        values.update(resources=tuple(resources), write_subjects=tuple(payload["write_subjects"]))
        return DbtWorkspaceActivationRequest(**values)
    except (ValueError, TypeError, AttributeError, KeyError):
        raise DbtWorkspaceActivationError("stored_request") from None


def _subjects(values: tuple[str, ...]) -> None:
    if (
        not isinstance(values, tuple)
        or not 1 <= len(values) <= MAX_WORKSPACE_ACTIVATION_RESOURCES
        or any(not is_canonical_sha256_digest(value) for value in values)
        or tuple(sorted(values)) != values
        or len(set(values)) != len(values)
    ):
        raise DbtWorkspaceActivationError("historical_write_closure")
