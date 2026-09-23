"""Exact original ACTIVE snapshot retained by privileged legacy adoption."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields

from dpone.contracts.airflow_deployment import canonical_fingerprint as activation_fingerprint
from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment, DesiredStateRevision
from dpone.contracts.airflow_desired_state_validation import canonical_uuid, digest
from dpone.contracts.dbt_contract_validation import canonical_fingerprint
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest
from dpone.contracts.dbt_workspace_channel import (
    WorkspaceChannel,
    WorkspaceHandoverError,
    require_workspace_document,
    require_workspace_size,
)
from dpone.contracts.dbt_workspace_lifecycle import (
    DbtWorkspaceHistoricalGuard,
    DbtWorkspaceLifecycleIdentity,
    DbtWorkspaceLifecycleReadback,
    parse_workspace_request,
)
from dpone.contracts.strict_json import strict_json_object

BASELINE_SCHEMA = "dpone.dbt-workspace-adopted-current.v1"


@dataclass(frozen=True, slots=True)
class WorkspaceAdoptedCurrent:
    """Replay payload, not a claim that an absent historical handover occurred."""

    activation_id: str
    state: str
    desired_state_json: str
    desired_state_sha256: str
    observed_remote_revision: str
    request_json: str
    request_sha256: str
    guard_epochs: tuple[DbtWorkspaceHistoricalGuard, ...]
    authorization_subject_sha256: str

    def __post_init__(self) -> None:
        try:
            canonical_uuid(self.activation_id, field="activation_id")
            if self.state != "ACTIVE":
                raise ValueError
            digest(self.authorization_subject_sha256, field="authorization_subject_sha256")
            DesiredStateRevision(self.observed_remote_revision)
            desired = AirflowDesiredDeployment.from_json(self.desired_state_json)
            request = self.request
            if (
                desired.sha256 != self.desired_state_sha256
                or request.request_sha256 != self.request_sha256
                or desired.source.occurrence_id != self.activation_id
                or request.activation_id != self.activation_id
                or desired.promotion.release_id != request.release_id
                or desired.promotion.deployment_id != request.deployment_id
                or desired.environment != request.environment
            ):
                raise ValueError
            DbtWorkspaceLifecycleReadback(
                DbtWorkspaceLifecycleIdentity.from_request(request), self.request_sha256, self.state, self.guard_epochs
            )
            expected = {
                item.guard_id: (activation_fingerprint(item.to_dict()), item.write_subjects)
                for item in request.resources
            }
            if {item.guard_id: (item.resource_sha256, item.write_subjects) for item in self.guard_epochs} != expected:
                raise ValueError
            require_workspace_size(self.to_dict(), 31 * 1024 * 1024)
        except (TypeError, ValueError, AttributeError, UnicodeError):
            raise WorkspaceHandoverError("adoption_baseline") from None

    @property
    def request(self) -> DbtWorkspaceActivationRequest:
        """Decode the exact original request, never rebuild today's observation."""
        try:
            if not isinstance(self.request_json, str) or len(self.request_json.encode()) > 16 * 1024 * 1024:
                raise ValueError
            return parse_workspace_request(strict_json_object(self.request_json))
        except (ValueError, TypeError, UnicodeError):
            raise WorkspaceHandoverError("adoption_request") from None

    def require_channel(self, channel: WorkspaceChannel) -> None:
        """Bind archived desired content to the operator-declared trusted channel."""
        desired = AirflowDesiredDeployment.from_json(self.desired_state_json)
        if (desired.environment, desired.promotion.registry_scope_id, desired.source.project, desired.source.ref) != (
            channel.environment,
            channel.registry_scope_id,
            channel.source_project,
            channel.source_ref,
        ):
            raise WorkspaceHandoverError("adoption_channel")

    def _body(self) -> dict[str, object]:
        return {
            "schema": BASELINE_SCHEMA,
            **asdict(self),
            "guard_epochs": [
                {**asdict(item), "write_subjects": list(item.write_subjects)} for item in self.guard_epochs
            ],
        }

    @property
    def baseline_sha256(self) -> str:
        """Include all original payload bytes and exact historical guard epochs."""
        return canonical_fingerprint(self._body())

    def to_dict(self) -> dict[str, object]:
        return {**self._body(), "baseline_sha256": self.baseline_sha256}

    @classmethod
    def from_mapping(cls, value: object) -> WorkspaceAdoptedCurrent:
        data = require_workspace_document(
            value,
            schema=BASELINE_SCHEMA,
            names={item.name for item in fields(cls)},
            digest_field="baseline_sha256",
            maximum=31 * 1024 * 1024,
        )
        guards = data["guard_epochs"]
        if not isinstance(guards, list) or len(guards) > 8192:
            raise WorkspaceHandoverError("adoption_guards")
        parsed = []
        for item in guards:
            if not isinstance(item, dict) or set(item) != {field.name for field in fields(DbtWorkspaceHistoricalGuard)}:
                raise WorkspaceHandoverError("adoption_guard_shape")
            if not isinstance(item["write_subjects"], list):
                raise WorkspaceHandoverError("adoption_guard_subjects")
            parsed.append(DbtWorkspaceHistoricalGuard(**{**item, "write_subjects": tuple(item["write_subjects"])}))
        data["guard_epochs"] = tuple(parsed)
        return cls(**data)
