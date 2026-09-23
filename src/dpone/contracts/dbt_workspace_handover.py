"""Immutable desired-occurrence claim for a recoverable workspace handover.

The applied predecessor is explicit and need not match the previous publication.
Creating this value does not establish a durable claim; only the protected store
can do that by compare-and-swap against its current channel revision.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields

from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment, DesiredStateRevision
from dpone.contracts.airflow_desired_state_validation import canonical_uuid, digest
from dpone.contracts.dbt_contract_validation import canonical_fingerprint
from dpone.contracts.dbt_workspace_channel import (
    WorkspaceChannel,
    WorkspaceHandoverError,
    decode_workspace_document,
    require_workspace_document,
    require_workspace_size,
)

CLAIM_SCHEMA = "dpone.dbt-workspace-handover-claim.v1"
MAX_CHANNEL_REVISION = 2**63 - 1


@dataclass(frozen=True, slots=True)
class WorkspaceHandoverClaim:
    """Exact bounded transition selected before any predecessor retirement."""

    channel: WorkspaceChannel
    expected_channel_revision: int
    claim_revision: int
    predecessor_activation_id: str | None
    predecessor_deployment_id: str | None
    predecessor_request_sha256: str | None
    successor_activation_id: str
    successor_release_id: str
    successor_deployment_id: str
    desired_state_sha256: str
    desired_state_json: str
    observed_remote_revision: str
    source_inventory_sha256: str
    runtime_context_sha256: str
    authorization_subject_sha256: str

    def __post_init__(self) -> None:
        try:
            if not isinstance(self.channel, WorkspaceChannel):
                raise ValueError
            self.channel.__post_init__()
            if (
                type(self.expected_channel_revision) is not int
                or type(self.claim_revision) is not int
                or not 0 <= self.expected_channel_revision <= MAX_CHANNEL_REVISION - 2
                or self.claim_revision != self.expected_channel_revision + 1
            ):
                raise ValueError
            previous = (self.predecessor_activation_id, self.predecessor_deployment_id, self.predecessor_request_sha256)
            if any(item is None for item in previous) and previous != (None, None, None):
                raise ValueError
            if self.predecessor_activation_id is not None:
                canonical_uuid(self.predecessor_activation_id, field="predecessor_activation_id")
                digest(self.predecessor_deployment_id, field="predecessor_deployment_id")
                digest(self.predecessor_request_sha256, field="predecessor_request_sha256")
            canonical_uuid(self.successor_activation_id, field="successor_activation_id")
            if self.successor_activation_id == self.predecessor_activation_id:
                raise ValueError
            for field_name in (
                "successor_release_id",
                "successor_deployment_id",
                "desired_state_sha256",
                "source_inventory_sha256",
                "runtime_context_sha256",
                "authorization_subject_sha256",
            ):
                digest(getattr(self, field_name), field=field_name)
            DesiredStateRevision(self.observed_remote_revision)
            if not isinstance(self.desired_state_json, str):
                raise ValueError
            desired = AirflowDesiredDeployment.from_json(self.desired_state_json)
            if (
                desired.sha256 != self.desired_state_sha256
                or desired.source.occurrence_id != self.successor_activation_id
                or desired.promotion.release_id != self.successor_release_id
                or desired.promotion.deployment_id != self.successor_deployment_id
                or desired.promotion.registry_scope_id != self.channel.registry_scope_id
                or desired.environment != self.channel.environment
                or desired.source.project != self.channel.source_project
                or desired.source.ref != self.channel.source_ref
            ):
                raise ValueError
            require_workspace_size(self.to_dict(), 256 * 1024)
        except (TypeError, ValueError, AttributeError):
            raise WorkspaceHandoverError("claim_identity") from None

    def _body(self) -> dict[str, object]:
        return {"schema": CLAIM_SCHEMA, **asdict(self), "channel": self.channel.to_dict()}

    @property
    def claim_sha256(self) -> str:
        """Return the immutable claim fingerprint, including exact desired bytes."""
        return canonical_fingerprint(self._body())

    def to_dict(self) -> dict[str, object]:
        """Serialize a fresh mapping suitable for durable closed-document storage."""
        return {**self._body(), "claim_sha256": self.claim_sha256}

    @classmethod
    def from_mapping(cls, value: object) -> WorkspaceHandoverClaim:
        """Rehydrate a saved claim without generating a new occurrence UUID."""
        data = require_workspace_document(
            value,
            schema=CLAIM_SCHEMA,
            names={item.name for item in fields(cls)},
            digest_field="claim_sha256",
            maximum=256 * 1024,
        )
        data["channel"] = WorkspaceChannel.from_mapping(data["channel"])
        return cls(**data)

    @classmethod
    def from_json(cls, value: bytes | str) -> WorkspaceHandoverClaim:
        return cls.from_mapping(decode_workspace_document(value, 256 * 1024))
