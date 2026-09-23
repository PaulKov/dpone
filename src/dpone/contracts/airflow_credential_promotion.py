"""Credential-free authority claims sealed by neutral deployment promotion."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone_airflow_pack.credential_projection_contract import require_projection_descriptor

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_workspace_attempt import require_workspace_authority_connection_ref

CREDENTIAL_PROMOTION_SCHEMA = "dpone.airflow-deployment-promotion.v1"
_FIELDS = frozenset({"credential_projection", "workspace_authority_connection_ref", "publish_authority_sha256"})


@dataclass(frozen=True, slots=True)
class CredentialPromotionEvidence:
    """Immutable projection subjects; contains references only, never values."""

    artifact_ref: str
    sha256: str
    bytes: int
    workspace_authority_connection_ref: str
    publish_authority_sha256: str

    def __post_init__(self) -> None:
        require_projection_descriptor(self.descriptor())
        require_workspace_authority_connection_ref(self.workspace_authority_connection_ref)
        if not is_canonical_sha256_digest(self.publish_authority_sha256):
            raise ValueError("credential projection publication authority digest is invalid")

    def descriptor(self) -> dict[str, Any]:
        return {"artifact_ref": self.artifact_ref, "sha256": self.sha256, "bytes": self.bytes}

    def to_dict(self) -> dict[str, Any]:
        return {
            "credential_projection": self.descriptor(),
            "workspace_authority_connection_ref": self.workspace_authority_connection_ref,
            "publish_authority_sha256": self.publish_authority_sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> CredentialPromotionEvidence:
        if not isinstance(value, Mapping) or set(value) != _FIELDS:
            raise ValueError("credential projection evidence fields are invalid")
        descriptor = require_projection_descriptor(value["credential_projection"])
        return cls(
            artifact_ref=descriptor["artifact_ref"],
            sha256=descriptor["sha256"],
            bytes=descriptor["bytes"],
            workspace_authority_connection_ref=value["workspace_authority_connection_ref"],
            publish_authority_sha256=value["publish_authority_sha256"],
        )

    def require_authority(self, *, control_ref: str | None, authority_sha256: str) -> None:
        if control_ref != self.workspace_authority_connection_ref or authority_sha256 != self.publish_authority_sha256:
            raise ValueError("credential projection evidence differs from the trusted publication authority")
