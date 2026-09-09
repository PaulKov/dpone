"""Deployment-side records for run-neutral semantic-refresh DAG sidecars."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)

_DESCRIPTOR_FIELDS = frozenset(
    {
        "artifact_bytes",
        "artifact_sha256",
        "dag_id",
        "dag_projection_sha256",
        "projection_id",
        "workflow_name",
    }
)
_AUTHORITY_FIELDS = frozenset(
    {
        "authority_record_sha256",
        "dag_projection_sha256",
        "deployment_id",
        "package_artifacts_sha256",
        "plan_bundle_sha256",
        "pre_release_bundle_sha256",
        "release_id",
        "schema",
        "template_pack_fingerprint",
        "topology_sha256",
        "workflow_plan_sha256",
    }
)


class SemanticRefreshDagSidecarFactory(Protocol):
    """Build deployment-bound sidecars after the deployment ID is known."""

    def build(
        self,
        *,
        release_id: str,
        deployment_id: str,
    ) -> tuple[SemanticRefreshDeploymentSidecar, ...]: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshDeploymentSidecar:
    """Canonical sidecar bytes plus their closed index authority record."""

    content: bytes
    descriptor: Mapping[str, object]
    authority: Mapping[str, object]

    def index_descriptor(
        self,
        *,
        release_id: str,
        deployment_id: str,
        environment: str,
    ) -> tuple[str, dict[str, object]]:
        """Validate and return the exact deployment-local index descriptor."""

        descriptor = dict(self.descriptor)
        authority = dict(self.authority)
        if set(descriptor) != _DESCRIPTOR_FIELDS:
            raise _invalid("semantic-refresh sidecar descriptor is not closed")
        if set(authority) != _AUTHORITY_FIELDS:
            raise _invalid("semantic-refresh sidecar authority is not closed")
        projection_sha256 = _digest(
            descriptor.get("dag_projection_sha256"),
            "dag_projection_sha256",
        )
        artifact_sha256 = _digest(
            descriptor.get("artifact_sha256"),
            "artifact_sha256",
        )
        artifact_bytes = descriptor.get("artifact_bytes")
        if (
            isinstance(artifact_bytes, bool)
            or not isinstance(artifact_bytes, int)
            or artifact_bytes <= 0
            or artifact_bytes != len(self.content)
            or artifact_sha256 != "sha256:" + hashlib.sha256(self.content).hexdigest()
        ):
            raise _invalid("semantic-refresh sidecar byte identity differs")
        dag_id = _text(descriptor.get("dag_id"), "dag_id")
        projection_id = _text(descriptor.get("projection_id"), "projection_id")
        if projection_id != f"semantic_refresh_v2::{dag_id}":
            raise _invalid("semantic-refresh projection_id differs from dag_id")
        _text(descriptor.get("workflow_name"), "workflow_name")
        if (
            authority.get("schema") != "dpone.semantic-refresh-v2-dag-projection-authority.v1"
            or authority.get("dag_projection_sha256") != projection_sha256
            or authority.get("release_id") != release_id
            or authority.get("deployment_id") != deployment_id
        ):
            raise _invalid("semantic-refresh sidecar authority identity differs")
        for field in _AUTHORITY_FIELDS - {"schema"}:
            _digest(authority.get(field), field)
        supplied = authority.get("authority_record_sha256")
        unsigned = {key: value for key, value in authority.items() if key != "authority_record_sha256"}
        if supplied != _canonical_digest(unsigned):
            raise _invalid("semantic-refresh sidecar authority digest differs")
        filename = f"semantic-refresh-{projection_sha256.removeprefix('sha256:')}.dag-projection.json"
        return filename, {
            **descriptor,
            "artifact_ref": (f"cache://deployments/{environment}/{deployment_id.replace(':', '-', 1)}/{filename}"),
            "authority": authority,
        }


def _canonical_digest(value: Mapping[str, object]) -> str:
    raw = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _digest(value: object, field: str) -> str:
    text = _text(value, field)
    if (
        len(text) != 71
        or not text.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in text[7:])
    ):
        raise _invalid(f"semantic-refresh {field} must be a canonical digest")
    return text


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 512:
        raise _invalid(f"semantic-refresh {field} is invalid")
    return value


def _invalid(message: str) -> AirflowDeploymentProjectionError:
    return AirflowDeploymentProjectionError(
        "DPONE_SEMANTIC_REFRESH_DAG_PROJECTION_INVALID",
        message,
    )


__all__ = [
    "SemanticRefreshDagSidecarFactory",
    "SemanticRefreshDeploymentSidecar",
]
