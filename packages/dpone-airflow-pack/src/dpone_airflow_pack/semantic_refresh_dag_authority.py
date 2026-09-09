"""Local deployment-index authority for one run-neutral DAG sidecar."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SCHEMA = "dpone.semantic-refresh-v2-dag-projection-authority.v1"
_FIELDS = {
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


class SemanticRefreshDagAuthorityError(ValueError):
    """Raised when verified-index projection authority is invalid."""


@dataclass(frozen=True, slots=True)
class SemanticRefreshDagProjectionIdentity:
    """Exact deployment-side identity pinned by the local Airflow index."""

    dag_projection_sha256: str
    release_id: str
    deployment_id: str
    topology_sha256: str
    template_pack_fingerprint: str
    plan_bundle_sha256: str
    workflow_plan_sha256: str
    pre_release_bundle_sha256: str
    package_artifacts_sha256: str


class SemanticRefreshDagProjectionAuthority(Protocol):
    """Authorize an exact run-neutral sidecar from a verified local index."""

    def assert_authorized(self, identity: SemanticRefreshDagProjectionIdentity) -> None: ...


@dataclass(frozen=True, slots=True)
class LocalSemanticRefreshDagProjectionAuthority:
    """Pure authority record stored inside the immutable deployment index."""

    identity: SemanticRefreshDagProjectionIdentity
    authority_record_sha256: str

    @classmethod
    def build(cls, identity: SemanticRefreshDagProjectionIdentity) -> LocalSemanticRefreshDagProjectionAuthority:
        return cls(identity, _canonical_digest(_unsigned(identity)))

    @classmethod
    def from_mapping(cls, value: object) -> LocalSemanticRefreshDagProjectionAuthority:
        if not isinstance(value, Mapping) or set(value) != _FIELDS or value.get("schema") != _SCHEMA:
            raise SemanticRefreshDagAuthorityError("DAG projection authority fields are not closed")
        identity = SemanticRefreshDagProjectionIdentity(
            dag_projection_sha256=_digest(value.get("dag_projection_sha256"), "dag_projection_sha256"),
            release_id=_digest(value.get("release_id"), "release_id"),
            deployment_id=_digest(value.get("deployment_id"), "deployment_id"),
            topology_sha256=_digest(value.get("topology_sha256"), "topology_sha256"),
            template_pack_fingerprint=_digest(value.get("template_pack_fingerprint"), "template_pack_fingerprint"),
            plan_bundle_sha256=_digest(value.get("plan_bundle_sha256"), "plan_bundle_sha256"),
            workflow_plan_sha256=_digest(value.get("workflow_plan_sha256"), "workflow_plan_sha256"),
            pre_release_bundle_sha256=_digest(value.get("pre_release_bundle_sha256"), "pre_release_bundle_sha256"),
            package_artifacts_sha256=_digest(value.get("package_artifacts_sha256"), "package_artifacts_sha256"),
        )
        supplied = _digest(value.get("authority_record_sha256"), "authority_record_sha256")
        if _canonical_digest(_unsigned(identity)) != supplied:
            raise SemanticRefreshDagAuthorityError("DAG projection authority digest differs")
        return cls(identity, supplied)

    def to_mapping(self) -> dict[str, str]:
        return {**_unsigned(self.identity), "authority_record_sha256": self.authority_record_sha256}

    def assert_authorized(self, identity: SemanticRefreshDagProjectionIdentity) -> None:
        if identity != self.identity:
            raise SemanticRefreshDagAuthorityError("DAG projection differs from local deployment authority")


def _unsigned(identity: SemanticRefreshDagProjectionIdentity) -> dict[str, str]:
    return {
        "dag_projection_sha256": identity.dag_projection_sha256,
        "deployment_id": identity.deployment_id,
        "package_artifacts_sha256": identity.package_artifacts_sha256,
        "plan_bundle_sha256": identity.plan_bundle_sha256,
        "pre_release_bundle_sha256": identity.pre_release_bundle_sha256,
        "release_id": identity.release_id,
        "schema": _SCHEMA,
        "template_pack_fingerprint": identity.template_pack_fingerprint,
        "topology_sha256": identity.topology_sha256,
        "workflow_plan_sha256": identity.workflow_plan_sha256,
    }


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise SemanticRefreshDagAuthorityError(f"DAG projection {field} must be a canonical digest")
    return value


def _canonical_digest(value: Mapping[str, object]) -> str:
    raw = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "LocalSemanticRefreshDagProjectionAuthority",
    "SemanticRefreshDagAuthorityError",
    "SemanticRefreshDagProjectionAuthority",
    "SemanticRefreshDagProjectionIdentity",
]
