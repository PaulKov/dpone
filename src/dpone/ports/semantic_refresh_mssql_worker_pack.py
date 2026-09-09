"""Canonical static projection and worker-pack identity primitives."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from dpone.ports.semantic_refresh_mssql_primitives import _require_digest, _require_text

_WORKER_PACK_IDENTITY_SCHEMA = "dpone.dbt-semantic-refresh-worker-pack-identity.v1"


@dataclass(frozen=True, order=True, slots=True)
class MssqlStaticProjectionIdentity:
    """Closed run-neutral sidecar identity persisted with each worker pack."""

    dag_projection_sha256: str
    deployment_id: str
    package_artifacts_sha256: str
    plan_bundle_sha256: str
    pre_release_bundle_sha256: str
    release_id: str
    template_pack_fingerprint: str
    topology_sha256: str
    workflow_plan_sha256: str

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            _require_digest(getattr(self, field_name), field_name)

    def to_mapping(self) -> dict[str, str]:
        """Return the exact canonical projection embedded by the sidecar."""

        return {field_name: getattr(self, field_name) for field_name in sorted(self.__dataclass_fields__)}


def mssql_static_projection_identity_json(identity: MssqlStaticProjectionIdentity) -> str:
    """Serialize one exact run-neutral sidecar identity for MSSQL storage."""

    if not isinstance(identity, MssqlStaticProjectionIdentity):
        raise TypeError("projection identity must be canonical and typed")
    return json.dumps(identity.to_mapping(), ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def mssql_static_projection_identity_from_json(raw: str) -> MssqlStaticProjectionIdentity:
    """Parse one closed sidecar identity without accepting missing coordinates."""

    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("stored static projection identity JSON is invalid") from exc
    expected = set(MssqlStaticProjectionIdentity.__dataclass_fields__)
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("stored static projection identity fields are not closed")
    return MssqlStaticProjectionIdentity(**{field: str(value[field]) for field in expected})


def mssql_worker_pack_fingerprint(
    *,
    projection_identity: MssqlStaticProjectionIdentity,
    run_execution_bundle_sha256: str,
    activation_authority_receipt_sha256: str,
    authority_store_ref: str,
    run_guard_closure_sha256: str,
) -> str:
    """Recompute the canonical dbt worker-pack fingerprint at the MSSQL boundary."""

    if not isinstance(projection_identity, MssqlStaticProjectionIdentity):
        raise TypeError("projection_identity must be canonical and typed")
    for field_name, value in (
        ("run_execution_bundle_sha256", run_execution_bundle_sha256),
        ("activation_authority_receipt_sha256", activation_authority_receipt_sha256),
        ("run_guard_closure_sha256", run_guard_closure_sha256),
    ):
        _require_digest(value, field_name)
    _require_text(authority_store_ref, "authority_store_ref")
    if len(authority_store_ref) > 1024:
        raise ValueError("authority_store_ref exceeds its protected length")
    payload = {
        "activation_authority_receipt_sha256": activation_authority_receipt_sha256,
        "authority_store_ref": authority_store_ref,
        "projection_identity": projection_identity.to_mapping(),
        "run_execution_bundle_sha256": run_execution_bundle_sha256,
        "run_guard_closure_sha256": run_guard_closure_sha256,
        "schema": _WORKER_PACK_IDENTITY_SCHEMA,
    }
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "MssqlStaticProjectionIdentity",
    "mssql_static_projection_identity_from_json",
    "mssql_static_projection_identity_json",
    "mssql_worker_pack_fingerprint",
]
