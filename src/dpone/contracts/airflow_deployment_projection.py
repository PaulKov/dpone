"""Pure validation rules shared by Airflow deployment projections."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import deployment_id, is_canonical_sha256_digest

_MIRRORED_FIELDS = (
    "binding_set_ref",
    "connection_registry_ref",
    "credential_runtime_ref",
    "runtime_image_digest",
    "airflow_bundle_ref",
    "runtime_artifact_delivery",
    "dev_evidence_delivery",
    "mssql_asset_outlet_projection",
)
_DELIVERY_MODES = frozenset({"local_preview", "init_fetch", "shared_pvc", "embedded_bundle", "csi_volume", "inline"})
_OPTIONAL_DIGEST_FIELDS = (
    "binding_set_ref",
    "connection_registry_ref",
    "credential_runtime_ref",
    "runtime_image_digest",
)
_VERSIONED_GIT_BUNDLE_REF = re.compile(r"^git:[0-9a-f]{40}$")
_EXECUTABLE_DEPLOYMENT_SCHEMAS = frozenset(
    {
        "dpone.deployment-set.v2",
        "dpone.deployment-set.v3",
    }
)


@dataclass(frozen=True, slots=True)
class DeploymentProjectionViolation:
    code: str
    message: str


def deployment_projection_violation(
    deployment: Mapping[str, Any],
    airflow_index: Mapping[str, Any],
) -> DeploymentProjectionViolation | None:
    """Return the first deterministic identity or mirror-contract violation."""

    declared_id = deployment.get("deployment_id")
    index_id = airflow_index.get("deployment_id")
    if not is_canonical_sha256_digest(declared_id) or not is_canonical_sha256_digest(index_id):
        return _violation("DPONE_DEPLOYMENT_ID_INVALID", "deployment identities must be sha256 digests")
    if declared_id != index_id:
        return _violation("DPONE_DEPLOYMENT_ID_MISMATCH", "deployment and index identities do not match")
    release_ref = deployment.get("release_ref")
    release_id = airflow_index.get("release_id")
    if release_ref is None and release_id is None:
        return _violation("DPONE_RELEASE_NOT_FOUND", "deployment release reference is missing")
    if not is_canonical_sha256_digest(release_ref) or not is_canonical_sha256_digest(release_id):
        return _violation("DPONE_RELEASE_ID_INVALID", "deployment release identities must be sha256 digests")
    if release_ref != release_id:
        return _violation("DPONE_RELEASE_ID_MISMATCH", "deployment release_ref does not match index release_id")
    deployment_type = deployment.get("deployment_type")
    if deployment_type is not None and deployment_type not in {"preview", "environment"}:
        return _violation("DPONE_DEPLOYMENT_SCHEMA_INVALID", "deployment_type is invalid")
    runnable = deployment.get("runnable")
    if runnable is not None and not isinstance(runnable, bool):
        return _violation("DPONE_DEPLOYMENT_SCHEMA_INVALID", "deployment runnable must be boolean")
    if (
        deployment.get("schema") in _EXECUTABLE_DEPLOYMENT_SCHEMAS
        and runnable is True
        and not is_versioned_airflow_bundle_ref(deployment.get("airflow_bundle_ref"))
    ):
        return _violation(
            "DPONE_DEPLOYMENT_SCHEMA_INVALID",
            "runnable deployment-set requires airflow_bundle_ref git:<40-hex-commit>",
        )
    delivery = deployment.get("runtime_artifact_delivery")
    if not isinstance(delivery, Mapping) or delivery.get("mode") not in _DELIVERY_MODES:
        return _violation(
            "DPONE_DEPLOYMENT_SCHEMA_INVALID",
            "deployment runtime_artifact_delivery mode is required and invalid",
        )
    if delivery.get("mode") == "init_fetch" and not _valid_init_fetch(delivery):
        return _violation(
            "DPONE_DEPLOYMENT_SCHEMA_INVALID",
            "deployment init_fetch delivery contract is incomplete or unpinned",
        )
    evidence_delivery = deployment.get("dev_evidence_delivery")
    if evidence_delivery is not None and not _valid_dev_evidence_delivery(
        evidence_delivery,
        trust_tier=deployment.get("trust_tier"),
    ):
        return _violation(
            "DPONE_DEPLOYMENT_SCHEMA_INVALID",
            "deployment dev evidence delivery contract is invalid",
        )
    for field in _OPTIONAL_DIGEST_FIELDS:
        value = deployment.get(field)
        if value is not None and not is_canonical_sha256_digest(value):
            return _violation("DPONE_DEPLOYMENT_SCHEMA_INVALID", f"deployment field {field} must be a sha256 digest")
    projection_violation = _mssql_outlet_projection_mirror_violation(deployment, airflow_index)
    if projection_violation is not None:
        return projection_violation
    if deployment_id(deployment) != declared_id:
        return _violation(
            "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH",
            "deployment content does not match its content-addressed identity",
        )
    for field in _MIRRORED_FIELDS:
        if field == "mssql_asset_outlet_projection":
            continue
        if deployment.get(field) != airflow_index.get(field):
            return _violation(
                "DPONE_DEPLOYMENT_INDEX_MIRROR_MISMATCH",
                f"deployment field {field} does not match the Airflow index",
            )
    return None


def _mssql_outlet_projection_mirror_violation(
    deployment: Mapping[str, Any],
    airflow_index: Mapping[str, Any],
) -> DeploymentProjectionViolation | None:
    """Parse deployment/index projections separately before accepting deployment_id."""

    deployment_projection = deployment.get("mssql_asset_outlet_projection")
    index_projection = airflow_index.get("mssql_asset_outlet_projection")
    if deployment_projection is None and index_projection is None:
        return None
    mismatch_code = "DPONE_MSSQL_ASSET_OUTLET_PROJECTION_MISMATCH"
    try:
        from dpone_airflow_pack.mssql_outlet_projection_contract import (
            mirror_projections_or_raise,
        )
    except ImportError:  # pragma: no cover - provider pack is a hard runtime dep
        return _violation(
            "DPONE_MSSQL_ASSET_OUTLET_PROJECTION_INVALID",
            "mssql outlet projection validator is unavailable",
        )
    try:
        mirror_projections_or_raise(
            deployment_projection=deployment_projection,
            index_projection=index_projection,
            expected_environment=str(deployment.get("environment") or "") or None,
            expected_binding_set_ref=str(deployment.get("binding_set_ref") or "") or None,
            expected_connection_registry_ref=str(deployment.get("connection_registry_ref") or "") or None,
        )
    except Exception as exc:  # noqa: BLE001 - map provider codes into projection violations
        code = getattr(exc, "code", None)
        message = str(getattr(exc, "message", None) or exc)
        if isinstance(code, str) and code.startswith("DPONE_MSSQL_ASSET_OUTLET_PROJECTION_"):
            return _violation(code, message)
        return _violation(mismatch_code, message)
    if deployment.get("mssql_asset_outlet_projection") != airflow_index.get("mssql_asset_outlet_projection"):
        return _violation(
            mismatch_code,
            "mssql_asset_outlet_projection must mirror between deployment and airflow-index",
        )
    return None


def _violation(code: str, message: str) -> DeploymentProjectionViolation:
    return DeploymentProjectionViolation(code=code, message=message)


def _valid_init_fetch(delivery: Mapping[str, Any]) -> bool:
    registry_ref = delivery.get("artifact_registry_ref")
    identity = delivery.get("identity")
    source = delivery.get("source")
    verify = delivery.get("verify")
    if not isinstance(registry_ref, str) or not registry_ref or any(char.isspace() for char in registry_ref):
        return False
    if registry_ref == "current" or registry_ref.startswith("cache://current"):
        return False
    return (
        isinstance(identity, Mapping)
        and identity.get("method") == "kubernetes_workload_identity"
        and isinstance(identity.get("service_account"), str)
        and bool(identity.get("service_account"))
        and isinstance(source, Mapping)
        and source.get("artifact_registry_ref") == registry_ref
        and isinstance(verify, Mapping)
        and verify.get("checksums") == "required"
        and verify.get("attestations") in {"optional", "required_for_prod"}
    )


def _valid_dev_evidence_delivery(
    value: object,
    *,
    trust_tier: object,
) -> bool:
    return (
        trust_tier == "non_production"
        and isinstance(value, Mapping)
        and set(value) == {"mode", "claim_name", "mount_path", "worker_queue"}
        and value.get("mode") == "shared_pvc"
        and value.get("mount_path") == "/var/lib/dpone/dev-evidence"
        and all(
            isinstance(value.get(field), str) and bool(str(value[field]).strip())
            for field in ("claim_name", "worker_queue")
        )
    )


def is_versioned_airflow_bundle_ref(value: object) -> bool:
    """Return whether a bundle reference pins one immutable Git commit."""

    return isinstance(value, str) and _VERSIONED_GIT_BUNDLE_REF.fullmatch(value) is not None


__all__ = [
    "DeploymentProjectionViolation",
    "deployment_projection_violation",
    "is_versioned_airflow_bundle_ref",
]
