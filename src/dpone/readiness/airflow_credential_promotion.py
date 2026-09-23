"""Derive publication claims from verified immutable non-secret artifacts."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from dpone.contracts.airflow_credential_promotion import CredentialPromotionEvidence
from dpone.contracts.airflow_deployment_projection import deployment_projection_violation
from dpone.gitops.schema_validation import GitOpsSchemaValidator

if TYPE_CHECKING:
    from dpone.ports.airflow_desired_state_publish import DesiredStatePublicationAuthority


def build_credential_promotion_evidence(
    *,
    authority: DesiredStatePublicationAuthority,
    deployment_payload: bytes,
    index_payload: bytes,
    projection_payload: bytes,
    binding_set_payload: bytes,
    registry_payload: bytes,
) -> CredentialPromotionEvidence:
    """Produce exact promotion subjects, not a claim that external gates passed.

    The protected promotion producer remains responsible for source/CI identity
    and attestation admission. This function independently verifies content
    addressing, mirrored deployment/index fields, projection and registry parity.
    It reads no credentials, files or network and never creates a passed receipt.
    """
    from dpone.runtime.runtime_credential_projection import verify_deployment_credential_projection

    deployment = _object(deployment_payload)
    index = _object(index_payload)
    if (deployment.get("schema"), index.get("schema")) != (
        "dpone.deployment-set.v6",
        "dpone.airflow-deployment-index.v6",
    ):
        raise ValueError("credential projection promotion requires deployment/index v6")
    validator = GitOpsSchemaValidator()
    if validator.validate(deployment, expected_kind="dpone.deployment-set.v6") or validator.validate(
        index, expected_kind="dpone.airflow-deployment-index.v6"
    ):
        raise ValueError("credential projection promotion artifact schema is invalid")
    violation = deployment_projection_violation(deployment, index)
    if violation is not None:
        raise ValueError(f"credential projection promotion rejected: {violation.code}")
    descriptor = index.get("deployment")
    if (
        not isinstance(descriptor, dict)
        or descriptor.get("sha256") != "sha256:" + hashlib.sha256(deployment_payload).hexdigest()
        or descriptor.get("bytes") != len(deployment_payload)
        or isinstance(descriptor.get("bytes"), bool)
    ):
        raise ValueError("credential projection promotion deployment bytes differ from index")
    if deployment.get("environment") != authority.environment:
        raise ValueError("credential projection promotion environment differs from authority")
    control_ref = getattr(authority, "workspace_authority_connection_ref", None)
    if control_ref is None:
        raise ValueError("credential projection promotion requires workspace authority")
    projection = verify_deployment_credential_projection(
        deployment=deployment,
        projection_payload=projection_payload,
        binding_set_payload=binding_set_payload,
        registry_payload=registry_payload,
    )
    projection.require_authority(control_ref=control_ref, authority_sha256=authority.publish_authority_sha256)
    return CredentialPromotionEvidence.from_dict(
        {
            "credential_projection": deployment["credential_projection"],
            "workspace_authority_connection_ref": control_ref,
            "publish_authority_sha256": authority.publish_authority_sha256,
        }
    )


def _object(payload: bytes) -> dict[str, Any]:
    if not payload or len(payload) > 16 * 1024 * 1024:
        raise ValueError("credential projection promotion artifact exceeds bounds")
    value = json.loads(payload.decode("utf-8", errors="strict"), object_pairs_hook=_unique)
    if not isinstance(value, dict):
        raise ValueError("credential projection promotion artifact must be an object")
    return value


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("credential projection promotion artifact contains duplicate keys")
        result[key] = value
    return result
