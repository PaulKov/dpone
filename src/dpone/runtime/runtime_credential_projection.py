"""Secret-free credential projection verification at runtime trust boundaries."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone_airflow_pack.credential_projection_contract import (
    CredentialProjection,
    CredentialProjectionError,
    parse_credential_projection,
    require_registry_parity,
)

if TYPE_CHECKING:
    from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan


def verify_deployment_credential_projection(
    *,
    deployment: Mapping[str, Any],
    projection_payload: bytes,
    binding_set_payload: bytes,
    registry_payload: bytes,
) -> CredentialProjection:
    """Verify exact descriptors and path/binding parity; performs no secret I/O.

    Root publication/activation callers additionally call ``require_authority``
    with the protected control ref and publication-authority SHA-256. Base callers
    use the pointer-projected ref. The immutable deployment must already have
    passed its identity/trust verification before this helper is called.
    """
    try:
        binding, registry = deployment["binding_set"], deployment["connection_registry"]
        for descriptor, payload in ((binding, binding_set_payload), (registry, registry_payload)):
            if descriptor["sha256"] != _sha(payload) or descriptor["bytes"] != len(payload):
                raise CredentialProjectionError("MISMATCH")
        projection = parse_credential_projection(
            projection_payload,
            descriptor=deployment["credential_projection"],
            environment=deployment["environment"],
            release_id=deployment["release_ref"],
            binding_set_sha256=binding["sha256"],
            runtime_registry_sha256=registry["sha256"],
        )
        require_registry_parity(
            projection, binding_set=json.loads(binding_set_payload), registry=json.loads(registry_payload)
        )
        return projection
    except (KeyError, TypeError, json.JSONDecodeError, UnicodeError):
        raise CredentialProjectionError("MISMATCH") from None


def verify_runtime_credential_projection(
    plan: RuntimeInitFetchPlan,
    payloads: Mapping[str, bytes],
) -> CredentialProjection | None:
    """Check the v6 descriptor transitively through a verified deployment/plan.

    This is used during init-fetch and repeated by base before connector access.
    Ready manifests need no new field: they bind the exact plan and deployment.
    """
    descriptor = plan.credential_projection
    if descriptor is None:
        return None
    try:
        deployment = json.loads(payloads[plan.deployment.artifact_ref])
        if deployment.get("credential_projection") != descriptor.to_dict():
            raise CredentialProjectionError("MISMATCH")
        projection = verify_deployment_credential_projection(
            deployment=deployment,
            projection_payload=payloads[descriptor.artifact_ref],
            binding_set_payload=payloads[plan.binding_set.artifact_ref],
            registry_payload=payloads[plan.connection_registry.artifact_ref],
        )
        projection.membership(plan.workload_pack.id)
        return projection
    except (KeyError, TypeError, json.JSONDecodeError, UnicodeError):
        raise CredentialProjectionError("MISMATCH") from None


def _sha(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()
