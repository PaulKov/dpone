"""Pure build-plane contract for runnable Airflow init-fetch projections."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_deployment import deployment_id
from dpone.contracts.runtime_artifact_delivery import (
    attestations_for_trust_tier,
    is_safe_artifact_registry_logical_ref,
    normalize_config_map_ref,
    normalize_trust_tier,
    validate_runtime_image_reference,
)
from dpone.kubernetes_names import is_valid_kubernetes_dns_label


class InitFetchProjectionContractError(ValueError):
    """Stable build-plane validation failure without sensitive values."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def build_init_fetch_delivery(
    *,
    trust_tier: object,
    binding_set: Mapping[str, Any],
    runtime_image_ref: object,
    runtime_image_digest: object,
    artifact_registry_ref: object,
    registry_config_ref: object,
    trust_policy_ref: object | None,
) -> tuple[str, dict[str, Any]]:
    """Validate and return the exact image plus secret-free delivery block."""

    try:
        normalized_trust_tier = normalize_trust_tier(trust_tier)
    except ValueError as exc:
        raise InitFetchProjectionContractError(
            "DPONE_DEPLOYMENT_TRUST_TIER_INVALID",
            str(exc),
        ) from exc
    try:
        image_ref = validate_runtime_image_reference(runtime_image_ref, runtime_image_digest)
    except ValueError as exc:
        raise InitFetchProjectionContractError(
            "DPONE_DEPLOYMENT_RUNTIME_IMAGE_INVALID",
            str(exc),
        ) from exc
    if not is_safe_artifact_registry_logical_ref(artifact_registry_ref):
        raise InitFetchProjectionContractError(
            "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_INVALID",
            "artifact_registry_ref must be a bounded logical name",
        )
    if normalized_trust_tier == "production" and trust_policy_ref is None:
        raise InitFetchProjectionContractError(
            "DPONE_DEPLOYMENT_TRUST_POLICY_REQUIRED",
            "production trust_tier requires trust_policy_ref",
        )
    try:
        registry_ref = normalize_config_map_ref(
            registry_config_ref,
            field="registry_config_ref",
        )
        trust_ref = (
            normalize_config_map_ref(trust_policy_ref, field="trust_policy_ref")
            if trust_policy_ref is not None
            else None
        )
    except ValueError as exc:
        raise InitFetchProjectionContractError(
            "DPONE_DEPLOYMENT_CONFIG_REF_INVALID",
            str(exc),
        ) from exc
    delivery: dict[str, Any] = {
        "mode": "init_fetch",
        "trust_tier": normalized_trust_tier,
        "artifact_registry_ref": str(artifact_registry_ref),
        "identity": _workload_identity(binding_set),
        "registry_config_ref": registry_ref,
        "source": {"artifact_registry_ref": str(artifact_registry_ref)},
        "verify": {
            "checksums": "required",
            "attestations": attestations_for_trust_tier(normalized_trust_tier),
        },
    }
    if trust_ref is not None:
        delivery["trust_policy_ref"] = trust_ref
    return image_ref, delivery


def _build_local_safe_sample_delivery(
    *,
    binding_set: Mapping[str, Any],
    artifact_registry_ref: str,
) -> dict[str, Any]:
    """Build the frozen v1 local lane without inventing strict KPO resources."""

    raw_runtime = binding_set.get("runtime")
    runtime = raw_runtime if isinstance(raw_runtime, Mapping) else {}
    service_account = str(runtime.get("service_account") or "dpone-runtime")
    return {
        "mode": "init_fetch",
        "artifact_registry_ref": artifact_registry_ref,
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": service_account,
        },
        "source": {"artifact_registry_ref": artifact_registry_ref},
        "verify": {
            "checksums": "required",
            "attestations": "optional",
        },
    }


def build_local_safe_sample_v1_projection(
    *,
    release_id: str,
    environment: str,
    runtime_image_digest: str,
    artifact_registry_ref: str,
    airflow_bundle_ref: str | None,
    binding_set: Mapping[str, Any],
    binding_fingerprint: str,
    registry_fingerprint: str,
    credential_runtime_fingerprint: str,
    dag_specs: list[dict[str, Any]],
    workload_packs: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build only the frozen local v1 wire; never synthesize strict resources."""

    delivery = _build_local_safe_sample_delivery(
        binding_set=binding_set,
        artifact_registry_ref=artifact_registry_ref,
    )
    deployment = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "",
        "deployment_type": "environment",
        "runnable": bool(workload_packs),
        "environment": environment,
        "release_ref": release_id,
        "binding_set_ref": binding_fingerprint,
        "connection_registry_ref": registry_fingerprint,
        "credential_runtime_ref": credential_runtime_fingerprint,
        "runtime_image_digest": runtime_image_digest,
        "airflow_bundle_ref": airflow_bundle_ref,
        "runtime_artifact_delivery": delivery,
    }
    computed_deployment_id = deployment_id(deployment)
    deployment["deployment_id"] = computed_deployment_id
    index = {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": release_id,
        "deployment_id": computed_deployment_id,
        "environment": environment,
        "dag_specs": dag_specs,
        "workload_packs": workload_packs,
        "binding_set_ref": binding_fingerprint,
        "connection_registry_ref": registry_fingerprint,
        "credential_runtime_ref": credential_runtime_fingerprint,
        "runtime_image_digest": runtime_image_digest,
        "airflow_bundle_ref": airflow_bundle_ref,
        "runtime_artifact_delivery": delivery,
    }
    return deployment, index


def _workload_identity(binding_set: Mapping[str, Any]) -> dict[str, str]:
    raw_runtime = binding_set.get("runtime")
    runtime = raw_runtime if isinstance(raw_runtime, Mapping) else {}
    service_account = runtime.get("service_account")
    namespace = runtime.get("kubernetes_namespace")
    if not is_valid_kubernetes_dns_label(service_account):
        raise InitFetchProjectionContractError(
            "DPONE_DEPLOYMENT_IDENTITY_INVALID",
            "runtime service_account must be a Kubernetes DNS label",
        )
    if not is_valid_kubernetes_dns_label(namespace):
        raise InitFetchProjectionContractError(
            "DPONE_DEPLOYMENT_IDENTITY_INVALID",
            "runtime kubernetes_namespace must be a Kubernetes DNS label",
        )
    return {
        "method": "kubernetes_workload_identity",
        "service_account": str(service_account),
        "namespace": str(namespace),
    }


__all__ = [
    "build_init_fetch_delivery",
    "build_local_safe_sample_v1_projection",
    "InitFetchProjectionContractError",
]
