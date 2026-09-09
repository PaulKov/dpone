"""Strict parsing of init-fetch delivery and trust configuration."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone_airflow_pack.init_fetch_contract import (
    ConfigMapReference,
    DevEvidenceDelivery,
    InitFetchProviderError,
    VerificationPolicy,
    WorkloadIdentity,
)
from dpone_airflow_pack.init_fetch_validation import (
    CONFIG_KEY_RE,
    digest,
    dns_label,
    exact_mapping,
    field_invalid,
    registry_ref,
    required_text,
)

_DELIVERY_KEYS = frozenset(
    {
        "mode",
        "trust_tier",
        "artifact_registry_ref",
        "identity",
        "registry_config_ref",
        "trust_policy_ref",
        "source",
        "verify",
    }
)
_DEV_EVIDENCE_KEYS = frozenset({"mode", "claim_name", "mount_path", "worker_queue"})
_WORKER_QUEUE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_DEV_EVIDENCE_MOUNT_PATH = "/var/lib/dpone/dev-evidence"


@dataclass(frozen=True, slots=True)
class ParsedInitFetchDelivery:
    """Validated delivery policy used to assemble an init-fetch context."""

    trust_tier: str
    artifact_registry_ref: str
    registry_configuration: ConfigMapReference
    trust_policy: ConfigMapReference | None
    identity: WorkloadIdentity
    verify: VerificationPolicy
    dev_evidence_delivery: DevEvidenceDelivery | None


def parse_init_fetch_delivery(
    payload: Mapping[str, Any],
    *,
    path: Path | None,
) -> ParsedInitFetchDelivery:
    """Validate deployment-owned artifact delivery and trust policy."""

    delivery = exact_mapping(
        payload.get("runtime_artifact_delivery"),
        "runtime_artifact_delivery",
        _DELIVERY_KEYS,
        optional=frozenset({"trust_policy_ref"}),
        path=path,
    )
    if delivery["mode"] != "init_fetch":
        raise InitFetchProviderError(
            "DPONE_RUNTIME_ARTIFACT_DELIVERY_MODE_UNSUPPORTED",
            "wire v2 supports only strict init_fetch execution",
            path=_path_text(path),
        )
    trust_tier = _trust_tier(payload, delivery, path)
    logical_registry_ref = registry_ref(delivery.get("artifact_registry_ref"), path)
    _validate_source(delivery, logical_registry_ref, path)
    trust_policy = _optional_trust_policy(delivery, path)
    if trust_tier == "production" and trust_policy is None:
        raise field_invalid("production trust_tier requires trust_policy_ref", path)
    return ParsedInitFetchDelivery(
        trust_tier=trust_tier,
        artifact_registry_ref=logical_registry_ref,
        registry_configuration=_config_map_ref(
            delivery.get("registry_config_ref"),
            "runtime_artifact_delivery.registry_config_ref",
            path,
        ),
        trust_policy=trust_policy,
        identity=_identity(delivery.get("identity"), path),
        verify=_verify(delivery.get("verify"), trust_tier=trust_tier, path=path),
        dev_evidence_delivery=_dev_evidence_delivery(
            payload.get("dev_evidence_delivery"),
            trust_tier=trust_tier,
            path=path,
        ),
    )


def _dev_evidence_delivery(
    value: object,
    *,
    trust_tier: str,
    path: Path | None,
) -> DevEvidenceDelivery | None:
    if value is None:
        return None
    if trust_tier != "non_production":
        raise field_invalid("dev_evidence_delivery is allowed only for non_production deployments", path)
    item = exact_mapping(value, "dev_evidence_delivery", _DEV_EVIDENCE_KEYS, path=path)
    if item["mode"] != "shared_pvc" or item["mount_path"] != _DEV_EVIDENCE_MOUNT_PATH:
        raise field_invalid("dev_evidence_delivery mode or mount_path is invalid", path)
    worker_queue = item["worker_queue"]
    if not isinstance(worker_queue, str) or _WORKER_QUEUE_RE.fullmatch(worker_queue) is None:
        raise field_invalid("dev_evidence_delivery.worker_queue is invalid", path)
    return DevEvidenceDelivery(
        mode="shared_pvc",
        claim_name=dns_label(item["claim_name"], "dev_evidence_delivery.claim_name", path),
        mount_path=_DEV_EVIDENCE_MOUNT_PATH,
        worker_queue=worker_queue,
    )


def _trust_tier(
    payload: Mapping[str, Any],
    delivery: Mapping[str, Any],
    path: Path | None,
) -> str:
    value = required_text(delivery, "trust_tier", path)
    if value not in {"production", "non_production"}:
        raise field_invalid("runtime_artifact_delivery.trust_tier is invalid", path)
    if required_text(payload, "trust_tier", path) != value:
        raise field_invalid("trust_tier must match runtime_artifact_delivery.trust_tier", path)
    return value


def _validate_source(
    delivery: Mapping[str, Any],
    logical_registry_ref: str,
    path: Path | None,
) -> None:
    source = exact_mapping(
        delivery.get("source"),
        "runtime_artifact_delivery.source",
        frozenset({"artifact_registry_ref"}),
        path=path,
    )
    if source["artifact_registry_ref"] != logical_registry_ref:
        raise field_invalid("runtime_artifact_delivery.source does not match registry reference", path)


def _identity(value: object, path: Path | None) -> WorkloadIdentity:
    item = exact_mapping(
        value,
        "runtime_artifact_delivery.identity",
        frozenset({"method", "service_account", "namespace"}),
        path=path,
    )
    if item["method"] != "kubernetes_workload_identity":
        raise field_invalid(
            "runtime_artifact_delivery.identity.method must be kubernetes_workload_identity",
            path,
        )
    return WorkloadIdentity(
        method="kubernetes_workload_identity",
        service_account=dns_label(item["service_account"], "identity.service_account", path),
        namespace=dns_label(item["namespace"], "identity.namespace", path),
    )


def _verify(
    value: object,
    *,
    trust_tier: str,
    path: Path | None,
) -> VerificationPolicy:
    item = exact_mapping(
        value,
        "runtime_artifact_delivery.verify",
        frozenset({"checksums", "attestations"}),
        path=path,
    )
    if item["checksums"] != "required":
        raise field_invalid("runtime_artifact_delivery.verify.checksums must be required", path)
    attestations = item["attestations"]
    if attestations not in {"optional", "required_for_prod"}:
        raise field_invalid("runtime_artifact_delivery.verify.attestations is invalid", path)
    required = "required_for_prod" if trust_tier == "production" else "optional"
    if attestations != required:
        raise field_invalid(f"{trust_tier} trust_tier requires attestations={required}", path)
    return VerificationPolicy(checksums="required", attestations=str(attestations))


def _optional_trust_policy(
    delivery: Mapping[str, Any],
    path: Path | None,
) -> ConfigMapReference | None:
    if "trust_policy_ref" not in delivery:
        return None
    value = delivery["trust_policy_ref"]
    if value is None:
        raise field_invalid(
            "runtime_artifact_delivery.trust_policy_ref must be a ConfigMap reference",
            path,
        )
    reference = _config_map_ref(
        value,
        "runtime_artifact_delivery.trust_policy_ref",
        path,
    )
    if reference.key != "policy.json":
        raise field_invalid(
            "runtime_artifact_delivery.trust_policy_ref.key must be policy.json",
            path,
        )
    return reference


def _config_map_ref(
    value: object,
    field: str,
    path: Path | None,
) -> ConfigMapReference:
    item = exact_mapping(
        value,
        field,
        frozenset({"kind", "name", "key", "sha256"}),
        path=path,
    )
    if item["kind"] != "kubernetes_config_map":
        raise field_invalid(f"{field}.kind must be kubernetes_config_map", path)
    key = item["key"]
    if not isinstance(key, str) or not CONFIG_KEY_RE.fullmatch(key):
        raise field_invalid(f"{field}.key is invalid", path)
    return ConfigMapReference(
        kind="kubernetes_config_map",
        name=dns_label(item["name"], f"{field}.name", path),
        key=key,
        sha256=digest(item["sha256"], f"{field}.sha256", path),
    )


def _path_text(path: Path | None) -> str | None:
    return path.as_posix() if path is not None else None


__all__ = ["ParsedInitFetchDelivery", "parse_init_fetch_delivery"]
