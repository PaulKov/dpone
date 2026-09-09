from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.kubernetes_names import is_valid_kubernetes_dns_label

ARTIFACT_REGISTRY_REF_PATTERN = (
    r"^(?!.*%)"
    r"(?!(?:[Cc][Uu][Rr][Rr][Ee][Nn][Tt]|[Ll][Aa][Tt][Ee][Ss][Tt])(?:$|[/:@?#=&]))"
    r"(?!.*[/:@?#=&](?:[Cc][Uu][Rr][Rr][Ee][Nn][Tt]|[Ll][Aa][Tt][Ee][Ss][Tt])"
    r"(?:$|[/:@?#=&]))\S+$"
)
ARTIFACT_REGISTRY_LOGICAL_REF_PATTERN = (
    r"^(?=.{1,128}$)"
    r"(?!(?:[Cc][Uu][Rr][Rr][Ee][Nn][Tt]|[Ll][Aa][Tt][Ee][Ss][Tt])$)"
    r"(?!(?:sk_(?:live|test|prod)_[A-Za-z0-9_-]{8,}"
    r"|sk-(?:proj|svcacct)-[A-Za-z0-9_-]{8,}"
    r"|gh[pousr]_[A-Za-z0-9]{20,}"
    r"|github_pat_[A-Za-z0-9_]{20,}"
    r"|xox[baprs]-[A-Za-z0-9-]{16,})$)"
    r"[A-Za-z0-9][A-Za-z0-9_.-]*$"
)
CONFIG_MAP_KEY_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$"
# Optional decimal registry ports are bounded to the transport range 1..65535.
OCI_RUNTIME_IMAGE_REF_PATTERN = (
    r"^(?!.*(?:\s|://|\?|#|=))"
    r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?"
    r"(?::(?!0+/)(?:[0-9]{1,4}|[0-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5]))?"
    r"/[a-z0-9]+(?:[._/-][a-z0-9]+)*"
    r"@sha256:[0-9a-f]{64}$"
)
INIT_FETCH_REQUIRED_FIELDS = (
    "artifact_registry_ref",
    "identity",
    "source",
    "verify",
)
INIT_FETCH_REQUIRED_NESTED_FIELDS = {
    "identity": ("method", "service_account"),
    "source": ("artifact_registry_ref",),
    "verify": ("checksums", "attestations"),
}
STRICT_INIT_FETCH_REQUIRED_FIELDS = (
    "trust_tier",
    "artifact_registry_ref",
    "identity",
    "registry_config_ref",
    "source",
    "verify",
)
TRUST_TIERS = ("production", "non_production")
_MUTABLE_REF_ALIASES = frozenset({"current", "latest"})
_REF_SEGMENT_SEPARATOR = re.compile(r"[/:@?#=&]+")
_LOGICAL_REF_RE = re.compile(ARTIFACT_REGISTRY_LOGICAL_REF_PATTERN)
_CONFIG_MAP_KEY_RE = re.compile(CONFIG_MAP_KEY_PATTERN)
_OCI_RUNTIME_IMAGE_REF_RE = re.compile(OCI_RUNTIME_IMAGE_REF_PATTERN)


def missing_init_fetch_delivery_fields(delivery: Mapping[str, Any]) -> tuple[str, ...]:
    """Return missing top-level fields for a declared init_fetch delivery block."""

    if delivery.get("mode") != "init_fetch":
        return ()
    return tuple(field for field in INIT_FETCH_REQUIRED_FIELDS if field not in delivery)


def missing_init_fetch_delivery_paths(
    delivery: Mapping[str, Any],
    *,
    parent_path: str = "runtime_artifact_delivery",
) -> tuple[str, ...]:
    """Return user-facing missing paths for a declared init_fetch delivery block."""

    if delivery.get("mode") != "init_fetch":
        return ()
    top_level_paths = [f"{parent_path}.{field}" for field in missing_init_fetch_delivery_fields(delivery)]
    nested_paths = [
        f"{parent_path}.{field}.{nested_field}"
        for field, nested_fields in INIT_FETCH_REQUIRED_NESTED_FIELDS.items()
        for nested_field in nested_fields
        if _is_missing_nested_field(delivery, field, nested_field)
    ]
    return tuple((*top_level_paths, *nested_paths))


def is_pinned_artifact_registry_ref(value: object) -> bool:
    """Return whether a registry reference cannot resolve a mutable cache pointer."""

    if not isinstance(value, str) or not value or "%" in value or any(character.isspace() for character in value):
        return False
    return all(segment.casefold() not in _MUTABLE_REF_ALIASES for segment in _REF_SEGMENT_SEPARATOR.split(value))


def is_safe_artifact_registry_logical_ref(value: object) -> bool:
    """Return whether an indexed deployment uses a bounded non-secret logical ref."""

    return isinstance(value, str) and bool(_LOGICAL_REF_RE.fullmatch(value))


def normalize_trust_tier(value: object) -> str:
    """Return one explicit trust tier without inferring it from environment names."""

    if value not in TRUST_TIERS:
        raise ValueError("trust_tier must be production or non_production")
    return str(value)


def attestations_for_trust_tier(trust_tier: object) -> str:
    """Return the strict attestation request derived only from the trust tier."""

    normalized = normalize_trust_tier(trust_tier)
    return "required_for_prod" if normalized == "production" else "optional"


def validate_runtime_image_reference(runtime_image_ref: object, runtime_image_digest: object) -> str:
    """Validate one exact OCI image reference and its mirrored canonical digest."""

    if not isinstance(runtime_image_ref, str) or not _OCI_RUNTIME_IMAGE_REF_RE.fullmatch(runtime_image_ref):
        raise ValueError("runtime image reference must be an exact OCI ref with a sha256 digest")
    if not is_canonical_sha256_digest(runtime_image_digest):
        raise ValueError("runtime image digest must be a canonical sha256 digest")
    if runtime_image_ref.rpartition("@")[2] != runtime_image_digest:
        raise ValueError("runtime image digest does not match runtime image reference")
    return runtime_image_ref


def normalize_config_map_ref(value: object, *, field: str) -> dict[str, str]:
    """Return one closed, digest-pinned Kubernetes ConfigMap reference."""

    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    expected_keys = {"kind", "name", "key", "sha256"}
    if set(value) != expected_keys:
        raise ValueError(f"{field} must contain exactly kind, name, key and sha256")
    kind = value.get("kind")
    name = value.get("name")
    key = value.get("key")
    digest = value.get("sha256")
    if kind != "kubernetes_config_map":
        raise ValueError(f"{field}.kind must be kubernetes_config_map")
    if not is_valid_kubernetes_dns_label(name):
        raise ValueError(f"{field}.name must be a Kubernetes DNS label")
    if not isinstance(key, str) or not _CONFIG_MAP_KEY_RE.fullmatch(key):
        raise ValueError(f"{field}.key must be a safe ConfigMap key")
    if not is_canonical_sha256_digest(digest):
        raise ValueError(f"{field}.sha256 must be a canonical sha256 digest")
    return {
        "kind": "kubernetes_config_map",
        "name": str(name),
        "key": key,
        "sha256": str(digest),
    }


def _is_missing_nested_field(delivery: Mapping[str, Any], field: str, nested_field: str) -> bool:
    nested = delivery.get(field)
    if not isinstance(nested, Mapping):
        return False
    return nested_field not in nested


__all__ = [
    "ARTIFACT_REGISTRY_REF_PATTERN",
    "ARTIFACT_REGISTRY_LOGICAL_REF_PATTERN",
    "CONFIG_MAP_KEY_PATTERN",
    "INIT_FETCH_REQUIRED_FIELDS",
    "INIT_FETCH_REQUIRED_NESTED_FIELDS",
    "STRICT_INIT_FETCH_REQUIRED_FIELDS",
    "TRUST_TIERS",
    "attestations_for_trust_tier",
    "is_safe_artifact_registry_logical_ref",
    "is_pinned_artifact_registry_ref",
    "missing_init_fetch_delivery_fields",
    "missing_init_fetch_delivery_paths",
    "normalize_config_map_ref",
    "normalize_trust_tier",
    "OCI_RUNTIME_IMAGE_REF_PATTERN",
    "validate_runtime_image_reference",
]
