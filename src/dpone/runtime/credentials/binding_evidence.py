"""Build bounded, secret-free evidence for runtime credential resolution."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint

_UTC = timezone(timedelta(0))
_SHA256_PREFIX = "sha256:"
_DIGEST_CONTEXT_KEYS = frozenset(
    {
        "release_id",
        "deployment_id",
        "pack_fingerprint",
        "binding_set_fingerprint",
        "connection_registry_fingerprint",
        "credential_runtime_fingerprint",
        "runtime_image_digest",
        "deployment_fingerprint",
    }
)
_TEXT_CONTEXT_KEYS = frozenset(
    {
        "credential_runtime_environment",
        "credential_runtime_auth_method",
        "airflow_bundle_ref",
    }
)
_SAFE_CONTEXT_KEYS = _DIGEST_CONTEXT_KEYS | _TEXT_CONTEXT_KEYS
_SECRET_ASSIGNMENT_MARKERS = frozenset(("password=", "token=", "secret=", "api_key=", "client_secret="))


def safe_evidence_context(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    safe: dict[str, Any] = {}
    for key, item in value.items():
        if key not in _SAFE_CONTEXT_KEYS or item in (None, ""):
            continue
        if key in _DIGEST_CONTEXT_KEYS:
            if not _is_sha256_digest(item):
                raise ValueError(f"evidence_context {key} must be a sha256 digest")
        elif not _is_safe_text(item):
            raise ValueError(f"evidence_context {key} must be a safe metadata value")
        safe[key] = item
    return safe


def payload_format_metadata(credentials: Mapping[str, Any]) -> dict[str, str]:
    payload_format = _string(credentials.get("payload_format"))
    return {"payload_format": payload_format} if payload_format else {}


def credential_reference_metadata(resolver: str, credentials: Mapping[str, Any]) -> dict[str, str]:
    payload = _credential_reference_payload(resolver, credentials)
    if not payload:
        return {}
    return {"credential_ref_fingerprint": canonical_fingerprint(payload)}


def credential_policy_metadata(credentials: Mapping[str, Any]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for key in ("version_policy", "resolution_scope"):
        value = _string(credentials.get(key))
        if value:
            metadata[key] = value
    return metadata


def resolved_at_metadata(resolved_version: int | None, clock: Callable[[], datetime]) -> dict[str, str]:
    if resolved_version is None:
        return {}
    value = clock()
    if value.tzinfo is None:
        value = value.replace(tzinfo=_UTC)
    return {"resolved_at": value.astimezone(_UTC).isoformat().replace("+00:00", "Z")}


def _credential_reference_payload(resolver: str, credentials: Mapping[str, Any]) -> dict[str, Any]:
    if resolver == "kubernetes_secret_volume":
        payload = _safe_subset(credentials, "resolver", "secret_name", "mount_path", "fields", "payload_format")
        return payload if payload.get("secret_name") and payload.get("mount_path") else {}
    if resolver == "kubernetes_secret_api":
        payload = _safe_subset(credentials, "resolver", "namespace", "name", "fields")
        return payload if payload.get("namespace") and payload.get("name") else {}
    return {}


def _safe_subset(source: Mapping[str, Any], *keys: str) -> dict[str, Any]:
    return {key: source[key] for key in keys if source.get(key) not in (None, "")}


def _is_sha256_digest(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    digest = value[len(_SHA256_PREFIX) :]
    return (
        value.startswith(_SHA256_PREFIX)
        and len(digest) == 64
        and all(char in "0123456789abcdefABCDEF" for char in digest)
    )


def _is_safe_text(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        return False
    lowered = value.lower()
    return (
        "\n" not in value and "\r" not in value and not any(marker in lowered for marker in _SECRET_ASSIGNMENT_MARKERS)
    )


def _string(value: Any) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "credential_policy_metadata",
    "credential_reference_metadata",
    "payload_format_metadata",
    "resolved_at_metadata",
    "safe_evidence_context",
]
