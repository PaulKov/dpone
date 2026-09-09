"""Contracts for dpone Airflow release/deployment identities.

The helpers in this module are deliberately dependency-free and side-effect
free. They are used by build-plane code, tests, docs, and future CLI surfaces
to compute stable content fingerprints without importing Airflow, Kubernetes,
Vault, or connector SDKs.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

_RELEASE_EXCLUDED_FIELDS = frozenset(
    {
        "release_id",
        "created_at",
        "build_id",
        "source_commit",
        "label",
        "attestation",
        "attestations",
        "signature",
        "signatures",
    }
)
_DEPLOYMENT_EXCLUDED_FIELDS = frozenset(
    {
        "deployment_id",
        "created_at",
        "label",
        "attestation",
        "attestations",
        "signature",
        "signatures",
    }
)
_PROVENANCE_KEYS = frozenset({"provenance"})
_SHA256_PREFIX = "sha256:"


def release_id(payload: Mapping[str, Any]) -> str:
    """Return the content-addressed release identity.

    Provenance such as source commit and build timestamps is intentionally
    excluded so identical artifacts can be promoted by digest across
    environments.
    """

    return canonical_fingerprint(
        _canonical_release_locators(payload),
        exclude_top_level=_RELEASE_EXCLUDED_FIELDS | _PROVENANCE_KEYS,
    )


def _canonical_release_locators(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    """Normalize the legacy release artifact locator alias before hashing."""

    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return payload
    normalized_artifacts: dict[str, Any] = {}
    for section, raw_items in artifacts.items():
        if not isinstance(raw_items, list):
            normalized_artifacts[str(section)] = raw_items
            continue
        items: list[Any] = []
        for raw_item in raw_items:
            if isinstance(raw_item, Mapping) and "path" not in raw_item and "artifact_ref" in raw_item:
                item = dict(raw_item)
                item["path"] = item.pop("artifact_ref")
                items.append(item)
            else:
                items.append(raw_item)
        normalized_artifacts[str(section)] = items
    normalized = dict(payload)
    normalized["artifacts"] = normalized_artifacts
    return normalized


def deployment_id(payload: Mapping[str, Any]) -> str:
    """Return the environment-specific deployment identity."""

    return canonical_fingerprint(payload, exclude_top_level=_DEPLOYMENT_EXCLUDED_FIELDS)


def requires_release_set_v2_for_runtime_payloads(
    *, release_schema: object, has_runtime_payloads: bool, trust_tier: object
) -> bool:
    """Identify the v1 compatibility bridge that cannot authorize production dbt."""

    return trust_tier == "production" and release_schema == "dpone.release-set.v1" and has_runtime_payloads


def is_sha256_digest(value: object) -> bool:
    """Return whether ``value`` is a well-formed SHA-256 digest string."""

    if not isinstance(value, str) or not value.startswith(_SHA256_PREFIX):
        return False
    digest = value[len(_SHA256_PREFIX) :]
    return len(digest) == 64 and all(char in "0123456789abcdefABCDEF" for char in digest)


def is_canonical_sha256_digest(value: object) -> bool:
    """Return whether an identity uses canonical lowercase SHA-256 text."""

    return is_sha256_digest(value) and value == str(value).lower()


@dataclass(frozen=True, slots=True)
class CurrentPointerViolation:
    code: str
    message: str


def current_pointer_violation(
    pointer: Mapping[str, Any],
    *,
    expected_environment: str | None = None,
) -> CurrentPointerViolation | None:
    """Validate the pure authorization and identity contract for ``current``."""

    environment = pointer.get("environment")
    if pointer.get("schema") != "dpone.current-pointer.v1":
        return _pointer_violation("current pointer schema is invalid")
    if not isinstance(environment, str) or not environment:
        return _pointer_violation("current pointer environment is required")
    if expected_environment is not None and environment != expected_environment:
        return CurrentPointerViolation(
            "DPONE_CURRENT_POINTER_ENVIRONMENT_MISMATCH",
            "current pointer does not match requested environment",
        )
    for field in ("deployment_id", "release_id"):
        if not is_canonical_sha256_digest(pointer.get(field)):
            return _pointer_violation(f"current pointer {field} must be a canonical lowercase sha256 digest")
    if not isinstance(pointer.get("promoted_by"), str) or not pointer["promoted_by"]:
        return _pointer_violation("current pointer promoted_by is required")
    activation_id = pointer.get("activation_id")
    if activation_id is not None and (
        not isinstance(activation_id, str)
        or re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            activation_id,
        )
        is None
    ):
        return _pointer_violation("current pointer activation_id is invalid")
    if not _is_aware_datetime(pointer.get("promoted_at")):
        return _pointer_violation("current pointer promoted_at must be an offset-aware date-time")
    previous = pointer.get("previous_deployment_id")
    if previous is not None and not is_canonical_sha256_digest(previous):
        return _pointer_violation("current pointer previous_deployment_id is invalid")
    workspace_ref = pointer.get("workspace_authority_connection_ref")
    if workspace_ref is not None and (
        not isinstance(workspace_ref, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,127}", workspace_ref) is None
    ):
        return _pointer_violation("current pointer workspace authority connection_ref is invalid")
    return None


def _pointer_violation(message: str) -> CurrentPointerViolation:
    return CurrentPointerViolation("DPONE_CURRENT_POINTER_INVALID", message)


def _is_aware_datetime(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def canonical_fingerprint(
    payload: Mapping[str, Any],
    *,
    exclude_fields: frozenset[str] = frozenset(),
    exclude_top_level: frozenset[str] = frozenset(),
) -> str:
    """Compute a deterministic SHA-256 fingerprint for a JSON-like payload.

    This is a small project-local canonicalization routine inspired by RFC 8785:
    mappings are sorted by key, whitespace is removed, and unordered artifact
    lists are sorted by logical ``id``. The implementation intentionally avoids
    optional dependencies and handles the contract shapes dpone owns.
    """

    normalized = _normalize(
        payload, exclude_fields=exclude_fields, is_top_level=True, exclude_top_level=exclude_top_level
    )
    raw = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return _SHA256_PREFIX + hashlib.sha256(raw).hexdigest()


def _normalize(
    value: Any,
    *,
    exclude_fields: frozenset[str],
    is_top_level: bool = False,
    exclude_top_level: frozenset[str] = frozenset(),
) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key)
            if normalized_key in exclude_fields:
                continue
            if is_top_level and normalized_key in exclude_top_level:
                continue
            result[normalized_key] = _normalize(item, exclude_fields=exclude_fields)
        return result
    if isinstance(value, list):
        normalized_items = [_normalize(item, exclude_fields=exclude_fields) for item in value]
        if all(isinstance(item, dict) and isinstance(item.get("id"), str) for item in normalized_items):
            return sorted(normalized_items, key=lambda item: str(item["id"]))
        return normalized_items
    if isinstance(value, tuple):
        return [_normalize(item, exclude_fields=exclude_fields) for item in value]
    if isinstance(value, str):
        return value.replace("\\", "/")
    return value


__all__ = [
    "CurrentPointerViolation",
    "canonical_fingerprint",
    "current_pointer_violation",
    "deployment_id",
    "is_canonical_sha256_digest",
    "is_sha256_digest",
    "release_id",
    "requires_release_set_v2_for_runtime_payloads",
]
