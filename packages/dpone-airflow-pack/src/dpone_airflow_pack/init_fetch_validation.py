"""Closed primitive validators for the provider's strict v2 wire."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError

SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
CONFIG_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$")
ENVIRONMENT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")

_DNS_LABEL_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_EXECUTION_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")
_REGISTRY_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_CACHE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
# Keep this dependency-light copy behaviorally aligned with
# dpone.contracts.runtime_artifact_delivery. Registry ports are 1..65535.
_OCI_IMAGE_RE = re.compile(
    r"^(?!.*(?:\s|://|\?|#|=))"
    r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?"
    r"(?::(?!0+/)(?:[0-9]{1,4}|[0-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5]))?"
    r"/[a-z0-9]+(?:[._/-][a-z0-9]+)*"
    r"@sha256:[0-9a-f]{64}$"
)
_SECRET_SHAPES = (
    re.compile(r"^sk_(?:live|test|prod)_[A-Za-z0-9_-]{8,}$"),
    re.compile(r"^sk-(?:proj|svcacct)-[A-Za-z0-9_-]{8,}$"),
    re.compile(r"^gh[pousr]_[A-Za-z0-9]{20,}$"),
    re.compile(r"^github_pat_[A-Za-z0-9_]{20,}$"),
    re.compile(r"^xox[baprs]-[A-Za-z0-9-]{16,}$"),
)


def exact_mapping(
    value: object,
    field: str,
    keys: frozenset[str],
    *,
    optional: frozenset[str] = frozenset(),
    path: Path | None,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise field_invalid(f"{field} must be an object", path)
    actual = frozenset(str(key) for key in value)
    missing = sorted(keys - optional - actual)
    if missing:
        raise field_invalid(f"{field}.{missing[0]} is required", path)
    unknown = sorted(actual - keys)
    if unknown:
        raise field_invalid(
            f"{field} contains unknown fields: {', '.join(unknown)}",
            path,
        )
    return value


def required_text(payload: Mapping[str, Any], key: str, path: Path | None) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise field_invalid(f"{key} is required", path)
    return value


def digest(value: object, field: str, path: Path | None) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise field_invalid(f"{field} must be a canonical sha256 digest", path)
    return value


def positive_integer(value: object, field: str, path: Path | None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise field_invalid(f"{field} must be a positive integer", path)
    return value


def execution_token(value: object, field: str, path: Path | None) -> str:
    if (
        not isinstance(value, str)
        or not _EXECUTION_TOKEN_RE.fullmatch(value)
        or any(pattern.fullmatch(value) for pattern in _SECRET_SHAPES)
    ):
        raise field_invalid(f"{field} must be a bounded logical token", path)
    return value


def dns_label(value: object, field: str, path: Path | None) -> str:
    if not isinstance(value, str) or not _DNS_LABEL_RE.fullmatch(value):
        raise field_invalid(f"{field} must be a Kubernetes DNS label", path)
    return value


def registry_ref(value: object, path: Path | None) -> str:
    if (
        not isinstance(value, str)
        or not _REGISTRY_REF_RE.fullmatch(value)
        or value.casefold() in {"current", "latest"}
        or any(pattern.fullmatch(value) for pattern in _SECRET_SHAPES)
    ):
        raise field_invalid(
            "artifact_registry_ref must be a bounded non-secret logical reference",
            path,
        )
    return value


def runtime_image(value: object, image_digest: str, path: Path | None) -> str:
    if not isinstance(value, str) or not _OCI_IMAGE_RE.fullmatch(value):
        raise field_invalid(
            "runtime_image_ref must be an exact digest-pinned OCI reference",
            path,
        )
    if value.rpartition("@")[2] != image_digest:
        raise field_invalid(
            "runtime_image_ref does not match runtime_image_digest",
            path,
        )
    return value


def airflow_bundle_ref(value: object, path: Path | None) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise field_invalid("airflow_bundle_ref must be a bounded string or null", path)
    lowered = value.casefold()
    unsafe = (
        "x-amz-signature=",
        "x-goog-signature=",
        "signature=",
        "credential=",
        "token=",
        "password=",
    )
    authority = value.partition("://")[2].partition("/")[0]
    if (
        any(character.isspace() for character in value)
        or any(marker in lowered for marker in unsafe)
        or (authority and "@" in authority)
    ):
        raise field_invalid("airflow_bundle_ref contains an unsafe value", path)


def cache_ref(value: object, field: str, path: Path | None) -> str:
    if not isinstance(value, str):
        raise field_invalid(f"{field} must be a cache:// reference", path)
    cache_parts(value, field, path)
    return value


def cache_parts(value: str, field: str, path: Path | None) -> tuple[str, ...]:
    prefix = "cache://"
    if not value.startswith(prefix) or len(value) > 1024 or "\\" in value:
        raise field_invalid(f"{field} must be a bounded cache:// reference", path)
    relative = value[len(prefix) :]
    pure = PurePosixPath(relative)
    if (
        not relative
        or pure.is_absolute()
        or any(part in {"", ".", ".."} for part in pure.parts)
        or any(part.casefold() in {"current", "latest"} for part in pure.parts)
        or any(not _CACHE_SEGMENT_RE.fullmatch(part) for part in pure.parts)
        or any(pattern.fullmatch(part) for part in pure.parts for pattern in _SECRET_SHAPES)
        or pure.as_posix() != relative
    ):
        raise field_invalid(f"{field} is not a confined cache reference", path)
    return pure.parts


def require_cache_prefix(
    artifact_ref: str,
    prefix: tuple[str, ...],
    field: str,
    path: Path | None,
) -> None:
    parts = cache_parts(artifact_ref, field, path)
    if len(parts) <= len(prefix) or parts[: len(prefix)] != prefix:
        raise field_invalid(
            f"{field} artifact_ref is outside its pinned identity",
            path,
        )


def field_invalid(message: str, path: Path | None) -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
        message,
        path=path.as_posix() if path is not None else None,
    )


__all__ = [
    "airflow_bundle_ref",
    "cache_parts",
    "cache_ref",
    "CONFIG_KEY_RE",
    "digest",
    "dns_label",
    "ENVIRONMENT_RE",
    "exact_mapping",
    "execution_token",
    "field_invalid",
    "positive_integer",
    "registry_ref",
    "required_text",
    "require_cache_prefix",
    "runtime_image",
]
