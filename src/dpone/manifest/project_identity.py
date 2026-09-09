"""Canonical identity primitives for project discovery projections."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint as _canonical_fingerprint,
)
from dpone.contracts.airflow_deployment import (
    is_sha256_digest as _is_sha256_digest,
)


def project_identity_fingerprint(payload: Mapping[str, Any]) -> str:
    """Return the shared deterministic digest for manifest-owned identities."""

    return _canonical_fingerprint(payload)


def is_sha256_digest(value: object) -> bool:
    """Return whether a value has the shared SHA-256 digest shape."""

    return _is_sha256_digest(value)


__all__ = ["is_sha256_digest", "project_identity_fingerprint"]
