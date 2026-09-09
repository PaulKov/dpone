"""Bounded, digest-verified reads for artifacts listed by an Airflow index."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.runtime.artifact_delivery import InitFetchError, LocalArtifactRegistry

_CACHE_RELATIVE_PATH = ".dpone-cache"


class IndexedAirflowArtifactReadError(ValueError):
    """Safe failure while reading one bounded deployment-index artifact."""


class IndexedAirflowArtifactChecksumMismatch(IndexedAirflowArtifactReadError):
    """The local artifact does not match its deployment-index digest."""


def read_indexed_airflow_artifact(
    root: Path,
    item: Mapping[str, Any],
    *,
    max_artifact_bytes: int = 64 * 1024 * 1024,
) -> bytes:
    """Read one pinned artifact after validating all index integrity metadata."""

    artifact_ref = item.get("artifact_ref")
    if not isinstance(artifact_ref, str) or not artifact_ref:
        raise IndexedAirflowArtifactReadError("indexed artifact reference is missing")
    expected_sha = item.get("sha256")
    if not is_canonical_sha256_digest(expected_sha):
        raise IndexedAirflowArtifactReadError("indexed artifact checksum is missing or invalid")
    declared_bytes = item.get("bytes")
    if not isinstance(declared_bytes, int) or isinstance(declared_bytes, bool) or declared_bytes <= 0:
        raise IndexedAirflowArtifactReadError("indexed artifact byte size is missing or invalid")
    try:
        data = LocalArtifactRegistry(
            root / _CACHE_RELATIVE_PATH,
            max_artifact_bytes=max_artifact_bytes,
        ).read_bytes(
            artifact_ref,
            declared_bytes=declared_bytes,
        )
    except InitFetchError as exc:
        raise IndexedAirflowArtifactReadError("indexed artifact is unavailable") from exc
    actual_sha = "sha256:" + hashlib.sha256(data).hexdigest()
    if actual_sha != expected_sha:
        raise IndexedAirflowArtifactChecksumMismatch("indexed artifact checksum does not match")
    return data


__all__ = [
    "IndexedAirflowArtifactChecksumMismatch",
    "IndexedAirflowArtifactReadError",
    "read_indexed_airflow_artifact",
]
