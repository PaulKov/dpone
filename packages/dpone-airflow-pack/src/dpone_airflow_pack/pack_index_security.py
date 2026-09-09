"""Validation helpers for remote pack index consumption."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

from dpone_airflow_pack.pack_index import AirflowPackIndexEntry

# Single path segment: git-sha hex (preferred) or safe opaque ids used in tests/fixtures.
_GENERATION_RE = re.compile(r"^(?:[0-9a-fA-F]{6,40}|[A-Za-z0-9][A-Za-z0-9._-]{0,127})$")
_CANONICAL_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_RAW_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def validate_index_generation(generation: str) -> None:
    """Reject path traversal and multi-segment generation identifiers."""

    if (
        not generation
        or generation in {".", ".."}
        or ".." in generation
        or "/" in generation
        or "\\" in generation
        or not _GENERATION_RE.fullmatch(generation)
    ):
        raise ValueError(f"invalid pack index generation: {generation!r}")


def require_entry_sha256(entry: AirflowPackIndexEntry, *, artifact_kind: str) -> str:
    """Return normalized ``sha256:<hex>`` or raise when the index omits a checksum."""

    raw = entry.sha256
    if not raw or not str(raw).strip():
        raise ValueError(f"missing sha256 for {artifact_kind} {entry.workload_id!r}")
    text = str(raw).strip()
    if _CANONICAL_SHA256_RE.fullmatch(text):
        return text
    if _RAW_SHA256_RE.fullmatch(text):
        return f"sha256:{text}"
    raise ValueError(f"invalid sha256 for {artifact_kind} {entry.workload_id!r}")


def normalize_index_checksum(raw: str) -> str:
    text = raw.strip()
    if _CANONICAL_SHA256_RE.fullmatch(text):
        return text.removeprefix("sha256:")
    if _RAW_SHA256_RE.fullmatch(text):
        return text
    raise ValueError("invalid sha256 digest")


def release_uri_prefix(*, index_uri: str, generation: str) -> str:
    """Return the immutable generation root URI implied by ``index_uri``."""

    parsed = urlparse(index_uri)
    if parsed.scheme in ("", "file"):
        index_path = Path(parsed.path if parsed.scheme == "file" else index_uri)
        return (index_path.parents[1] / generation).resolve().as_uri().rstrip("/")
    root_key = "/".join(parsed.path.strip("/").split("/")[:-2])
    return f"{parsed.scheme}://{parsed.netloc}/{root_key}/{generation}".rstrip("/")


def derived_entry_uri(*, index_uri: str, generation: str, entry: AirflowPackIndexEntry) -> str:
    """Build the artifact URI implied by ``index_uri``, generation, and relative path."""

    return f"{release_uri_prefix(index_uri=index_uri, generation=generation)}/{entry.path}"


def validate_entry_uri(
    *,
    index_uri: str,
    generation: str,
    entry: AirflowPackIndexEntry,
    declared_uri: str,
) -> None:
    """Ensure an explicit index URI stays under the release prefix for ``index_uri``.

    Rejects other buckets/hosts and ``file://`` escapes outside the generation root.
    """

    expected_prefix = release_uri_prefix(index_uri=index_uri, generation=generation)
    parsed_declared = urlparse(declared_uri)
    parsed_prefix = urlparse(expected_prefix)
    if not parsed_declared.scheme or parsed_declared.scheme != parsed_prefix.scheme:
        raise ValueError(f"artifact uri scheme mismatch for {entry.workload_id}")
    if parsed_declared.netloc != parsed_prefix.netloc:
        raise ValueError(f"artifact uri host/bucket mismatch for {entry.workload_id}")
    if parsed_declared.scheme == "file":
        declared_path = Path(parsed_declared.path).resolve()
        release_root = Path(parsed_prefix.path).resolve()
        try:
            declared_path.relative_to(release_root)
        except ValueError as exc:
            raise ValueError(f"artifact uri escapes release root for {entry.workload_id}") from exc
        return
    normalized = declared_uri.rstrip("/")
    if normalized != expected_prefix and not normalized.startswith(expected_prefix + "/"):
        raise ValueError(f"artifact uri prefix mismatch for {entry.workload_id}")


def resolve_entry_uri(*, index_uri: str, generation: str, entry: AirflowPackIndexEntry) -> str:
    """Prefer derived URI; when ``entry.uri`` is set, validate it then use it."""

    if entry.uri:
        validate_entry_uri(
            index_uri=index_uri,
            generation=generation,
            entry=entry,
            declared_uri=entry.uri,
        )
        return entry.uri
    return derived_entry_uri(index_uri=index_uri, generation=generation, entry=entry)


def assert_path_under_cache_dir(path: Path, *, cache_dir: Path) -> None:
    resolved = path.resolve()
    cache_root = cache_dir.resolve()
    if not resolved.is_relative_to(cache_root):
        raise ValueError("cache path escapes configured cache_dir")


__all__ = [
    "assert_path_under_cache_dir",
    "derived_entry_uri",
    "normalize_index_checksum",
    "release_uri_prefix",
    "require_entry_sha256",
    "resolve_entry_uri",
    "validate_entry_uri",
    "validate_index_generation",
]
