"""Closed candidate-inventory and local-byte sources for PyPI publication."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Sequence
from pathlib import Path
from typing import Final

if __package__:
    from .pypi_core_metadata import verify_archive_core_metadata
    from .pypi_prepublication_codec import decode_json
    from .pypi_prepublication_contract import EXPECTED_PACKAGES, Candidate, fail
else:
    from pypi_core_metadata import verify_archive_core_metadata
    from pypi_prepublication_codec import decode_json
    from pypi_prepublication_contract import EXPECTED_PACKAGES, Candidate, fail

MAX_CANDIDATE_INVENTORY_BYTES: Final = 1024 * 1024
MAX_CANDIDATE_FILE_BYTES: Final = 128 * 1024 * 1024
MAX_CANDIDATE_TOTAL_BYTES: Final = 512 * 1024 * 1024
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_KEYS = frozenset({"artifact_type", "filename", "package", "sha256", "size_bytes", "version"})
_INVENTORY_KEYS = frozenset(
    {"artifacts", "blockers", "decision", "expected_version", "schema_version", "status", "summary"}
)
_SUMMARY_KEYS = frozenset(
    {"artifact_count", "distribution_count", "expected_artifact_count", "expected_distribution_count"}
)


def _metadata_identity(value: os.stat_result) -> tuple[int, ...]:
    return value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _read_bounded(path: Path) -> bytes:
    file_fd: int | None = None
    try:
        flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        file_fd = os.open(path, flags)
        before = os.fstat(file_fd)
        if not stat.S_ISREG(before.st_mode):
            raise fail("PYPI_PREPUBLICATION_CANDIDATE_INVENTORY_UNAVAILABLE")
        chunks: list[bytes] = []
        remaining = MAX_CANDIDATE_INVENTORY_BYTES + 1
        while remaining:
            chunk = os.read(file_fd, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if _metadata_identity(os.fstat(file_fd)) != _metadata_identity(before):
            raise fail("PYPI_PREPUBLICATION_CANDIDATE_INVENTORY_CHANGED")
    except (AttributeError, OSError) as exc:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_INVENTORY_UNAVAILABLE") from exc
    finally:
        if file_fd is not None:
            os.close(file_fd)
    if len(raw) > MAX_CANDIDATE_INVENTORY_BYTES:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_INVENTORY_OVERSIZED")
    return raw


def _candidate(value: object, *, expected_version: str) -> Candidate:
    if not isinstance(value, dict) or set(value) != _CANDIDATE_KEYS:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_SHAPE_INVALID")
    filename, package, version = value["filename"], value["package"], value["version"]
    artifact_type, sha256, size_bytes = value["artifact_type"], value["sha256"], value["size_bytes"]
    if not all(isinstance(item, str) for item in (filename, package, version, artifact_type, sha256)):
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_TYPE_INVALID")
    if filename != Path(filename).name or "/" in filename or chr(92) in filename:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_FILENAME_INVALID")
    if package not in EXPECTED_PACKAGES or version != expected_version:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_IDENTITY_INVALID")
    suffix = ".whl" if artifact_type == "wheel" else ".tar.gz" if artifact_type == "sdist" else None
    if suffix is None or not filename.endswith(suffix):
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_FORMAT_INVALID", package=package, filename=filename)
    positive_size = (
        isinstance(size_bytes, int) and not isinstance(size_bytes, bool) and 0 < size_bytes <= MAX_CANDIDATE_FILE_BYTES
    )
    if not _SHA256.fullmatch(sha256) or not positive_size:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_DIGEST_INVALID", package=package, filename=filename)
    return Candidate(filename, package, version, artifact_type, sha256, size_bytes)


def load_candidate_inventory(path: Path, *, expected_version: str) -> tuple[tuple[Candidate, ...], str]:
    """Load the build receipt as one closed, canonical eight-file inventory."""

    raw = _read_bounded(path)
    payload = decode_json(raw, invalid_code="PYPI_PREPUBLICATION_CANDIDATE_JSON_INVALID")
    if not isinstance(payload, dict) or set(payload) != _INVENTORY_KEYS:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_INVENTORY_SHAPE_INVALID")
    summary = payload["summary"]
    if not isinstance(summary, dict) or set(summary) != _SUMMARY_KEYS:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_SUMMARY_INVALID")
    expected_summary = {
        "artifact_count": 8,
        "distribution_count": 4,
        "expected_artifact_count": 8,
        "expected_distribution_count": 4,
    }
    if (
        type(payload["schema_version"]) is not int
        or payload["schema_version"] != 1
        or payload["status"] != "passed"
        or payload["decision"] != "GO"
        or payload["blockers"] != []
        or payload["expected_version"] != expected_version
        or summary != expected_summary
        or any(type(summary[key]) is not int for key in expected_summary)
    ):
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_INVENTORY_NOT_GO")
    artifacts = payload["artifacts"]
    if not isinstance(artifacts, list) or len(artifacts) != 8:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_COUNT_INVALID")
    candidates = tuple(_candidate(item, expected_version=expected_version) for item in artifacts)
    if candidates != tuple(sorted(candidates)) or len({candidate.filename for candidate in candidates}) != 8:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_ORDER_OR_UNIQUENESS_INVALID")
    observed = {(candidate.package, candidate.artifact_type) for candidate in candidates}
    expected = {(package, kind) for package in EXPECTED_PACKAGES for kind in ("sdist", "wheel")}
    if observed != expected:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_PACKAGE_SET_INVALID")
    if sum(candidate.size_bytes for candidate in candidates) > MAX_CANDIDATE_TOTAL_BYTES:
        raise fail("PYPI_PREPUBLICATION_CANDIDATE_TOTAL_SIZE_INVALID")
    return candidates, hashlib.sha256(raw).hexdigest()


def _bounded_names(root_fd: int, *, maximum: int) -> tuple[str, ...]:
    names: list[str] = []
    with os.scandir(root_fd) as entries:
        for entry in entries:
            if len(names) == maximum:
                raise fail("PYPI_PREPUBLICATION_DIST_SET_MISMATCH")
            names.append(entry.name)
    return tuple(sorted(names))


def verify_local_candidates(dist_dir: Path, candidates: Sequence[Candidate]) -> None:
    """Rehash the exact no-follow handoff before the first network mutation."""

    try:
        path_metadata = os.lstat(dist_dir)
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        root_fd = os.open(dist_dir, flags)
    except (AttributeError, OSError) as exc:
        raise fail("PYPI_PREPUBLICATION_DIST_UNAVAILABLE") from exc
    try:
        root_metadata = os.fstat(root_fd)
        if not stat.S_ISDIR(path_metadata.st_mode) or not os.path.samestat(path_metadata, root_metadata):
            raise fail("PYPI_PREPUBLICATION_DIST_IDENTITY_INVALID")
        names = _bounded_names(root_fd, maximum=len(candidates))
        if names != tuple(candidate.filename for candidate in candidates):
            raise fail("PYPI_PREPUBLICATION_DIST_SET_MISMATCH")
        for candidate in candidates:
            _verify_candidate(root_fd, candidate)
        if _bounded_names(root_fd, maximum=len(candidates)) != names or _metadata_identity(
            os.fstat(root_fd)
        ) != _metadata_identity(root_metadata):
            raise fail("PYPI_PREPUBLICATION_DIST_CHANGED")
    except OSError as exc:
        raise fail("PYPI_PREPUBLICATION_DIST_UNAVAILABLE") from exc
    finally:
        os.close(root_fd)


def _verify_candidate(root_fd: int, candidate: Candidate) -> None:
    before = os.stat(candidate.filename, dir_fd=root_fd, follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise fail("PYPI_PREPUBLICATION_DIST_ENTRY_CHANGED", filename=candidate.filename)
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    file_fd = os.open(candidate.filename, flags, dir_fd=root_fd)
    try:
        opened = os.fstat(file_fd)
        if not stat.S_ISREG(opened.st_mode) or _metadata_identity(before) != _metadata_identity(opened):
            raise fail("PYPI_PREPUBLICATION_DIST_ENTRY_CHANGED", filename=candidate.filename)
        digest, remaining = hashlib.sha256(), candidate.size_bytes
        while remaining:
            chunk = os.read(file_fd, min(1024 * 1024, remaining))
            if not chunk:
                raise fail("PYPI_PREPUBLICATION_DIST_SIZE_MISMATCH", filename=candidate.filename)
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(file_fd, 1) or digest.hexdigest() != candidate.sha256:
            raise fail("PYPI_PREPUBLICATION_DIST_DIGEST_MISMATCH", filename=candidate.filename)
        verify_archive_core_metadata(
            file_fd,
            filename=candidate.filename,
            package=candidate.package,
            version=candidate.version,
            artifact_type=candidate.artifact_type,
            archive_size=candidate.size_bytes,
        )
        if _metadata_identity(os.fstat(file_fd)) != _metadata_identity(opened):
            raise fail("PYPI_PREPUBLICATION_DIST_ENTRY_CHANGED", filename=candidate.filename)
    finally:
        os.close(file_fd)
