"""Build immutable, non-authoritative compatibility-candidate manifests."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import stat
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from dpone.manifest.confined_files import ConfinedFileError, read_confined_leaf

SCHEMA_VERSION = "dpone.compatibility-candidate.v1"
EXPECTED_DISTRIBUTIONS = (
    "apache-airflow-providers-dpone",
    "dpone",
    "dpone-airflow-pack",
)
MAX_WHEEL_BYTES = 512 * 1024 * 1024
MAX_TOTAL_BYTES = 1024 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024


class CandidateManifestError(ValueError):
    """A candidate cannot be represented without weakening its identity."""


@dataclass(frozen=True, slots=True)
class CandidateEntry:
    """One immutable wheel entry in the candidate inventory."""

    filename: str
    distribution: str
    version: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, str | int]:
        return {
            "filename": self.filename,
            "distribution": self.distribution,
            "version": self.version,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


def build_manifest(files: Mapping[str, bytes], *, expected_versions: Mapping[str, str]) -> dict[str, object]:
    """Return a canonical v1 manifest from already stable, bounded wheel bytes."""

    if len(files) != len(EXPECTED_DISTRIBUTIONS):
        raise CandidateManifestError("candidate directory must contain exactly three wheels")
    entries = [_entry_from_bytes(filename, content, expected_versions) for filename, content in files.items()]
    _validate_entries(entries)
    projection: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "entries": [entry.to_dict() for entry in sorted(entries, key=lambda item: item.filename)],
    }
    manifest = dict(projection)
    manifest["inventory_digest"] = _sha256(_canonical_json(projection))
    encoded = _canonical_json(manifest)
    if len(encoded) > MAX_MANIFEST_BYTES:
        raise CandidateManifestError("candidate manifest exceeds its byte limit")
    return manifest


def build_manifest_from_directory(dist: Path, *, expected_versions: Mapping[str, str]) -> dict[str, object]:
    """Read exactly three descriptor-confined wheel leaves and build their manifest."""

    directory_fd = _open_directory(dist)
    try:
        names = sorted(os.listdir(directory_fd))
        if len(names) != len(EXPECTED_DISTRIBUTIONS):
            raise CandidateManifestError("candidate directory must contain exactly three wheels")
        files: dict[str, bytes] = {}
        total = 0
        for name in names:
            _validate_filename(name)
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if not stat.S_ISREG(metadata.st_mode):
                raise CandidateManifestError("candidate directory contains a non-regular entry")
            snapshot = read_confined_leaf(directory_fd, name, max_bytes=MAX_WHEEL_BYTES)
            total += len(snapshot.content)
            if total > MAX_TOTAL_BYTES:
                raise CandidateManifestError("candidate wheels exceed their aggregate byte limit")
            files[name] = snapshot.content
        return build_manifest(files, expected_versions=expected_versions)
    except ConfinedFileError as exc:
        raise CandidateManifestError("candidate input is unsafe or changed") from exc
    finally:
        os.close(directory_fd)


def installed_expected_versions() -> dict[str, str]:
    """Return the exact normalized versions expected from this checkout's packages."""

    versions: dict[str, str] = {}
    fallback_version = _workspace_version()
    for distribution in EXPECTED_DISTRIBUTIONS:
        try:
            versions[distribution] = str(Version(importlib.metadata.version(distribution)))
        except importlib.metadata.PackageNotFoundError:
            versions[distribution] = fallback_version
        except InvalidVersion as exc:
            raise CandidateManifestError("installed project metadata is unavailable") from exc
    return versions


def _workspace_version() -> str:
    """Read the checked-out project version when a workspace member is not installed."""

    try:
        payload = tomllib.loads((Path(__file__).parents[3] / "pyproject.toml").read_text(encoding="utf-8"))
        return str(Version(str(payload["project"]["version"])))
    except (KeyError, OSError, TypeError, ValueError, InvalidVersion) as exc:
        raise CandidateManifestError("installed project metadata is unavailable") from exc


def write_new_manifest(path: Path, manifest: Mapping[str, object]) -> None:
    """Create one fsynced manifest without replacing an existing artifact."""

    parent_fd = _open_directory(path.parent)
    descriptor: int | None = None
    created = False
    try:
        _validate_filename(path.name)
        encoded = _canonical_json(dict(manifest)) + b"\n"
        if len(encoded) > MAX_MANIFEST_BYTES:
            raise CandidateManifestError("candidate manifest exceeds its byte limit")
        descriptor = os.open(
            path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _flag("O_CLOEXEC"), 0o600, dir_fd=parent_fd
        )
        created = True
        _write_all(descriptor, encoded)
        os.fsync(descriptor)
        os.fsync(parent_fd)
    except FileExistsError as exc:
        raise CandidateManifestError("candidate output already exists") from exc
    except OSError as exc:
        if created:
            try:
                os.unlink(path.name, dir_fd=parent_fd)
                os.fsync(parent_fd)
            except OSError:
                pass
        raise CandidateManifestError("candidate output could not be created safely") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent_fd)


def _entry_from_bytes(filename: str, content: bytes, expected_versions: Mapping[str, str]) -> CandidateEntry:
    _validate_filename(filename)
    if not filename.endswith(".whl"):
        raise CandidateManifestError("candidate entry is not a wheel")
    parts = filename[:-4].split("-", 2)
    if len(parts) != 3:
        raise CandidateManifestError("candidate wheel filename is invalid")
    distribution, version_text, _tags = parts
    normalized_distribution = canonicalize_name(distribution)
    try:
        normalized_version = str(Version(version_text))
    except InvalidVersion as exc:
        raise CandidateManifestError("candidate wheel version is invalid") from exc
    if normalized_distribution not in EXPECTED_DISTRIBUTIONS:
        raise CandidateManifestError("candidate wheel distribution is not supported")
    if expected_versions.get(normalized_distribution) != normalized_version:
        raise CandidateManifestError("candidate wheel version does not match project metadata")
    if len(content) > MAX_WHEEL_BYTES:
        raise CandidateManifestError("candidate wheel exceeds its byte limit")
    return CandidateEntry(
        filename=filename,
        distribution=normalized_distribution,
        version=normalized_version,
        sha256=_sha256(content),
        size_bytes=len(content),
    )


def _validate_entries(entries: list[CandidateEntry]) -> None:
    distributions = [entry.distribution for entry in entries]
    if set(distributions) != set(EXPECTED_DISTRIBUTIONS) or len(set(distributions)) != len(entries):
        raise CandidateManifestError("candidate wheels must contain each supported distribution exactly once")
    if len({entry.filename for entry in entries}) != len(entries):
        raise CandidateManifestError("candidate wheel filenames must be unique")
    if sum(entry.size_bytes for entry in entries) > MAX_TOTAL_BYTES:
        raise CandidateManifestError("candidate wheels exceed their aggregate byte limit")


def _open_directory(path: Path) -> int:
    try:
        return os.open(path, os.O_RDONLY | os.O_DIRECTORY | _flag("O_CLOEXEC") | _flag("O_NOFOLLOW"))
    except OSError as exc:
        raise CandidateManifestError("candidate directory is unavailable or unsafe") from exc


def _validate_filename(name: str) -> None:
    if not name or name in {".", ".."} or "/" in name or "\\" in name or len(name.encode("utf-8")) > 255:
        raise CandidateManifestError("candidate filename is invalid")
    if any(ord(character) < 32 for character in name):
        raise CandidateManifestError("candidate filename contains controls")


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _flag(name: str) -> int:
    return int(getattr(os, name, 0))


def _write_all(descriptor: int, content: bytes) -> None:
    offset = 0
    while offset < len(content):
        written = os.write(descriptor, content[offset:])
        if written <= 0:
            raise OSError("candidate output write did not make progress")
        offset += written


__all__ = [
    "CandidateManifestError",
    "EXPECTED_DISTRIBUTIONS",
    "SCHEMA_VERSION",
    "build_manifest",
    "build_manifest_from_directory",
    "installed_expected_versions",
    "write_new_manifest",
]
