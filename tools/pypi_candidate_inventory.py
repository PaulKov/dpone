from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TypeAlias

from packaging.utils import canonicalize_name, parse_sdist_filename, parse_wheel_filename

if TYPE_CHECKING:
    import pypi_candidate_contract as _candidate_contract
elif __package__:
    from tools import pypi_candidate_contract as _candidate_contract
else:
    import pypi_candidate_contract as _candidate_contract

CandidateArtifact: TypeAlias = _candidate_contract.CandidateArtifact
CandidateInventoryArtifact: TypeAlias = _candidate_contract.CandidateInventoryArtifact
CandidateInventoryLimits: TypeAlias = _candidate_contract.CandidateInventoryLimits
CandidateInventoryReport: TypeAlias = _candidate_contract.CandidateInventoryReport
DistributionRelease: TypeAlias = _candidate_contract.DistributionRelease
DEFAULT_MAX_ENTRIES = _candidate_contract.DEFAULT_MAX_ENTRIES
DEFAULT_MAX_FILE_BYTES = _candidate_contract.DEFAULT_MAX_FILE_BYTES
DEFAULT_MAX_TOTAL_BYTES = _candidate_contract.DEFAULT_MAX_TOTAL_BYTES

__all__ = [
    "CandidateArtifact",
    "CandidateInventoryArtifact",
    "CandidateInventoryLimits",
    "CandidateInventoryReport",
    "DEFAULT_MAX_ENTRIES",
    "DEFAULT_MAX_FILE_BYTES",
    "DEFAULT_MAX_TOTAL_BYTES",
    "DistributionRelease",
    "discover_distribution_releases",
    "evaluate_candidate_inventory",
    "revalidate_candidate_inventory",
    "validate_distribution_inventory",
]

EXPECTED_DISTRIBUTIONS = frozenset("apache-airflow-providers-dpone dpone dpone-airflow-pack dpone-native-accel".split())
_HASH_CHUNK_BYTES = 1024 * 1024
_METADATA_FIELDS = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
_IDENTIFIER_HASH_FACTORY = hashlib.sha256


class _Digest(Protocol):
    def update(self, chunk: bytes) -> None: ...

    def hexdigest(self) -> str: ...


HashFactory = Callable[[], _Digest]


_CandidateEntry = tuple[str, DistributionRelease, os.stat_result]


@dataclass(frozen=True, slots=True)
class _CandidateScan:
    entry_count: int
    releases: tuple[DistributionRelease, ...] = ()
    artifacts: tuple[CandidateInventoryArtifact, ...] = ()
    blockers: tuple[str, ...] = ()
    source_identity: tuple[int, ...] = ()


def _metadata_identity(value: os.stat_result) -> tuple[int, ...]:
    return tuple(getattr(value, field) for field in _METADATA_FIELDS)


def _candidate_reference(name: str) -> str:
    digest = _IDENTIFIER_HASH_FACTORY(os.fsencode(name)).hexdigest()[:16]
    return f"candidate_ref=sha256:{digest}"


def _release_reference(release: DistributionRelease) -> str:
    filenames = "\0".join(artifact.filename for artifact in release.candidate_artifacts)
    identity = f"{release.package}\0{release.version}\0{filenames}"
    digest = _IDENTIFIER_HASH_FACTORY(os.fsencode(identity)).hexdigest()[:16]
    return f"release_ref=sha256:{digest}"


def _bounded_expected_version(value: str) -> str:
    safe_characters = frozenset(".!+_-")
    if 0 < len(value) <= 64 and all(
        character.isascii() and (character.isalnum() or character in safe_characters) for character in value
    ):
        return value
    digest = _IDENTIFIER_HASH_FACTORY(value.encode("utf-8", "surrogatepass")).hexdigest()[:16]
    return f"version_ref=sha256:{digest}"


def _entry_metadata(root_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=root_fd, follow_symlinks=False)
    except OSError:
        return None


def _open_candidate_root(dist_dir: Path) -> tuple[int | None, os.stat_result | None, tuple[str, ...]]:
    try:
        path_metadata = os.lstat(dist_dir)
    except OSError:
        return None, None, ("PYPI_CANDIDATE_DIRECTORY_UNAVAILABLE: candidate directory cannot be read",)
    if stat.S_ISLNK(path_metadata.st_mode):
        return None, None, ("PYPI_CANDIDATE_ROOT_SYMLINK: candidate directory must not be a symlink",)
    if not stat.S_ISDIR(path_metadata.st_mode):
        return None, None, ("PYPI_CANDIDATE_ROOT_UNSAFE: candidate root must be a directory",)
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        return None, None, ("PYPI_CANDIDATE_NOFOLLOW_UNAVAILABLE: no-follow directory access is required",)
    root_fd: int | None = None
    try:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
        root_fd = os.open(dist_dir, flags)
        opened_metadata = os.fstat(root_fd)
    except OSError:
        if root_fd is not None:
            os.close(root_fd)
        return None, None, ("PYPI_CANDIDATE_DIRECTORY_UNAVAILABLE: candidate directory cannot be read",)
    if not os.path.samestat(path_metadata, opened_metadata):
        os.close(root_fd)
        return None, None, ("PYPI_CANDIDATE_ROOT_CHANGED: candidate directory identity changed",)
    return root_fd, opened_metadata, ()


def _bounded_entry_names(root_fd: int, *, maximum: int) -> tuple[tuple[str, ...], str | None]:
    names: list[str] = []
    try:
        with os.scandir(root_fd) as iterator:
            for item in iterator:
                names.append(item.name)
                if len(names) > maximum:
                    return (
                        tuple(sorted(names)),
                        f"PYPI_CANDIDATE_ENTRY_LIMIT_EXCEEDED: maximum={maximum} observed_at_least={len(names)}",
                    )
    except OSError:
        return (), "PYPI_CANDIDATE_DIRECTORY_UNAVAILABLE: candidate directory cannot be read"
    return tuple(sorted(names)), None


def _parse_distribution_filename(name: str) -> DistributionRelease | None:
    try:
        if name.endswith(".whl"):
            package, version, *_ = parse_wheel_filename(name)
            return DistributionRelease(package=str(canonicalize_name(package)), version=str(version))
        if name.endswith(".tar.gz") or name.endswith(".zip"):
            package, version = parse_sdist_filename(name)
            return DistributionRelease(package=str(canonicalize_name(package)), version=str(version))
    except ValueError:
        return None
    return None


def _inspect_entries(
    root_fd: int,
    names: Sequence[str],
    *,
    limits: CandidateInventoryLimits,
) -> tuple[tuple[_CandidateEntry, ...], tuple[str, ...]]:
    entries: list[_CandidateEntry] = []
    blockers: list[str] = []
    total_bytes = 0
    for name in names:
        metadata = _entry_metadata(root_fd, name)
        if metadata is None:
            blockers.append(f"PYPI_CANDIDATE_ENTRY_UNAVAILABLE: {_candidate_reference(name)}")
            continue
        if not stat.S_ISREG(metadata.st_mode):
            blockers.append(f"PYPI_CANDIDATE_ENTRY_UNSAFE: {_candidate_reference(name)}")
            continue
        parsed = _parse_distribution_filename(name)
        if parsed is None:
            blockers.append(f"PYPI_CANDIDATE_ARTIFACT_UNRECOGNIZED: {_candidate_reference(name)}")
            continue
        if metadata.st_size > limits.max_file_bytes:
            blockers.append(
                "PYPI_CANDIDATE_FILE_SIZE_LIMIT_EXCEEDED: "
                f"{_candidate_reference(name)} maximum_bytes={limits.max_file_bytes} actual_bytes={metadata.st_size}"
            )
        total_bytes += metadata.st_size
        entries.append((name, parsed, metadata))
    if total_bytes > limits.max_total_bytes:
        blockers.append(
            "PYPI_CANDIDATE_TOTAL_SIZE_LIMIT_EXCEEDED: "
            f"maximum_bytes={limits.max_total_bytes} actual_bytes={total_bytes}"
        )
    return tuple(entries), tuple(blockers)


def _hash_entry(root_fd: int, entry: _CandidateEntry, *, hash_factory: HashFactory) -> str | None:
    name, _, metadata = entry
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0)
    file_fd: int | None = None
    try:
        file_fd = os.open(name, flags, dir_fd=root_fd)
        opened_metadata = os.fstat(file_fd)
        if not stat.S_ISREG(opened_metadata.st_mode) or _metadata_identity(opened_metadata) != _metadata_identity(
            metadata
        ):
            return None
        digest = hash_factory()
        remaining = metadata.st_size
        while remaining:
            chunk = os.read(file_fd, min(_HASH_CHUNK_BYTES, remaining))
            if not chunk:
                return None
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(file_fd, 1) or _metadata_identity(os.fstat(file_fd)) != _metadata_identity(metadata):
            return None
        current = _entry_metadata(root_fd, name)
        if current is None:
            return None
        return digest.hexdigest() if _metadata_identity(current) == _metadata_identity(metadata) else None
    except (OSError, TypeError, NotImplementedError):
        return None
    finally:
        if file_fd is not None:
            os.close(file_fd)


def _snapshot_blockers(
    dist_dir: Path,
    root_fd: int,
    root_metadata: os.stat_result,
    names: Sequence[str],
    entries: Sequence[_CandidateEntry],
    *,
    limits: CandidateInventoryLimits,
) -> list[str]:
    blockers: list[str] = []
    if _bounded_entry_names(root_fd, maximum=limits.max_entries) != (tuple(names), None):
        blockers.append("PYPI_CANDIDATE_ROOT_CHANGED: candidate directory entries changed")
    try:
        current_root = os.stat(dist_dir, follow_symlinks=False)
    except OSError:
        current_root = None
    if current_root is None or _metadata_identity(current_root) != _metadata_identity(root_metadata):
        blockers.append("PYPI_CANDIDATE_ROOT_CHANGED: candidate directory identity changed")
    for name, _, metadata in entries:
        current = _entry_metadata(root_fd, name)
        if current is None or _metadata_identity(current) != _metadata_identity(metadata):
            blockers.append(f"PYPI_CANDIDATE_ENTRY_CHANGED: {_candidate_reference(name)}")
    return blockers


def _artifact_type(filename: str) -> str:
    if filename.endswith(".whl"):
        return "wheel"
    if filename.endswith(".tar.gz"):
        return "sdist"
    return "unsupported_sdist"


def _group_releases(artifacts: Sequence[CandidateInventoryArtifact]) -> tuple[DistributionRelease, ...]:
    grouped: dict[tuple[str, str], list[CandidateArtifact]] = {}
    for artifact in artifacts:
        grouped.setdefault((artifact.package, artifact.version), []).append(
            CandidateArtifact(artifact.filename, artifact.sha256)
        )
    return tuple(
        DistributionRelease(package, version, tuple(sorted(candidates)))
        for (package, version), candidates in sorted(grouped.items())
    )


def _scan_candidate_directory(
    dist_dir: Path,
    *,
    limits: CandidateInventoryLimits,
    hash_factory: HashFactory,
) -> _CandidateScan:
    root_fd, root_metadata, root_blockers = _open_candidate_root(dist_dir)
    if root_fd is None or root_metadata is None:
        return _CandidateScan(entry_count=0, blockers=root_blockers)
    try:
        names, names_error = _bounded_entry_names(root_fd, maximum=limits.max_entries)
        if names_error is not None:
            return _CandidateScan(entry_count=len(names), blockers=(names_error,))
        entries, blockers = _inspect_entries(root_fd, names, limits=limits)
        if blockers:
            return _CandidateScan(entry_count=len(names), blockers=blockers)
        artifacts: list[CandidateInventoryArtifact] = []
        for entry in entries:
            name, release, _ = entry
            digest = _hash_entry(root_fd, entry, hash_factory=hash_factory)
            if digest is None:
                blocker = f"PYPI_CANDIDATE_ENTRY_CHANGED: {_candidate_reference(name)}"
                return _CandidateScan(len(names), blockers=(blocker,))
            metadata = entry[2]
            artifacts.append(
                CandidateInventoryArtifact(
                    filename=name,
                    package=release.package,
                    version=release.version,
                    artifact_type=_artifact_type(name),
                    sha256=digest,
                    size_bytes=metadata.st_size,
                    source_identity=_metadata_identity(metadata),
                )
            )
        snapshot_blockers = _snapshot_blockers(
            dist_dir,
            root_fd,
            root_metadata,
            names,
            entries,
            limits=limits,
        )
        if snapshot_blockers:
            return _CandidateScan(entry_count=len(names), blockers=tuple(dict.fromkeys(snapshot_blockers)))
        stable_artifacts = tuple(sorted(artifacts))
        return _CandidateScan(
            len(names),
            _group_releases(stable_artifacts),
            stable_artifacts,
            source_identity=_metadata_identity(root_metadata),
        )
    finally:
        os.close(root_fd)


def discover_distribution_releases(
    dist_dir: Path,
    *,
    limits: CandidateInventoryLimits = CandidateInventoryLimits(),
    hash_factory: HashFactory = hashlib.sha256,
) -> tuple[DistributionRelease, ...]:
    """Discover only a fully inspected directory; never ignore stray entries."""

    scan = _scan_candidate_directory(dist_dir, limits=limits, hash_factory=hash_factory)
    if scan.blockers:
        raise RuntimeError("; ".join(scan.blockers))
    return scan.releases


def validate_distribution_inventory(
    releases: Sequence[DistributionRelease],
    *,
    expected_version: str,
) -> tuple[str, ...]:
    """Validate the complete four-package candidate set before network access."""

    blockers = [
        f"PYPI_CANDIDATE_{kind}_COUNT_MISMATCH: expected={expected} actual={actual}"
        for kind, expected, actual in (
            ("ARTIFACT", len(EXPECTED_DISTRIBUTIONS) * 2, sum(len(item.candidate_artifacts) for item in releases)),
            ("DISTRIBUTION", len(EXPECTED_DISTRIBUTIONS), len(releases)),
        )
        if actual != expected
    ]
    actual_packages = {release.package for release in releases}
    if actual_packages != EXPECTED_DISTRIBUTIONS:
        missing = sorted(EXPECTED_DISTRIBUTIONS - actual_packages)
        unexpected = [release for release in releases if release.package not in EXPECTED_DISTRIBUTIONS]
        blockers.append(f"PYPI_CANDIDATE_PACKAGE_SET_MISMATCH: missing={missing} unexpected_count={len(unexpected)}")
        blockers.extend(f"PYPI_CANDIDATE_PACKAGE_UNEXPECTED: {_release_reference(release)}" for release in unexpected)
    expected_version_label = _bounded_expected_version(expected_version)
    for release in releases:
        release_reference = _release_reference(release)
        if release.version != expected_version:
            blockers.append(f"PYPI_CANDIDATE_VERSION_MISMATCH: {release_reference} expected={expected_version_label}")
        filenames = tuple(artifact.filename for artifact in release.candidate_artifacts)
        for kind, suffix in (("WHEEL", ".whl"), ("SDIST", ".tar.gz")):
            count = sum(filename.endswith(suffix) for filename in filenames)
            if count != 1:
                blockers.append(f"PYPI_CANDIDATE_{kind}_COUNT_MISMATCH: {release_reference} expected=1 actual={count}")
                if count == 0:
                    blockers.append(f"PYPI_CANDIDATE_{kind}_MISSING: {release_reference}")
        blockers.extend(
            f"PYPI_CANDIDATE_SDIST_FORMAT_UNSUPPORTED: {_candidate_reference(filename)}"
            for filename in filenames
            if filename.endswith(".zip")
        )
    return tuple(blockers)


def evaluate_candidate_inventory(
    dist_dir: Path,
    *,
    expected_version: str,
    limits: CandidateInventoryLimits = CandidateInventoryLimits(),
    hash_factory: HashFactory = hashlib.sha256,
) -> CandidateInventoryReport:
    """Evaluate a closed local candidate directory without network access."""

    scan = _scan_candidate_directory(dist_dir, limits=limits, hash_factory=hash_factory)
    blockers = list(scan.blockers)
    expected_count = len(EXPECTED_DISTRIBUTIONS) * 2
    if scan.entry_count != expected_count:
        blockers.append(f"PYPI_CANDIDATE_ENTRY_COUNT_MISMATCH: expected={expected_count} actual={scan.entry_count}")
    blockers.extend(validate_distribution_inventory(scan.releases, expected_version=expected_version))
    return CandidateInventoryReport(
        expected_version=expected_version,
        entry_count=scan.entry_count,
        releases=scan.releases,
        artifacts=scan.artifacts,
        blockers=tuple(dict.fromkeys(blockers)),
        expected_artifact_count=expected_count,
        expected_distribution_count=len(EXPECTED_DISTRIBUTIONS),
        source_identity=scan.source_identity,
    )


def _artifact_handoff_changed(
    initial: CandidateInventoryArtifact,
    current: CandidateInventoryArtifact,
) -> bool:
    return initial.exact_identity != current.exact_identity or bool(
        initial.source_identity and current.source_identity and initial.source_identity != current.source_identity
    )


def revalidate_candidate_inventory(
    dist_dir: Path,
    *,
    initial: CandidateInventoryReport,
    limits: CandidateInventoryLimits = CandidateInventoryLimits(),
    hash_factory: HashFactory = hashlib.sha256,
) -> CandidateInventoryReport:
    """Bind a final decision to the exact complete set seen before network checks."""

    current = evaluate_candidate_inventory(
        dist_dir,
        expected_version=initial.expected_version,
        limits=limits,
        hash_factory=hash_factory,
    )
    if current.blockers:
        blocker = "PYPI_CANDIDATE_FINAL_REVALIDATION_FAILED: candidate set is no longer valid"
        return replace(current, blockers=(blocker, *current.blockers))

    initial_by_name = {artifact.filename: artifact for artifact in initial.artifacts}
    current_by_name = {artifact.filename: artifact for artifact in current.artifacts}
    changed_names = {
        name
        for name in initial_by_name.keys() | current_by_name.keys()
        if name not in initial_by_name
        or name not in current_by_name
        or _artifact_handoff_changed(initial_by_name[name], current_by_name[name])
    }
    directory_changed = bool(
        initial.source_identity and current.source_identity and initial.source_identity != current.source_identity
    )
    if not changed_names and not directory_changed:
        return current

    blockers = ["PYPI_CANDIDATE_FINAL_REVALIDATION_FAILED: candidate handoff differs from initial inventory"]
    if directory_changed:
        blockers.append("PYPI_CANDIDATE_FINAL_DIRECTORY_CHANGED: candidate directory metadata changed")
    blockers.extend(
        f"PYPI_CANDIDATE_FINAL_ARTIFACT_CHANGED: {_candidate_reference(name)}" for name in sorted(changed_names)
    )
    return replace(current, blockers=tuple(blockers))
