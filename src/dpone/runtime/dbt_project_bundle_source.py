"""Stable source scanning and canonical archive production for dbt projects."""

from __future__ import annotations

import gzip
import hashlib
import io
import os
import stat
import tarfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.dbt_contract_validation import (
    DbtPublishingError,
    sha256_bytes,
)
from dpone.contracts.dbt_project_bundle import (
    DbtProjectBundle,
    DbtProjectBundleArtifact,
    DbtProjectBundleLimits,
    DbtProjectFile,
)
from dpone.runtime.dbt_package_readiness import require_resolved_packages
from dpone.runtime.dbt_project_bundle_safety import (
    READ_BYTES,
    identity,
    limit_error,
    open_at,
    open_directory,
    open_relative_file,
    path_matches,
    read_root_yaml_mapping,
    require_collision_free,
    root_regular_file,
    source_changed,
)
from dpone.runtime.dbt_project_bundle_secret_scan import (
    DbtProjectBundleSecretDetected,
    DbtProjectBundleSecretScanner,
)

_EXCLUDED_DIRECTORIES = frozenset(
    {
        ".dpone-ci",
        ".git",
        ".pytest_cache",
        "__pycache__",
        "logs",
        "target",
    }
)
_EXCLUDED_FILES = frozenset({"profiles.yml"})
_ROOT_FILES = frozenset(
    {
        "dbt_project.yml",
        "dependencies.yml",
        "package-lock.yml",
        "packages.yml",
        "selectors.yml",
    }
)
_PROJECT_PATHS = {
    "analysis-paths": ("analyses",),
    "asset-paths": (),
    "docs-paths": (),
    "macro-paths": ("macros",),
    "model-paths": ("models",),
    "seed-paths": ("seeds",),
    "snapshot-paths": ("snapshots",),
    "test-paths": ("tests",),
}
_SENSITIVE_FILES = frozenset(
    {
        ".netrc",
        ".npmrc",
        ".pypirc",
        "credentials.json",
        "credentials.yml",
        "id_dsa",
        "id_ecdsa",
        "id_ed25519",
        "id_rsa",
        "secrets.json",
        "secrets.yml",
        "service-account.json",
        "service_account.json",
    }
)
_SENSITIVE_SUFFIXES = (".jks", ".key", ".keystore", ".p12", ".pem", ".pfx")


@dataclass(frozen=True, slots=True)
class _SourceFile:
    path: str
    size: int
    identity: tuple[int, int, int, int, int]


@dataclass(frozen=True, slots=True)
class _ProjectTopology:
    roots: tuple[PurePosixPath, ...]
    root_files: frozenset[str]

    def includes_file(self, path: PurePosixPath) -> bool:
        return (len(path.parts) == 1 and path.name in self.root_files) or any(
            root in path.parents for root in self.roots
        )

    def should_descend(self, path: PurePosixPath) -> bool:
        return any(path == root or path in root.parents or root in path.parents for root in self.roots)


class _ArchiveLimitExceeded(OSError):
    pass


class _BoundedBuffer(io.BytesIO):
    def __init__(self, max_bytes: int) -> None:
        super().__init__()
        self._max_bytes = max_bytes

    def write(self, value: Any) -> int:
        if self.tell() + len(value) > self._max_bytes:
            raise _ArchiveLimitExceeded
        return super().write(value)


class _DigestReader:
    def __init__(self, descriptor: int) -> None:
        self._descriptor = descriptor
        self.bytes_read = 0
        self._digest = hashlib.sha256()
        self._secret_scanner = DbtProjectBundleSecretScanner()

    def read(self, size: int = -1) -> bytes:
        chunk = os.read(self._descriptor, READ_BYTES if size < 0 else min(size, READ_BYTES))
        self.bytes_read += len(chunk)
        try:
            self._secret_scanner.feed(chunk)
        except DbtProjectBundleSecretDetected as exc:
            raise DbtPublishingError(
                "DPONE_DBT_BUNDLE_INVALID",
                "dbt project contains secret-like source content",
            ) from exc
        self._digest.update(chunk)
        return chunk

    @property
    def sha256(self) -> str:
        return "sha256:" + self._digest.hexdigest()


def build_archive(
    project_root: Path,
    limits: DbtProjectBundleLimits,
    *,
    package_environment: Mapping[str, str],
) -> DbtProjectBundleArtifact:
    root = Path(project_root).absolute()
    root_descriptor = open_directory(root)
    try:
        root_identity = identity(os.fstat(root_descriptor))
        source_files = _scan_source(
            root_descriptor,
            limits=limits,
            package_environment=package_environment,
        )
        output = _BoundedBuffer(limits.max_archive_bytes)
        inventory: list[DbtProjectFile] = []
        try:
            with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
                    inventory.extend(_add_source_file(archive, root_descriptor, item) for item in source_files)
        except _ArchiveLimitExceeded as exc:
            raise limit_error("dbt project archive exceeds the compressed byte limit") from exc
        if identity(os.fstat(root_descriptor)) != root_identity or not path_matches(root, root_identity):
            raise source_changed()
        if (
            _scan_source(
                root_descriptor,
                limits=limits,
                package_environment=package_environment,
            )
            != source_files
        ):
            raise source_changed()
    finally:
        os.close(root_descriptor)
    archive_bytes = output.getvalue()
    bundle = DbtProjectBundle(
        archive_sha256=sha256_bytes(archive_bytes),
        archive_bytes=len(archive_bytes),
        extracted_bytes=sum(item.bytes for item in inventory),
        files=tuple(inventory),
    )
    return DbtProjectBundleArtifact(bundle=bundle, archive=archive_bytes)


def _scan_source(
    root_descriptor: int,
    *,
    limits: DbtProjectBundleLimits,
    package_environment: Mapping[str, str],
) -> tuple[_SourceFile, ...]:
    files: list[_SourceFile] = []
    directories: set[str] = set()
    topology = _project_topology(
        root_descriptor,
        limits,
        package_environment=package_environment,
    )
    _scan_directory(root_descriptor, PurePosixPath(), files, directories, limits, topology)
    paths = [item.path for item in files]
    require_collision_free(paths)
    files.sort(key=lambda item: item.path)
    if not files or "dbt_project.yml" not in paths:
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project must contain dbt_project.yml")
    return tuple(files)


def _scan_directory(
    descriptor: int,
    prefix: PurePosixPath,
    files: list[_SourceFile],
    directories: set[str],
    limits: DbtProjectBundleLimits,
    topology: _ProjectTopology,
) -> None:
    try:
        entries = sorted(os.scandir(descriptor), key=lambda item: item.name)
    except OSError as exc:
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project cannot be scanned safely") from exc
    for entry in entries:
        relative = prefix / entry.name
        if len(relative.parts) > 64:
            raise limit_error("dbt project exceeds the path-depth limit")
        metadata = entry.stat(follow_symlinks=False)
        mode = metadata.st_mode
        if stat.S_ISLNK(mode):
            raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project contains a symbolic link")
        if stat.S_ISDIR(mode):
            if entry.name.casefold() in _EXCLUDED_DIRECTORIES:
                continue
            if not topology.should_descend(relative):
                continue
            directories.add(relative.as_posix())
            if len(directories) > limits.max_files:
                raise limit_error("dbt project exceeds the directory-count limit")
            child = open_at(descriptor, entry.name, directory=True)
            try:
                if identity(os.fstat(child)) != identity(metadata):
                    raise source_changed()
                _scan_directory(child, relative, files, directories, limits, topology)
            finally:
                os.close(child)
            continue
        if not stat.S_ISREG(mode):
            raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project contains a forbidden entry type")
        if _sensitive_file(entry.name):
            raise DbtPublishingError(
                "DPONE_DBT_BUNDLE_INVALID",
                "dbt project contains a forbidden credential or private-key file",
            )
        if _excluded_file(entry.name):
            continue
        if not topology.includes_file(relative):
            continue
        _require_ustar_path(relative.as_posix())
        if metadata.st_size > limits.max_file_bytes:
            raise limit_error("dbt project file exceeds the per-file byte limit")
        files.append(_SourceFile(relative.as_posix(), metadata.st_size, identity(metadata)))
        if len(files) > limits.max_files or sum(item.size for item in files) > limits.max_extracted_bytes:
            raise limit_error("dbt project exceeds its file-count or extracted-byte limit")


def _add_source_file(
    archive: tarfile.TarFile,
    root_descriptor: int,
    source: _SourceFile,
) -> DbtProjectFile:
    descriptor = open_relative_file(root_descriptor, PurePosixPath(source.path))
    try:
        if identity(os.fstat(descriptor)) != source.identity:
            raise source_changed()
        reader = _DigestReader(descriptor)
        member = tarfile.TarInfo(source.path)
        member.size = source.size
        member.mode = 0o644
        member.mtime = member.uid = member.gid = 0
        member.uname = member.gname = ""
        archive.addfile(member, reader)
        if reader.bytes_read != source.size or identity(os.fstat(descriptor)) != source.identity:
            raise source_changed()
        return DbtProjectFile(path=source.path, sha256=reader.sha256, bytes=source.size)
    finally:
        os.close(descriptor)


def _require_ustar_path(path: str) -> None:
    """Reject paths that would require unbounded PAX/GNU extension metadata."""

    try:
        tarfile.TarInfo(path).tobuf(format=tarfile.USTAR_FORMAT)
    except (UnicodeError, ValueError) as exc:
        raise DbtPublishingError(
            "DPONE_DBT_BUNDLE_INVALID",
            "dbt project path cannot be represented safely in the canonical archive",
        ) from exc


def _excluded_file(name: str) -> bool:
    folded = name.casefold()
    return (
        folded in _EXCLUDED_FILES
        or folded == ".env"
        or folded.startswith(".env.")
        or folded.endswith((".tmp", ".temp", ".swp", ".pyc", "~"))
    )


def _project_topology(
    root_descriptor: int,
    limits: DbtProjectBundleLimits,
    *,
    package_environment: Mapping[str, str],
) -> _ProjectTopology:
    project = read_root_yaml_mapping(
        root_descriptor,
        "dbt_project.yml",
        maximum=limits.max_file_bytes,
        error_code="DPONE_DBT_BUNDLE_INVALID",
    )
    roots: list[PurePosixPath] = []
    for key, defaults in _PROJECT_PATHS.items():
        roots.extend(_configured_paths(project.get(key, defaults), key))
    package_roots = _configured_paths(
        project.get("packages-install-path", "dbt_packages"),
        "packages-install-path",
    )
    declaration_files = tuple(
        name for name in ("packages.yml", "dependencies.yml") if root_regular_file(root_descriptor, name)
    )
    package_declarations = bool(declaration_files)
    require_resolved_packages(
        root_descriptor,
        package_roots,
        declaration_files=declaration_files,
        maximum=limits.max_file_bytes,
        package_environment=package_environment,
    )
    roots.extend(package_roots)
    canonical = tuple(sorted(set(roots), key=lambda item: item.as_posix()))
    require_collision_free([item.as_posix() for item in canonical])
    root_files = _ROOT_FILES if package_declarations else _ROOT_FILES - {"package-lock.yml"}
    return _ProjectTopology(canonical, root_files)


def _configured_paths(value: object, field: str) -> tuple[PurePosixPath, ...]:
    values: Sequence[object]
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        values = value
    else:
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", f"{field} must contain relative directory paths")
    paths: list[PurePosixPath] = []
    for raw in values:
        if not isinstance(raw, str):
            raise DbtPublishingError(
                "DPONE_DBT_BUNDLE_INVALID",
                f"{field} must contain relative directory paths",
            )
        path = PurePosixPath(raw)
        if (
            not raw
            or raw != raw.strip()
            or "\\" in raw
            or path.is_absolute()
            or path.as_posix() != raw
            or path == PurePosixPath(".")
            or any(part in {"", ".", ".."} for part in path.parts)
        ):
            raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", f"{field} contains an unsafe directory path")
        paths.append(path)
    return tuple(paths)


def _sensitive_file(name: str) -> bool:
    folded = name.casefold()
    return folded in _SENSITIVE_FILES or folded.endswith(_SENSITIVE_SUFFIXES)


__all__ = ["build_archive"]
