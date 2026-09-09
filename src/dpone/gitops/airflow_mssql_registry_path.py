"""Confined path resolution for env-bound MSSQL connection registries."""

from __future__ import annotations

import errno
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from dpone.gitops.airflow_mssql_asset_authority import AssetUriIssue

MSSQL_REGISTRY_PATH_INVALID = "DPONE_MSSQL_REGISTRY_PATH_INVALID"
MSSQL_REGISTRY_ENVIRONMENT_MISMATCH = "DPONE_MSSQL_REGISTRY_ENVIRONMENT_MISMATCH"
MSSQL_REGISTRY_SOURCE_AMBIGUOUS = "DPONE_MSSQL_REGISTRY_SOURCE_AMBIGUOUS"

# Soft upper bound for one connection-registry YAML (authority metadata only).
MAX_MSSQL_REGISTRY_BYTES = 1024 * 1024

_REGISTRY_RELATIVE_CANDIDATES = (
    Path(".dpone") / "registry" / "connection-registries",
    Path("platform") / "connection-registries",
)


@dataclass(frozen=True, slots=True)
class MssqlRegistryPathResolution:
    """One confined registry path or structured blockers."""

    path: Path | None
    relative_path: str | None
    issues: tuple[AssetUriIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return self.path is not None and not self.issues


def connection_registry_path_for_env(repo_root: Path, env: str) -> Path | None:
    """Resolve the registry path for ``env`` or ``None`` when absent/invalid.

    Prefer :func:`resolve_connection_registry_path` when structured blockers are
    required. This helper preserves the historical ``Path | None`` shape for
    call sites that only need presence.
    """

    resolution = resolve_connection_registry_path(repo_root, env)
    return resolution.path if resolution.ok else None


def resolve_connection_registry_path(repo_root: Path, env: str) -> MssqlRegistryPathResolution:
    """Resolve one regular, non-symlinked registry file inside ``repo_root``."""

    env_name = (env or "").strip()
    if not env_name or any(part in env_name for part in ("/", "\\", "..")):
        return MssqlRegistryPathResolution(
            path=None,
            relative_path=None,
            issues=(
                AssetUriIssue(
                    code=MSSQL_REGISTRY_PATH_INVALID,
                    message="MSSQL connection registry environment name is invalid",
                    path=env_name or None,
                ),
            ),
        )
    try:
        root = repo_root.resolve(strict=True)
    except OSError:
        return MssqlRegistryPathResolution(
            path=None,
            relative_path=None,
            issues=(
                AssetUriIssue(
                    code=MSSQL_REGISTRY_PATH_INVALID,
                    message="MSSQL connection registry repo_root is not resolvable",
                    path=str(repo_root),
                ),
            ),
        )
    found: list[tuple[Path, str]] = []
    blockers: list[AssetUriIssue] = []
    for relative_dir in _REGISTRY_RELATIVE_CANDIDATES:
        relative = (relative_dir / f"{env_name}.yaml").as_posix()
        candidate = root / relative
        confined = _confined_regular_file(root, candidate, relative_path=relative)
        if confined.issue is not None:
            if confined.present:
                blockers.append(confined.issue)
            continue
        if confined.path is None:
            continue
        found.append((confined.path, relative))
    if len(found) > 1:
        sources = ", ".join(item[1] for item in found)
        return MssqlRegistryPathResolution(
            path=None,
            relative_path=None,
            issues=(
                AssetUriIssue(
                    code=MSSQL_REGISTRY_SOURCE_AMBIGUOUS,
                    message=(
                        "MSSQL connection registry source is ambiguous; both "
                        f"locations exist for env {env_name!r}: {sources}. "
                        "Keep exactly one source of truth."
                    ),
                    path=env_name,
                ),
            ),
        )
    if blockers:
        # Any present-but-invalid alternate source blocks even when another
        # candidate is valid — operators must keep a single clean SoT.
        return MssqlRegistryPathResolution(path=None, relative_path=None, issues=tuple(blockers))
    if not found:
        return MssqlRegistryPathResolution(path=None, relative_path=None, issues=())
    path, relative = found[0]
    return MssqlRegistryPathResolution(path=path, relative_path=relative, issues=())


@dataclass(frozen=True, slots=True)
class _ConfinedCandidate:
    path: Path | None
    present: bool
    issue: AssetUriIssue | None


def _confined_regular_file(root: Path, candidate: Path, *, relative_path: str) -> _ConfinedCandidate:
    """Reject symlink components, escapes, and non-regular files (no-follow)."""

    current = root
    parts = Path(relative_path).parts
    for index, part in enumerate(parts):
        current = current / part
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            return _ConfinedCandidate(path=None, present=False, issue=None)
        except OSError as exc:
            return _ConfinedCandidate(
                path=None,
                present=True,
                issue=AssetUriIssue(
                    code=MSSQL_REGISTRY_PATH_INVALID,
                    message=f"MSSQL connection registry path is not readable: {exc.strerror or exc}",
                    path=relative_path,
                ),
            )
        if stat.S_ISLNK(metadata.st_mode):
            return _ConfinedCandidate(
                path=None,
                present=True,
                issue=AssetUriIssue(
                    code=MSSQL_REGISTRY_PATH_INVALID,
                    message="MSSQL connection registry path must not contain symlink components",
                    path=relative_path,
                ),
            )
        is_last = index == len(parts) - 1
        if is_last:
            if not stat.S_ISREG(metadata.st_mode):
                return _ConfinedCandidate(
                    path=None,
                    present=True,
                    issue=AssetUriIssue(
                        code=MSSQL_REGISTRY_PATH_INVALID,
                        message="MSSQL connection registry must be a regular file",
                        path=relative_path,
                    ),
                )
        elif not stat.S_ISDIR(metadata.st_mode):
            return _ConfinedCandidate(
                path=None,
                present=True,
                issue=AssetUriIssue(
                    code=MSSQL_REGISTRY_PATH_INVALID,
                    message="MSSQL connection registry path escapes through a non-directory component",
                    path=relative_path,
                ),
            )
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        return _ConfinedCandidate(
            path=None,
            present=True,
            issue=AssetUriIssue(
                code=MSSQL_REGISTRY_PATH_INVALID,
                message="MSSQL connection registry path could not be resolved inside the repository",
                path=relative_path,
            ),
        )
    if not resolved.is_relative_to(root):
        return _ConfinedCandidate(
            path=None,
            present=True,
            issue=AssetUriIssue(
                code=MSSQL_REGISTRY_PATH_INVALID,
                message="MSSQL connection registry path escapes the repository root",
                path=relative_path,
            ),
        )
    return _ConfinedCandidate(path=resolved, present=True, issue=None)


def read_mssql_registry_bytes(path: Path, *, max_bytes: int = MAX_MSSQL_REGISTRY_BYTES) -> bytes:
    """Bounded no-follow read of one registry file."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        if getattr(exc, "errno", None) in {errno.ELOOP, errno.EINVAL}:
            raise OSError(errno.ELOOP, "symlink rejected for MSSQL connection registry read") from exc
        raise
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError(errno.EINVAL, "MSSQL connection registry is not a regular file")
        if metadata.st_size > max_bytes:
            raise OSError(errno.EFBIG, f"MSSQL connection registry exceeds {max_bytes} byte budget")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(65536, max_bytes - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise OSError(errno.EFBIG, f"MSSQL connection registry exceeds {max_bytes} byte budget")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


__all__ = [
    "MAX_MSSQL_REGISTRY_BYTES",
    "MSSQL_REGISTRY_ENVIRONMENT_MISMATCH",
    "MSSQL_REGISTRY_PATH_INVALID",
    "MSSQL_REGISTRY_SOURCE_AMBIGUOUS",
    "MssqlRegistryPathResolution",
    "connection_registry_path_for_env",
    "read_mssql_registry_bytes",
    "resolve_connection_registry_path",
]
