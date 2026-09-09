"""CAS-protected migration of the generated Airflow provider loader."""

from __future__ import annotations

import hashlib
import os
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path

from dpone.manifest.confined_mutations import ConfinedMutationError, replace_file_if_digest
from dpone.manifest.project_root import ProjectRootIdentity
from dpone.readiness.airflow_scaffold_apply import ScaffoldFileSystem
from dpone.readiness.airflow_self_service_templates import known_legacy_airflow_loader_fingerprint

AIRFLOW_DAG_FILE = "dags/dpone.py"
_AIRFLOW_LOADER_MAX_BYTES = 64 * 1024
_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)


class AirflowLoaderMigrationConflict(OSError):
    """The generated loader changed before a safe upgrade could commit."""

    def __init__(
        self,
        message: str,
        *,
        committed: bool = False,
        cleanup_required: bool = False,
        recovery_artifacts: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.committed = committed
        self.cleanup_required = cleanup_required
        self.recovery_artifacts = recovery_artifacts


@dataclass(frozen=True, slots=True)
class AirflowLoaderUpgrade:
    content: bytes
    expected_sha256: str


@dataclass(frozen=True, slots=True)
class AirflowLoaderMigrationReceipt:
    """Truthful loader mutation and recovery state."""

    committed: bool
    restored: bool | None = None
    cleanup_required: bool = False
    recovery_artifacts: tuple[str, ...] = ()
    issues: tuple[str, ...] = ()


class AirflowLoaderMigrator:
    """Plan and CAS-apply upgrades for exact dpone-generated loader bytes."""

    def __init__(
        self,
        root: Path,
        *,
        root_identity: ProjectRootIdentity | None = None,
    ) -> None:
        self._root = root
        self._root_identity = root_identity

    def plan(self) -> AirflowLoaderUpgrade | None:
        try:
            existing = ScaffoldFileSystem(
                self._root,
                root_identity=self._root_identity,
            ).read(Path(AIRFLOW_DAG_FILE))
        except OSError:
            return None
        if existing is None or not existing.complete:
            return None
        fingerprint = known_legacy_airflow_loader_fingerprint(existing.content)
        if fingerprint is None:
            return None
        return AirflowLoaderUpgrade(content=existing.content, expected_sha256=fingerprint)

    def apply(self, upgrade: AirflowLoaderUpgrade, desired: bytes) -> AirflowLoaderMigrationReceipt:
        return self._replace(expected_sha256=upgrade.expected_sha256, replacement=desired)

    def rollback(self, upgrade: AirflowLoaderUpgrade, desired: bytes) -> AirflowLoaderMigrationReceipt:
        expected_sha256 = "sha256:" + hashlib.sha256(desired).hexdigest()
        try:
            receipt = self._replace(expected_sha256=expected_sha256, replacement=upgrade.content)
        except AirflowLoaderMigrationConflict as exc:
            return AirflowLoaderMigrationReceipt(
                committed=exc.committed,
                restored=False,
                cleanup_required=exc.cleanup_required,
                recovery_artifacts=exc.recovery_artifacts,
                issues=(str(exc),),
            )
        return AirflowLoaderMigrationReceipt(
            committed=receipt.committed,
            restored=receipt.committed and not receipt.cleanup_required,
            cleanup_required=receipt.cleanup_required,
            recovery_artifacts=receipt.recovery_artifacts,
            issues=receipt.issues,
        )

    def _replace(
        self,
        *,
        expected_sha256: str,
        replacement: bytes,
    ) -> AirflowLoaderMigrationReceipt:
        root_fd: int | None = None
        parent_fd: int | None = None
        temporary: str | None = None
        preserve_temporary = False
        try:
            root_fd = os.open(self._root, _DIRECTORY_FLAGS)
            if self._root_identity is not None and not self._root_identity.matches(os.fstat(root_fd)):
                raise AirflowLoaderMigrationConflict("Project root changed during loader migration.")
            parent_fd = os.open("dags", _DIRECTORY_FLAGS, dir_fd=root_fd)
            metadata = os.stat("dpone.py", dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISREG(metadata.st_mode):
                raise AirflowLoaderMigrationConflict("Airflow loader is not a regular file.")
            temporary = f".dpone.py.dpone-upgrade-{secrets.token_hex(8)}"
            descriptor = os.open(temporary, _WRITE_FLAGS, 0o600, dir_fd=parent_fd)
            try:
                os.fchmod(descriptor, stat.S_IMODE(metadata.st_mode))
                _write_all(descriptor, replacement)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            outcome = replace_file_if_digest(
                parent_fd,
                "dpone.py",
                temporary,
                expected_sha256=expected_sha256,
                max_bytes=_AIRFLOW_LOADER_MAX_BYTES,
            )
            preserve_temporary = outcome.cleanup_required
            recovery_artifacts = _existing_loader_recovery_artifacts(
                parent_fd,
                outcome.recovery_name,
                temporary if preserve_temporary else None,
            )
            return AirflowLoaderMigrationReceipt(
                committed=outcome.committed,
                cleanup_required=outcome.cleanup_required,
                recovery_artifacts=recovery_artifacts,
                issues=(("Airflow loader transaction cleanup requires recovery.",) if outcome.cleanup_required else ()),
            )
        except ConfinedMutationError as exc:
            preserve_temporary = exc.committed or exc.cleanup_required
            recovery_artifacts = (
                _existing_loader_recovery_artifacts(
                    parent_fd,
                    exc.recovery_name,
                    temporary if preserve_temporary else None,
                )
                if parent_fd is not None
                else ()
            )
            recovery = f" Preserved conflicting bytes as {exc.recovery_name}." if exc.recovery_name else ""
            raise AirflowLoaderMigrationConflict(
                f"Airflow loader changed while its generated upgrade was applied.{recovery}",
                committed=exc.committed,
                cleanup_required=exc.cleanup_required,
                recovery_artifacts=recovery_artifacts,
            ) from exc
        except AirflowLoaderMigrationConflict:
            raise
        except OSError as exc:
            raise AirflowLoaderMigrationConflict(
                "Airflow loader could not be upgraded without risking existing bytes."
            ) from exc
        finally:
            if temporary is not None and parent_fd is not None and not preserve_temporary:
                try:
                    os.unlink(temporary, dir_fd=parent_fd)
                except FileNotFoundError:
                    pass
            if parent_fd is not None:
                os.close(parent_fd)
            if root_fd is not None:
                os.close(root_fd)


def _write_all(descriptor: int, content: bytes) -> None:
    remaining = memoryview(content)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("Airflow loader upgrade write made no progress.")
        remaining = remaining[written:]


def _existing_loader_recovery_artifacts(parent_fd: int, *names: str | None) -> tuple[str, ...]:
    existing: list[str] = []
    for name in dict.fromkeys(item for item in names if item is not None):
        try:
            os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        existing.append((Path("dags") / name).as_posix())
    return tuple(existing)


__all__ = [
    "AIRFLOW_DAG_FILE",
    "AirflowLoaderMigrationConflict",
    "AirflowLoaderMigrationReceipt",
    "AirflowLoaderMigrator",
    "AirflowLoaderUpgrade",
]
