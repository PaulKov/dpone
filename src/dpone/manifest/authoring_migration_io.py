"""Confined authoring-source reads and checksum-guarded migration writes."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.confined_files import ConfinedFileSnapshot


import hashlib
import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.manifest.authoring_migration_transaction import (
    MigrationFragmentState,
    fragment_state_is_current,
    read_migration_file,
    rollback_owned_fragment,
)
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError
from dpone.manifest.confined_mutations import (
    ConfinedMutationError,
    ConfinedReplaceOutcome,
    replace_file_if_digest,
)
from dpone.manifest.pipeline_source_reference import (
    PipelineSourceReferenceError,
    resolve_pipeline_source_relative,
)

_LIMITS = BoundedYamlLimits()
_PATH_INVALID = "DPONE_AUTHORING_MIGRATION_PATH_INVALID"
_SOURCE_CHANGED = "DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED"
_SOURCE_CHANGED_MESSAGE = "Pipeline source changed while the migration was being applied."
_SOURCE_PATH_MESSAGE = "Pipeline source is missing, unsafe, or outside the project root."
_OPTIONAL_PATH_MESSAGE = "Migration candidate path is unsafe or unavailable."


class AuthoringMigrationIoError(OSError):
    def __init__(self, code: str, message: str, *, recovery_path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.recovery_path = recovery_path


@dataclass(frozen=True, slots=True)
class AuthoringSourceSnapshot:
    path: Path
    relative_path: str
    content: bytes
    payload: dict[str, Any]
    sha256: str


@dataclass(frozen=True, slots=True)
class AuthoringApplyOutcome:
    source_sha256: str
    fragment_created: bool


class AuthoringMigrationFileSystem:
    """Keep filesystem mechanics outside migration policy."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve(strict=True)

    def read_source(self, target: str | Path) -> AuthoringSourceSnapshot:
        relative = self._resolve_target(target)
        try:
            content = self._read_relative(relative)
            payload = load_bounded_yaml(content, limits=_LIMITS)
        except OSError as exc:
            if isinstance(exc, AuthoringMigrationIoError) and exc.code == _SOURCE_CHANGED:
                raise
            raise AuthoringMigrationIoError(_PATH_INVALID, _SOURCE_PATH_MESSAGE) from exc
        except BoundedYamlError as exc:
            raise AuthoringMigrationIoError(
                "DPONE_AUTHORING_MIGRATION_SOURCE_INVALID",
                "Pipeline source is not bounded, unique-key UTF-8 YAML.",
            ) from exc
        if not isinstance(payload, dict):
            raise AuthoringMigrationIoError(
                "DPONE_AUTHORING_MIGRATION_SOURCE_INVALID",
                "Pipeline source must be a YAML object.",
            )
        return AuthoringSourceSnapshot(
            path=self.root.joinpath(*PurePosixPath(relative).parts),
            relative_path=relative,
            content=content,
            payload=payload,
            sha256=_sha256(content),
        )

    def read_optional(self, relative_path: str) -> bytes | None:
        try:
            return self._read_relative(relative_path)
        except OSError as exc:
            if isinstance(exc, FileNotFoundError) or isinstance(exc.__cause__, FileNotFoundError):
                return None
            if isinstance(exc, AuthoringMigrationIoError) and exc.code == _SOURCE_CHANGED:
                raise
            raise AuthoringMigrationIoError(_PATH_INVALID, _OPTIONAL_PATH_MESSAGE) from exc

    def apply(
        self,
        *,
        source: AuthoringSourceSnapshot,
        desired_source: bytes,
        fragment_relative: str | None,
        desired_fragment: bytes | None,
    ) -> AuthoringApplyOutcome:
        source_parent = PurePosixPath(source.relative_path).parent.as_posix()
        fragment_name: str | None = None
        if fragment_relative is not None:
            fragment_path = PurePosixPath(fragment_relative)
            if fragment_path.parent.as_posix() != source_parent or desired_fragment is None:
                raise AuthoringMigrationIoError(
                    _PATH_INVALID,
                    "Migration fragment must be a sibling of the primary source.",
                )
            fragment_name = fragment_path.name

        with self._open_parent(source.relative_path) as parent_fd:
            current = _read_fd_file(parent_fd, PurePosixPath(source.relative_path).name)
            if _sha256(current.content) != source.sha256:
                raise AuthoringMigrationIoError(_SOURCE_CHANGED, _SOURCE_CHANGED_MESSAGE)
            source_mode = stat.S_IMODE(
                os.stat(
                    PurePosixPath(source.relative_path).name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                ).st_mode
            )

            fragment_state: MigrationFragmentState | None = None
            if fragment_name is not None and desired_fragment is not None:
                fragment_state = _create_or_compare(
                    parent_fd,
                    fragment_name,
                    desired_fragment,
                    file_mode=source_mode,
                )
            try:
                replace_state = _replace_file(
                    parent_fd,
                    PurePosixPath(source.relative_path).name,
                    desired_source,
                    file_mode=source_mode,
                    expected_sha256=source.sha256,
                    fragment_name=fragment_name,
                    fragment_state=fragment_state,
                )
                if not replace_state.committed:
                    raise AuthoringMigrationIoError(
                        _SOURCE_CHANGED,
                        _SOURCE_CHANGED_MESSAGE,
                        recovery_path=replace_state.recovery_name,
                    )
            except Exception as exc:
                rollback_recovery = rollback_owned_fragment(
                    parent_fd,
                    fragment_name,
                    fragment_state,
                    max_bytes=_LIMITS.max_bytes,
                )
                if isinstance(exc, AuthoringMigrationIoError):
                    raise _with_recovery_paths(
                        exc,
                        source.relative_path,
                        rollback_recovery,
                    ) from exc
                raise
            if replace_state.cleanup_required:
                error = AuthoringMigrationIoError(
                    "DPONE_AUTHORING_MIGRATION_APPLY_FAILED",
                    "Pipeline source was committed, but transaction cleanup requires recovery.",
                    recovery_path=replace_state.recovery_name,
                )
                raise _with_recovery_paths(error, source.relative_path)
            _fsync_directory(parent_fd)
        fragment_created = fragment_state is not None and fragment_state.owned is not None
        return AuthoringApplyOutcome(source_sha256=_sha256(desired_source), fragment_created=fragment_created)

    def _resolve_target(self, target: str | Path) -> str:
        try:
            return resolve_pipeline_source_relative(self.root, target)
        except PipelineSourceReferenceError as exc:
            raise AuthoringMigrationIoError(
                _PATH_INVALID,
                "Pipeline source must stay inside the project root.",
            ) from exc

    def _read_relative(self, relative_path: str) -> bytes:
        with self._open_parent(relative_path) as parent_fd:
            return _read_fd_file(parent_fd, PurePosixPath(relative_path).name).content

    @contextmanager
    def _open_parent(self, relative_path: str) -> Iterator[int]:
        path = PurePosixPath(relative_path)
        if not relative_path or not path.parts or "\\" in relative_path or path.is_absolute() or ".." in path.parts:
            raise AuthoringMigrationIoError(_PATH_INVALID, "Pipeline source path is invalid.")
        parts = path.parts
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        try:
            root_fd = os.open(self.root, flags | nofollow)
        except OSError as exc:
            raise AuthoringMigrationIoError(_PATH_INVALID, "Project root could not be opened safely.") from exc
        parent_fd = root_fd
        try:
            try:
                for part in parts[:-1]:
                    next_fd = os.open(part, flags | nofollow, dir_fd=parent_fd)
                    if parent_fd != root_fd:
                        os.close(parent_fd)
                    parent_fd = next_fd
            except OSError as exc:
                raise AuthoringMigrationIoError(
                    _PATH_INVALID,
                    "Pipeline parent directory could not be opened safely.",
                ) from exc
            yield parent_fd
        finally:
            if parent_fd != root_fd:
                os.close(parent_fd)
            os.close(root_fd)


def _read_fd_file(parent_fd: int, name: str) -> ConfinedFileSnapshot:
    try:
        return read_migration_file(parent_fd, name, max_bytes=_LIMITS.max_bytes)
    except ConfinedFileError as exc:
        if exc.code == "file_not_found":
            raise FileNotFoundError(name) from exc
        code = _SOURCE_CHANGED if exc.code == "source_changed" else "DPONE_AUTHORING_MIGRATION_SOURCE_INVALID"
        raise AuthoringMigrationIoError(code, str(exc)) from exc


def _create_or_compare(parent_fd: int, name: str, desired: bytes, *, file_mode: int) -> MigrationFragmentState:
    try:
        existing = _read_fd_file(parent_fd, name)
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if existing.content == desired:
            return MigrationFragmentState.existing(existing)
        raise AuthoringMigrationIoError(
            "DPONE_AUTHORING_MIGRATION_FILE_CONFLICT",
            "Migration fragment exists with different content.",
        )
    temporary = _write_temporary(parent_fd, name, desired, file_mode=file_mode)
    try:
        prepared = _read_fd_file(parent_fd, temporary)
        try:
            os.link(temporary, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd, follow_symlinks=False)
        except FileExistsError:
            existing = _read_fd_file(parent_fd, name)
            if existing.content != desired:
                raise AuthoringMigrationIoError(
                    "DPONE_AUTHORING_MIGRATION_FILE_CONFLICT",
                    "Migration fragment was created concurrently with different content.",
                )
            return MigrationFragmentState.existing(existing)
        return MigrationFragmentState.created(prepared)
    finally:
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass


def _replace_file(
    parent_fd: int,
    name: str,
    desired: bytes,
    *,
    file_mode: int,
    expected_sha256: str,
    fragment_name: str | None,
    fragment_state: MigrationFragmentState | None,
) -> ConfinedReplaceOutcome:
    temporary = _write_temporary(parent_fd, name, desired, file_mode=file_mode)
    try:
        if not fragment_state_is_current(
            parent_fd,
            fragment_name,
            fragment_state,
            max_bytes=_LIMITS.max_bytes,
        ):
            raise AuthoringMigrationIoError(
                "DPONE_AUTHORING_MIGRATION_FILE_CONFLICT",
                "Migration fragment changed before primary source activation.",
            )
        try:
            return replace_file_if_digest(
                parent_fd,
                name,
                temporary,
                expected_sha256=expected_sha256,
                max_bytes=_LIMITS.max_bytes,
            )
        except ConfinedMutationError as exc:
            if exc.committed:
                return ConfinedReplaceOutcome(
                    committed=True,
                    cleanup_required=True,
                    recovery_name=exc.recovery_name,
                )
            raise AuthoringMigrationIoError(
                _SOURCE_CHANGED,
                _SOURCE_CHANGED_MESSAGE,
                recovery_path=exc.recovery_name,
            ) from exc
    finally:
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass


def _write_temporary(parent_fd: int, name: str, content: bytes, *, file_mode: int) -> str:
    temporary = f".{name}.dpone-migrate-{secrets.token_hex(8)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(temporary, flags, 0o600, dir_fd=parent_fd)
    try:
        os.fchmod(descriptor, file_mode)
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    except Exception:
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(descriptor)
    return temporary


def _with_recovery_paths(
    error: AuthoringMigrationIoError,
    source_relative: str,
    additional_name: str | None = None,
) -> AuthoringMigrationIoError:
    paths = tuple(
        path
        for name in (error.recovery_path, additional_name)
        if name is not None and (path := _project_recovery_path(source_relative, name)) is not None
    )
    if not paths:
        return AuthoringMigrationIoError(error.code, str(error))
    unique_paths = tuple(dict.fromkeys(paths))
    return AuthoringMigrationIoError(
        error.code,
        f"{error} Recovery artifact(s): {', '.join(unique_paths)}.",
        recovery_path=unique_paths[0],
    )


def _project_recovery_path(source_relative: str, name: str) -> str | None:
    path = PurePosixPath(name)
    if not name or "\\" in name or path.is_absolute() or len(path.parts) != 1 or path.name != name:
        return None
    return PurePosixPath(source_relative).with_name(name).as_posix()


def _fsync_directory(parent_fd: int) -> None:
    try:
        os.fsync(parent_fd)
    except OSError:
        return


def _sha256(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


__all__ = [
    "AuthoringApplyOutcome",
    "AuthoringMigrationFileSystem",
    "AuthoringMigrationIoError",
    "AuthoringSourceSnapshot",
]
