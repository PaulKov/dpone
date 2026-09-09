"""Win32 handle lease for path-confined runtime artifact writes."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from pathlib import Path
from shutil import disk_usage

from dpone.runtime.pinned_directory_win32_api import (
    FILE_ATTRIBUTE_DIRECTORY,
    FILE_ATTRIBUTE_REPARSE_POINT,
    Win32DirectoryApi,
    Win32DirectoryApiProtocol,
    WindowsDirectoryIdentity,
)
from dpone.runtime.pinned_file_consumer import PinnedFileConsumer


class WindowsPinnedDirectoryLease:
    """Hold non-delete-sharing handles for every resolved path component.

    Win32 has no Python ``openat``/``unlinkat`` API. Holding the complete
    component chain without ``FILE_SHARE_DELETE`` prevents rename, removal,
    junction replacement, or ancestor replacement while path-based file
    operations run. Every handle is also checked for directory type, reparse
    status, and its kernel-reported final path.
    """

    __slots__ = ("path", "device", "inode", "_api", "_handles")

    def __init__(
        self,
        path: Path,
        handles: tuple[int, ...],
        *,
        api: Win32DirectoryApiProtocol,
        identity: WindowsDirectoryIdentity,
    ) -> None:
        self.path = path
        self.device = identity.volume
        self.inode = identity.file_index
        self._api = api
        self._handles = handles

    @classmethod
    def open(
        cls,
        path: Path,
        *,
        api: Win32DirectoryApiProtocol | None = None,
    ) -> WindowsPinnedDirectoryLease:
        """Lock and verify the absolute directory chain from anchor to leaf."""

        backend = api or Win32DirectoryApi()
        handles: list[int] = []
        try:
            for component in _directory_chain(path):
                handle = backend.open_directory(component)
                handles.append(handle)
                identity = backend.identity(handle)
                if not identity.attributes & FILE_ATTRIBUTE_DIRECTORY:
                    raise OSError("win32_pinned_path_component_not_directory")
                if identity.attributes & FILE_ATTRIBUTE_REPARSE_POINT:
                    raise OSError("win32_pinned_path_component_reparse_point")
                backend.require_final_path(handle, component)
            if not handles:  # pragma: no cover - an absolute path always has an anchor.
                raise OSError("win32_pinned_directory_chain_empty")
            identity = backend.identity(handles[-1])
            return cls(path, tuple(handles), api=backend, identity=identity)
        except BaseException as primary:
            _close_preserving_primary(backend, handles, primary)
            raise

    def require_identity(self) -> None:
        """Re-prove the live leaf handle and its immutable pathname."""

        handle = self._leaf_handle()
        identity = self._api.identity(handle)
        if (identity.volume, identity.file_index) != (self.device, self.inode):
            raise OSError("win32_pinned_directory_identity_changed")
        self._api.require_final_path(handle, self.path)

    def create_file(self, entry_name: str) -> tuple[int, str]:
        """Create one exclusive non-inheritable file under the locked chain."""

        self._leaf_handle()
        path = self.path / entry_name
        descriptor = self._api.create_exclusive_file(path)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):  # pragma: no cover - O_EXCL invariant.
                raise OSError("created_spool_is_not_regular")
            return descriptor, str(path)
        except BaseException:
            os.close(descriptor)
            try:
                os.unlink(path)
            except OSError:
                pass
            raise

    def pin_file_for_consumer(
        self,
        entry_name: str,
        *,
        writer_descriptor: int,
    ) -> PinnedFileConsumer:
        """Duplicate a read identity whose sharing contract blocks replacement."""

        self._leaf_handle()
        source_path = self.path / entry_name
        expected = self._api.descriptor_identity(writer_descriptor)
        handle = self._api.duplicate_read_lock(writer_descriptor)
        try:
            observed = self._api.identity(handle)
            if observed.attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT):
                raise OSError("win32_pinned_consumer_not_regular")
            if not observed.same_file(expected):
                raise OSError("win32_pinned_consumer_identity_changed")
            self._api.require_final_path(handle, source_path)
        except BaseException:
            self._api.close(handle)
            raise
        resource = _WindowsPinnedFileResource(
            api=self._api,
            handle=handle,
            expected=observed,
            path=source_path,
        )
        return PinnedFileConsumer(
            source_path=str(source_path),
            consumer_path=str(source_path),
            inherited_file_descriptors=(),
            prepare_callback=resource.require_identity,
            cleanup_callback=resource.cleanup,
            release_callback=resource.close,
        )

    def pending_file_cleanup(
        self,
        entry_name: str,
        *,
        writer_descriptor: int,
    ) -> Callable[[], None]:
        """Capture exact cleanup authority before serialization can fail."""

        self._leaf_handle()
        expected = self._api.descriptor_identity(writer_descriptor)
        path = self.path / entry_name

        def cleanup() -> None:
            self._leaf_handle()
            self._api.delete_exact_file(path, expected=expected)

        return cleanup

    def unlink(self, entry_name: str, *, missing_ok: bool) -> None:
        """Delete one leaf while every parent component remains locked."""

        self._leaf_handle()
        try:
            os.unlink(self.path / entry_name)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def free_bytes(self) -> int:
        """Measure free bytes without permitting the path chain to move."""

        self._leaf_handle()
        return int(disk_usage(self.path).free)

    def close(self) -> None:
        """Release every component handle in leaf-to-anchor order."""

        failures: list[int] = []
        first_error: OSError | None = None
        for handle in reversed(self._handles):
            try:
                self._api.close(handle)
            except OSError as error:
                failures.append(handle)
                if first_error is None:
                    first_error = error
        self._handles = tuple(reversed(failures))
        if first_error is not None:
            raise first_error

    def _leaf_handle(self) -> int:
        if not self._handles:
            raise OSError("win32_pinned_directory_closed")
        return self._handles[-1]


class _WindowsPinnedFileResource:
    """Retain one read identity and delete only that identity after BCP."""

    __slots__ = ("_api", "_handle", "_expected", "_path")

    def __init__(
        self,
        *,
        api: Win32DirectoryApiProtocol,
        handle: int,
        expected: WindowsDirectoryIdentity,
        path: Path,
    ) -> None:
        self._api = api
        self._handle: int | None = handle
        self._expected = expected
        self._path = path

    def require_identity(self) -> None:
        handle = self._require_handle()
        observed = self._api.identity(handle)
        if not observed.same_file(self._expected):
            raise OSError("win32_pinned_consumer_identity_changed")
        self._api.require_final_path(handle, self._path)

    def cleanup(self) -> None:
        # BCP compatibility requires releasing the read/share lock before a
        # DELETE-capable handle can be opened. The reopened handle is verified
        # against the held identity before that exact object is marked for
        # deletion. A missing or rebound pathname therefore fails closed and
        # leaves residue instead of deleting a replacement.
        self.close()
        self._api.delete_exact_file(self._path, expected=self._expected)

    def close(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        self._api.close(handle)

    def _require_handle(self) -> int:
        if self._handle is None:
            raise OSError("win32_pinned_consumer_closed")
        return self._handle


def _directory_chain(path: Path) -> tuple[Path, ...]:
    absolute = Path(os.path.abspath(os.fspath(path)))
    return (*reversed(absolute.parents), absolute)


def _close_preserving_primary(
    api: Win32DirectoryApiProtocol,
    handles: list[int],
    primary: BaseException,
) -> None:
    for handle in reversed(handles):
        try:
            api.close(handle)
        except OSError:
            add_note = getattr(primary, "add_note", None)
            if callable(add_note):
                add_note("win32_pinned_directory_close_failed")


__all__ = ["WindowsDirectoryIdentity", "WindowsPinnedDirectoryLease"]
