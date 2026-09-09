"""Identity-confined directory operations for local runtime artifacts."""

from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Callable
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING

from dpone.runtime.pinned_file_consumer import PinnedFileConsumer
from dpone.runtime.pinned_file_integrity import (
    PosixDescriptorIntegrityAuthority,
    descriptor_file_identity,
)
from dpone.runtime.pinned_file_posix import (
    create_anonymous_consumer_file,
    descriptor_consumer_path,
    require_read_only_descriptor,
    require_regular_descriptor,
    rewind_descriptor,
)

if TYPE_CHECKING:
    from dpone.runtime.pinned_directory_windows import WindowsPinnedDirectoryLease


class PinnedDirectoryIdentityError(OSError):
    """The configured pathname no longer resolves to the opened directory."""


class PinnedDirectory:
    """Own one immutable directory authority for mutable artifact operations.

    POSIX uses descriptor-relative ``*at`` calls. Windows holds verified
    non-delete-sharing handles across the complete absolute path chain. The
    pathname remains available only for consumers such as BCP that require a
    filename; neither backend permits a directory swap to redirect writes or
    cleanup into a replacement path.
    """

    __slots__ = ("path", "device", "inode", "_fd", "_windows_lease", "_closed", "_lock")

    def __init__(
        self,
        path: Path,
        descriptor: int | None,
        *,
        device: int,
        inode: int,
        windows_lease: WindowsPinnedDirectoryLease | None = None,
    ) -> None:
        self.path = path
        self.device = int(device)
        self.inode = int(inode)
        self._fd = descriptor
        self._windows_lease = windows_lease
        self._closed = False
        self._lock = RLock()

    @classmethod
    def open(cls, path: Path) -> PinnedDirectory:
        """Open an already-resolved directory without following a final link."""

        if os.name == "nt":
            from dpone.runtime.pinned_directory_windows import WindowsPinnedDirectoryLease

            lease = WindowsPinnedDirectoryLease.open(path)
            return cls(
                path,
                None,
                device=lease.device,
                inode=lease.inode,
                windows_lease=lease,
            )
        if os.open not in os.supports_dir_fd or os.unlink not in os.supports_dir_fd:
            raise OSError("descriptor_relative_directory_operations_unsupported")
        flags = os.O_RDONLY
        flags |= getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(os.fspath(path), flags)
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISDIR(opened.st_mode):
                raise OSError("not_a_directory")
            return cls(
                path,
                descriptor,
                device=opened.st_dev,
                inode=opened.st_ino,
            )
        except BaseException:
            os.close(descriptor)
            raise

    @property
    def descriptor(self) -> int:
        """Return the live POSIX descriptor or fail on Windows/after release."""

        with self._lock:
            self._require_open_locked()
            return self._require_descriptor_locked()

    def require_identity(self) -> None:
        """Prove both the descriptor and its required external pathname."""

        with self._lock:
            self._require_open_locked()
            if self._windows_lease is not None:
                try:
                    self._windows_lease.require_identity()
                except OSError:
                    raise PinnedDirectoryIdentityError("directory_path_identity_changed") from None
                return
            opened = os.fstat(self._require_descriptor_locked())
            if (int(opened.st_dev), int(opened.st_ino)) != (self.device, self.inode):
                raise PinnedDirectoryIdentityError("directory_descriptor_identity_changed")
            try:
                current = self.path.stat()
            except OSError:
                raise PinnedDirectoryIdentityError("directory_path_identity_changed") from None
            if (int(current.st_dev), int(current.st_ino)) != (self.device, self.inode):
                raise PinnedDirectoryIdentityError("directory_path_identity_changed")

    def probe_writable(self) -> None:
        """Create, write, and unlink one attempt-owned probe through the authority."""

        with self._lock:
            descriptor, entry_name, _path = self._create_file_locked(
                prefix=".dpone_write_probe_",
                suffix="",
            )
            try:
                os.write(descriptor, b"ok")
            finally:
                try:
                    os.close(descriptor)
                finally:
                    self._unlink_locked(entry_name, missing_ok=False)

    def create_file(self, *, prefix: str, suffix: str) -> tuple[int, str, str]:
        """Create one unpredictable regular file inside the pinned directory."""

        with self._lock:
            return self._create_file_locked(prefix=prefix, suffix=suffix)

    def create_consumer_file(
        self,
        *,
        prefix: str,
        suffix: str,
    ) -> tuple[int, int, str, str, Callable[[], None]]:
        """Create an external-consumer file without exposing a durable POSIX name."""

        with self._lock:
            self._require_open_locked()
            if self._windows_lease is not None:
                descriptor, entry_name, path = self._create_file_locked(prefix=prefix, suffix=suffix)
                try:
                    cleanup = self._windows_lease.pending_file_cleanup(
                        entry_name,
                        writer_descriptor=descriptor,
                    )
                except OSError as error:
                    os.close(descriptor)
                    setattr(error, "residue_possible", True)
                    raise
                return descriptor, -1, entry_name, path, cleanup
            entry_name = _random_entry_name(prefix=prefix, suffix=suffix)
            path = str(self.path / entry_name)
            return create_anonymous_consumer_file(
                directory_descriptor=self._require_descriptor_locked(),
                entry_name=entry_name,
                path=path,
                create_named_file=lambda: self._create_file_locked(
                    prefix=prefix,
                    suffix=suffix,
                ),
                unlink_named_file=lambda name: self._unlink_locked(
                    name,
                    missing_ok=False,
                ),
            )

    def probe_consumer_file(self) -> None:
        """Prove anonymous consumer-file support before source extraction."""

        with self._lock:
            if self._windows_lease is not None:
                return
            writer_descriptor, reader_descriptor, _entry_name, _path, _cleanup = self.create_consumer_file(
                prefix=".dpone_consumer_probe_",
                suffix="",
            )
            primary_error: BaseException | None = None
            try:
                descriptor_file_identity(reader_descriptor)
            except BaseException as error:
                primary_error = error
                raise
            finally:
                _close_probe_descriptors(
                    (reader_descriptor, writer_descriptor),
                    primary_error=primary_error,
                )

    def pin_file_for_consumer(
        self,
        entry_name: str,
        *,
        writer_descriptor: int,
        reader_descriptor: int | None = None,
    ) -> PinnedFileConsumer:
        """Hold the exact written inode through external-process consumption."""

        with self._lock:
            _require_entry_name(entry_name)
            self._require_open_locked()
            if self._windows_lease is not None:
                return self._windows_lease.pin_file_for_consumer(
                    entry_name,
                    writer_descriptor=writer_descriptor,
                )
            if reader_descriptor is None or reader_descriptor < 0:
                raise OSError("pinned_consumer_read_descriptor_required")
            writer_identity = os.fstat(writer_descriptor)
            descriptor = reader_descriptor
            try:
                consumer_identity = os.fstat(descriptor)
                if not stat.S_ISREG(consumer_identity.st_mode):
                    raise OSError("pinned_consumer_is_not_regular")
                if (int(consumer_identity.st_dev), int(consumer_identity.st_ino)) != (
                    int(writer_identity.st_dev),
                    int(writer_identity.st_ino),
                ):
                    raise OSError("pinned_consumer_file_identity_changed")
                require_read_only_descriptor(descriptor)
                consumer_path = descriptor_consumer_path(descriptor, consumer_identity)
            except BaseException:
                os.close(descriptor)
                raise
            source_path = str(self.path / entry_name)
            try:
                # BCP needs only read access. Remove write permission before
                # the writer closes. The consumer file was already anonymized
                # before serialization, so cleanup cannot target another inode.
                os.fchmod(descriptor, stat.S_IRUSR)
                integrity_authority = PosixDescriptorIntegrityAuthority.for_pinned_directory(
                    descriptor,
                    directory_path=str(self.path),
                    device=self.device,
                    inode=self.inode,
                )
            except BaseException:
                os.close(descriptor)
                raise
            return PinnedFileConsumer(
                source_path=source_path,
                consumer_path=consumer_path,
                inherited_file_descriptors=(descriptor,),
                prepare_callback=lambda: rewind_descriptor(descriptor),
                cleanup_callback=lambda: None,
                release_callback=lambda: os.close(descriptor),
                integrity_authority=integrity_authority,
            )

    def _create_file_locked(
        self,
        *,
        prefix: str,
        suffix: str,
        read_write: bool = False,
    ) -> tuple[int, str, str]:
        self._require_open_locked()
        flags = (os.O_RDWR if read_write else os.O_WRONLY) | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        for _attempt in range(128):
            entry_name = _random_entry_name(prefix=prefix, suffix=suffix)
            try:
                if self._windows_lease is not None:
                    descriptor, path = self._windows_lease.create_file(entry_name)
                else:
                    descriptor = os.open(
                        entry_name,
                        flags,
                        0o600,
                        dir_fd=self._require_descriptor_locked(),
                    )
                    path = str(self.path / entry_name)
            except FileExistsError:
                continue
            try:
                require_regular_descriptor(descriptor)
            except BaseException:  # pragma: no cover - O_EXCL invariant.
                os.close(descriptor)
                self.unlink(entry_name, missing_ok=True)
                raise
            return descriptor, entry_name, path
        raise OSError("unique_spool_name_unavailable")

    def unlink(self, entry_name: str, *, missing_ok: bool = False) -> None:
        """Delete exactly one basename through the pinned directory authority."""

        with self._lock:
            self._unlink_locked(entry_name, missing_ok=missing_ok)

    def _unlink_locked(self, entry_name: str, *, missing_ok: bool) -> None:
        _require_entry_name(entry_name)
        self._require_open_locked()
        if self._windows_lease is not None:
            self._windows_lease.unlink(entry_name, missing_ok=missing_ok)
            return
        try:
            os.unlink(entry_name, dir_fd=self._require_descriptor_locked())
        except FileNotFoundError:
            if not missing_ok:
                raise

    def free_bytes(self) -> int:
        """Measure available bytes on the descriptor's exact filesystem."""

        with self._lock:
            self._require_open_locked()
            if self._windows_lease is not None:
                return int(self._windows_lease.free_bytes())
            usage = os.fstatvfs(self._require_descriptor_locked())
            return int(usage.f_bavail) * int(usage.f_frsize)

    def close(self) -> None:
        """Release descriptor or handle-chain ownership exactly once."""

        with self._lock:
            if self._closed:
                return
            if self._windows_lease is not None:
                self._windows_lease.close()
                self._closed = True
            else:
                descriptor = self._require_descriptor_locked()
                # POSIX close errors leave descriptor state unspecified; mark
                # ownership released before close so a reused fd is never
                # closed by a retry.
                self._closed = True
                self._fd = None
                os.close(descriptor)

    def _require_descriptor_locked(self) -> int:
        if self._fd is None:
            raise OSError("pinned_directory_closed")
        return self._fd

    def _require_open_locked(self) -> None:
        if self._closed:
            raise OSError("pinned_directory_closed")

    def __enter__(self) -> PinnedDirectory:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def _require_entry_name(entry_name: str) -> None:
    if not entry_name or entry_name in {".", ".."} or Path(entry_name).name != entry_name:
        raise ValueError("pinned_directory_entry_name_invalid")


def _random_entry_name(*, prefix: str, suffix: str) -> str:
    entry_name = f"{prefix}{secrets.token_hex(16)}{suffix}"
    _require_entry_name(entry_name)
    return entry_name


def _close_probe_descriptors(
    descriptors: tuple[int, ...],
    *,
    primary_error: BaseException | None,
) -> None:
    first_close_error: OSError | None = None
    for descriptor in descriptors:
        try:
            os.close(descriptor)
        except OSError as error:
            if first_close_error is None:
                first_close_error = error
    if first_close_error is None:
        return
    if primary_error is None:
        raise first_close_error
    add_note = getattr(primary_error, "add_note", None)
    if callable(add_note):
        add_note("pinned_consumer_probe_release_failed")


__all__ = ["PinnedDirectory", "PinnedDirectoryIdentityError", "PinnedFileConsumer"]
