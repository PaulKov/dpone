"""Exclusive originals under the separately enrolled nonroot capture profile.

Provisioning owns the root and its protected ancestry; this adapter never repairs
permissions or adopts old root-owned captures. The mandatory custody callback
must independently compare fresh host facts with the configured enrollment. It
runs before and after each operation and must raise on drift, not return a bool.
"""

from __future__ import annotations

import os
import stat
import sys
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

from dpone.adapters.composition_supervisor_filesystem import PROTECTED_FLAGS, absolute_supervisor_path
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot_capture import SnapshotCaptureRecord, SnapshotCaptureSubject, capture_digest

_STABLE = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")


def _require(value: bool, reason: str) -> None:
    if not value:
        raise CompositionAdmissionError("snapshot_service_" + reason)


class ServiceSnapshotFiles:
    """Pinned service-owned volume with one irrevocable directory per attempt.

    ``require_custody`` is a trusted fresh-observation validator, never a cached
    readiness predicate. The owner must close this instance after all operations
    unwind; closing neither removes originals nor makes an attempt replayable.
    """

    def __init__(
        self,
        root: Path,
        *,
        dispatcher_uid: int,
        dispatcher_gid: int,
        root_device: int,
        root_inode: int,
        require_custody: Callable[[], None],
    ) -> None:
        _require(all(type(v) is int and 0 < v < 2**31 for v in (dispatcher_uid, dispatcher_gid)), "identity")
        _require(type(root_device) is int and root_device >= 0 and type(root_inode) is int and root_inode > 0, "volume")
        _require(callable(require_custody), "custody")
        self.root = absolute_supervisor_path(root)
        _require(self.root != Path("/"), "volume")
        self._uid, self._gid = dispatcher_uid, dispatcher_gid
        self._volume = root_device, root_inode
        self._custody = require_custody
        self._root_fd: int | None = None
        try:
            self._boundary()
            self._root_fd = self._open_root()
            self._check()
        except Exception:
            self.close()
            raise CompositionAdmissionError("snapshot_service_custody") from None

    def _boundary(self) -> None:
        _require(sys.platform == "linux" and os.geteuid() == self._uid and os.getegid() == self._gid, "identity")
        _require(self._custody() is None, "custody")

    def _shape(self, info: os.stat_result, *, directory: bool) -> None:
        _require(
            (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
            and (info.st_uid, info.st_gid) == (self._uid, self._gid)
            and stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600)
            and (directory or info.st_nlink == 1),
            "file_shape",
        )

    def _open_root(self) -> int:
        """Reopen protected ancestry without the root profile's sticky exception."""
        descriptor = os.open("/", PROTECTED_FLAGS)
        try:
            for part in self.root.parts[1:]:
                info = os.fstat(descriptor)
                _require(info.st_uid == 0 and not info.st_mode & 0o022, "ancestry")
                child = os.open(part, PROTECTED_FLAGS, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            info = os.fstat(descriptor)
            self._shape(info, directory=True)
            _require((info.st_dev, info.st_ino) == self._volume, "volume")
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _check(self) -> int:
        self._boundary()
        _require(self._root_fd is not None, "closed")
        assert self._root_fd is not None
        info = os.fstat(self._root_fd)
        self._shape(info, directory=True)
        _require((info.st_dev, info.st_ino) == self._volume, "volume")
        reopened = self._open_root()
        os.close(reopened)
        return self._root_fd

    def _directory(self, root: int, subject: SnapshotCaptureSubject) -> int:
        descriptor = os.open(subject.attempt.attempt_sha256[7:], PROTECTED_FLAGS, dir_fd=root)
        try:
            self._shape(os.fstat(descriptor), directory=True)
            _require(os.fstat(descriptor).st_dev == self._volume[0], "volume")
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    @staticmethod
    def _source_max(subject: SnapshotCaptureSubject) -> int:
        return subject.limits.max_source_bytes + subject.limits.max_rows + 65536

    def write_once(self, subject: SnapshotCaptureSubject, source: bytes, payload: bytes) -> None:
        """Exclusively install both originals; partial attempts remain occupied."""
        parent = None
        try:
            _require(type(subject) is SnapshotCaptureSubject, "subject")
            subject.__post_init__()
            _require(type(source) is bytes and 0 < len(source) <= self._source_max(subject), "source_budget")
            _require(type(payload) is bytes and len(payload) <= subject.limits.max_wire_bytes, "payload_budget")
            root = self._check()
            os.mkdir(subject.attempt.attempt_sha256[7:], 0o700, dir_fd=root)
            os.fsync(root)
            parent = self._directory(root, subject)
            self._put(parent, "source.json", source)
            self._put(parent, "payload.native", payload)
            self._check()
        except Exception:
            raise CompositionAdmissionError("snapshot_service_write") from None
        finally:
            if parent is not None:
                self._finish(parent, subject)

    def _finish(self, parent: int, subject: SnapshotCaptureSubject) -> None:
        """Recheck custody even after an uncertain operation; preserve all files."""
        try:
            root = self._check()
            before = os.fstat(parent)
            after = os.stat(subject.attempt.attempt_sha256[7:], dir_fd=root, follow_symlinks=False)
            self._shape(before, directory=True)
            self._shape(after, directory=True)
            _require((before.st_dev, before.st_ino) == (after.st_dev, after.st_ino), "directory_changed")
        except Exception:
            raise CompositionAdmissionError("snapshot_service_custody") from None
        finally:
            os.close(parent)

    def _put(self, parent: int, name: str, document: bytes) -> None:
        self._check()
        temporary = ".capture-" + uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        with os.fdopen(fd, "wb") as stream:
            self._shape(os.fstat(stream.fileno()), directory=False)
            _require(stream.write(document) == len(document), "partial_write")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
        os.unlink(temporary, dir_fd=parent)
        os.fsync(parent)
        _require(self._read_file(parent, name, len(document)) == document, "write_readback")
        self._check()

    def read(self, subject: SnapshotCaptureSubject, record: SnapshotCaptureRecord) -> tuple[bytes, bytes]:
        """Read bounded stable originals and compare SQL-pinned digests and size."""
        parent = None
        try:
            _require(type(subject) is SnapshotCaptureSubject, "subject")
            subject.__post_init__()
            _require(type(record) is SnapshotCaptureRecord, "record")
            record.__post_init__()
            _require(record.subject_sha256 == subject.subject_sha256, "subject")
            parent = self._directory(self._check(), subject)
            source = self._read_file(parent, "source.json", self._source_max(subject))
            payload = self._read_file(parent, "payload.native", subject.limits.max_wire_bytes)
            _require(
                capture_digest(source) == record.source_document_sha256
                and capture_digest(payload) == record.payload_sha256
                and len(payload) == record.wire_bytes,
                "digests",
            )
            self._check()
            return source, payload
        except Exception:
            raise CompositionAdmissionError("snapshot_service_read") from None
        finally:
            if parent is not None:
                self._finish(parent, subject)

    def _read_file(self, parent: int, name: str, maximum: int) -> bytes:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            before = os.fstat(fd)
            self._shape(before, directory=False)
            _require(before.st_dev == self._volume[0] and 0 <= before.st_size <= maximum, "file_budget")
            chunks, remaining = [], before.st_size + 1
            while remaining:
                chunk = os.read(fd, min(remaining, 65536))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            body = b"".join(chunks)
            after = os.fstat(fd)
            named = os.stat(name, dir_fd=parent, follow_symlinks=False)
            _require(
                all(getattr(before, n) == getattr(after, n) == getattr(named, n) for n in _STABLE)
                and len(body) == before.st_size,
                "file_changed",
            )
            return body
        finally:
            os.close(fd)

    def close(self) -> None:
        """Release the pinned descriptor only; never remove retained originals."""
        if self._root_fd is not None:
            os.close(self._root_fd)
            self._root_fd = None

    def __enter__(self) -> ServiceSnapshotFiles:
        self._check()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
