"""Synchronized publication of connector certification report projections."""

from __future__ import annotations

import os
import re
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path


class CertificationArtifactPublisher:
    """Publish authoritative JSON and a regenerable Markdown projection.

    A process lock prevents concurrent writers from interleaving updates and
    catchable failures restore the previous Markdown projection. The JSON file
    is the only authoritative completion artifact. A process crash between the
    two atomic file replacements can leave stale Markdown, which readers must
    never use as certification evidence.
    """

    def publish(
        self,
        artifact_dir: str | Path,
        *,
        base_name: str,
        json_content: str,
        markdown_content: str,
    ) -> tuple[Path, Path]:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", base_name) is None:
            raise ValueError("base_name must be an option-safe filename stem")
        root = Path(artifact_dir)
        root.mkdir(parents=True, exist_ok=True)
        json_path = root / f"{base_name}.json"
        markdown_path = root / f"{base_name}.md"
        with _publication_lock(root, base_name=base_name):
            return self._publish_locked(
                root=root,
                json_path=json_path,
                markdown_path=markdown_path,
                json_content=json_content,
                markdown_content=markdown_content,
            )

    def _publish_locked(
        self,
        *,
        root: Path,
        json_path: Path,
        markdown_path: Path,
        json_content: str,
        markdown_content: str,
    ) -> tuple[Path, Path]:
        json_temp: Path | None = None
        markdown_temp: Path | None = None
        markdown_backup: Path | None = None
        markdown_installed = False
        json_committed = False
        try:
            json_temp = self._write_temp(root, ".json.tmp", json_content)
            markdown_temp = self._write_temp(root, ".md.tmp", markdown_content)
            if markdown_path.exists():
                markdown_backup = self._reserve_temp_path(root, ".md.backup")
                os.replace(markdown_path, markdown_backup)
            os.replace(markdown_temp, markdown_path)
            markdown_installed = True
            os.replace(json_temp, json_path)
            json_committed = True
            self._fsync_directory(root)
            if markdown_backup is not None:
                markdown_backup.unlink(missing_ok=True)
            return json_path, markdown_path
        except BaseException:
            if not json_committed:
                if markdown_installed:
                    markdown_path.unlink(missing_ok=True)
                if markdown_backup is not None and markdown_backup.exists():
                    os.replace(markdown_backup, markdown_path)
                self._fsync_directory(root)
            raise
        finally:
            if json_temp is not None:
                json_temp.unlink(missing_ok=True)
            if markdown_temp is not None:
                markdown_temp.unlink(missing_ok=True)
            if json_committed and markdown_backup is not None:
                markdown_backup.unlink(missing_ok=True)

    @staticmethod
    def _write_temp(root: Path, suffix: str, content: str) -> Path:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=".dpone-certification-",
            suffix=suffix,
            dir=root,
            text=True,
        )
        path = Path(raw_path)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        return path

    @staticmethod
    def _reserve_temp_path(root: Path, suffix: str) -> Path:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=".dpone-certification-",
            suffix=suffix,
            dir=root,
        )
        os.close(descriptor)
        path = Path(raw_path)
        path.unlink()
        return path

    @staticmethod
    def _fsync_directory(root: Path) -> None:
        descriptor = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


@contextmanager
def _publication_lock(root: Path, *, base_name: str) -> Iterator[None]:
    lock_path = root / f".{base_name}.lock"
    descriptor = os.open(
        lock_path,
        os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    locked = False
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("certification publication lock must be a regular file")
        _lock_descriptor(descriptor)
        locked = True
        yield
    finally:
        try:
            if locked:
                _unlock_descriptor(descriptor)
        finally:
            os.close(descriptor)


def _lock_descriptor(descriptor: int) -> None:
    if os.name == "nt":
        windows_lock = import_module("msvcrt")
        if os.fstat(descriptor).st_size == 0:
            os.write(descriptor, b"\0")
            os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        windows_lock.locking(descriptor, windows_lock.LK_LOCK, 1)
        return
    file_lock = import_module("fcntl")
    file_lock.flock(descriptor, file_lock.LOCK_EX)


def _unlock_descriptor(descriptor: int) -> None:
    if os.name == "nt":
        windows_lock = import_module("msvcrt")
        os.lseek(descriptor, 0, os.SEEK_SET)
        windows_lock.locking(descriptor, windows_lock.LK_UNLCK, 1)
        return
    file_lock = import_module("fcntl")
    file_lock.flock(descriptor, file_lock.LOCK_UN)


__all__ = ["CertificationArtifactPublisher"]
