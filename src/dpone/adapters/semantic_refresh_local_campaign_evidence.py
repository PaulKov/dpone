"""Durable create-only publication for local semantic-refresh evidence."""

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path


class LocalCampaignEvidenceWriteError(RuntimeError):
    """Raised when local campaign evidence cannot be published safely."""


class CreateOnlyLocalCampaignReportWriter:
    """Publish one complete report without replacing an earlier observation."""

    def __call__(self, output_path: Path, report_json: str) -> None:
        target = Path(output_path)
        data = report_json.encode("utf-8")
        parent = target.parent
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            parent_mode = parent.lstat().st_mode
            if not stat.S_ISDIR(parent_mode) or stat.S_ISLNK(parent_mode):
                raise LocalCampaignEvidenceWriteError("local campaign evidence parent is unsafe")
            if target.exists() or target.is_symlink():
                raise LocalCampaignEvidenceWriteError("local campaign evidence already exists")
            _publish_create_only(parent=parent, target=target, data=data)
        except LocalCampaignEvidenceWriteError:
            raise
        except OSError as exc:
            raise LocalCampaignEvidenceWriteError("local campaign evidence publication failed") from exc


def _publish_create_only(*, parent: Path, target: Path, data: bytes) -> None:
    temporary: Path | None = None
    try:
        descriptor, raw_path = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=parent)
        temporary = Path(raw_path)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600, follow_symlinks=False)
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError as exc:
            raise LocalCampaignEvidenceWriteError("local campaign evidence already exists") from exc
        _fsync_directory(parent)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["CreateOnlyLocalCampaignReportWriter", "LocalCampaignEvidenceWriteError"]
