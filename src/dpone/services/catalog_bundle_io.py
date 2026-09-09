"""Create-only filesystem primitives for immutable catalog artifacts."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path, PurePosixPath

from dpone.services.catalog_supply_chain_support import (
    MAX_FILE_BYTES,
    MAX_PAYLOAD_FILES,
    MAX_TOTAL_BYTES,
    CatalogBundleError,
    RecipeBundleSupportError,
    read_project_file,
)


def write_private_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        pending = memoryview(payload)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError("failed to write catalog artifact")
            pending = pending[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_staging_directory(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=".dpone-catalog-", dir=root))


def publish_directory(staging: Path, destination: Path) -> None:
    try:
        os.rename(staging, destination)
    except OSError as exc:
        if destination.exists():
            raise FileExistsError(destination) from exc
        raise


def cleanup_directory(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def read_bundle_file(bundle_dir: Path, relative: str, *, max_bytes: int = MAX_FILE_BYTES) -> bytes:
    _safe_relative(relative)
    try:
        return read_project_file(bundle_dir, relative, max_bytes=max_bytes)
    except RecipeBundleSupportError as exc:
        raise CatalogBundleError(
            "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED",
            "Catalog bundle contains an unsafe, missing, or oversized file.",
        ) from exc


def listed_regular_files(bundle_dir: Path) -> tuple[str, ...]:
    root = bundle_dir.resolve(strict=True)
    found: list[str] = []
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                relative = Path(entry.path).relative_to(root).as_posix()
                if entry.is_symlink():
                    raise CatalogBundleError(
                        "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED",
                        "Catalog bundle symlinks are forbidden.",
                    )
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    size = entry.stat(follow_symlinks=False).st_size
                    if size > MAX_FILE_BYTES:
                        raise CatalogBundleError(
                            "DPONE_CATALOG_BUNDLE_LIMIT_EXCEEDED",
                            "Catalog bundle file exceeds the size limit.",
                        )
                    total += size
                    found.append(relative)
                else:
                    raise CatalogBundleError(
                        "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED",
                        "Catalog bundle contains a special file.",
                    )
    if len(found) > MAX_PAYLOAD_FILES + 2 or total > MAX_TOTAL_BYTES:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_LIMIT_EXCEEDED", "Catalog bundle exceeds safety limits.")
    return tuple(sorted(found))


def _safe_relative(value: str) -> None:
    parsed = PurePosixPath(value)
    if not value or "\\" in value or parsed.is_absolute() or ".." in parsed.parts or "." in parsed.parts:
        raise CatalogBundleError("DPONE_CATALOG_PATH_UNSAFE", "Catalog bundle path is unsafe.")


__all__ = [
    "cleanup_directory",
    "create_staging_directory",
    "listed_regular_files",
    "publish_directory",
    "read_bundle_file",
    "write_private_file",
]
