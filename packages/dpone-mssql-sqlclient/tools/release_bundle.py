"""Pure helpers for a deterministic, closed SqlClient companion release bundle."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import tarfile
from pathlib import Path
from typing import Any

from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_VERSION = re.compile(r"0\.83\.[0-9]+")
RUNTIME_FILE_LIMIT = 512 * 1024**2
RUNTIME_TOTAL_LIMIT = 2 * 1024**3
RUNTIME_ENTRY_LIMIT = 8192


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024**2), b""):
            value.update(chunk)
    return value.hexdigest()


def runtime_inventory(root: Path) -> dict[str, str]:
    """Measure a closed runtime tree under its fixed distribution budgets."""
    if not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir():
        raise ValueError("release.runtime_inventory_invalid")
    result: dict[str, str] = {}
    total = 0
    for path in sorted(root.rglob("*")):
        metadata = path.lstat()
        if path.is_symlink() or not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)):
            raise ValueError("release.runtime_inventory_invalid")
        if path.is_file():
            name = path.relative_to(root).as_posix()
            if (
                len(result) >= RUNTIME_ENTRY_LIMIT
                or metadata.st_size > RUNTIME_FILE_LIMIT
                or metadata.st_mode & (stat.S_ISUID | stat.S_ISGID)
                or (name == "dotnet" and not metadata.st_mode & 0o111)
            ):
                raise ValueError("release.runtime_inventory_invalid")
            total += metadata.st_size
            if total > RUNTIME_TOTAL_LIMIT:
                raise ValueError("release.runtime_inventory_invalid")
            result[name] = digest(path)
    if not result:
        raise ValueError("release.runtime_inventory_invalid")
    return result


def secure_copy_tree(
    source: Path,
    target: Path,
    expected: dict[str, str],
    *,
    maximum_files: int,
    maximum_file_bytes: int,
    maximum_total_bytes: int,
    executable: frozenset[str] = frozenset(),
) -> None:
    """Copy one exact admitted tree while binding reads to stable regular files."""
    actual = _tree_names(source, maximum_files)
    if actual != set(expected) or not actual:
        raise ValueError("release.inventory_mismatch")
    target.mkdir(parents=True)
    total = 0
    for name in sorted(expected):
        source_path, target_path = source / name, target / name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(source_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            before = os.fstat(fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_mode & (stat.S_ISUID | stat.S_ISGID)
                or before.st_size > maximum_file_bytes
            ):
                raise ValueError("release.inventory_invalid")
            digest_value = hashlib.sha256()
            copied = 0
            with target_path.open("xb") as destination:
                while chunk := os.read(fd, min(1024**2, maximum_file_bytes + 1 - copied)):
                    copied += len(chunk)
                    if copied > maximum_file_bytes:
                        raise ValueError("release.inventory_invalid")
                    digest_value.update(chunk)
                    destination.write(chunk)
            after, named = os.fstat(fd), source_path.lstat()

            if _identity(before) != _identity(after) or _identity(after) != _identity(named):
                raise ValueError("release.inventory_mismatch")
            if copied != before.st_size or digest_value.hexdigest() != expected[name]:
                raise ValueError("release.inventory_mismatch")
            total += copied
            if total > maximum_total_bytes:
                raise ValueError("release.inventory_invalid")
            target_path.chmod(0o555 if name in executable else 0o444)
        finally:
            os.close(fd)
    if _tree_names(source, maximum_files) != actual:
        raise ValueError("release.inventory_mismatch")


def _tree_names(root: Path, maximum_files: int) -> set[str]:
    if not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir():
        raise ValueError("release.inventory_invalid")
    result: set[str] = set()
    for path in root.rglob("*"):
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)):
            raise ValueError("release.inventory_invalid")
        if stat.S_ISREG(metadata.st_mode):
            if len(result) >= maximum_files:
                raise ValueError("release.inventory_invalid")
            result.add(path.relative_to(root).as_posix())
    return result


def read_json(path: Path, maximum: int = 2 * 1024**2) -> dict[str, Any]:
    return strict_json_object(secure_read(path, maximum))


def secure_read(path: Path, maximum: int) -> bytes:
    """Read one bounded regular file while rejecting links and replacement."""
    if not path.is_absolute():
        raise ValueError("release.input_invalid")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_mode & (stat.S_ISUID | stat.S_ISGID)
            or before.st_size > maximum
        ):
            raise ValueError("release.input_invalid")
        chunks = bytearray()
        while chunk := os.read(fd, min(65536, maximum + 1 - len(chunks))):
            chunks.extend(chunk)
            if len(chunks) > maximum:
                raise ValueError("release.input_too_large")
        after, named = os.fstat(fd), path.lstat()
        if _identity(before) != _identity(after) or _identity(after) != _identity(named):
            raise ValueError("release.input_invalid")
        return bytes(chunks)
    finally:
        os.close(fd)


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def validate_version(version: str) -> None:
    if type(version) is not str or _VERSION.fullmatch(version) is None:
        raise ValueError("release.version_incompatible")


def dependency_rows(receipt: dict[str, Any]) -> list[dict[str, str]]:
    rows = receipt.get("dependencies")
    if type(rows) is not list or not rows:
        raise ValueError("release.dependencies_invalid")
    result: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        if type(row) is not dict or not {"name", "version", "package_sha256"} <= set(row):
            raise ValueError("release.dependencies_invalid")
        item = {key: row[key] for key in ("name", "version", "package_sha256")}
        if any(type(value) is not str or not value for value in item.values()):
            raise ValueError("release.dependencies_invalid")
        key = item["name"], item["version"]
        if key in seen or not re.fullmatch(r"[0-9a-f]{64}", item["package_sha256"]):
            raise ValueError("release.dependencies_invalid")
        seen.add(key)
        result.append(item)
    return sorted(result, key=lambda row: (row["name"].lower(), row["version"]))


def license_rows(review: dict[str, Any], dependencies: list[dict[str, str]]) -> list[dict[str, str]]:
    if set(review) != {"schema_version", "packages"} or review["schema_version"] != "dpone.licenses.v1":
        raise ValueError("release.licenses_invalid")
    rows = review["packages"]
    if type(rows) is not list:
        raise ValueError("release.licenses_invalid")
    expected = {(row["name"], row["version"]) for row in dependencies}
    actual: set[tuple[str, str]] = set()
    result = []
    for row in rows:
        if type(row) is not dict or set(row) != {"name", "version", "spdx_expression", "license_text"}:
            raise ValueError("release.licenses_invalid")
        if any(type(row[key]) is not str or not row[key].strip() for key in row):
            raise ValueError("release.licenses_invalid")
        key = row["name"], row["version"]
        if key in actual:
            raise ValueError("release.licenses_invalid")
        actual.add(key)
        result.append(row)
    if actual != expected:
        raise ValueError("release.licenses_incomplete")
    return sorted(result, key=lambda row: (row["name"].lower(), row["version"]))


def inventory(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise ValueError("release.inventory_invalid")
        if path.is_file():
            result[path.relative_to(root).as_posix()] = digest(path)
    return result


def validate_release_tree(root: Path) -> dict[str, Any]:
    """Reject extra, missing or changed files in the closed unsigned tree."""
    manifest_path = root / "release-manifest.json"
    signature_path = root / "signature-input.json"
    manifest = read_json(manifest_path)
    if set(manifest) != {
        "schema_version",
        "artifact",
        "version",
        "platform",
        "dpone_compatibility",
        "deployment_build_sha256",
        "files",
        "control_files",
    } or manifest["control_files"] != ["release-manifest.json", "signature-input.json"]:
        raise ValueError("release.manifest_invalid")
    rows = manifest["files"]
    if type(rows) is not list or not rows:
        raise ValueError("release.manifest_invalid")
    expected: dict[str, str] = {}
    for row in rows:
        if type(row) is not dict or set(row) != {"path", "sha256"}:
            raise ValueError("release.manifest_invalid")
        name, value = row["path"], row["sha256"]
        if (
            type(name) is not str
            or type(value) is not str
            or not re.fullmatch(r"[A-Za-z0-9_.+-]+(?:/[A-Za-z0-9_.+-]+)*", name)
            or not re.fullmatch(r"[0-9a-f]{64}", value)
            or name in expected
        ):
            raise ValueError("release.manifest_invalid")
        expected[name] = value
    actual = inventory(root)
    controls = set(manifest["control_files"])
    if set(actual) != set(expected) | controls or any(actual[name] != value for name, value in expected.items()):
        raise ValueError("release.inventory_mismatch")
    signature = read_json(signature_path)
    if signature != {
        "schema_version": "dpone.mssql-sqlclient.signature-input.v1",
        "subject": "release-manifest",
        "subject_sha256": digest(manifest_path),
        "signature": None,
    }:
        raise ValueError("release.signature_input_invalid")
    return manifest


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value))


def deterministic_tar(source: Path, target: Path) -> None:
    """Archive one immutable tree without host timestamps, IDs, names or modes."""
    with tarfile.open(target, "x", format=tarfile.PAX_FORMAT) as archive:
        for path in [source, *sorted(source.rglob("*"))]:
            relative = Path(source.name) / path.relative_to(source)
            info = archive.gettarinfo(str(path), arcname=relative.as_posix())
            info.uid = info.gid = info.mtime = 0
            info.uname = info.gname = ""
            info.mode = 0o555 if path.is_dir() or path.stat().st_mode & 0o111 else 0o444
            if path.is_file():
                with path.open("rb") as stream:
                    archive.addfile(info, stream)
            else:
                archive.addfile(info)


def make_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        path.chmod(0o555 if path.is_dir() or path.stat().st_mode & 0o111 else 0o444)
    root.chmod(0o555)
