"""Stable, no-follow worktree and index acquisition including ignored files.

Directory descriptors prevent ancestor symlink races. A second complete content
inventory catches additions, removals and byte changes before evidence is issued.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
from pathlib import Path
from typing import Any

from tools.agent_policy.public_clean_archives import inspect_content
from tools.agent_policy.public_clean_candidate import Git
from tools.agent_policy.public_clean_policy import Scanner
from tools.agent_policy.public_clean_receipts import GateError, digest, read_file
from tools.agent_policy.tenant_hygiene import MAX_SOURCE_BLOB_BYTES

_LIMIT = 100_000
_OID = re.compile(rb"[0-9a-f]{40}\Z")


def _identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _flags(*, directory: bool = False) -> int:
    if not hasattr(os, "O_NOFOLLOW"):
        raise GateError("NOFOLLOW_UNAVAILABLE")
    return os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | (os.O_DIRECTORY if directory else 0)


def _admin(git: Git) -> dict[str, Any]:
    """Exclude only the actual root administration directory or gitdir marker."""
    root = git.root.resolve(strict=True)
    if git.run("rev-parse", "--show-toplevel").decode("utf-8", "strict").rstrip("\n") != str(root):
        raise GateError("WORKTREE_ROOT_INVALID")
    administrative = Path(git.run("rev-parse", "--absolute-git-dir").decode("utf-8", "strict").rstrip("\n"))
    marker = root / ".git"
    observed = marker.lstat()
    if stat.S_ISLNK(observed.st_mode):
        raise GateError("ADMIN_INVALID")
    if stat.S_ISDIR(observed.st_mode):
        if marker.resolve(strict=True) != administrative.resolve(strict=True):
            raise GateError("ADMIN_INVALID")
        return {"path": ".git", "kind": "directory", "identity": _identity(observed)[:3]}
    if not stat.S_ISREG(observed.st_mode):
        raise GateError("ADMIN_INVALID")
    descriptor = os.open(marker, _flags())
    try:
        raw = os.read(descriptor, 4097)
        if len(raw) > 4096 or _identity(os.fstat(descriptor)) != _identity(observed):
            raise GateError("ADMIN_INVALID")
    finally:
        os.close(descriptor)
    if not raw.startswith(b"gitdir: ") or raw.count(b"\n") > 1:
        raise GateError("ADMIN_INVALID")
    target = Path(raw[8:].decode("utf-8", "strict").rstrip("\n"))
    if not target.is_absolute():
        target = root / target
    if target.resolve(strict=True) != administrative.resolve(strict=True) or _identity(marker.lstat()) != _identity(
        observed
    ):
        raise GateError("ADMIN_INVALID")
    return {
        "path": ".git",
        "kind": "gitdir-file",
        "identity": _identity(observed),
        "digest": hashlib.sha256(raw).hexdigest(),
    }


def _inventory(git: Git, scanner: Scanner, *, inspect: bool) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []

    def walk(directory: int, prefix: str) -> None:
        before = _identity(os.fstat(directory))
        for name in sorted(os.listdir(directory)):
            scanner.budget.tick()
            if not prefix and name == ".git":
                continue
            path = f"{prefix}/{name}" if prefix else name
            encoded = path.encode("utf-8", "strict")
            if len(entries) >= _LIMIT:
                raise GateError("WORKTREE_LIMIT")
            if inspect:
                scanner.scan(f"path:{path}", encoded)
            observed = os.stat(name, dir_fd=directory, follow_symlinks=False)
            directory_entry = stat.S_ISDIR(observed.st_mode)
            if not directory_entry and not stat.S_ISREG(observed.st_mode):
                raise GateError("WORKTREE_ENTRY_UNSUPPORTED")
            child = os.open(name, _flags(directory=directory_entry), dir_fd=directory)
            try:
                if _identity(os.fstat(child)) != _identity(observed):
                    raise GateError("WORKTREE_CHANGED")
                entry: dict[str, Any] = {"path": path, "identity": _identity(observed)}
                entries.append(entry)
                if directory_entry:
                    walk(child, path)
                else:
                    if observed.st_size > MAX_SOURCE_BLOB_BYTES:
                        raise GateError("BLOB_LIMIT")
                    chunks, size = [], 0
                    while chunk := os.read(child, 65536):
                        scanner.budget.tick()
                        size += len(chunk)
                        if size > MAX_SOURCE_BLOB_BYTES:
                            raise GateError("BLOB_LIMIT")
                        chunks.append(chunk)
                    raw = b"".join(chunks)
                    entry["digest"] = hashlib.sha256(raw).hexdigest()
                    if inspect:
                        inspect_content(path, raw, scanner)
                if _identity(os.fstat(child)) != _identity(observed) or _identity(
                    os.stat(name, dir_fd=directory, follow_symlinks=False)
                ) != _identity(observed):
                    raise GateError("WORKTREE_CHANGED")
            finally:
                os.close(child)
        if before != _identity(os.fstat(directory)):
            raise GateError("WORKTREE_CHANGED")

    root = os.open(git.root, _flags(directory=True))
    try:
        expected = _identity(os.fstat(root))
        walk(root, "")
        if expected != _identity(git.root.lstat()):
            raise GateError("WORKTREE_CHANGED")
    finally:
        os.close(root)
    return entries


def _index(git: Git, scanner: Scanner, raw: bytes) -> None:
    if raw and not raw.endswith(b"\0"):
        raise GateError("INDEX_INVALID")
    paths: set[str] = set()
    blobs: dict[str, bytes] = {}
    for record in raw.split(b"\0")[:-1]:
        scanner.budget.tick()
        fields, separator, path_raw = record.partition(b"\t")
        metadata = fields.split(b" ")
        if (
            not separator
            or len(metadata) != 3
            or metadata[0] not in {b"100644", b"100755"}
            or metadata[2] != b"0"
            or not _OID.fullmatch(metadata[1])
        ):
            raise GateError("INDEX_ENTRY_UNSUPPORTED")
        path = path_raw.decode("utf-8", "strict")
        if (
            not path
            or path.startswith("/")
            or any(part in {"", ".", ".."} for part in path.split("/"))
            or path in paths
        ):
            raise GateError("INDEX_INVALID")
        paths.add(path)
        if len(paths) > _LIMIT:
            raise GateError("INDEX_LIMIT")
        scanner.scan(f"index-path:{path}", path_raw)
        oid = metadata[1].decode("ascii")
        if oid not in blobs:
            blobs[oid] = git.blob(oid)
        inspect_content(f"index:{path}", blobs[oid], scanner)


def scan_worktree(git: Git, scanner: Scanner) -> dict[str, Any]:
    """Scan full index and filesystem, rejecting unsupported or changing input."""
    admin = _admin(git)
    head = git.run("rev-parse", "--verify", "HEAD")
    index = git.run("ls-files", "--stage", "-z")
    index_path = Path(git.run("rev-parse", "--git-path", "index").decode("utf-8", "strict").rstrip("\n"))
    if not index_path.is_absolute():
        index_path = git.root / index_path
    index_bytes = read_file(index_path, 64 * 1024 * 1024)
    _index(git, scanner, index)
    inventory = _inventory(git, scanner, inspect=True)
    if inventory != _inventory(git, scanner, inspect=False):
        raise GateError("WORKTREE_CHANGED")
    if (
        index != git.run("ls-files", "--stage", "-z")
        or index_bytes != read_file(index_path, 64 * 1024 * 1024)
        or head != git.run("rev-parse", "--verify", "HEAD")
        or admin != _admin(git)
    ):
        raise GateError("WORKTREE_CHANGED")
    return {
        "head": head.decode("ascii").strip(),
        "index_digest": hashlib.sha256(index).hexdigest(),
        "raw_index_digest": hashlib.sha256(index_bytes).hexdigest(),
        "inventory_digest": digest(inventory),
        "entries": len(inventory),
        "admin_exclusions": [admin],
    }
