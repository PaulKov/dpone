"""Prove artifact-tree separation using pinned mount mappings and actual inodes.

Supports ext4/xfs/tmpfs bind and named-volume subtrees, including disjoint paths
on one filesystem. Mount ancestry alone does not prove hardlink isolation: two
complete bounded metadata inventories additionally reject links and aliases to
any enrolled immutable entry. No file contents, credentials, SQL or host RPC are
read here. The caller authenticates fresh enrollment/host/process observations
and brackets this proof with unchanged mount tables before opening admission.
"""

from __future__ import annotations

import math
import os
import re
import stat
import time
from collections.abc import Mapping
from contextlib import ExitStack
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from dpone.adapters.composition_clickhouse_supervisor_enrollment import ClickHouseSupervisorEnrollment
from dpone.adapters.composition_supervisor_filesystem import open_protected
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

MAX_ARTIFACT_ENTRIES = 8192
MAX_ARTIFACT_DEPTH = 32
_STABLE = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")


class ArtifactIsolationPolicy(Protocol):
    """Read-only settings obtained from the launch-validated service policy."""

    @property
    def bootstrap_file(self) -> Path: ...
    @property
    def context_root(self) -> Path: ...
    @property
    def dispatcher_gid(self) -> int: ...


def _require(condition: bool) -> None:
    if not condition:
        raise CompositionAdmissionError("dispatcher_artifact_isolation")


def _digest(value: bytes) -> str:
    return "sha256:" + sha256(value).hexdigest()


def _path(value: object) -> PurePosixPath:
    _require(type(value) is str)
    assert isinstance(value, str)
    path = PurePosixPath(value)
    _require(0 < len(value) <= 4096 and value.startswith("/") and not value.startswith("//"))
    _require(
        str(path) == value and ".." not in path.parts and all(ord(char) >= 32 and ord(char) != 127 for char in value)
    )
    return path


def _overlap(left: PurePosixPath, right: PurePosixPath) -> bool:
    return left.is_relative_to(right) or right.is_relative_to(left)


def _mounts(rows: Any) -> list[dict[str, Any]]:
    _require(type(rows) is list and 1 <= len(rows) <= 1024)
    ids, destinations = set(), set()
    roots = []
    for row in rows:
        _require(type(row) is dict)
        for name in ("id", "parent"):
            _require(type(row[name]) is int and row[name] >= 0)
        _require(row["id"] not in ids)
        ids.add(row["id"])
        destination = _path(row["destination"])
        _path(row["root"])
        _require(destination not in destinations)
        destinations.add(destination)
        if destination == PurePosixPath("/"):
            roots.append(row)
        _require(
            type(row["device"]) is str
            and re.fullmatch(r"(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)", row["device"]) is not None
        )
    _require(len(roots) == 1)
    for row in rows:
        if row is not roots[0]:
            parents = [parent for parent in rows if parent["id"] == row["parent"]]
            _require(len(parents) == 1)
            _require(_path(row["destination"]).is_relative_to(_path(parents[0]["destination"])))
            _require(row["destination"] != parents[0]["destination"])
    return rows


def _mapping(path: PurePosixPath, rows: list[dict[str, Any]]) -> tuple[str, str, PurePosixPath]:
    covering = [row for row in rows if path.is_relative_to(_path(row["destination"]))]
    _require(bool(covering))
    selected = max(covering, key=lambda row: len(_path(row["destination"]).parts))
    _require(selected["filesystem"] in {"ext4", "xfs", "tmpfs"} and selected["propagation"] == [])
    _require(
        not any(_path(row["destination"]) != path and _path(row["destination"]).is_relative_to(path) for row in rows)
    )
    return (
        selected["device"],
        selected["filesystem"],
        _path(selected["root"]) / path.relative_to(_path(selected["destination"])),
    )


def _device(value: int) -> str:
    _require(type(value) is int and value >= 0)
    return f"{os.major(value)}:{os.minor(value)}"


def _immutable(
    body: dict[str, Any], tables: dict[str, Any]
) -> tuple[str, list[tuple[str, str, PurePosixPath]], set[tuple[int, int]]]:
    roles, roots = body["policy"]["roles"], body["policy"]["config_roots"]
    protected = {key for key, role in roles.items() if role in {"clickhouse", "dispatcher"}}
    _require(set(tables) == set(roots) == protected and len(protected) == 2)
    dispatcher = next(key for key, role in roles.items() if role == "dispatcher")
    mappings, inodes = [], set()
    for container in sorted(protected):
        facts = body["facts"]["linux"]["containers"][container]
        _require(_digest(canonical_json_bytes(tables[container])) == facts["mounts"]["mountinfo_sha256"])
        rows = _mounts(tables[container])
        _require(set(facts["configs"]) == set(roots[container]))
        for root in roots[container]:
            mapping = _mapping(_path(root), rows)
            mappings.append(mapping)
            entries = facts["configs"][root]
            _require(type(entries) is list and 1 <= len(entries) <= 256)
            names = set()
            for entry in entries:
                name = entry["path"]
                _require(type(name) is str and name not in names)
                names.add(name)
                relative = PurePosixPath(name)
                _require(not relative.is_absolute() and ".." not in relative.parts and str(relative) == name)
                _require(_device(entry["device"]) == mapping[0] and type(entry["inode"]) is int and entry["inode"] > 0)
                inodes.add((entry["device"], entry["inode"]))
            _require("." in names)
    return dispatcher, mappings, inodes


def _require_artifact(info: Any, gid: int) -> None:
    directory = stat.S_ISDIR(info.st_mode)
    _require(directory or stat.S_ISREG(info.st_mode))
    access = 0o050 if directory else 0o040
    _require(info.st_uid == 0 and info.st_gid == gid and not info.st_mode & 0o027 and info.st_mode & access == access)
    _require(directory or info.st_nlink == 1)


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return tuple(getattr(info, name) for name in _STABLE)


def _inventory(descriptor: int, gid: int, budget: list[int], check: Any) -> dict[str, tuple[int, ...]]:
    result = {}

    def visit(current: int, relative: str, depth: int) -> None:
        check()
        _require(depth <= MAX_ARTIFACT_DEPTH and budget[0] > 0)
        budget[0] -= 1
        before = os.fstat(current)
        check()
        _require_artifact(before, gid)
        result[relative] = _identity(before)
        if stat.S_ISDIR(before.st_mode):
            check()
            with os.scandir(current) as entries:
                for entry in entries:
                    check()
                    _require(budget[0] > 0)
                    child = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=current)
                    try:
                        check()
                        child_name = entry.name if relative == "." else relative + "/" + entry.name
                        visit(child, child_name, depth + 1)
                        check()
                        _require(
                            _identity(os.stat(entry.name, dir_fd=current, follow_symlinks=False)) == result[child_name]
                        )
                        check()
                    finally:
                        os.close(child)
                    check()
            check()
        _require(_identity(os.fstat(current)) == _identity(before))
        check()

    visit(descriptor, ".", 0)
    return result


def require_dispatcher_artifact_isolation(
    enrollment: ClickHouseSupervisorEnrollment,
    policy: ArtifactIsolationPolicy,
    mount_tables: Mapping[str, Any],
    deadline: float,
) -> None:
    """Require full mount/inode separation without granting startup admission.

    At most 8192 artifact entries across both roots are observed per pass, with
    depth at most 32. Both complete passes must agree. Temporary descriptors are
    closed before the final deadline check; late cleanup cannot certify success.
    """
    try:
        _require(type(deadline) in (float, int) and math.isfinite(deadline))

        def check() -> None:
            _require(time.monotonic() < deadline)

        check()
        _require(type(enrollment) is ClickHouseSupervisorEnrollment)
        enrollment.__post_init__()
        _require(type(policy.dispatcher_gid) is int and 0 < policy.dispatcher_gid < 2**31)
        artifacts = (_path(str(policy.bootstrap_file.parent)), _path(str(policy.context_root)))
        tables = strict_json_object(canonical_json_bytes(dict(mount_tables)))
        body = enrollment.body
        dispatcher, immutable, inodes = _immutable(body, tables)
        mappings = [_mapping(path, tables[dispatcher]) for path in artifacts]
        for path, mapping in zip(artifacts, mappings, strict=True):
            _require(not any(_overlap(path, _path(root)) for root in body["policy"]["config_roots"][dispatcher]))
            for other in immutable:
                if mapping[0] == other[0]:
                    _require(mapping[1] == other[1] and not _overlap(mapping[2], other[2]))
        check()
        with ExitStack() as stack:
            descriptors = []
            for path in artifacts:
                check()
                descriptor = open_protected(Path(path), traversable=False, require_current=check)
                stack.callback(os.close, descriptor)
                descriptors.append(descriptor)
                check()
            previous = None
            for _ in range(2):
                budget = [MAX_ARTIFACT_ENTRIES]
                snapshots = [_inventory(fd, policy.dispatcher_gid, budget, check) for fd in descriptors]
                for snapshot, mapping in zip(snapshots, mappings, strict=True):
                    _require(_device(snapshot["."][0]) == mapping[0])
                    _require(
                        all(
                            _device(info[0]) == mapping[0] and (info[0], info[1]) not in inodes
                            for info in snapshot.values()
                        )
                    )
                _require(previous is None or snapshots == previous)
                previous = snapshots
            for path, snapshot in zip(artifacts, snapshots, strict=True):
                check()
                current = open_protected(Path(path), traversable=False, require_current=check)
                stack.callback(os.close, current)
                check()
                _require(_identity(os.fstat(current)) == snapshot["."])
                check()
        check()
    except Exception:
        raise CompositionAdmissionError("dispatcher_artifact_isolation") from None
