"""Trusted deployment admission, separate from authored pipeline configuration.

The composition root supplies independently admitted companion inventory/digests.
Hashing detects drift; immutable roots and pinned OS libraries remain prerequisites.
This module never downloads dependencies or treats a manifest's own hash as trust.
"""

from __future__ import annotations

import hashlib
import os
import platform
import re
import stat
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from dpone.adapters.mssql_tds_installation import worker_installation_digest
from dpone.contracts.mssql_tds_api import canonical_json_bytes, strict_json_object
from dpone.contracts.mssql_tds_validation import _hash

_ERROR = "mssql_native.sqlclient_installation_invalid"
_DOMAIN = b"dpone.sqlclient.deployment.v1\x00"


@dataclass(frozen=True)
class AdmittedCompanionFile:
    """One immutable root-produced distribution inventory entry, not manifest self-trust."""

    relative_path: str
    sha256: str

    def __post_init__(self) -> None:
        _relative(self.relative_path)
        _hash(self.sha256)


@dataclass(frozen=True)
class AdmittedSqlClientInstallation:
    """Internal injected deployment; no executable selection from user manifests.

    Dedicated companion/runtime roots and Python/source identity are already
    trusted by composition. Rechecking their bytes does not establish provenance.
    The root-owned distribution producer and fixed resource lookup are required.
    """

    python_executable: Path
    python_sha256: str
    package_root: Path
    source_sha256: str
    dotnet_host: Path
    runtime_root: Path
    companion_root: Path
    worker_assembly: Path
    build_manifest: Path
    build_sha256: str
    companion_inventory: tuple[AdmittedCompanionFile, ...]

    def __post_init__(self) -> None:
        for path in (
            self.python_executable,
            self.package_root,
            self.dotnet_host,
            self.runtime_root,
            self.companion_root,
            self.worker_assembly,
            self.build_manifest,
        ):
            if not isinstance(path, Path) or not path.is_absolute():
                raise ValueError(_ERROR)
        for digest in (self.python_sha256, self.source_sha256, self.build_sha256):
            _hash(digest)
        if type(self.companion_inventory) is not tuple or any(
            type(entry) is not AdmittedCompanionFile for entry in self.companion_inventory
        ):
            raise ValueError(_ERROR)

    def assert_admitted(self, *, deadline_ns: int) -> dict[str, Any]:
        """Check actual files/profile and independent inventories before spawning."""
        _before(deadline_ns)
        if platform.system() != "Linux" or platform.machine() not in {"aarch64", "arm64"}:
            raise ValueError(_ERROR)
        for admitted_digest in (self.python_sha256, self.source_sha256, self.build_sha256):
            _hash(admitted_digest)
        for path in (self.package_root, self.runtime_root, self.companion_root):
            _root(path)
        if _file_hash(self.python_executable, deadline_ns) != self.python_sha256:
            raise ValueError(_ERROR)
        if (
            not os.access(self.python_executable, os.X_OK)
            or worker_installation_digest(self.package_root) != self.source_sha256
        ):
            raise ValueError(_ERROR)
        body = _read(self.build_manifest, deadline_ns, 2 * 1024**2)
        value = validate_manifest(body, self.build_sha256)
        expected = {(x["origin"], x["path"]): x for x in value["files"]}
        inventory = {entry.relative_path: entry.sha256 for entry in self.companion_inventory}
        if len(inventory) != len(self.companion_inventory) or not inventory:
            raise ValueError(_ERROR)
        for name, digest in inventory.items():
            _relative(name)
            _hash(digest)
        companion = {path: row["sha256"] for (origin, path), row in expected.items() if origin == "companion"}
        if companion != inventory or set(companion) != _inventory(self.companion_root, deadline_ns):
            raise ValueError(_ERROR)
        runtime = {path for (origin, path) in expected if origin == "dotnet"}
        if runtime != _runtime_inventory(self.runtime_root, deadline_ns):
            raise ValueError(_ERROR)
        for (origin, name), entry in expected.items():
            root = self.companion_root if origin == "companion" else self.runtime_root
            if _file_hash(root / name, deadline_ns) != entry["sha256"]:
                raise ValueError(_ERROR)
        roles = {
            row["role"]: row["path"]
            for row in value["files"]
            if row["role"].startswith("worker_") or row["role"] == "dotnet_host"
        }
        if (
            self.dotnet_host != self.runtime_root / roles["dotnet_host"]
            or self.worker_assembly != self.companion_root / roles["worker_assembly"]
        ):
            raise ValueError(_ERROR)
        if not os.access(self.dotnet_host, os.X_OK):
            raise ValueError(_ERROR)
        stem = str(Path(roles["worker_assembly"]).with_suffix(""))
        if roles["worker_runtimeconfig"] != stem + ".runtimeconfig.json" or roles["worker_deps"] != stem + ".deps.json":
            raise ValueError(_ERROR)
        if any(
            (name.endswith(".runtimeconfig.json") and name != stem + ".runtimeconfig.json")
            or (name.endswith(".deps.json") and name != stem + ".deps.json")
            for name in companion
        ):
            raise ValueError(_ERROR)
        _profile(strict_json_object(_read(self.companion_root / roles["worker_runtimeconfig"], deadline_ns, 65536)))
        _deps(
            strict_json_object(_read(self.companion_root / roles["worker_deps"], deadline_ns, 2 * 1024**2)), companion
        )
        _before(deadline_ns)
        return value


def _before(deadline_ns: int) -> None:
    if type(deadline_ns) is not int or not 0 < deadline_ns < 2**63 or time.monotonic_ns() >= deadline_ns:
        raise ValueError("mssql_native.sqlclient_deadline")


def _relative(value: str) -> None:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 1024
        or not re.fullmatch(r"[A-Za-z0-9_.+-]+(?:/[A-Za-z0-9_.+-]+)*", value)
    ):
        raise ValueError(_ERROR)
    if any(piece in {".", ".."} for piece in value.split("/")):
        raise ValueError(_ERROR)


def _root(path: Path) -> None:
    if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_dir():
        raise ValueError(_ERROR)


def _read(path: Path, deadline_ns: int, maximum: int) -> bytes:
    _before(deadline_ns)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_mode & (stat.S_ISUID | stat.S_ISGID)
            or before.st_size > maximum
        ):
            raise ValueError(_ERROR)
        try:
            reader = getattr(os, "getxattr", None)
            if not callable(reader):
                raise ValueError(_ERROR)
            get_attribute = cast(Callable[[int, str], bytes], reader)
            if get_attribute(fd, "security.capability"):
                raise ValueError(_ERROR)
        except OSError as error:
            if error.errno not in {61, 95, 93}:  # ENODATA/ENOTSUP, no capability metadata.
                raise ValueError(_ERROR) from None
        chunks = bytearray()
        while len(chunks) <= maximum:
            _before(deadline_ns)
            data = os.read(fd, min(65536, maximum + 1 - len(chunks)))
            if not data:
                break
            chunks.extend(data)
        after, named = os.fstat(fd), path.lstat()

        def key(value: os.stat_result) -> tuple[int, ...]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_mode,
                value.st_size,
                value.st_mtime_ns,
                value.st_ctime_ns,
            )

        if len(chunks) > maximum or key(before) != key(after) or key(after) != key(named):
            raise ValueError(_ERROR)
        return bytes(chunks)
    finally:
        os.close(fd)


def _file_hash(path: Path, deadline_ns: int) -> str:
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError(_ERROR)
    return hashlib.sha256(_read(path, deadline_ns, 256 * 1024**2)).hexdigest()


def _inventory(root: Path, deadline_ns: int) -> set[str]:
    result: set[str] = set()
    for path in root.rglob("*"):
        _before(deadline_ns)
        if path.is_symlink() or len(result) >= 4096:
            raise ValueError(_ERROR)
        if path.is_file():
            name = path.relative_to(root).as_posix()
            _relative(name)
            if name.endswith(".runtimeconfig.dev.json"):
                raise ValueError(_ERROR)
            result.add(name)
        elif not path.is_dir():
            raise ValueError(_ERROR)
    return result


def _runtime_inventory(root: Path, deadline_ns: int) -> set[str]:
    fxr = root / "host/fxr"
    if {p.name for p in fxr.iterdir()} != {"8.0.31"}:
        raise ValueError(_ERROR)
    required = {"dotnet"}
    for base in ("host/fxr/8.0.31", "shared/Microsoft.NETCore.App/8.0.31"):
        _root(root / base)
        required.update(base + "/" + path for path in _inventory(root / base, deadline_ns))
    if (
        not {
            "host/fxr/8.0.31/libhostfxr.so",
            "shared/Microsoft.NETCore.App/8.0.31/libhostpolicy.so",
            "shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so",
        }
        <= required
    ):
        raise ValueError(_ERROR)
    return required


def validate_manifest(body: bytes, expected_sha256: str) -> dict[str, Any]:
    """Validate root-owned closed schema and compare its independently expected digest."""
    _hash(expected_sha256)
    if type(body) is not bytes or not 0 < len(body) <= 2 * 1024**2:
        raise ValueError(_ERROR)
    value = strict_json_object(body)
    fixed = {
        "schema_version": 1,
        "backend": "mssql_sqlclient",
        "platform": "linux_arm64",
        "runtime_version": "8.0.31",
        "sqlclient_version": "7.0.2",
        "arrow_version": "23.0.0",
    }
    if set(value) != {*fixed, "files"} or any(type(value[k]) is not type(v) or value[k] != v for k, v in fixed.items()):
        raise ValueError(_ERROR)
    files = value["files"]
    if type(files) is not list or not 7 <= len(files) <= 4096:
        raise ValueError(_ERROR)
    keys, roles = [], []
    for row in files:
        if type(row) is not dict or set(row) != {"origin", "path", "role", "sha256"}:
            raise ValueError(_ERROR)
        _relative(row["path"])
        _hash(row["sha256"])
        origin, role = row["origin"], row["role"]
        if (
            type(origin) is not str
            or type(role) is not str
            or origin not in {"companion", "dotnet"}
            or role
            not in {
                "dotnet_host",
                "runtime_library",
                "worker_assembly",
                "worker_runtimeconfig",
                "worker_deps",
                "managed_dependency",
                "companion_native",
            }
        ):
            raise ValueError(_ERROR)
        if (origin == "dotnet") != (role in {"dotnet_host", "runtime_library"}):
            raise ValueError(_ERROR)
        keys.append((origin, row["path"]))
        roles.append(role)
    if keys != sorted(set(keys)) or any(
        roles.count(role) != 1 for role in ("dotnet_host", "worker_assembly", "worker_runtimeconfig", "worker_deps")
    ):
        raise ValueError(_ERROR)
    if "runtime_library" not in roles or any(
        sum(row["role"] == "managed_dependency" and Path(row["path"]).name == name for row in files) != 1
        for name in ("Microsoft.Data.SqlClient.dll", "Apache.Arrow.dll")
    ):
        raise ValueError(_ERROR)
    if hashlib.sha256(_DOMAIN + canonical_json_bytes(value)).hexdigest() != expected_sha256:
        raise ValueError(_ERROR)
    return value


def _profile(value: dict[str, Any]) -> None:
    options = value.get("runtimeOptions", {})
    properties = options.get("configProperties", {})
    if (
        options.get("framework") != {"name": "Microsoft.NETCore.App", "version": "8.0.31"}
        or options.get("rollForward") != "Disable"
        or properties.get("System.GC.HeapHardLimit") != 536870912
        or type(properties.get("System.GC.HeapHardLimit")) is not int
        or properties.get("System.GC.Server") is not False
        or set(options) - {"tfm", "framework", "rollForward", "configProperties"}
        or any("GC" in k and k not in {"System.GC.HeapHardLimit", "System.GC.Server"} for k in properties)
    ):
        raise ValueError(_ERROR)


def _deps(value: dict[str, Any], inventory: dict[str, str]) -> None:
    target = value.get("runtimeTarget", {}).get("name")
    libraries = value.get("libraries", {})
    if not {"Microsoft.Data.SqlClient/7.0.2", "Apache.Arrow/23.0.0"} <= set(libraries):
        raise ValueError(_ERROR)
    selected = value.get("targets", {}).get(target)
    if type(selected) is not dict:
        raise ValueError(_ERROR)
    resolved: set[str] = set()
    for library in selected.values():
        for category in ("runtime", "native", "runtimeTargets", "resources"):
            for path in library.get(category, {}):
                _relative(path)
                parts = path.split("/")
                reduced = "/".join(parts[parts.index("lib") + 2 :]) if "lib" in parts else path
                candidates = [path, reduced, Path(path).name]
                match = next((candidate for candidate in candidates if candidate in inventory), None)
                if match is None:
                    raise ValueError(_ERROR)
                resolved.add(match)
    if any((name.endswith(".dll") or ".so" in Path(name).name) and name not in resolved for name in inventory):
        raise ValueError(_ERROR)
