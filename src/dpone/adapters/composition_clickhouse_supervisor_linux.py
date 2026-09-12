"""Bounded Linux /proc observations for the protected Docker supervisor.

No missing process, unreadable namespace or truncated inventory is treated as
absence. Pure parsers are independently testable; production reads fixed /proc.
"""

from __future__ import annotations

import os
import re
import socket
import stat
import sys
import time
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise CompositionAdmissionError("clickhouse_supervisor_" + reason)


def digest(value: bytes) -> str:
    return "sha256:" + sha256(value).hexdigest()


def parse_process_stat(raw: bytes, pid: int) -> dict[str, int]:
    require(type(raw) is bytes and len(raw) <= 65536 and raw.startswith(f"{pid} (".encode()), "process_stat")
    end = raw.rfind(b") ")
    require(end > 0, "process_stat")
    fields = raw[end + 2 :].split()
    require(
        len(fields) >= 20 and fields[0] not in {b"Z", b"X", b"x"} and all(fields[i].isdigit() for i in (1, 2, 3, 19)),
        "process_stat",
    )
    require(int(fields[19]) > 0, "process_start")
    return {
        "pid": pid,
        "parent_pid": int(fields[1]),
        "process_group": int(fields[2]),
        "session": int(fields[3]),
        "start_ticks": int(fields[19]),
    }


def parse_status(raw: bytes) -> dict[str, object]:
    require(type(raw) is bytes and len(raw) <= 65536, "process_status")
    fields: dict[str, str] = {}
    for line in raw.decode("ascii").splitlines():
        key, separator, value = line.partition(":")
        require(bool(separator) and key not in fields, "process_status")
        fields[key] = value.strip()
    result: dict[str, object] = {}
    for name in ("Uid", "Gid", "NSpid"):
        parts = fields.get(name, "").split()
        require(bool(parts) and all(value.isdecimal() for value in parts), "process_status")
        require(len(parts) == 4 if name in {"Uid", "Gid"} else 1 <= len(parts) <= 8, "process_status")
        result[name] = tuple(int(value) for value in parts)
    for name in ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"):
        require(re.fullmatch(r"[0-9a-fA-F]{16}", fields.get(name, "")) is not None, "process_capability")
        result[name] = int(fields[name], 16)
    for name in ("NoNewPrivs", "Seccomp"):
        require(fields.get(name, "").isdecimal(), "process_security")
        result[name] = int(fields[name])
    return result


def _unescape(value: str) -> str:
    require(re.search(r"\\(?!040|011|012|134)", value) is None, "mount_escape")
    return re.sub(r"\\(040|011|012|134)", lambda match: chr(int(match[1], 8)), value)


def parse_mountinfo(raw: bytes) -> tuple[dict[str, object], ...]:
    require(type(raw) is bytes and len(raw) <= 1048576, "mount_budget")
    rows: list[dict[str, object]] = []
    for line in raw.decode("utf-8").splitlines():
        left, separator, right = line.partition(" - ")
        a, b = left.split(), right.split()
        require(
            bool(separator)
            and len(a) >= 6
            and len(b) == 3
            and a[0].isdecimal()
            and a[1].isdecimal()
            and re.fullmatch(r"[0-9]+:[0-9]+", a[2]) is not None,
            "mount_shape",
        )
        rows.append(
            {
                "id": int(a[0]),
                "parent": int(a[1]),
                "device": a[2],
                "root": _unescape(a[3]),
                "destination": _unescape(a[4]),
                "options": tuple(sorted(a[5].split(","))),
                "propagation": tuple(sorted(a[6:])),
                "filesystem": b[0],
                "source": _unescape(b[1]),
                "super_options": tuple(sorted(b[2].split(","))),
            }
        )
    require(len(rows) <= 1024 and len({row["id"] for row in rows}) == len(rows), "mount_inventory")
    return tuple(rows)


def parse_listeners(raw: bytes, *, ipv6: bool) -> tuple[tuple[str, int, int, int], ...]:
    require(type(raw) is bytes and len(raw) <= 1048576, "socket_budget")
    lines = raw.decode("ascii").splitlines()
    require(bool(lines) and "local_address" in lines[0] and len(lines) <= 8193, "socket_inventory")
    listeners = []
    for line in lines[1:]:
        fields = line.split()
        require(len(fields) >= 10 and re.fullmatch(r"[0-9A-Fa-f]{2}", fields[3]) is not None, "socket_row")
        if fields[3] != "0A":
            continue
        address, separator, port = fields[1].partition(":")
        require(
            bool(separator)
            and len(address) == (32 if ipv6 else 8)
            and re.fullmatch(r"[0-9A-Fa-f]+", address) is not None
            and re.fullmatch(r"[0-9A-Fa-f]{4}", port) is not None
            and fields[7].isdigit()
            and fields[9].isdigit(),
            "socket_row",
        )
        raw_address = bytes.fromhex(address)
        raw_address = b"".join(raw_address[i : i + 4][::-1] for i in range(0, len(raw_address), 4))
        text = socket.inet_ntop(socket.AF_INET6 if ipv6 else socket.AF_INET, raw_address)
        require(0 < int(fields[9]) < 2**64, "socket_inode")
        listeners.append((text, int(port, 16), int(fields[9]), int(fields[7])))
    return tuple(sorted(listeners))


class LinuxSupervisorProbe:
    """Host supervisor reads actual local proc; callers supply one bounded deadline."""

    def __init__(self) -> None:
        self._proc = Path("/proc")

    def _check(self, deadline: float) -> None:
        require(sys.platform == "linux" and sys.byteorder == "little" and time.monotonic() < deadline, "linux_deadline")

    def _read(self, path: Path, deadline: float, maximum: int = 65536) -> bytes:
        self._check(deadline)
        with path.open("rb") as stream:
            value = stream.read(maximum + 1)
        self._check(deadline)
        require(len(value) <= maximum, "proc_budget")
        return value

    def host_boot(self, deadline: float) -> str:
        return self._read(self._proc / "sys/kernel/random/boot_id", deadline, 64).decode("ascii").strip()

    def namespace(self, pid: int, name: str, deadline: float) -> str:
        self._check(deadline)
        require(type(pid) is int and 0 < pid < 2**31 and name in {"net", "pid", "mnt"}, "namespace_subject")
        value = os.stat(self._proc / str(pid) / "ns" / name)
        require(value.st_ino > 0, "namespace_inode")
        return str(value.st_ino)

    def process(self, pid: int, deadline: float) -> dict[str, Any]:
        base = self._proc / str(pid)
        original = parse_process_stat(self._read(base / "stat", deadline), pid)
        status = parse_status(self._read(base / "status", deadline))
        cgroup = self._read(base / "cgroup", deadline).decode("ascii").strip()
        require(re.fullmatch(r"0::/[^\r\n]+", cgroup) is not None, "cgroup_v2_required")
        namespaces = {name: self.namespace(pid, name, deadline) for name in ("net", "pid", "mnt")}
        executable = os.stat(base / "exe")
        require(stat.S_ISREG(executable.st_mode), "process_executable")
        require(parse_process_stat(self._read(base / "stat", deadline), pid) == original, "process_reused")
        return {
            **original,
            **status,
            "cgroup": cgroup,
            "namespaces": namespaces,
            "executable": {"device": executable.st_dev, "inode": executable.st_ino},
        }

    def processes(self, deadline: float) -> tuple[int, ...]:
        self._check(deadline)
        values = []
        with os.scandir(self._proc) as entries:
            for entry in entries:
                self._check(deadline)
                if entry.name.isascii() and entry.name.isdecimal():
                    with os.scandir(self._proc / entry.name / "task") as tasks:
                        for task in tasks:
                            self._check(deadline)
                            require(task.name.isascii() and task.name.isdecimal(), "task_inventory")
                            values.append(int(task.name))
                            require(len(values) <= 8192, "process_inventory_budget")
                require(len(values) <= 8192, "process_inventory_budget")
        return tuple(sorted(values))

    def mounts(self, pid: int, deadline: float) -> tuple[dict[str, object], ...]:
        return parse_mountinfo(self._read(self._proc / str(pid) / "mountinfo", deadline, 1048576))

    def listeners(self, pid: int, deadline: float) -> tuple[tuple[str, int, int, int], ...]:
        base = self._proc / str(pid) / "net"
        for name in ("udp", "udp6", "raw", "raw6", "packet"):
            rows = self._read(base / name, deadline, 1048576).splitlines()
            require(len(rows) == 1 and bool(rows[0]), "unmodeled_protocol")
        return tuple(
            sorted(
                (
                    *parse_listeners(self._read(base / "tcp", deadline, 1048576), ipv6=False),
                    *parse_listeners(self._read(base / "tcp6", deadline, 1048576), ipv6=True),
                )
            )
        )

    def socket_inodes(self, pid: int, deadline: float) -> tuple[int, ...]:
        self._check(deadline)
        result = []
        with os.scandir(self._proc / str(pid) / "fd") as entries:
            for count, entry in enumerate(entries, 1):
                self._check(deadline)
                require(count <= 8192, "fd_budget")
                link = os.readlink(entry.path)
                if match := re.fullmatch(r"socket:\[([0-9]+)\]", link):
                    result.append(int(match[1]))
        return tuple(sorted(set(result)))

    def path_identity(self, path: str, deadline: float) -> dict[str, int]:
        self._check(deadline)
        if not path.startswith(str(self._proc) + "/"):
            require(os.path.realpath(path, strict=True) == path, "source_symlink")
        value = os.stat(path)
        return {
            "device": value.st_dev,
            "inode": value.st_ino,
            "mode": stat.S_IMODE(value.st_mode),
            "uid": value.st_uid,
            "gid": value.st_gid,
        }

    def config_tree(self, pid: int, destination: str, deadline: float) -> tuple[dict[str, object], ...]:
        """Hash complete bounded local readonly configuration; no symlink/FIFO fallback."""
        require(destination.startswith("/") and ".." not in Path(destination).parts, "config_path")
        root = self._proc / str(pid) / "root" / destination.lstrip("/")
        require(not root.is_symlink(), "config_symlink")
        paths = [root]
        rows: list[dict[str, object]] = []
        while paths:
            self._check(deadline)
            path = paths.pop()
            value = path.lstat()
            require(not stat.S_ISLNK(value.st_mode), "config_symlink")
            relative = "." if path == root else path.relative_to(root).as_posix()
            if stat.S_ISDIR(value.st_mode):
                with os.scandir(path) as entries:
                    for entry in entries:
                        paths.append(Path(entry.path))
                        require(len(paths) + len(rows) <= 256, "config_inventory_budget")
                content = None
            else:
                require(stat.S_ISREG(value.st_mode) and value.st_size <= 1048576, "config_file")
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor, "rb") as stream:
                    opened = os.fstat(stream.fileno())
                    require(
                        (opened.st_dev, opened.st_ino) == (value.st_dev, value.st_ino) and stat.S_ISREG(opened.st_mode),
                        "config_replaced",
                    )
                    original = stream.read(1048577)
                    require(len(original) <= 1048576, "config_budget")
                    content = digest(original)
            identity = self.path_identity(str(path), deadline)
            require((value.st_dev, value.st_ino) == (identity["device"], identity["inode"]), "config_replaced")
            rows.append({"path": relative, **identity, "sha256": content})
            require(len(rows) <= 256, "config_inventory_budget")
        return tuple(sorted(rows, key=lambda value: str(value["path"])))

    def mount_fingerprint(self, pid: int, deadline: float) -> str:
        return digest(canonical_json_bytes(self.mounts(pid, deadline)))
