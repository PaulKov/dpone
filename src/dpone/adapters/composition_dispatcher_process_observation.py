"""Bind bootstrap to the actual enrolled process, socket and immutable policy.

This is a read-only local Linux observation, not SQL enrollment authority or
cross-container alias proof. The caller must independently validate protected
P/E originals, fresh host facts and bootstrap/context filesystem separation.
All reads reuse the supervisor probe and share one caller-owned deadline.
"""

from __future__ import annotations

import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Protocol

from dpone.adapters.composition_clickhouse_supervisor_enrollment import ClickHouseSupervisorEnrollment
from dpone.adapters.composition_clickhouse_supervisor_linux import LinuxSupervisorProbe, digest
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.strict_json import canonical_json_bytes


class _ListenPolicy(Protocol):
    @property
    def address(self) -> str: ...

    @property
    def port(self) -> int: ...


class DispatcherProcessPolicy(Protocol):
    """Read-only policy projection; the application owns complete P validation."""

    @property
    def dispatcher_uid(self) -> int: ...

    @property
    def dispatcher_gid(self) -> int: ...

    @property
    def sha256(self) -> str: ...

    @property
    def listen(self) -> _ListenPolicy: ...


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise CompositionAdmissionError("dispatcher_process_" + reason)


def _check(deadline: float) -> None:
    _require(sys.platform == "linux" and time.monotonic() < deadline, "linux_deadline")


def _identity(
    probe: LinuxSupervisorProbe,
    pid: int,
    expected: dict[str, Any],
    host_pid: int,
    policy: DispatcherProcessPolicy,
    deadline: float,
) -> dict[str, Any]:
    _check(deadline)
    if sys.platform != "linux":
        raise CompositionAdmissionError("dispatcher_process_linux_deadline")
    _require(
        os.getpid() == pid
        and os.getresuid() == (policy.dispatcher_uid,) * 3
        and os.getresgid() == (policy.dispatcher_gid,) * 3,
        "identity",
    )
    actual = probe.process(pid, deadline)
    for key in (
        "start_ticks",
        "namespaces",
        "executable",
        "CapInh",
        "CapPrm",
        "CapEff",
        "CapBnd",
        "CapAmb",
        "NoNewPrivs",
        "Seccomp",
    ):
        _require(actual[key] == expected[key], "identity")
    for field, identifier in (("Uid", policy.dispatcher_uid), ("Gid", policy.dispatcher_gid)):
        _require(
            type(identifier) is int
            and 0 < identifier < 2**31
            and tuple(actual[field]) == tuple(expected[field]) == (identifier,) * 4,
            "identity",
        )
    local, enrolled = actual["NSpid"], expected["NSpid"]
    _require(
        type(local) in {list, tuple}
        and type(enrolled) in {list, tuple}
        and 1 <= len(local) <= len(enrolled) <= 8
        and all(type(value) is int and 0 < value < 2**31 for value in (*local, *enrolled)),
        "pid",
    )
    # Host observations include ancestor PID namespaces; local /proc can expose
    # only the suffix. Parent/group/session IDs and cgroup strings are view-local.
    _require(
        type(host_pid) is int
        and expected["pid"] == enrolled[0] == host_pid
        and actual["pid"] == pid == local[-1]
        and tuple(local) == tuple(enrolled[-len(local) :]),
        "pid",
    )
    _check(deadline)
    return actual


def _listener(
    probe: LinuxSupervisorProbe,
    pid: int,
    facts: dict[str, Any],
    identifier: str,
    policy: DispatcherProcessPolicy,
    deadline: float,
) -> None:
    rows = [row for row in facts["listeners"] if row["container_id"] == identifier]
    _require(len(rows) == 1, "listener")
    enrolled = rows[0]
    _require(
        set(enrolled) == {"address", "port", "inode", "uid", "container_id"}
        and enrolled["address"] == policy.listen.address
        and enrolled["port"] == policy.listen.port
        and enrolled["uid"] == policy.dispatcher_uid
        and type(enrolled["inode"]) is int
        and 0 < enrolled["inode"] < 2**64,
        "listener",
    )
    expected = tuple(enrolled[key] for key in ("address", "port", "inode", "uid"))
    observed = [row for row in probe.listeners(pid, deadline) if row[:2] == expected[:2]]
    _require(observed == [expected] and enrolled["inode"] in probe.socket_inodes(pid, deadline), "listener")


def _policy_file(
    probe: LinuxSupervisorProbe,
    pid: int,
    roots: list[str],
    configs: dict[str, Any],
    path: Path,
    policy_sha256: str,
    deadline: float,
) -> None:
    _require(set(configs) == set(roots), "config_roots")
    candidates = [root for root in roots if path.is_relative_to(Path(root))]
    _require(len(candidates) == 1, "policy_path")
    for root in roots:
        observed = probe.config_tree(pid, root, deadline)
        _require(canonical_json_bytes(observed) == canonical_json_bytes(configs[root]), "config_tree")
    root = candidates[0]
    relative = path.relative_to(root).as_posix()
    rows = [row for row in configs[root] if row["path"] == relative]
    _require(len(rows) == 1 and rows[0]["sha256"] == policy_sha256, "policy_original")
    expected = {key: rows[0][key] for key in ("device", "inode", "mode", "uid", "gid")}
    _require(probe.path_identity(str(path), deadline) == expected, "policy_identity")


def require_dispatcher_process(
    enrollment: ClickHouseSupervisorEnrollment,
    policy: DispatcherProcessPolicy,
    policy_path: Path,
    deadline: float,
) -> None:
    """Require the enrolled process/socket and exact P observation without writes.

    Two complete local observations and a final process read reject drift around
    policy/configuration reads. The probe's bounded inventories reject missing,
    truncated or unavailable data; no cached observation or fallback is accepted.
    """
    try:
        _require(type(deadline) in {int, float} and math.isfinite(deadline), "deadline")
        _check(deadline)
        require_digest(policy.sha256)
        path = Path(policy_path)
        _require(path.is_absolute() and ".." not in path.parts, "policy_path")
        value = ClickHouseSupervisorEnrollment(enrollment.enrollment_sha256, enrollment.document)
        identifier = value.role_id("dispatcher")
        facts = value.body["facts"]
        retained = facts["linux"]["containers"][identifier]
        host_pid = facts["docker"]["containers"][identifier]["pid"]
        probe, pid = LinuxSupervisorProbe(), os.getpid()
        first = _identity(probe, pid, retained["init"], host_pid, policy, deadline)
        for _ in range(2):
            _policy_file(
                probe, pid, value.policy["config_roots"][identifier], retained["configs"], path, policy.sha256, deadline
            )
            _require(
                digest(canonical_json_bytes(probe.mounts(pid, deadline))) == retained["mounts"]["mountinfo_sha256"],
                "mount_pin",
            )
            _listener(probe, pid, facts["linux"], identifier, policy, deadline)
            _require(_identity(probe, pid, retained["init"], host_pid, policy, deadline) == first, "drift")
        _check(deadline)
    except CompositionAdmissionError:
        raise
    except Exception:
        raise CompositionAdmissionError("dispatcher_process_unavailable") from None
