"""Observe the enrolled local sole-writer topology, without adoption authority.

The SQL enrollment is supplied by an external trusted provisioner. Observed
facts must equal its original twice in the same pinned control transaction.
Server/database UUID queries remain independently mandatory in the gate admin.
"""

from __future__ import annotations

import ipaddress
import math
import re
import time
from pathlib import PurePosixPath
from typing import Any, cast

from dpone.adapters.composition_clickhouse_gate_queries import ClickHouseLocalSupervisorObservation
from dpone.adapters.composition_clickhouse_principal import require_uuid
from dpone.adapters.composition_clickhouse_supervisor_docker import LocalDockerSupervisorClient
from dpone.adapters.composition_clickhouse_supervisor_enrollment import (
    ClickHouseSupervisorEnrollment,
    SupervisorEnrollmentReader,
)
from dpone.adapters.composition_clickhouse_supervisor_linux import LinuxSupervisorProbe, digest, require
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_persistence import CompositionAttemptIdentity
from dpone.contracts.composition_snapshot import SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def _secure_process(process: dict[str, Any], container: dict[str, Any]) -> None:
    identifier, uid = container["id"], int(container["user"].split(":")[0])
    require(
        tuple(process["Uid"]) == (uid,) * 4
        and all(value > 0 for value in process["Gid"])
        and process["NoNewPrivs"] == 1
        and process["Seccomp"] == 2
        and all(process[key] == 0 for key in ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")),
        "process_privilege",
    )
    require(
        re.fullmatch(r"0::/(?:[^\r\n]*/)?(?:docker-" + identifier + r"\.scope|" + identifier + ")", process["cgroup"])
        is not None,
        "process_cgroup",
    )


def _contains(parent: str, child: str) -> bool:
    return PurePosixPath(child).is_relative_to(PurePosixPath(parent))


def _mounts(probe: LinuxSupervisorProbe, container: dict[str, Any], deadline: float) -> dict[str, Any]:
    pid = container["pid"]
    observed = cast(tuple[dict[str, Any], ...], probe.mounts(pid, deadline))
    roots = [row for row in observed if row["destination"] == "/"]
    require(len(roots) == 1 and (not container["readonly_root"] or "ro" in roots[0]["options"]), "mount_root")
    mounts = []
    for mount in container["mounts"]:
        rows = [row for row in observed if row["destination"] == mount["destination"]]
        require(len(rows) == 1 and ("ro" in rows[0]["options"]) == mount["readonly"], "mount_readonly")
        if mount["type"] == "tmpfs":
            require(
                rows[0]["filesystem"] == "tmpfs"
                and all(flag in rows[0]["options"] for flag in ("nosuid", "nodev", "noexec")),
                "secret_tmpfs",
            )
            source = None
        else:
            source = probe.path_identity(mount["source"], deadline)
            inside = probe.path_identity(f"/proc/{pid}/root" + mount["destination"], deadline)
            require((source["device"], source["inode"]) == (inside["device"], inside["inode"]), "mount_source_alias")
        mounts.append({"destination": mount["destination"], "source_identity": source})
    return {"mountinfo_sha256": digest(canonical_json_bytes(observed)), "sources": mounts}


def _protected_sources(containers: dict[str, Any], linux: dict[str, Any], protected: set[str]) -> None:
    """Reject direct, ancestor, descendant and inode aliases to protected mounts."""
    for owner in protected:
        for mount in containers[owner]["mounts"]:
            if mount["type"] == "tmpfs":
                continue
            original = next(
                row["source_identity"]
                for row in linux[owner]["mounts"]["sources"]
                if row["destination"] == mount["destination"]
            )
            for candidate_id, candidate in containers.items():
                if candidate_id == owner:
                    continue
                for other in candidate["mounts"]:
                    if other["type"] == "tmpfs":
                        continue
                    identity = next(
                        row["source_identity"]
                        for row in linux[candidate_id]["mounts"]["sources"]
                        if row["destination"] == other["destination"]
                    )
                    require(
                        not _contains(mount["source"], other["source"])
                        and not _contains(other["source"], mount["source"])
                        and (identity["device"], identity["inode"]) != (original["device"], original["inode"]),
                        "protected_mount_shared",
                    )


def capture_supervisor_facts(
    docker: LocalDockerSupervisorClient, linux: LinuxSupervisorProbe, policy: dict[str, Any], deadline: float
) -> dict[str, Any]:
    """Read concrete facts; this function neither enrolls nor authorizes them.

    Trusted provisioning can call this same normalizer before writing its own
    reviewed original. Runtime accepts results only through SQL comparison.
    """
    observed = docker.snapshot(policy, deadline)
    containers = observed["containers"]
    roles = policy["roles"]
    ch = next(key for key, role in roles.items() if role == "clickhouse")
    dispatcher = next(key for key, role in roles.items() if role == "dispatcher")
    protected = {ch, dispatcher}
    host_boot = linux.host_boot(deadline)
    require_uuid(host_boot)
    host_ns = {name: linux.namespace(1, name, deadline) for name in ("net", "pid", "mnt")}
    processes = {}
    for identifier, container in containers.items():
        process = linux.process(container["pid"], deadline)
        _secure_process(process, container)
        require(
            process["NSpid"][-1] == 1 and all(process["namespaces"][name] != host_ns[name] for name in host_ns),
            "host_namespace",
        )
        configs = {}
        mounts = _mounts(linux, container, deadline)
        for root in policy["config_roots"].get(identifier, []):
            covering = [row for row in container["mounts"] if _contains(row["destination"], root)]
            require(
                all(row["readonly"] for row in covering) and (container["readonly_root"] or bool(covering)),
                "config_writable",
            )
            configs[root] = linux.config_tree(container["pid"], root, deadline)
        processes[identifier] = {"init": process, "mounts": mounts, "configs": configs}
    anchor, frontend = processes[ch]["init"], processes[dispatcher]["init"]
    namespace = anchor["namespaces"]["net"]
    require(
        frontend["namespaces"]["net"] == namespace
        and all(frontend["namespaces"][key] != anchor["namespaces"][key] for key in ("pid", "mnt")),
        "dispatcher_isolation",
    )
    require(
        all(row["init"]["namespaces"]["net"] != namespace for key, row in processes.items() if key not in protected),
        "namespace_shared",
    )
    _protected_sources(containers, processes, protected)
    owners: dict[int, set[str]] = {}
    for pid in linux.processes(deadline):
        if linux.namespace(pid, "net", deadline) != namespace:
            continue
        process = linux.process(pid, deadline)
        matches = [key for key in protected if process["cgroup"] == processes[key]["init"]["cgroup"]]
        require(len(matches) == 1, "unattributed_process")
        owner = matches[0]
        _secure_process(process, containers[owner])
        require(process["namespaces"] == processes[owner]["init"]["namespaces"], "process_namespace_escape")
        for inode in linux.socket_inodes(pid, deadline):
            owners.setdefault(inode, set()).add(owner)
    ip = containers[ch]["networks"][0]["ip"]
    require(not ipaddress.ip_address(ip).is_loopback, "frontend_address")
    expected = {("127.0.0.1", 8123): ch, (ip, policy["frontend_port"]): dispatcher}
    listeners = linux.listeners(containers[ch]["pid"], deadline)
    require(len(listeners) == 2 and {(row[0], row[1]) for row in listeners} == set(expected), "raw_listener")
    sockets = []
    for address, port, inode, uid in listeners:
        owner = expected[(address, port)]
        require(owners.get(inode) == {owner} and uid == int(containers[owner]["user"].split(":")[0]), "listener_owner")
        sockets.append({"address": address, "port": port, "inode": inode, "uid": uid, "container_id": owner})
    require(time.monotonic() < deadline, "capture_deadline")
    # JSON normalization removes Python-only tuple/list variation.
    return strict_json_object(
        canonical_json_bytes(
            {
                "docker": observed,
                "linux": {
                    "host_boot_id": host_boot,
                    "host_namespaces": host_ns,
                    "containers": processes,
                    "network_namespace_id": namespace,
                    "listeners": sockets,
                },
            }
        )
    )


class DockerClickHouseLocalSupervisor:
    """Fail closed on unavailable Linux, Docker, enrollment or identity changes."""

    def __init__(
        self,
        *,
        enrollment_sha256: str,
        docker: LocalDockerSupervisorClient,
        linux: LinuxSupervisorProbe,
        timeout_seconds: float = 10.0,
    ) -> None:
        require_digest(enrollment_sha256)
        require(
            type(timeout_seconds) in {int, float} and math.isfinite(timeout_seconds) and 0 < timeout_seconds <= 60,
            "deadline_budget",
        )
        self._reference, self._docker, self._linux, self._timeout = enrollment_sha256, docker, linux, timeout_seconds

    def _capture(self, enrollment: ClickHouseSupervisorEnrollment, deadline: float) -> dict[str, Any]:
        return capture_supervisor_facts(self._docker, self._linux, enrollment.policy, deadline)

    def observe(
        self, context: CompositionMssqlLedger, *, attempt: CompositionAttemptIdentity, target: SnapshotTarget
    ) -> ClickHouseLocalSupervisorObservation:
        deadline = time.monotonic() + self._timeout
        try:
            reader = SupervisorEnrollmentReader(context, self._reference, attempt, target)
            enrollment = reader.read()
            expected = enrollment.body
            for _ in range(2):
                reader.check()
                facts = self._capture(enrollment, deadline)
                reader.check()
                require(facts == expected["facts"], "enrollment_drift")
                require(reader.read().document == enrollment.document, "enrollment_changed")
            require(time.monotonic() < deadline, "observation_deadline")
            fields = {name: expected[name] for name in ("service_id", "database_uuid", "boot_id", "isolation_id")}
            fields.update(
                enrollment_sha256=self._reference, network_namespace_id=facts["linux"]["network_namespace_id"]
            )
            document = canonical_json_bytes(
                {"schema": "dpone.composition-clickhouse-local-supervisor.v1", **fields, "facts": facts}
            )
            return ClickHouseLocalSupervisorObservation(
                **fields, evidence_sha256=digest(document), evidence_document=document
            )
        except Exception:
            raise CompositionAdmissionError("clickhouse_supervisor_unverified") from None
