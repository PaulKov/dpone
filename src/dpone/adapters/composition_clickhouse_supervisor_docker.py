"""Fixed local Docker inventory, closed API paths and stable inspect facts.

Only the rootful local Unix socket is supported; Docker contexts, proxies,
remote endpoints, CLI configuration and automatic API negotiation are excluded.
All running containers must appear in the protected enrollment's role map.
"""

from __future__ import annotations

import http.client
import ipaddress
import re
import socket
import time
from threading import Timer
from typing import Any

from dpone.adapters.composition_clickhouse_http import CompleteHttpResponse
from dpone.adapters.composition_clickhouse_supervisor_enrollment import absolute_path, container_id
from dpone.adapters.composition_clickhouse_supervisor_linux import digest, require
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


class _DockerConnection(http.client.HTTPConnection):
    response_class = CompleteHttpResponse

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/var/run/docker.sock")


def _closed_mount(value: Any) -> dict[str, Any]:
    require(type(value) is dict and value.get("Type") in {"bind", "volume", "tmpfs"}, "docker_mount")
    destination = absolute_path(value.get("Destination"))
    source = value.get("Source", "")
    require(type(value.get("RW")) is bool and value.get("Propagation", "") in {"", "rprivate"}, "docker_mount")
    if value["Type"] == "tmpfs":
        require(source in {"", "tmpfs"}, "docker_tmpfs_source")
    else:
        absolute_path(source)
        require(
            source not in {"/proc", "/sys", "/dev", "/run", "/var/run"}
            and not source.endswith("/docker.sock")
            and not destination.endswith("/docker.sock"),
            "docker_socket_mount",
        )
    return {
        "type": value["Type"],
        "source": source,
        "destination": destination,
        "readonly": not value["RW"],
        "propagation": value.get("Propagation", ""),
        "name": value.get("Name", ""),
    }


def normalize_container(raw: Any, *, role: str, clickhouse_id: str) -> dict[str, Any]:
    """Whitelist stable fields and reject access that can escape the enrolled cell."""
    require(type(raw) is dict and type(raw.get("HostConfig")) is dict, "docker_container")
    host = raw["HostConfig"]
    require(
        host.get("Privileged") is False
        and not host.get("CapAdd")
        and host.get("CapDrop") == ["ALL"]
        and not any(
            host.get(key)
            for key in (
                "Devices",
                "DeviceRequests",
                "DeviceCgroupRules",
                "VolumesFrom",
                "PortBindings",
                "PublishAllPorts",
                "Links",
            )
        )
        and host.get("PidMode") in {"", "private"}
        and host.get("IpcMode") in {"none", "private"}
        and host.get("CgroupnsMode") == "private"
        and host.get("SecurityOpt") == ["no-new-privileges:true"],
        "docker_escape",
    )
    require(host.get("NetworkMode") not in {"host", "none", "default", "bridge", ""}, "docker_network")
    if role == "dispatcher":
        require(host["NetworkMode"] == "container:" + clickhouse_id, "dispatcher_namespace")
    else:
        require(not host["NetworkMode"].startswith("container:"), "docker_namespace_sharer")
    config, state, network = raw.get("Config"), raw.get("State"), raw.get("NetworkSettings")
    require(type(config) is type(state) is type(network) is dict, "docker_inspect")
    require(
        state.get("Running") is True
        and state.get("Paused") is False
        and state.get("Restarting") is False
        and state.get("Dead") is False
        and type(state.get("Pid")) is int
        and state["Pid"] > 1
        and type(state.get("StartedAt")) is str
        and bool(state["StartedAt"])
        and not network.get("Ports")
        and not network.get("SecondaryIPAddresses")
        and not network.get("SecondaryIPv6Addresses"),
        "docker_running",
    )
    require(re.fullmatch(r"[1-9][0-9]*(?::[1-9][0-9]*)?", config.get("User", "")) is not None, "docker_uid")
    require(
        type(raw.get("Image")) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", raw["Image"]) is not None, "docker_image"
    )
    require(type(raw.get("Mounts")) is list and len(raw["Mounts"]) <= 32, "docker_mount_budget")
    mounts = sorted((_closed_mount(value) for value in raw["Mounts"]), key=lambda value: value["destination"])
    require(len({value["destination"] for value in mounts}) == len(mounts), "docker_mount_alias")
    if role in {"clickhouse", "dispatcher"}:
        require(host.get("ReadonlyRootfs") is True, "docker_root_writable")
        writable = [value for value in mounts if not value["readonly"]]
        expected = ("volume", "/var/lib/clickhouse") if role == "clickhouse" else ("tmpfs", "/run/dpone-secrets")
        require(
            len(writable) == 1 and (writable[0]["type"], writable[0]["destination"]) == expected,
            "docker_writable_mount",
        )
    networks = network.get("Networks")
    require(type(networks) is dict and len(networks) <= 1, "docker_extra_network")
    memberships = []
    for name, value in sorted(networks.items()):
        address = value.get("IPAddress")
        require(
            type(address) is str
            and str(ipaddress.IPv4Address(address)) == address
            and not ipaddress.ip_address(address).is_loopback,
            "docker_address",
        )
        require(not value.get("GlobalIPv6Address"), "docker_ipv6")
        memberships.append(
            {
                "name": name,
                "id": container_id(value.get("NetworkID")),
                "ip": address,
                "endpoint_id": container_id(value.get("EndpointID")),
            }
        )
    require(len(memberships) == (0 if role == "dispatcher" else 1), "docker_membership")
    # Config may contain secrets; retain only its digest, never Docker's raw Env.
    return {
        "id": container_id(raw.get("Id")),
        "role": role,
        "image": raw["Image"],
        "pid": state["Pid"],
        "started_at": state["StartedAt"],
        "user": config["User"],
        "config_sha256": digest(canonical_json_bytes(config)),
        "host_config_sha256": digest(canonical_json_bytes(host)),
        "network_mode": host["NetworkMode"],
        "readonly_root": host.get("ReadonlyRootfs") is True,
        "mounts": mounts,
        "networks": memberships,
    }


class LocalDockerSupervisorClient:
    """A fresh complete response per fixed GET, with one caller-owned deadline."""

    def _get(self, path: str, deadline: float) -> Any:
        require(
            re.fullmatch(
                r"/v1\.41/(info|containers/json\?all=0|containers/[0-9a-f]{64}/json|networks/[0-9a-f]{64})", path
            )
            is not None,
            "docker_path",
        )
        remaining = deadline - time.monotonic()
        require(remaining > 0, "docker_deadline")
        connection = _DockerConnection("localhost", timeout=remaining)

        def abort() -> None:
            if connection.sock is not None:
                try:
                    connection.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        timer = Timer(remaining, abort)
        timer.daemon = True
        timer.start()
        try:
            connection.request("GET", path, headers={"Connection": "close", "Accept": "application/json"})
            response = connection.getresponse()
            headers = response.headers
            require(
                response.status == 200 and not headers.get_all("Content-Encoding") and not headers.get_all("Location"),
                "docker_http",
            )
            lengths, encodings = headers.get_all("Content-Length", []), headers.get_all("Transfer-Encoding", [])
            if encodings:
                require(encodings == ["chunked"] and not lengths and response.chunked, "docker_framing")
            else:
                require(
                    len(lengths) == 1
                    and re.fullmatch(r"0|[1-9][0-9]*", lengths[0]) is not None
                    and int(lengths[0]) <= 1048576,
                    "docker_framing",
                )
            body = response.read(1048577)
            require(
                len(body) <= 1048576
                and (response.chunked or len(body) == int(lengths[0]))
                and time.monotonic() < deadline,
                "docker_complete",
            )
            return strict_json_object(b'{"value":' + body + b"}")["value"]
        except Exception:
            raise CompositionAdmissionError("clickhouse_supervisor_docker_unavailable") from None
        finally:
            timer.cancel()
            connection.close()

    def snapshot(self, policy: dict[str, Any], deadline: float) -> dict[str, Any]:
        """Complete running inventory must equal the external policy exactly."""
        info = self._get("/v1.41/info", deadline)
        require(
            type(info) is dict
            and info.get("OSType") == "linux"
            and type(info.get("ID")) is str
            and bool(info["ID"])
            and info.get("Swarm", {}).get("LocalNodeState") == "inactive",
            "docker_daemon",
        )
        inventory = self._get("/v1.41/containers/json?all=0", deadline)
        require(type(inventory) is list and 2 <= len(inventory) <= 64, "docker_inventory")
        ids = [container_id(value.get("Id")) for value in inventory]
        require(len(set(ids)) == len(ids) and set(ids) == set(policy["roles"]), "docker_inventory_changed")
        clickhouse = next(key for key, value in policy["roles"].items() if value == "clickhouse")
        containers = {}
        for identifier in sorted(ids):
            value = normalize_container(
                self._get("/v1.41/containers/" + identifier + "/json", deadline),
                role=policy["roles"][identifier],
                clickhouse_id=clickhouse,
            )
            require(
                value["id"] == identifier and all(row["id"] == policy["network_id"] for row in value["networks"]),
                "docker_subject",
            )
            containers[identifier] = value
        network = self._get("/v1.41/networks/" + container_id(policy["network_id"]), deadline)
        require(
            type(network) is dict
            and network.get("Id") == policy["network_id"]
            and network.get("Driver") == "bridge"
            and network.get("Scope") == "local"
            and network.get("Internal") is True
            and network.get("Ingress") is False
            and network.get("EnableIPv6") is False,
            "docker_bridge",
        )
        expected_members = set(ids) - {key for key, role in policy["roles"].items() if role == "dispatcher"}
        require(
            type(network.get("Containers")) is dict and set(network["Containers"]) == expected_members,
            "docker_network_members",
        )
        return {
            "daemon_id": info["ID"],
            "containers": containers,
            "network": {
                key: network.get(key) for key in ("Id", "Name", "Driver", "Internal", "Options", "IPAM", "Containers")
            },
        }
