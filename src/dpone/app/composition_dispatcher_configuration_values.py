"""Pure shared validation for immutable dispatcher listener and custody settings.

Separate wire versions reuse these field rules; no runtime or deployment
identity is inferred from paths or configured inode values.
"""

from __future__ import annotations

from dataclasses import dataclass
from ipaddress import ip_address
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.composition_snapshot_materialization import catalog_uuid


@dataclass(frozen=True, slots=True)
class DispatcherCaptureRootIdentity:
    """Configured inode pin, requiring independent enrollment/facts comparison."""

    device: int
    inode: int
    uid: int
    gid: int
    mode: int


@dataclass(frozen=True, slots=True)
class DispatcherListenConfig:
    """One numeric address and port, without resolution or socket creation."""

    address: str
    port: int


@dataclass(frozen=True, slots=True)
class DispatcherTlsConfig:
    """Protected credential coordinates; contents are not read by this loader."""

    certificate_file: Path
    private_key_file: Path


def _object(value: Any, fields: set[str]) -> dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise ValueError
    return value


def _integer(value: Any, minimum: int, maximum: int | None = None) -> int:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise ValueError
    return value


def _path(value: Any) -> Path:
    if (
        type(value) is not str
        or not 0 < len(value) <= 4096
        or not value.startswith("/")
        or value.startswith("//")
        or str(PurePosixPath(value)) != value
        or ".." in PurePosixPath(value).parts
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError
    return Path(value)


def _uuid(value: Any) -> str:
    if type(value) is not str:
        raise ValueError
    return catalog_uuid(value)


def settings(value: dict[str, Any], bootstrap_uid: int, bootstrap_gid: int) -> dict[str, Any]:
    """Decode stable listener/custody settings, shared by legacy and policy originals."""
    uid = _integer(value["dispatcher_uid"], 1, 2147483647)
    gid = _integer(value["dispatcher_gid"], 1, 2147483647)
    if (uid, gid) != (bootstrap_uid, bootstrap_gid) or value["capture_custody"] != "dispatcher_owned_v1":
        raise ValueError
    identity = _object(value["capture_root_identity"], {"device", "inode", "uid", "gid", "mode"})
    capture_identity = DispatcherCaptureRootIdentity(
        _integer(identity["device"], 0),
        _integer(identity["inode"], 1),
        _integer(identity["uid"], uid, uid),
        _integer(identity["gid"], gid, gid),
        _integer(identity["mode"], 448, 448),
    )
    listen = _object(value["listen"], {"address", "port"})
    address = listen["address"]
    if type(address) is not str or "%" in address or ip_address(address).is_unspecified:
        raise ValueError
    tls = _object(value["tls"], {"certificate_file", "private_key_file"})
    return {
        "dispatcher_id": _uuid(value["dispatcher_id"]),
        "dispatcher_uid": uid,
        "dispatcher_gid": gid,
        "capture_custody": value["capture_custody"],
        "context_root": _path(value["context_root"]),
        "host_probe_socket": _path(value["host_probe_socket"]),
        "capture_root": _path(value["capture_root"]),
        "capture_root_identity": capture_identity,
        "listen": DispatcherListenConfig(address, _integer(listen["port"], 1, 65535)),
        "tls": DispatcherTlsConfig(_path(tls["certificate_file"]), _path(tls["private_key_file"])),
        "bearer_file": _path(value["bearer_file"]),
        "accept_timeout_seconds": _integer(value["accept_timeout_seconds"], 1, 30),
        "execution_timeout_seconds": _integer(value["execution_timeout_seconds"], 1, 900),
        "max_concurrency": _integer(value["max_concurrency"], 1, 64),
    }
