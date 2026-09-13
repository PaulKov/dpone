"""Validate externally pinned, administrator-owned dispatcher startup originals.

Decoding establishes configuration shape and identity only. Capture metadata is
a declared pin: protected enrollment and fresh host facts must independently
establish custody. This module never opens credentials or starts a service.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from ipaddress import ip_address
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any

from dpone.adapters.composition_dispatcher_context_files import DispatcherContextFiles
from dpone.adapters.composition_mssql_layout import require_control_schema
from dpone.contracts.composition_dispatcher_binding import require_dispatcher_connection_ref
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_snapshot_materialization import catalog_uuid
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

SCHEMA = "dpone.composition-dispatcher-service.v2"
MAX_CONFIGURATION_BYTES = 1024 * 1024
_FIELDS = {
    "schema",
    "dispatcher_id",
    "dispatcher_uid",
    "dispatcher_gid",
    "capture_custody",
    "context_root",
    "host_probe_socket",
    "supervisor_enrollment_sha256",
    "capture_root",
    "capture_root_identity",
    "listen",
    "tls",
    "bearer_file",
    "accept_timeout_seconds",
    "execution_timeout_seconds",
    "max_concurrency",
    "authorities",
}


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


@dataclass(frozen=True, slots=True)
class DispatcherAuthorityConfig:
    """Fixed control binding and protected original for one runtime authority."""

    context_sha256: str
    control_connection_ref: str
    expected_control_service_id: str
    control_schema: str


@dataclass(frozen=True, slots=True)
class DispatcherServiceConfig:
    """Decoded immutable configuration, carrying its externally checked digest.

    Obtain validated instances through ``decode_dispatcher_service_config`` or the
    protected loader. Constructing this value alone grants no execution authority.
    """

    dispatcher_id: str
    dispatcher_uid: int
    dispatcher_gid: int
    capture_custody: str
    context_root: Path
    host_probe_socket: Path
    supervisor_enrollment_sha256: str
    capture_root: Path
    capture_root_identity: DispatcherCaptureRootIdentity
    listen: DispatcherListenConfig
    tls: DispatcherTlsConfig
    bearer_file: Path
    accept_timeout_seconds: int
    execution_timeout_seconds: int
    max_concurrency: int
    authorities: Mapping[str, DispatcherAuthorityConfig]
    configuration_sha256: str
    document: bytes = field(repr=False)

    @property
    def sha256(self) -> str:
        """Digest of the retained exact original for signed binding comparison."""
        return "sha256:" + sha256(self.document).hexdigest()


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


def _authorities(value: Any) -> Mapping[str, DispatcherAuthorityConfig]:
    if type(value) is not dict or not 0 < len(value) <= 1024:
        raise ValueError
    result = {}
    for digest, item in value.items():
        require_digest(digest)
        item = _object(
            item, {"context_sha256", "control_connection_ref", "expected_control_service_id", "control_schema"}
        )
        require_digest(item["context_sha256"])
        result[digest] = DispatcherAuthorityConfig(
            item["context_sha256"],
            require_dispatcher_connection_ref(item["control_connection_ref"]),
            _uuid(item["expected_control_service_id"]),
            require_control_schema(item["control_schema"]),
        )
    return MappingProxyType(result)


def decode_dispatcher_service_config(
    document: bytes,
    *,
    expected_sha256: str,
    bootstrap_uid: int,
    bootstrap_gid: int,
) -> DispatcherServiceConfig:
    """Pure bounded decoder; verify canonical originals and external identity pins.

    No filesystem or process state is consulted. Production startup must use the
    protected loader, which additionally checks the actual Linux process identity.
    """
    try:
        require_digest(expected_sha256)
        _integer(bootstrap_uid, 1, 2147483647)
        _integer(bootstrap_gid, 1, 2147483647)
        if type(document) is not bytes or not 0 < len(document) <= MAX_CONFIGURATION_BYTES:
            raise ValueError
        if "sha256:" + sha256(document).hexdigest() != expected_sha256:
            raise ValueError
        value = _object(strict_json_object(document), _FIELDS)
        if canonical_json_bytes(value) != document or value["schema"] != SCHEMA:
            raise ValueError
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
        require_digest(value["supervisor_enrollment_sha256"])
        return DispatcherServiceConfig(
            _uuid(value["dispatcher_id"]),
            uid,
            gid,
            value["capture_custody"],
            _path(value["context_root"]),
            _path(value["host_probe_socket"]),
            value["supervisor_enrollment_sha256"],
            _path(value["capture_root"]),
            capture_identity,
            DispatcherListenConfig(address, _integer(listen["port"], 1, 65535)),
            DispatcherTlsConfig(_path(tls["certificate_file"]), _path(tls["private_key_file"])),
            _path(value["bearer_file"]),
            _integer(value["accept_timeout_seconds"], 1, 30),
            _integer(value["execution_timeout_seconds"], 1, 900),
            _integer(value["max_concurrency"], 1, 64),
            _authorities(value["authorities"]),
            expected_sha256,
            document,
        )
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("dispatcher_service_config") from None


def load_dispatcher_service_config(
    root: Path,
    relative: str,
    *,
    expected_sha256: str,
    bootstrap_uid: int,
    bootstrap_gid: int,
) -> DispatcherServiceConfig:
    """Read an administrator-owned original under a fixed protected root.

    Bootstrap IDs and digest come from the external service launch configuration.
    Require real/effective/saved IDs to agree before any file read. ``relative`` is
    a canonical path such as ``startup/service.json`` beneath ``root``; the existing
    protected reader enforces no-follow access and root:dispatcher permissions.
    """
    try:
        _integer(bootstrap_uid, 1, 2147483647)
        _integer(bootstrap_gid, 1, 2147483647)
        require_digest(expected_sha256)
        if sys.platform != "linux" or os.getresuid() != (bootstrap_uid,) * 3 or os.getresgid() != (bootstrap_gid,) * 3:
            raise ValueError
        root = _path(str(root))
        if (
            type(relative) is not str
            or str(PurePosixPath(relative)) != relative
            or PurePosixPath(relative).is_absolute()
            or ".." in PurePosixPath(relative).parts
            or len(PurePosixPath(relative).parts) < 2
            or any(ord(char) < 32 or ord(char) == 127 for char in relative)
        ):
            raise ValueError
        document = DispatcherContextFiles(root, dispatcher_gid=bootstrap_gid).read(
            relative,
            max_bytes=MAX_CONFIGURATION_BYTES,
        )
        return decode_dispatcher_service_config(
            document,
            expected_sha256=expected_sha256,
            bootstrap_uid=bootstrap_uid,
            bootstrap_gid=bootstrap_gid,
        )
    except Exception:
        raise CompositionAdmissionError("dispatcher_service_config") from None
