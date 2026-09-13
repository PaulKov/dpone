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
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any

from dpone.adapters.composition_dispatcher_context_files import DispatcherContextFiles
from dpone.adapters.composition_mssql_layout import require_control_schema
from dpone.app.composition_dispatcher_configuration_values import (
    DispatcherCaptureRootIdentity,
    DispatcherListenConfig,
    DispatcherTlsConfig,
    _integer,
    _object,
    _path,
    _uuid,
    settings,
)
from dpone.app.composition_dispatcher_service_policy import DispatcherServicePolicy, decode_dispatcher_service_policy
from dpone.contracts.composition_dispatcher_binding import require_dispatcher_connection_ref
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
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
    service_policy: DispatcherServicePolicy | None = None

    @property
    def service_policy_sha256(self) -> str | None:
        """Stable v3 policy identity, absent for a legacy full-configuration binding."""
        return self.service_policy.sha256 if self.service_policy is not None else None

    @property
    def binding_identity_kind(self) -> str:
        return "policy" if self.service_policy is not None else "configuration"

    @property
    def binding_identity_sha256(self) -> str:
        return self.service_policy.sha256 if self.service_policy is not None else self.sha256

    @property
    def sha256(self) -> str:
        """Exact bootstrap digest; use binding_identity_sha256 for the selected binding."""
        return "sha256:" + sha256(self.document).hexdigest()


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
        value = strict_json_object(document)
        if canonical_json_bytes(value) != document:
            raise ValueError
        policy = None
        if value.get("schema") == "dpone.composition-dispatcher-service.v3":
            _object(value, {"schema", "policy", "supervisor_enrollment_sha256", "authorities"})
            policy_original = canonical_json_bytes(value["policy"])
            policy = decode_dispatcher_service_policy(
                policy_original,
                expected_sha256="sha256:" + sha256(policy_original).hexdigest(),
                bootstrap_uid=bootstrap_uid,
                bootstrap_gid=bootstrap_gid,
            )
            common = settings(strict_json_object(policy.document), bootstrap_uid, bootstrap_gid)
        else:
            _object(value, _FIELDS)
            if value["schema"] != SCHEMA:
                raise ValueError
            common = settings(value, bootstrap_uid, bootstrap_gid)
        require_digest(value["supervisor_enrollment_sha256"])
        return DispatcherServiceConfig(
            **common,
            supervisor_enrollment_sha256=value["supervisor_enrollment_sha256"],
            authorities=_authorities(value["authorities"]),
            configuration_sha256=expected_sha256,
            document=document,
            service_policy=policy,
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
