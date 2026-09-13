"""Immutable acyclic service policy, independent of enrollment and release catalogs.

This decoder proves original shape and external digest agreement only. A listener
built from policy must keep admission closed until protected bootstrap and fresh
enrollment verification have completed against that same process and socket.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path

from dpone.app.composition_dispatcher_configuration_values import (
    DispatcherCaptureRootIdentity,
    DispatcherListenConfig,
    DispatcherTlsConfig,
    _integer,
    _object,
    _path,
    settings,
)
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

SCHEMA = "dpone.composition-dispatcher-service-policy.v1"
MAX_POLICY_BYTES = 1024 * 1024
_FIELDS = {
    "schema",
    "dispatcher_id",
    "dispatcher_uid",
    "dispatcher_gid",
    "capture_custody",
    "context_root",
    "host_probe_socket",
    "capture_root",
    "capture_root_identity",
    "listen",
    "tls",
    "bearer_file",
    "accept_timeout_seconds",
    "execution_timeout_seconds",
    "max_concurrency",
    "bootstrap_file",
    "startup_timeout_seconds",
}


@dataclass(frozen=True, slots=True)
class DispatcherServicePolicy:
    """Stable listener settings; paths/inode pins require actual runtime observation.

    Settings mirror the legacy DTO to preserve its constructor and public fields.
    Their shared parser owns validation for both wire versions.
    """

    dispatcher_id: str
    dispatcher_uid: int
    dispatcher_gid: int
    capture_custody: str
    context_root: Path
    host_probe_socket: Path
    capture_root: Path
    capture_root_identity: DispatcherCaptureRootIdentity
    listen: DispatcherListenConfig
    tls: DispatcherTlsConfig
    bearer_file: Path
    accept_timeout_seconds: int
    execution_timeout_seconds: int
    max_concurrency: int
    bootstrap_file: Path
    startup_timeout_seconds: int
    document: bytes = field(repr=False)

    @property
    def sha256(self) -> str:
        """Digest every byte of this canonical policy; no fields are excluded."""
        return "sha256:" + sha256(self.document).hexdigest()


def decode_dispatcher_service_policy(
    document: bytes,
    *,
    expected_sha256: str,
    bootstrap_uid: int,
    bootstrap_gid: int,
) -> DispatcherServicePolicy:
    """Validate one externally pinned policy without opening files or a listener."""
    try:
        require_digest(expected_sha256)
        _integer(bootstrap_uid, 1, 2147483647)
        _integer(bootstrap_gid, 1, 2147483647)
        if type(document) is not bytes or not 0 < len(document) <= MAX_POLICY_BYTES:
            raise ValueError
        if "sha256:" + sha256(document).hexdigest() != expected_sha256:
            raise ValueError
        value = _object(strict_json_object(document), _FIELDS)
        if value["schema"] != SCHEMA or canonical_json_bytes(value) != document:
            raise ValueError
        bootstrap = _path(value["bootstrap_file"])
        if len(bootstrap.parts) < 3:
            raise ValueError
        return DispatcherServicePolicy(
            **settings(value, bootstrap_uid, bootstrap_gid),
            bootstrap_file=bootstrap,
            startup_timeout_seconds=_integer(value["startup_timeout_seconds"], 1, 900),
            document=document,
        )
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("dispatcher_service_policy") from None
