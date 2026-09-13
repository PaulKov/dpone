"""Compose a provisioned Linux dispatcher listener from protected originals.

Certificate loading uses /proc/self/fd paths for held, no-follow regular files,
so OpenSSL consumes the verified inode rather than reopening a mutable pathname.
Private keys are never copied to temporary files. Startup checks original bytes
and metadata again before binding; rotation requires an explicit service restart.
This constructor neither provisions files nor establishes execution readiness.
"""

from __future__ import annotations

import os
import ssl
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from ipaddress import ip_address
from pathlib import Path
from typing import Any

from dpone.adapters.composition_dispatch_http_server import (
    DispatchHttpError,
    DispatchHttpServer,
    DispatchRequestContext,
    create_dispatch_http_server,
    make_bearer_authenticator,
)
from dpone.adapters.composition_dispatcher_context_files import DispatcherContextFiles
from dpone.adapters.composition_supervisor_filesystem import open_protected
from dpone.adapters.dbt_runtime import build_hvac_kubernetes_vault_kv_v2_reader
from dpone.app.composition_dispatcher_context import StagedDispatcherContextLoader
from dpone.app.composition_dispatcher_service_config import DispatcherServiceConfig, load_dispatcher_service_config
from dpone.app.composition_dispatcher_service_policy import DispatcherServicePolicy
from dpone.app.composition_dispatcher_transfer_handler import DispatcherTransferHandler
from dpone.contracts.composition_dispatch_rpc import require_bearer_token
from dpone.contracts.composition_dispatch_v2 import DispatchV2Request
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.runtime.composition_execution_budget import CompositionExecutionBudget, ExecutionStopSignal
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContextLoader

_MAX_TLS_BYTES = 1024 * 1024
_STABLE = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")


def _coordinates(path: Path) -> tuple[Path, str]:
    if not path.is_absolute() or len(path.parts) < 3 or ".." in path.parts or str(path).startswith("//"):
        raise CompositionAdmissionError("dispatcher_service_path")
    return path.parent.parent, "/".join(path.parts[-2:])


def _protected_read(path: Path, gid: int, maximum: int) -> bytes:
    root, relative = _coordinates(path)
    return DispatcherContextFiles(root, dispatcher_gid=gid).read(relative, max_bytes=maximum)


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return tuple(getattr(info, field) for field in _STABLE)


def _require_file(info: os.stat_result, gid: int) -> None:
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != 0
        or info.st_gid != gid
        or info.st_nlink != 1
        or info.st_mode & 0o027
        or not info.st_mode & 0o040
        or not 0 < info.st_size <= _MAX_TLS_BYTES
    ):
        raise CompositionAdmissionError("dispatcher_service_credentials")


@contextmanager
def _held_original(path: Path, gid: int, original: bytes) -> Iterator[str]:
    parent = descriptor = None
    try:
        parent = open_protected(path.parent, traversable=False)
        parent_identity = _identity(os.fstat(parent))
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        before = os.fstat(descriptor)
        _require_file(before, gid)
        identity = _identity(before)
        if (
            before.st_size != len(original)
            or os.pread(descriptor, len(original) + 1, 0) != original
            or _identity(os.fstat(descriptor)) != identity
            or _identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) != identity
        ):
            raise CompositionAdmissionError("dispatcher_service_credentials")
        yield f"/proc/self/fd/{descriptor}"
        # Both the held inode and its protected pathname must still name the
        # exact original. Reopening the parent also detects directory rotation.
        reopened = open_protected(path.parent, traversable=False)
        try:
            stable_parent = _identity(os.fstat(reopened)) == parent_identity
        finally:
            os.close(reopened)
        if (
            not stable_parent
            or _identity(os.fstat(descriptor)) != identity
            or _identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) != identity
            or os.pread(descriptor, len(original) + 1, 0) != original
            or _protected_read(path, gid, _MAX_TLS_BYTES) != original
        ):
            raise CompositionAdmissionError("dispatcher_service_credentials")
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if parent is not None:
            os.close(parent)


def _credentials(config: DispatcherServiceConfig | DispatcherServicePolicy) -> tuple[ssl.SSLContext, str]:
    certificate, key = config.tls.certificate_file, config.tls.private_key_file
    gid = config.dispatcher_gid
    original_certificate = _protected_read(certificate, gid, _MAX_TLS_BYTES)
    original_key = _protected_read(key, gid, _MAX_TLS_BYTES)
    bearer = _protected_read(config.bearer_file, gid, 256)
    token = bearer.decode("ascii")
    require_bearer_token(token)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    def no_password() -> str:
        raise CompositionAdmissionError("dispatcher_service_encrypted_key")

    with _held_original(certificate, gid, original_certificate) as cert_fd:
        with _held_original(key, gid, original_key) as key_fd:
            context.load_cert_chain(cert_fd, key_fd, password=no_password)
    if _protected_read(config.bearer_file, gid, 256) != bearer:
        raise CompositionAdmissionError("dispatcher_service_credentials")
    return context, token


def _authorize(config: DispatcherServiceConfig, context: DispatchRequestContext) -> None:
    try:
        peer = context.peer_address
        if not isinstance(peer, tuple) or len(peer) not in {2, 4}:
            raise ValueError
        address = ip_address(peer[0])
        if address.is_unspecified or address.is_multicast or type(peer[1]) is not int or not 1 <= peer[1] <= 65535:
            raise ValueError
        request = context.request
        if request is not None and (
            not isinstance(request, DispatchV2Request)
            or request.dispatcher_id != config.dispatcher_id
            or request.runtime_authority_sha256 not in config.authorities
        ):
            raise ValueError
    except Exception:
        raise DispatchHttpError("rpc_unauthorized") from None


def _unavailable_v1(*args: Any) -> Any:
    raise DispatchHttpError("rpc_operation_unavailable")


def build_dispatcher_server(
    configuration_path: Path,
    *,
    expected_configuration_sha256: str,
    dispatcher_uid: int,
    dispatcher_gid: int,
) -> DispatchHttpServer:
    """Validate startup and credentials before creating the one bounded listener.

    Each authority catalog value pins the staged context original independently
    of its lookup key. The existing lazy Vault reader is injected explicitly;
    startup does not resolve source or target credentials. A single stop signal
    binds every request budget to this listener's admission lifecycle.
    """
    root, relative = _coordinates(configuration_path)
    config = load_dispatcher_service_config(
        root,
        relative,
        expected_sha256=expected_configuration_sha256,
        bootstrap_uid=dispatcher_uid,
        bootstrap_gid=dispatcher_gid,
    )
    if config.service_policy is not None:
        raise CompositionAdmissionError("dispatcher_policy_startup_required")
    tls, token = _credentials(config)
    loader = StagedDispatcherContextLoader(
        root=config.context_root,
        dispatcher_gid=config.dispatcher_gid,
        dispatcher_id=config.dispatcher_id,
        configuration_sha256=config.configuration_sha256,
        staged_authorities={key: value.context_sha256 for key, value in config.authorities.items()},
        runtime_loader=RuntimeConnectionContextLoader(vault_reader_factory=build_hvac_kubernetes_vault_kv_v2_reader),
    )
    stop = ExecutionStopSignal()
    return create_dispatch_http_server(
        (config.listen.address, config.listen.port),
        ssl_context=tls,
        authenticator=make_bearer_authenticator(token, lambda context: _authorize(config, context)),
        handler=_unavailable_v1,
        timeout_seconds=config.accept_timeout_seconds,
        max_concurrency=config.max_concurrency,
        v2_handler=DispatcherTransferHandler(config, loader),
        execution_timeout_seconds=config.execution_timeout_seconds,
        budget_factory=lambda seconds: CompositionExecutionBudget(seconds, stop_event=stop),
        stop_signal=stop,
    )
