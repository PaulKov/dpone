"""Bind one real policy listener before bootstrap, keeping application admission closed.

The serving owner must pass both returned objects to the existing coordinated
service loop. Request proxies remain unavailable until the retained bootstrap
and real SQL/host/process/listener verification admit one immutable handler.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from dpone.adapters.composition_dispatch_http_server import (
    DispatchHttpServer,
    DispatchRequestContext,
    create_dispatch_http_server,
    make_bearer_authenticator,
)
from dpone.adapters.composition_dispatcher_bootstrap_original import open_dispatcher_bootstrap
from dpone.adapters.dbt_runtime import build_hvac_kubernetes_vault_kv_v2_reader
from dpone.app.composition_dispatcher_context import StagedDispatcherContextLoader
from dpone.app.composition_dispatcher_service_config import DispatcherServiceConfig
from dpone.app.composition_dispatcher_service_factory import (
    _authorize,
    _coordinates,
    _credentials,
    _protected_read,
    _unavailable_v1,
)
from dpone.app.composition_dispatcher_service_policy import (
    MAX_POLICY_BYTES,
    DispatcherServicePolicy,
    decode_dispatcher_service_policy,
)
from dpone.app.composition_dispatcher_startup import DispatcherStartup
from dpone.app.composition_dispatcher_transfer_handler import DispatcherTransferHandler
from dpone.contracts.composition_dispatch_v2 import DispatchV2Request, DispatchV2Response
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.ports.composition_execution import ExecutionBudget
from dpone.runtime.composition_execution_budget import CompositionExecutionBudget, ExecutionStopSignal
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContextLoader


@dataclass(frozen=True, slots=True, repr=False)
class _Admitted:
    config: DispatcherServiceConfig
    handler: DispatcherTransferHandler


@dataclass(frozen=True, slots=True, repr=False)
class DispatcherPolicyService:
    """Listener and retained startup ownership; close startup only after server join."""

    server: DispatchHttpServer
    startup: DispatcherStartup[_Admitted] = field(repr=False)


def _policy(path: Path, digest: str, uid: int, gid: int) -> DispatcherServicePolicy:
    require_digest(digest)
    if (
        type(uid) is not int
        or not 0 < uid < 2147483648
        or type(gid) is not int
        or not 0 < gid < 2147483648
        or sys.platform != "linux"
        or os.getresuid() != (uid,) * 3
        or os.getresgid() != (gid,) * 3
    ):
        raise CompositionAdmissionError("dispatcher_policy_process")
    _coordinates(path)
    return decode_dispatcher_service_policy(
        _protected_read(path, gid, MAX_POLICY_BYTES),
        expected_sha256=digest,
        bootstrap_uid=uid,
        bootstrap_gid=gid,
    )


def _verify_bootstrap(
    config: DispatcherServiceConfig,
    loader: StagedDispatcherContextLoader,
    policy_path: Path,
    deadline: float,
) -> None:
    # This is a concrete lazy import, not optional verification or a success fallback.
    from dpone.app.composition_dispatcher_bootstrap_verification import require_dispatcher_bootstrap

    require_dispatcher_bootstrap(config, loader, policy_path, deadline)


def build_dispatcher_policy_server(
    policy_path: Path,
    *,
    expected_policy_sha256: str,
    dispatcher_uid: int,
    dispatcher_gid: int,
) -> DispatcherPolicyService:
    """Open protected P and TLS, then bind once with both application proxies closed.

    One fixed startup deadline begins before credential loading and binding.
    Preparation uses B's entire catalog but P's distinct binding identity. No
    enrollment, attempt, credential issuance or host observation is fabricated.
    The caller owns the returned listener and startup coordinator lifetimes.
    """
    startup = None
    server = None
    stop = None
    try:
        policy = _policy(policy_path, expected_policy_sha256, dispatcher_uid, dispatcher_gid)
        stop = ExecutionStopSignal(clock=time.monotonic)

        def prepare(config: DispatcherServiceConfig, deadline: float) -> _Admitted:
            loader = StagedDispatcherContextLoader(
                root=config.context_root,
                dispatcher_gid=config.dispatcher_gid,
                dispatcher_id=config.dispatcher_id,
                configuration_sha256=policy.sha256,
                identity_kind="policy",
                staged_authorities={key: value.context_sha256 for key, value in config.authorities.items()},
                runtime_loader=RuntimeConnectionContextLoader(
                    vault_reader_factory=build_hvac_kubernetes_vault_kv_v2_reader
                ),
            )
            _verify_bootstrap(config, loader, policy_path, deadline)
            return _Admitted(config, DispatcherTransferHandler(config, loader))

        coordinator = DispatcherStartup(
            policy,
            stop=stop,
            open_original=open_dispatcher_bootstrap,
            prepare=prepare,
            clock=time.monotonic,
        )
        startup = coordinator

        def authorize(context: DispatchRequestContext) -> None:
            _authorize(coordinator.handler.config, context)

        def execute(request: DispatchV2Request, budget: ExecutionBudget) -> DispatchV2Response:
            return coordinator.handler.handler(request, budget)

        tls, token = _credentials(policy)
        server = create_dispatch_http_server(
            (policy.listen.address, policy.listen.port),
            ssl_context=tls,
            authenticator=make_bearer_authenticator(token, authorize),
            handler=_unavailable_v1,
            timeout_seconds=policy.accept_timeout_seconds,
            max_concurrency=policy.max_concurrency,
            v2_handler=execute,
            execution_timeout_seconds=policy.execution_timeout_seconds,
            budget_factory=lambda seconds: CompositionExecutionBudget(seconds, stop_event=stop),
            stop_signal=stop,
        )
        return DispatcherPolicyService(server, coordinator)
    except Exception:
        if stop is not None:
            stop.notify()
        if server is not None:
            try:
                server.server_close()
            except Exception:
                pass
        if startup is not None:
            try:
                startup.close()
            except Exception:
                pass
        raise CompositionAdmissionError("dispatcher_policy_startup_unavailable") from None
