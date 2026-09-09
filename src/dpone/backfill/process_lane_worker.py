"""Spawn-child runtime for one long-lived backfill lane."""

from __future__ import annotations

import faulthandler
import multiprocessing
import os
import time
from collections.abc import Mapping
from contextlib import suppress
from datetime import datetime, timezone
from multiprocessing.connection import Connection
from typing import Any
from uuid import uuid4

from dpone.backfill.process_lane_contracts import (
    PROCESS_TREE_ISOLATION_ERROR,
    BackfillProcessLaneBootstrap,
    ProcessLaneBootstrapFailed,
    ProcessLaneContainmentAck,
    ProcessLaneFailed,
    ProcessLaneOperationLeaseReply,
    ProcessLaneOperationLeaseRequest,
    ProcessLaneReady,
    ProcessLaneReceiptProbeReply,
    ProcessLaneReceiptProbeRequest,
    ProcessLaneRun,
    ProcessLaneRuntimeReady,
    ProcessLaneStop,
    ProcessLaneSucceeded,
    ProcessTreeIdentity,
    operation_identity,
    operation_lease_expiry,
    parent_issued_operation_lease_expiry,
)
from dpone.backfill.process_lane_lease import PROCESS_LANE_RENEW_INTERVAL
from dpone.backfill.process_lane_preflight import resolve_process_lane_entrypoint
from dpone.contracts.mssql_transaction_governance import MssqlAttemptRequest, MssqlOperationRequest
from dpone.contracts.portable_scope_binding import PORTABLE_SCOPE_BINDING_OPTION
from dpone.security_redaction import redact_public_text

_IPC_ERROR = "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_IPC_FAILED"
_PARENT_RESPONSE_BUDGET_SECONDS = PROCESS_LANE_RENEW_INTERVAL.total_seconds()
_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


def isolate_current_process_tree() -> ProcessTreeIdentity:
    """Create and verify the child POSIX session before connector bootstrap."""

    if os.name != "posix":
        raise RuntimeError(PROCESS_TREE_ISOLATION_ERROR)
    try:
        os.setsid()
        identity = ProcessTreeIdentity(
            pid=os.getpid(),
            process_group_id=os.getpgrp(),
            session_id=os.getsid(0),
        )
    except OSError:
        raise RuntimeError(PROCESS_TREE_ISOLATION_ERROR) from None
    if not identity.isolated:
        raise RuntimeError(PROCESS_TREE_ISOLATION_ERROR)
    return identity


class ProcessLaneOperationLeaseError(RuntimeError):
    """Parent-owned operation lease could not be proven before expiry."""

    def __init__(self, error: str = _IPC_ERROR) -> None:
        super().__init__(error)


class _OperationLeaseController:
    """Use synchronous safe-point IPC while the parent renews independently."""

    def __init__(self, channel: _OperationLeaseFactory, operation: Any) -> None:
        self._channel = channel
        self._operation = operation
        self._operation_key, self._owner_digest, self._epoch = operation_identity(operation)
        self._expires_at = operation_lease_expiry(operation)
        self._started = False
        self._portable_scope_binding: Any | None = None

    def bind_load_config(self, load_config: Any) -> None:
        """Capture only the catalog-issued portable-scope proof for parent validation."""

        options = getattr(load_config, "options", {}) or {}
        self._portable_scope_binding = options.get(PORTABLE_SCOPE_BINDING_OPTION) if isinstance(options, dict) else None

    def start(self) -> None:
        if self._started:
            return
        self._exchange("register", operation=self._operation)
        self._started = True

    def assert_healthy(self) -> None:
        if not self._started:
            raise ProcessLaneOperationLeaseError()
        self._exchange("status")

    def stop(self) -> None:
        if not self._started:
            return
        try:
            self._exchange("unregister")
        finally:
            self._started = False

    def _exchange(self, action: str, *, operation: Any | None = None) -> None:
        command = self._channel.current
        if command is None:
            raise ProcessLaneOperationLeaseError()
        request_id = uuid4().hex
        request = ProcessLaneOperationLeaseRequest(
            request_id=request_id,
            action=action,  # type: ignore[arg-type]
            worker_id=self._channel.worker_id,
            command_id=command.command_id,
            binding=command.binding,
            operation_key=self._operation_key,
            owner_digest=self._owner_digest,
            epoch=self._epoch,
            operation=operation,
            portable_scope_binding=self._portable_scope_binding,
        )
        timeout = self._response_timeout(action)
        try:
            self._channel.connection.send(request)
            reply = self._receive_reply(request, timeout=timeout)
        except (EOFError, OSError, BrokenPipeError) as exc:
            raise ProcessLaneOperationLeaseError() from exc
        if not isinstance(reply, ProcessLaneOperationLeaseReply):
            raise ProcessLaneOperationLeaseError()
        exact = (
            reply.request_id == request_id
            and reply.worker_id == self._channel.worker_id
            and reply.command_id == command.command_id
        )
        if not exact or not reply.active:
            raise ProcessLaneOperationLeaseError(reply.error or _IPC_ERROR)
        if reply.lease_expires_at_utc is not None:
            expiry = reply.lease_expires_at_utc
            if expiry.tzinfo is None or expiry.utcoffset() is None:
                raise ProcessLaneOperationLeaseError()
            self._expires_at = expiry.astimezone(_UTC)

    def _receive_reply(
        self,
        request: ProcessLaneOperationLeaseRequest,
        *,
        timeout: float,
    ) -> Any:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self._channel.connection.poll(remaining):
                self._channel.abandon(request)
                raise ProcessLaneOperationLeaseError()
            reply = self._channel.connection.recv()
            if self._channel.discard_abandoned_reply(reply):
                continue
            return reply

    def _response_timeout(self, action: str) -> float:
        remaining = (self._expires_at - datetime.now(_UTC)).total_seconds()
        if remaining <= 0:
            if action == "register" or not self._started:
                raise ProcessLaneOperationLeaseError()
            # A long native call can outlive the child's stale projection while
            # the parent has continued renewing.  No new I/O occurs until this
            # bounded safe-point revalidation succeeds.
            return _PARENT_RESPONSE_BUDGET_SECONDS
        return min(_PARENT_RESPONSE_BUDGET_SECONDS, max(0.05, remaining / 3.0))


class _OperationLeaseFactory:
    """Bind admission-time lease construction to the current lane command."""

    def __init__(self, worker_id: int, connection: Connection) -> None:
        self.worker_id = worker_id
        self.connection = connection
        self.current: ProcessLaneRun | None = None
        self._abandoned: dict[str, tuple[int, str]] = {}

    def bind(self, command: ProcessLaneRun) -> None:
        if self.current is not None:
            raise RuntimeError("backfill.process_lane_command_overlap")
        self.current = command

    def release(self, command: ProcessLaneRun) -> None:
        if self.current is not command:
            raise RuntimeError("backfill.process_lane_command_binding_lost")
        self.current = None

    def abandon(self, request: ProcessLaneOperationLeaseRequest) -> None:
        """Remember one timed-out request so its exact late reply is harmless."""

        self._abandoned[request.request_id] = (request.worker_id, request.command_id)

    def discard_abandoned_reply(self, message: Any) -> bool:
        """Discard only a reply whose complete identity was previously abandoned."""

        if not isinstance(message, ProcessLaneOperationLeaseReply):
            return False
        expected = self._abandoned.get(message.request_id)
        if expected != (message.worker_id, message.command_id):
            return False
        del self._abandoned[message.request_id]
        return True

    def receive_command(self) -> Any:
        """Receive the next command without letting a late lease reply poison it."""

        while True:
            message = self.connection.recv()
            if not self.discard_abandoned_reply(message):
                return message

    def __call__(self, state: Any, admission: Any) -> _OperationLeaseController:
        del state
        operation = getattr(admission, "operation", None)
        if self.current is None or operation is None:
            raise ProcessLaneOperationLeaseError()
        return _OperationLeaseController(self, operation)

    def authorize_receipt_probe(
        self,
        load_config: Any,
        *,
        load_id: str,
    ) -> tuple[MssqlAttemptRequest, MssqlOperationRequest]:
        """Obtain parent-derived route authority before reading child state."""

        command = self.current
        if command is None:
            raise ProcessLaneOperationLeaseError()
        options = getattr(load_config, "options", {}) or {}
        portable_binding = options.get(PORTABLE_SCOPE_BINDING_OPTION) if isinstance(options, dict) else None
        request_id = uuid4().hex
        request = ProcessLaneReceiptProbeRequest(
            request_id=request_id,
            worker_id=self.worker_id,
            command_id=command.command_id,
            binding=command.binding,
            load_id=load_id,
            portable_scope_binding=portable_binding,
        )
        expiry = parent_issued_operation_lease_expiry(command.load_config)
        timeout = _response_timeout(expiry)
        try:
            self.connection.send(request)
            if not self.connection.poll(timeout):
                raise ProcessLaneOperationLeaseError()
            reply = self.connection.recv()
        except (EOFError, OSError, BrokenPipeError) as exc:
            raise ProcessLaneOperationLeaseError() from exc
        exact = (
            isinstance(reply, ProcessLaneReceiptProbeReply)
            and reply.request_id == request_id
            and reply.worker_id == self.worker_id
            and reply.command_id == command.command_id
        )
        if not exact or not reply.authorized:
            error = reply.error if isinstance(reply, ProcessLaneReceiptProbeReply) else None
            raise ProcessLaneOperationLeaseError(error or _IPC_ERROR)
        if not isinstance(reply.attempt_request, MssqlAttemptRequest) or not isinstance(
            reply.operation_request,
            MssqlOperationRequest,
        ):
            raise ProcessLaneOperationLeaseError()
        return reply.attempt_request, reply.operation_request


def _response_timeout(expires_at: datetime) -> float:
    remaining = (expires_at.astimezone(_UTC) - datetime.now(_UTC)).total_seconds()
    if remaining <= 0:
        raise ProcessLaneOperationLeaseError()
    return min(_PARENT_RESPONSE_BUDGET_SECONDS, max(0.05, remaining / 3.0))


def run_process_lane(worker_id: int, bootstrap: BackfillProcessLaneBootstrap, connection: Connection) -> None:
    """Open one process-owned runtime and consume commands serially."""

    lease_factory = _OperationLeaseFactory(worker_id, connection)
    runtime_ready = False
    try:
        process_tree = isolate_current_process_tree()
        enabled = _enable_faulthandler()
        ready = ProcessLaneReady(
            worker_id=worker_id,
            pid=process_tree.pid,
            process_group_id=process_tree.process_group_id,
            session_id=process_tree.session_id,
            process_tree_isolated=process_tree.isolated,
            start_method=multiprocessing.get_start_method(allow_none=False),
            faulthandler_enabled=enabled,
        )
        connection.send(ready)
        acknowledgement = connection.recv()
        if not _matches_containment_ack(ready, acknowledgement):
            raise RuntimeError("backfill.process_lane_containment_ack_invalid")
        opener = resolve_process_lane_entrypoint(bootstrap.entrypoint)
        with opener(worker_id, bootstrap.payload, lease_factory) as run_chunk:
            connection.send(ProcessLaneRuntimeReady(worker_id=worker_id))
            runtime_ready = True
            while True:
                command = lease_factory.receive_command()
                if isinstance(command, ProcessLaneStop):
                    break
                if not isinstance(command, ProcessLaneRun):
                    raise RuntimeError("backfill.process_lane_command_invalid")
                lease_factory.bind(command)
                try:
                    result = run_chunk(command.load_config)
                    if not isinstance(result, Mapping):
                        raise RuntimeError("backfill.process_lane_result_invalid")
                    connection.send(
                        ProcessLaneSucceeded(
                            worker_id=worker_id,
                            command_id=command.command_id,
                            result=dict(result),
                        )
                    )
                except Exception as exc:
                    connection.send(
                        ProcessLaneFailed(
                            worker_id=worker_id,
                            command_id=command.command_id,
                            error=redact_public_text(exc, fallback="backfill.process_lane_failed"),
                        )
                    )
                finally:
                    lease_factory.release(command)
    except (EOFError, BrokenPipeError):
        return
    except Exception as exc:
        if not runtime_ready:
            with suppress(Exception):
                connection.send(
                    ProcessLaneBootstrapFailed(
                        worker_id=worker_id,
                        error=redact_public_text(exc, fallback="backfill.process_lane_bootstrap_failed"),
                    )
                )
            return
        raise
    finally:
        connection.close()


def _matches_containment_ack(ready: ProcessLaneReady, message: Any) -> bool:
    """Accept only the parent's acknowledgement for this exact containment ID."""

    return (
        isinstance(message, ProcessLaneContainmentAck)
        and message.worker_id == ready.worker_id
        and message.pid == ready.pid
        and message.process_group_id == ready.process_group_id
        and message.session_id == ready.session_id
    )


def _enable_faulthandler() -> bool:
    try:
        faulthandler.enable(all_threads=True)
    except Exception:
        return False
    return faulthandler.is_enabled()


__all__ = [
    "ProcessLaneOperationLeaseError",
    "isolate_current_process_tree",
    "run_process_lane",
]
