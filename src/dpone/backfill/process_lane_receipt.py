"""Parent authority for pre-source receipt replay in spawned lanes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from multiprocessing.connection import Connection
from typing import Any

from dpone.backfill.process_lane_contracts import (
    ProcessLaneDispatch,
    ProcessLaneReceiptProbeReply,
    ProcessLaneReceiptProbeRequest,
)

PARENT_RECEIPT_VALIDATION_ERROR = "DPONE_BACKFILL_PROCESS_PARENT_RECEIPT_VALIDATION_FAILED"
RECEIPT_PROBE_AUTHORITY_ERROR = "DPONE_BACKFILL_PROCESS_RECEIPT_PROBE_AUTHORITY_REJECTED"
_RUNTIME_ERROR = "backfill.process_lane_receipt_authority_invalid"

ReceiptProbeIssuer = Callable[[ProcessLaneDispatch, str, Any | None], tuple[Any, Any] | None]
ReplayResultValidator = Callable[[ProcessLaneDispatch, Any, Any, Mapping[str, Any]], bool]


@dataclass(frozen=True, slots=True)
class _AuthorizedProbe:
    command_id: str
    binding: Any
    load_id: str
    attempt_request: Any
    operation_request: Any
    portable_scope_binding: Any | None


@dataclass(frozen=True, slots=True)
class _CachedReply:
    fingerprint: tuple[Any, ...]
    reply: ProcessLaneReceiptProbeReply


class ParentReceiptReplayCoordinator:
    """Authorize receipt probes and prove replay results before ledger success."""

    def __init__(
        self,
        *,
        issue_probe: ReceiptProbeIssuer | None,
        validate_result: ReplayResultValidator | None,
    ) -> None:
        if (issue_probe is None) != (validate_result is None):
            raise ValueError(_RUNTIME_ERROR)
        self._issue_probe = issue_probe
        self._validate_result = validate_result
        self._authorized: dict[int, _AuthorizedProbe] = {}
        self._replies: dict[int, _CachedReply] = {}

    def handle(
        self,
        connection: Connection,
        request: ProcessLaneReceiptProbeRequest,
        *,
        worker_id: int,
        dispatch: ProcessLaneDispatch | None,
        control_error: str | None,
    ) -> bool:
        """Return an exact ACK only for a parent-derived route candidate."""

        fingerprint = self._fingerprint(request)
        if control_error is not None:
            reply = self._reply(request, candidate=None, error=control_error)
            self._replies[worker_id] = _CachedReply(fingerprint, reply)
            return self._send(connection, reply) and reply.authorized
        cached = self._replies.get(worker_id)
        if cached is not None and cached.reply.request_id == request.request_id:
            active = self._authorized.get(worker_id)
            reusable = active is not None and active.command_id == request.command_id
            reply = (
                cached.reply
                if reusable and cached.fingerprint == fingerprint
                else self._reply(request, candidate=None, error=RECEIPT_PROBE_AUTHORITY_ERROR)
            )
            return self._send(connection, reply) and reply.authorized

        candidate: _AuthorizedProbe | None = None
        if control_error is None and self._request_matches(request, worker_id=worker_id, dispatch=dispatch):
            assert dispatch is not None
            current = self._authorized.get(worker_id)
            if current is not None and current.command_id == request.command_id:
                exact_retry = (
                    current.binding == request.binding
                    and current.load_id == request.load_id
                    and current.portable_scope_binding == request.portable_scope_binding
                )
                candidate = current if exact_retry else None
            elif self._issue_probe is not None:
                try:
                    issued = self._issue_probe(
                        dispatch,
                        request.load_id,
                        request.portable_scope_binding,
                    )
                except Exception:
                    issued = None
                if issued is not None:
                    try:
                        candidate = self._candidate(request, *issued)
                    except TypeError:
                        candidate = None
                    if candidate is not None:
                        self._authorized[worker_id] = candidate
        authorized = candidate is not None
        error = control_error or (None if authorized else RECEIPT_PROBE_AUTHORITY_ERROR)
        reply = self._reply(request, candidate=candidate, error=error)
        self._replies[worker_id] = _CachedReply(fingerprint, reply)
        return self._send(connection, reply) and authorized

    def validate_success(
        self,
        worker_id: int,
        dispatch: ProcessLaneDispatch,
        result: Mapping[str, Any],
        *,
        operation_registered: bool,
    ) -> bool:
        """Require a fresh parent receipt proof for operation-free success."""

        if operation_registered:
            self.clear(worker_id, dispatch)
            return True
        if self._issue_probe is None or self._validate_result is None:
            return True
        candidate = self._authorized.get(worker_id)
        if candidate is None or candidate.command_id != dispatch.command_id or candidate.binding != dispatch.binding:
            self.clear(worker_id, dispatch)
            return False
        try:
            return bool(
                self._validate_result(
                    dispatch,
                    candidate.attempt_request,
                    candidate.operation_request,
                    result,
                )
            )
        except Exception:
            return False
        finally:
            self.clear(worker_id, dispatch)

    def clear(self, worker_id: int, dispatch: ProcessLaneDispatch) -> None:
        """Discard bounded command authority after a terminal transition."""

        current = self._authorized.get(worker_id)
        if current is not None and current.command_id == dispatch.command_id:
            self._authorized.pop(worker_id, None)
        cached = self._replies.get(worker_id)
        if cached is not None and cached.reply.command_id == dispatch.command_id:
            self._replies.pop(worker_id, None)

    @staticmethod
    def _request_matches(
        request: ProcessLaneReceiptProbeRequest,
        *,
        worker_id: int,
        dispatch: ProcessLaneDispatch | None,
    ) -> bool:
        return bool(
            dispatch is not None
            and request.worker_id == worker_id
            and request.command_id == dispatch.command_id
            and request.binding == dispatch.binding
        )

    @staticmethod
    def _candidate(
        request: ProcessLaneReceiptProbeRequest,
        attempt_request: Any,
        operation_request: Any,
    ) -> _AuthorizedProbe:
        return _AuthorizedProbe(
            command_id=request.command_id,
            binding=request.binding,
            load_id=request.load_id,
            attempt_request=attempt_request,
            operation_request=operation_request,
            portable_scope_binding=request.portable_scope_binding,
        )

    @staticmethod
    def _reply(
        request: ProcessLaneReceiptProbeRequest,
        *,
        candidate: _AuthorizedProbe | None,
        error: str | None,
    ) -> ProcessLaneReceiptProbeReply:
        return ProcessLaneReceiptProbeReply(
            request_id=request.request_id,
            worker_id=request.worker_id,
            command_id=request.command_id,
            authorized=candidate is not None,
            error=error,
            attempt_request=candidate.attempt_request if candidate is not None else None,
            operation_request=candidate.operation_request if candidate is not None else None,
        )

    @staticmethod
    def _send(connection: Connection, reply: ProcessLaneReceiptProbeReply) -> bool:
        try:
            connection.send(reply)
        except (BrokenPipeError, EOFError, OSError):
            return False
        return True

    @staticmethod
    def _fingerprint(request: ProcessLaneReceiptProbeRequest) -> tuple[Any, ...]:
        return (
            request.worker_id,
            request.command_id,
            request.binding,
            request.load_id,
            request.portable_scope_binding,
        )


__all__ = [
    "PARENT_RECEIPT_VALIDATION_ERROR",
    "ParentReceiptReplayCoordinator",
    "RECEIPT_PROBE_AUTHORITY_ERROR",
    "ReceiptProbeIssuer",
    "ReplayResultValidator",
]
