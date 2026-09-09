"""Parent-side operation-lease coordination for spawned lanes."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from inspect import signature
from multiprocessing.connection import Connection
from typing import Any, cast

from dpone.backfill.process_lane_contracts import (
    ProcessLaneDispatch,
    ProcessLaneOperationLeaseReply,
    ProcessLaneOperationLeaseRequest,
    operation_identity,
    operation_lease_expiry,
    parent_issued_operation_lease_expiry,
)
from dpone.contracts.mssql_transaction_governance import MssqlTransactionOperation, operation_owner_digest
from dpone.contracts.portable_relation_scope import resolve_portable_relation_scope
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    PortableScopeBinding,
)

OPERATION_LEASE_ERROR = "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_LOST"
_IDENTITY_ERROR = "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_IDENTITY_MISMATCH"
_DUPLICATE_ERROR = "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_DUPLICATE_REGISTRATION"
_VALIDATOR_ERROR = "backfill.process_lane_operation_binding_validator_invalid"
_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.
_LOG = logging.getLogger(__name__)

OperationBindingValidator = Callable[[ProcessLaneDispatch, Any, Any | None], bool]
LegacyOperationBindingValidator = Callable[[ProcessLaneDispatch, Any], bool]


def is_mssql_transaction_operation(operation: Any) -> bool:
    """Keep the exact governance-type check inside the parent protocol layer."""

    return isinstance(operation, MssqlTransactionOperation)


@dataclass(slots=True)
class _RegisteredOperation:
    operation: Any
    ttl: timedelta
    expires_at: datetime
    next_renewal: float
    renewing: bool = True
    receipt_committed: bool = False


def _registration_authorized(registered: _RegisteredOperation | None) -> bool:
    """Allow a live renewal or the exact receipt-backed terminal transition."""

    return registered is not None and (registered.renewing or registered.receipt_committed)


@dataclass(frozen=True, slots=True)
class _CachedReply:
    fingerprint: tuple[Any, ...]
    reply: ProcessLaneOperationLeaseReply


class ParentOperationLeaseCoordinator:
    """Retain exact descriptors until the parent commits the chunk ledger."""

    def __init__(
        self,
        *,
        renew: Callable[[Any, datetime], bool] | None,
        validate_binding: Callable[..., bool] | None,
    ) -> None:
        self._renew = renew
        self._validate_binding = adapt_operation_binding_validator(validate_binding)
        self._operations: dict[tuple[int, str], _RegisteredOperation] = {}
        self._replies: dict[tuple[int, str], dict[str, _CachedReply]] = {}

    def handle(
        self,
        connection: Connection,
        request: ProcessLaneOperationLeaseRequest,
        *,
        worker_id: int,
        dispatch: ProcessLaneDispatch | None,
        control_error: str | None,
    ) -> bool:
        """Validate one safe-point request and return an exact acknowledgement."""

        key = (worker_id, request.command_id)
        fingerprint = self._fingerprint(request)
        if control_error is not None:
            reply = self._reply(request, False, control_error)
            self._replies.setdefault(key, {})[request.request_id] = _CachedReply(fingerprint, reply)
            return self._send(connection, reply) and reply.active
        cached = self._replies.get(key, {}).get(request.request_id)
        if cached is not None:
            registered = self._operations.get(key)
            registration_active = _registration_authorized(registered)
            stale_active = cached.reply.active and request.action in {"register", "status"} and not registration_active
            if cached.fingerprint != fingerprint:
                reply = self._reply(request, False, _IDENTITY_ERROR)
            elif stale_active:
                reply = self._reply(request, False, OPERATION_LEASE_ERROR)
            else:
                reply = cached.reply
            return self._send(connection, reply) and reply.active

        error = self._validate_request(request, worker_id=worker_id, dispatch=dispatch)
        registered = self._operations.get(key)
        if error is None and request.action == "register":
            if registered is not None or request.operation is None:
                error = _DUPLICATE_ERROR
            else:
                try:
                    assert dispatch is not None
                    expiry = parent_issued_operation_lease_expiry(dispatch.load_config)
                except ValueError:
                    error = _IDENTITY_ERROR
                else:
                    ttl = expiry - datetime.now(_UTC)
                    if ttl.total_seconds() <= 0 or self._renew is None or self._validate_binding is None:
                        error = OPERATION_LEASE_ERROR
                    else:
                        try:
                            current = bool(self._renew(request.operation, expiry))
                        except Exception:
                            current = False
                        if not current:
                            error = OPERATION_LEASE_ERROR
                        else:
                            registered = _RegisteredOperation(
                                operation=request.operation,
                                ttl=ttl,
                                expires_at=expiry,
                                next_renewal=time.monotonic() + _renew_interval(ttl),
                            )
                            self._operations[key] = registered
        if error is None and request.action in {"status", "unregister"} and not _registration_authorized(registered):
            error = OPERATION_LEASE_ERROR

        active = error is None and control_error is None
        reply = self._reply(
            request,
            active,
            error or control_error,
            expiry=registered.expires_at if active and registered is not None else None,
        )
        self._replies.setdefault(key, {})[request.request_id] = _CachedReply(fingerprint, reply)
        sent = self._send(connection, reply)
        if request.action == "unregister" and active and registered is not None:
            # The exact operation remains available for receipt recovery until
            # the parent durably completes the campaign-ledger CAS.
            registered.renewing = False
        return active and sent

    def tick(
        self,
        running: dict[tuple[int, str], ProcessLaneDispatch],
        *,
        on_failure: Callable[[tuple[int, str]], None],
        after_each: Callable[[], bool] | None = None,
    ) -> set[tuple[int, str]]:
        """Renew due descriptors and latch failures before yielding to IPC."""

        failed: set[tuple[int, str]] = set()
        now_monotonic = time.monotonic()
        for key, registered in tuple(self._operations.items()):
            if key not in running or self._operations.get(key) is not registered:
                continue
            if not registered.renewing or registered.receipt_committed or now_monotonic < registered.next_renewal:
                continue
            if self._renew is None:
                registered.renewing = False
                failed.add(key)
                on_failure(key)
                if after_each is not None and after_each():
                    break
                continue
            expiry = datetime.now(_UTC) + registered.ttl
            try:
                active = bool(self._renew(registered.operation, expiry))
            except Exception:
                registered.renewing = False
                failed.add(key)
                on_failure(key)
                if after_each is not None and after_each():
                    break
                continue
            if active:
                registered.expires_at = expiry
                registered.next_renewal = now_monotonic + _renew_interval(registered.ttl)
            else:
                registered.receipt_committed = True
                registered.renewing = False
            if after_each is not None and after_each():
                break
        return failed

    def operation_for(self, worker_id: int, dispatch: ProcessLaneDispatch) -> Any | None:
        registered = self._operations.get((worker_id, dispatch.command_id))
        return registered.operation if registered is not None else None

    def clear(self, worker_id: int, dispatch: ProcessLaneDispatch) -> None:
        key = (worker_id, dispatch.command_id)
        self._operations.pop(key, None)
        self._replies.pop(key, None)

    def _validate_request(
        self,
        request: ProcessLaneOperationLeaseRequest,
        *,
        worker_id: int,
        dispatch: ProcessLaneDispatch | None,
    ) -> str | None:
        if dispatch is None:
            return OPERATION_LEASE_ERROR
        exact = (
            request.worker_id == worker_id
            and request.command_id == dispatch.command_id
            and request.binding == dispatch.binding
        )
        if not exact:
            _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=command_binding")
            return _IDENTITY_ERROR
        if request.action == "register":
            operation = request.operation
            if operation is None:
                return _IDENTITY_ERROR
            try:
                identity = operation_identity(operation)
                invocation = operation.attempt.request.invocation
            except (AttributeError, ValueError):
                _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=operation_shape")
                return _IDENTITY_ERROR
            expected = (request.operation_key, request.owner_digest, request.epoch)
            if identity != expected:
                _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=operation_identity")
                return _IDENTITY_ERROR
            if invocation.run_id != f"dpone-backfill:{dispatch.binding.run_key}":
                _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=invocation")
                return _IDENTITY_ERROR
            if request.owner_digest != operation_owner_digest(dispatch.binding.owner):
                _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=owner")
                return _IDENTITY_ERROR
            try:
                expected_expiry = parent_issued_operation_lease_expiry(dispatch.load_config)
                child_expiry = operation_lease_expiry(operation).astimezone(_UTC)
            except ValueError:
                _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=lease_expiry")
                return _IDENTITY_ERROR
            if child_expiry != expected_expiry:
                _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=lease_expiry")
                return _IDENTITY_ERROR
            if self._validate_binding is None or not self._validate_binding(
                dispatch,
                operation,
                request.portable_scope_binding,
            ):
                _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=runtime_binding")
                return _IDENTITY_ERROR
            return None
        registered = self._operations.get((worker_id, request.command_id))
        if registered is None:
            return OPERATION_LEASE_ERROR
        if operation_identity(registered.operation) != (
            request.operation_key,
            request.owner_digest,
            request.epoch,
        ):
            _LOG.warning("event=dpone.backfill_process_operation_request_rejected reason=registered_identity")
            return _IDENTITY_ERROR
        return None

    @staticmethod
    def _reply(
        request: ProcessLaneOperationLeaseRequest,
        active: bool,
        error: str | None,
        *,
        expiry: datetime | None = None,
    ) -> ProcessLaneOperationLeaseReply:
        return ProcessLaneOperationLeaseReply(
            request_id=request.request_id,
            worker_id=request.worker_id,
            command_id=request.command_id,
            active=active,
            error=error,
            lease_expires_at_utc=expiry,
        )

    @staticmethod
    def _send(connection: Connection, reply: ProcessLaneOperationLeaseReply) -> bool:
        try:
            connection.send(reply)
        except (BrokenPipeError, EOFError, OSError):
            return False
        return True

    @staticmethod
    def _fingerprint(request: ProcessLaneOperationLeaseRequest) -> tuple[Any, ...]:
        return (
            request.action,
            request.worker_id,
            request.command_id,
            request.binding,
            request.operation_key,
            request.owner_digest,
            request.epoch,
            request.portable_scope_binding,
            request.operation,
        )


def _renew_interval(ttl: timedelta) -> float:
    return min(30.0, max(0.05, ttl.total_seconds() / 3.0))


def adapt_operation_binding_validator(
    validator: Callable[..., bool] | None,
) -> OperationBindingValidator | None:
    """Adapt the 0.74.26 two-argument callback before any chunk is claimed.

    Three-argument validators remain the primary contract and receive the
    child-returned portable-scope proof.  A legacy validator is invoked only
    after the adapter independently proves that this optional proof is the
    exact immutable binding already held by the parent dispatch.
    """

    if validator is None:
        return None
    try:
        validator_signature = signature(validator)
    except (TypeError, ValueError):
        raise ValueError(_VALIDATOR_ERROR) from None
    positional = (object(), object(), object())
    if _accepts_positional_arguments(validator_signature, positional):
        return cast(OperationBindingValidator, validator)
    if not _accepts_positional_arguments(validator_signature, positional[:2]):
        raise ValueError(_VALIDATOR_ERROR)
    legacy = cast(LegacyOperationBindingValidator, validator)

    def adapted(dispatch: ProcessLaneDispatch, operation: Any, portable_scope_binding: Any | None) -> bool:
        if not portable_binding_is_exact(dispatch, portable_scope_binding):
            return False
        return bool(legacy(dispatch, operation))

    return adapted


def _accepts_positional_arguments(validator_signature: Any, values: tuple[object, ...]) -> bool:
    try:
        validator_signature.bind(*values)
    except TypeError:
        return False
    return True


def portable_binding_is_exact(
    dispatch: ProcessLaneDispatch,
    portable_scope_binding: Any | None,
) -> bool:
    """Verify the child proof is the exact parent-issued portable binding."""

    load_config = dispatch.load_config
    expected = dispatch_portable_binding(dispatch)
    try:
        scope = resolve_portable_relation_scope(load_config)
    except ValueError:
        return False
    if scope is None:
        return portable_scope_binding is None and expected is None
    if not isinstance(expected, PortableScopeBinding) or portable_scope_binding != expected:
        return False
    try:
        expected.require_scope(scope)
    except ValueError:
        return False
    return True


def dispatch_portable_binding(dispatch: ProcessLaneDispatch) -> Any | None:
    """Return the immutable portable binding already carried by the parent."""

    options = getattr(dispatch.load_config, "options", {}) or {}
    return options.get(PORTABLE_SCOPE_BINDING_OPTION) if isinstance(options, Mapping) else None


__all__ = [
    "OPERATION_LEASE_ERROR",
    "OperationBindingValidator",
    "ParentOperationLeaseCoordinator",
    "adapt_operation_binding_validator",
    "dispatch_portable_binding",
    "is_mssql_transaction_operation",
    "portable_binding_is_exact",
]
