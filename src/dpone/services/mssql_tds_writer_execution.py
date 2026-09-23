"""One-shot bulk grant, result evidence and exact local-exit orchestration."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from inspect import getattr_static
from threading import Lock

from dpone.services.mssql_tds_writer_execution_custody import (
    SqlClientWriterHandledFailure,
    SqlClientWriterLocallyExited,
    _handled_failure,
    _locally_exited,
    _LocallyExitedOwner,
)
from dpone.services.mssql_tds_writer_execution_validation import (
    advance_contained,
    advance_containment_required,
    advance_exited,
    class_call,
    class_property,
    decode_exact_result,
    invalid,
    persist_local_exit,
    persist_result,
    reassert_final,
    validate_local_exit,
    validate_ready,
    validate_result_exit,
)
from dpone.services.mssql_tds_writer_observation_custody import (
    SqlClientWriterContinuationCleanup,
    SqlClientWriterGrantReady,
    _GrantReadyOwner,
)

ERROR = "mssql_native.sqlclient_writer_execution_unknown"


def _cleanup_once(value: object) -> None:
    descriptor = getattr_static(type(value), "_cleanup_once")
    descriptor.__get__(value, type(value))()


@dataclass(slots=True, repr=False)
class _ExecutionResources:
    owner: _GrantReadyOwner
    original_cleanup: SqlClientWriterContinuationCleanup
    local_exit: object | None = None
    result_bytes: bytes | None = None
    result: object | None = None
    result_receipt: object | None = None
    local_exit_receipt: object | None = None
    final_state: object | None = None
    process_attempted: bool = False
    gateway_attempted: set[int] = field(default_factory=set)
    observer_attempted: bool = False

    @property
    def deadline(self) -> float:
        return self.owner.refs.launch.containment_deadline

    def set_local_exit(self, value: object) -> None:
        self.local_exit = value

    def facts(self) -> _ExecutionUnknownFacts:
        return _ExecutionUnknownFacts(
            self.result_bytes,
            self.result,
            self.result_receipt,
            self.local_exit,
            self.local_exit_receipt,
            self.final_state,
        )

    def close_local_success(self, *, close_gateways: bool) -> None:
        failed = False
        process = self.owner.refs.launch.process
        self.process_attempted = True
        try:
            if class_call(process, "close") is not None:
                failed = True
        except BaseException:
            failed = True
        if close_gateways:
            failed = self.close_gateways() or failed
        self.observer_attempted = True
        try:
            type(self.owner.observer_cleanup)._settle_once(self.owner.observer_cleanup)
        except BaseException:
            failed = True
        if failed:
            invalid()

    def close_gateways(self, *, close_lifecycle: bool = True) -> bool:
        """Close each actor gateway once and report any containment uncertainty."""
        failed = False
        gateways: list[object] = [self.owner.refs.launch.evidence]
        if close_lifecycle:
            gateways.append(self.owner.refs.launch.lifecycle)
        for gateway in gateways:
            marker = id(gateway)
            if marker in self.gateway_attempted:
                continue
            self.gateway_attempted.add(marker)
            try:
                if class_call(gateway, "close", deadline=self.deadline) is not None:
                    failed = True
            except BaseException:
                failed = True
        return failed

    def cleanup_unknown(self) -> None:
        process = self.owner.refs.launch.process
        if self.local_exit is None:
            try:
                candidate = class_call(process, "terminate", deadline=self.deadline)
                self.local_exit = validate_local_exit(self.owner, candidate)
            except BaseException:
                pass
        if not self.process_attempted:
            self.process_attempted = True
            try:
                class_call(process, "close")
            except BaseException:
                pass
        for gateway in (self.owner.refs.launch.evidence, self.owner.refs.launch.lifecycle):
            marker = id(gateway)
            if marker in self.gateway_attempted:
                continue
            self.gateway_attempted.add(marker)
            try:
                class_call(gateway, "close", deadline=self.deadline)
            except BaseException:
                pass
        if not self.observer_attempted:
            self.observer_attempted = True
            try:
                type(self.owner.observer_cleanup)._cleanup_once(self.owner.observer_cleanup)
            except BaseException:
                pass


class _ExecutionCleanup:
    """Opaque one-shot cleanup; it exposes no writer effect authority."""

    __slots__ = ()

    def _cleanup_once(self) -> None:
        invalid()


@dataclass(frozen=True, slots=True, repr=False)
class _ExecutionUnknownFacts:
    """Credential-free immutable observations retained across UNKNOWN cleanup."""

    result_bytes: bytes | None = None
    result: object | None = None
    result_receipt: object | None = None
    local_exit: object | None = None
    local_exit_receipt: object | None = None
    final_state: object | None = None


def _execution_cleanup(resources: _ExecutionResources) -> _ExecutionCleanup:
    lock, used = Lock(), False
    exact: _ExecutionCleanup

    class _Exact(_ExecutionCleanup):
        __slots__ = ()

        def _cleanup_once(self) -> None:
            nonlocal used
            if self is not exact:
                invalid()
            with lock:
                if used:
                    return
                used = True
            resources.cleanup_unknown()

    exact = _Exact()
    return exact


class SqlClientWriterExecutionUnknown(RuntimeError):
    """Sticky post-claim ambiguity with no resend or receive authority."""

    __slots__ = ("_cleanup", "_facts")

    def __init__(self, cleanup: object, facts: _ExecutionUnknownFacts | None = None) -> None:
        self._cleanup = cleanup
        self._facts = _ExecutionUnknownFacts() if facts is None else deepcopy(facts)
        super().__init__(ERROR)

    def __repr__(self) -> str:
        return "SqlClientWriterExecutionUnknown(<opaque>)"


def execute_sqlclient_writer(
    ready: SqlClientWriterGrantReady,
    *,
    clock_ns: Callable[[], int],
    retain_lifecycle: bool = False,
) -> SqlClientWriterLocallyExited | SqlClientWriterHandledFailure:
    """Consume P10d once and stop after exact acknowledged local exit."""
    if not isinstance(ready, SqlClientWriterGrantReady) or not callable(clock_ns) or type(retain_lifecycle) is not bool:
        invalid()
    original_cleanup = type(ready)._prepare_p10e_cleanup(ready, ready)
    retained: _ExecutionResources | None = None
    unknown_cleanup: object = original_cleanup
    try:
        claim = type(ready)._claim_p10e_once(ready, ready, original_cleanup)
        owner = type(ready)._assert_p10e_claim(ready, ready, claim, original_cleanup)
        retained = _ExecutionResources(owner, original_cleanup)
        unknown_cleanup = _execution_cleanup(retained)
        now_ns = clock_ns()
        validate_ready(owner, now_ns=now_ns)
        process = owner.refs.launch.process
        if (
            class_call(process, "send_grant", owner.grant_bytes, deadline=owner.refs.launch.plan.operation_deadline)
            is not None
        ):
            invalid()
        raw = None
        retained_raw = None
        try:
            raw = class_call(process, "receive_result", deadline=owner.refs.launch.plan.operation_deadline)
        finally:
            retained_raw = class_property(process, "received_result")
            if type(retained_raw) is bytes:
                retained.result_bytes = retained_raw
        if type(raw) is not bytes or raw is not retained_raw:
            invalid()
        result = decode_exact_result(owner, raw)
        retained.result = result
        result_receipt = persist_result(owner, raw)
        retained.result_receipt = result_receipt
        local_exit = class_call(process, "wait", deadline=owner.refs.launch.plan.operation_deadline)
        validate_local_exit(owner, local_exit)
        retained.set_local_exit(local_exit)
        validate_result_exit(owner, result, local_exit)
        local_receipt = persist_local_exit(owner, result_receipt, local_exit)
        retained.local_exit_receipt = local_receipt
        state = advance_exited(owner, owner.state, local_exit, result_receipt.payload_sha256)
        retained.final_state = state
        reassert_final(owner, local_receipt, state)
        if result.result.error is not None:
            state = advance_containment_required(owner, state, result.result.error)
            retained.final_state = state
            reassert_final(owner, local_receipt, state)
            retained.close_local_success(close_gateways=False)
            state = advance_contained(owner, state, local_exit)
            retained.final_state = state
            reassert_final(owner, local_receipt, state)
            if retained.close_gateways(close_lifecycle=not retain_lifecycle):
                invalid()
            return _handled_failure(result.result.error)
        retained.close_local_success(close_gateways=False)
        plan = owner.refs.launch.plan
        p9_receipt = plan.p9_owner._receipt
        return _locally_exited(
            _LocallyExitedOwner(
                owner.refs.profile.writer_admission,
                owner.refs.launch.registration,
                owner.observation,
                owner.grant,
                result,
                raw,
                result_receipt,
                local_exit,
                local_receipt,
                state,
                owner.refs.launch.evidence,
                owner.refs.launch.lifecycle,
                plan.operation_deadline,
                plan.stage,
                plan.input_descriptor,
                plan.content_expectation,
                p9_receipt.payload_sha256,
            )
        )
    except BaseException:
        facts = None
        if retained is None:
            try:
                _cleanup_once(original_cleanup)
            except BaseException:
                pass
        else:
            _cleanup_once(unknown_cleanup)
            facts = retained.facts()
    raise SqlClientWriterExecutionUnknown(unknown_cleanup, facts) from None


__all__ = (
    "SqlClientWriterExecutionUnknown",
    "SqlClientWriterHandledFailure",
    "SqlClientWriterLocallyExited",
    "execute_sqlclient_writer",
)
