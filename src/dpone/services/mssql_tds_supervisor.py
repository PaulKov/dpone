"""Acknowledged TDS lifecycle ordering above injected process and journal ports.

Success here means complete worker input, zero exit and reaping, not verified SQL
or published data. Failed attempts require coordinator retirement before retry.
The request supplier is trusted bounded in-memory encoding, never network I/O.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import asdict
from hashlib import sha256
from time import monotonic
from typing import NoReturn

from dpone.contracts.mssql_tds_result import TdsWorkerResult, attempt_identity_digest, encode_result
from dpone.ports.mssql_tds_journal import TdsJournalGateway
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown, TdsManagedWorker, TdsUnresolvedLaunch, TdsWorkerLauncher
from dpone.services.mssql_tds_writer_contracts import (
    Contained,
    ContainmentRequired,
    Exited,
    ProcessRegistered,
    Running,
    TdsAttemptError,
    TdsAttemptPhase,
    TdsChildExit,
    TdsInputReceipt,
    TdsProcessIdentity,
    WindowOutcomeUnknown,
    canonical_json_bytes,
)


class TdsWorkerFailure(RuntimeError):
    """Contained attempt failure; SQL retirement remains mandatory."""

    def __init__(self, code: TdsAttemptError) -> None:
        self.code = code
        super().__init__("mssql_native.tds_worker_failed:" + code.value)


class TdsSupervisionUnknown(WindowOutcomeUnknown):
    """Unresolved authority; a retained worker must not be discarded or retried.

    worker is a process capability, never a request/credential buffer. Its presence
    requires explicit recovery, including descriptor cleanup after known reaping.
    """

    def __init__(
        self, worker: TdsManagedWorker | None, *, unresolved_launch: TdsUnresolvedLaunch | None = None
    ) -> None:
        self.worker = worker
        self.unresolved_launch = unresolved_launch
        super().__init__("mssql_native.tds_supervision_unknown")


def _before(deadline: float, clock: Callable[[], float]) -> None:
    now = clock()
    if not math.isfinite(now) or now >= deadline:
        raise TimeoutError


def _exit(value: TdsChildExit, identity: TdsProcessIdentity) -> None:
    if type(value) is not TdsChildExit or value.identity != identity or not value.reaped:
        raise TdsWorkerFailure(TdsAttemptError.PROCESS_UNKNOWN)


def _failed(
    writer: TdsJournalGateway,
    child: TdsManagedWorker | None,
    known_exit: TdsChildExit | None,
    expected_process: TdsProcessIdentity | None,
    original: BaseException,
    code: TdsAttemptError,
    termination_timeout: float,
    clock: Callable[[], float],
) -> NoReturn:
    # Capture one deadline before journal work; even a poisoned save cannot skip
    # containment. A slow journal does not earn the child a fresh timeout.
    if child is None:
        if isinstance(original, TdsLaunchUnknown):
            launch = original.launch
            try:
                deadline = clock() + termination_timeout
                if not math.isfinite(deadline):
                    raise ValueError
                _before(deadline, clock)
                launch.contain(deadline=deadline)
                _before(deadline, clock)
                launch.close()
            except BaseException:
                raise TdsSupervisionUnknown(None, unresolved_launch=launch) from None
        raise TdsSupervisionUnknown(None) from None
    try:
        deadline = clock() + termination_timeout
        if not math.isfinite(deadline):
            raise ValueError
    except BaseException:
        raise TdsSupervisionUnknown(child) from None
    # Process containment is independent of journal health. Never put a
    # potentially failing storage operation in front of the stop attempt.
    containment_unknown = False
    try:
        if known_exit is None:
            _before(deadline, clock)
            known_exit = child.terminate(deadline=deadline)
            _before(deadline, clock)
        if expected_process is None:
            raise TdsWorkerFailure(TdsAttemptError.PROCESS_UNKNOWN)
        _exit(known_exit, expected_process)
    except BaseException:
        containment_unknown = True
    journal_unknown = False
    try:
        writer.assert_authority(deadline=deadline)
        writer.advance(ContainmentRequired(code), expected_phase=writer.snapshot.state.phase, deadline=deadline)
    except BaseException:
        journal_unknown = True
    if containment_unknown or known_exit is None:
        raise TdsSupervisionUnknown(child) from None
    if not journal_unknown:
        try:
            writer.assert_authority(deadline=deadline)
            proof = sha256(canonical_json_bytes(asdict(known_exit))).hexdigest()
            writer.advance(Contained(proof), expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED, deadline=deadline)
        except BaseException:
            journal_unknown = True
    try:
        child.close()
    except BaseException:
        raise TdsSupervisionUnknown(child) from None
    if journal_unknown or isinstance(original, WindowOutcomeUnknown):
        raise TdsSupervisionUnknown(None) from None
    if not isinstance(original, Exception):
        raise original
    raise TdsWorkerFailure(code) from None


def run_tds_worker(
    writer: TdsJournalGateway,
    launcher: TdsWorkerLauncher,
    expected_input: TdsInputReceipt,
    request: Callable[[], bytes],
    *,
    operation_deadline: float,
    startup_timeout: float,
    termination_timeout: float,
    clock: Callable[[], float] = monotonic,
) -> TdsWorkerResult:
    """Run one admitted attempt, without fallback or an implicit retry.

    Caller supplies a deadline-bounded actor gateway, never a direct journal
    writer. It persists LAUNCH_INTENT first and retains all staging resources until
    independent SQL verification or retirement. Port calls must honor deadlines;
    this service never claims to interrupt arbitrary blocking Python callbacks.
    """
    for value in (operation_deadline, startup_timeout, termination_timeout):
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError("mssql_native.tds_supervisor_arguments_invalid")
    if type(expected_input) is not TdsInputReceipt or not callable(request):
        raise ValueError("mssql_native.tds_supervisor_arguments_invalid")
    if writer.snapshot.state.phase != TdsAttemptPhase.LAUNCH_INTENT:
        raise ValueError("mssql_native.tds_supervisor_launch_intent_required")
    identity = writer.snapshot.state.identity
    if expected_input.file_sha256 != identity.file_sha256:
        raise ValueError("mssql_native.tds_supervisor_input_identity_invalid")
    digest = attempt_identity_digest(identity)
    child: TdsManagedWorker | None = None
    known_exit: TdsChildExit | None = None
    expected_process: TdsProcessIdentity | None = None
    code = TdsAttemptError.FENCING
    try:
        writer.assert_authority(deadline=operation_deadline)
        _before(operation_deadline, clock)
        startup_deadline = min(operation_deadline, clock() + startup_timeout)
        code = TdsAttemptError.STARTUP_TIMEOUT
        child = launcher.spawn(startup_deadline=startup_deadline, operation_deadline=operation_deadline)
        expected_process = child.identity
        if type(expected_process) is not TdsProcessIdentity:
            raise ValueError
        _before(startup_deadline, clock)
        writer.assert_authority(deadline=startup_deadline)
        child.startup(deadline=startup_deadline)
        _before(startup_deadline, clock)
        code = TdsAttemptError.FENCING
        writer.advance(
            ProcessRegistered(expected_process), expected_phase=TdsAttemptPhase.LAUNCH_INTENT, deadline=startup_deadline
        )
        writer.advance(Running(), expected_phase=TdsAttemptPhase.SPAWNED_WAITING, deadline=operation_deadline)
        writer.assert_authority(deadline=operation_deadline)
        _before(operation_deadline, clock)
        code = TdsAttemptError.PROTOCOL
        body = request()
        try:
            if type(body) is not bytes or not 0 < len(body) <= 1 << 20:
                raise ValueError
            # Recheck after supplier execution: no credentials cross the pipe
            # after an observed lost fence or expired operation budget.
            writer.assert_authority(deadline=operation_deadline)
            _before(operation_deadline, clock)
            child.send(body, deadline=operation_deadline)
        finally:
            del body
        _before(operation_deadline, clock)
        writer.assert_authority(deadline=operation_deadline)
        result = child.receive(
            expected_attempt_sha256=digest, expected_input=expected_input, deadline=operation_deadline
        )
        _before(operation_deadline, clock)
        if type(result) is not TdsWorkerResult or result.attempt_sha256 != digest:
            raise TdsWorkerFailure(TdsAttemptError.PROTOCOL)
        if result.error is not None:
            raise TdsWorkerFailure(result.error)
        if result.receipt != expected_input:
            raise TdsWorkerFailure(TdsAttemptError.PROTOCOL)
        code = TdsAttemptError.PROCESS_UNKNOWN
        observed = child.wait(deadline=operation_deadline)
        _exit(observed, expected_process)
        known_exit = observed
        _before(operation_deadline, clock)
        if observed.exit_code != 0:
            raise TdsWorkerFailure(TdsAttemptError.DRIVER)
        code = TdsAttemptError.FENCING
        writer.assert_authority(deadline=operation_deadline)
        writer.advance(
            Exited(0, sha256(encode_result(result)).hexdigest()),
            expected_phase=TdsAttemptPhase.RUNNING,
            deadline=operation_deadline,
        )
        _before(operation_deadline, clock)
    except BaseException as error:
        if isinstance(error, TdsWorkerFailure):
            code = error.code
        elif isinstance(error, TimeoutError) and child is not None and code != TdsAttemptError.STARTUP_TIMEOUT:
            code = TdsAttemptError.OPERATION_TIMEOUT
        _failed(writer, child, known_exit, expected_process, error, code, termination_timeout, clock)
    try:
        child.close()
    except BaseException:
        raise TdsSupervisionUnknown(child) from None
    return result
