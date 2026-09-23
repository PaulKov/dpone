"""Exclusive local child resources shared by guarded process protocols.

This owner never launches an executable or interprets IPC. Closing a descriptor
is not containment; only authenticated pidfd wait/reaping establishes local exit.
Unknown launches retain their process and every still-owned resource.
"""

from __future__ import annotations

import math
import os
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from tempfile import TemporaryDirectory
from typing import Any, cast

import dpone.adapters.mssql_tds_natural_settlement as natural_settlement
from dpone.adapters.mssql_tds_child_process_port import LINUX_TDS_CHILD_PROCESS, TdsChildProcessPort
from dpone.adapters.mssql_tds_process import LinuxTdsProcess as LinuxTdsProcess
from dpone.contracts.mssql_tds_api import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown as TdsLaunchUnknown


def _close_all(actions: tuple[Callable[[], None], ...]) -> None:
    """Attempt every detached resource once, preserving the first close error."""
    error: BaseException | None = None
    for action in actions:
        try:
            action()
        except BaseException as failure:
            if error is None:
                error = failure
    if error is not None:
        raise error


def _close_fds(fds: tuple[int, ...]) -> None:
    _close_all(tuple(partial(os.close, fd) for fd in fds))


@dataclass
class _Resource:
    """Original close token and irreversible acknowledgment progress."""

    kind: str
    value: Any
    state: str = "OWNED"


class TdsChildProcess:
    """Authoritative local custody; protocol poison does not transfer cleanup."""

    def __init__(
        self,
        process: subprocess.Popen[bytes],
        handle: LinuxTdsProcess | None,
        descriptors: tuple[int, ...],
        cache: TemporaryDirectory[str] | None = None,
    ) -> None:
        self.process, self.handle, self.descriptors = process, handle, descriptors
        self._cache = cache
        self._owner = (os.getpid(), threading.current_thread())
        self._executor: threading.Thread | None = None
        self._process_port: TdsChildProcessPort = LINUX_TDS_CHILD_PROCESS
        self._closed = self._poisoned = self._settling = self._unknown = False
        self._budget_attempted = False
        self._deadline: float | None = None
        self._lock = threading.RLock()
        self.exit: TdsChildExit | None = None
        self._resources = [_Resource("fd", fd) for fd in descriptors]
        if handle is not None:
            self._resources.append(_Resource("pidfd", handle))
        if process is not None and getattr(process, "stdout", None) is not None:
            self._resources.append(_Resource("stdout", process.stdout))
        if cache is not None:
            self._resources.append(_Resource("cache", cache))

    @classmethod
    def launch(cls) -> TdsChildProcess:
        """Start custody before any allocation, with no invented process identity."""
        return cls(cast("subprocess.Popen[bytes]", None), None, ())

    def retain_process(self, process: subprocess.Popen[bytes]) -> None:
        self.process = process
        if process.stdout is not None:
            self._resources.append(_Resource("stdout", process.stdout))

    def retain_descriptors(self, descriptors: tuple[int, ...]) -> None:
        self.descriptors += descriptors
        self._resources.extend(_Resource("fd", fd) for fd in descriptors)

    def retain_cache(self, cache: TemporaryDirectory[str]) -> None:
        self._cache = cache
        self._resources.append(_Resource("cache", cache))

    def retain_handle(self, handle: Any) -> None:
        self.handle = handle
        self._resources.append(_Resource("pidfd", handle))

    def retain_acquisition(self, error: BaseException) -> None:
        failure = getattr(error, "acquisition", None)
        if failure is not None:
            self._resources.append(
                _Resource("acquisition", failure.descriptor, "CLOSE_UNKNOWN" if failure.close_unknown else "CLOSED")
            )

    def retain_socket(self, channel: Any) -> None:
        self._resources.append(_Resource("socket", channel))

    def poison(self) -> None:
        self._poisoned = True

    def check_owner(self) -> None:
        if (
            self._closed
            or self._poisoned
            or any(r.state == "CLOSE_UNKNOWN" for r in self._resources)
            or self._owner != (os.getpid(), threading.current_thread())
        ):
            self.poison()
            raise ValueError("mssql_native.tds_worker_owner_invalid")

    def check_cleanup_owner(self) -> None:
        if self._owner[0] != os.getpid() or threading.current_thread() not in (self._owner[1], self._executor):
            self.poison()
            raise ValueError("mssql_native.tds_worker_owner_invalid")

    def capture_budget(self, *, deadline: float | None = None, allowance: float | None = None) -> float:
        """Capture before the fallible clock; subsequent requests can only shorten."""
        with self._lock:
            if not self._budget_attempted:
                self._budget_attempted = True
                if allowance is not None:
                    now = time.monotonic()
                    if not math.isfinite(now) or not math.isfinite(allowance) or allowance <= 0:
                        raise WindowOutcomeUnknown("mssql_native.tds_budget_unknown")
                    self._deadline = now + allowance
                else:
                    self._deadline = deadline
            if self._deadline is None or not math.isfinite(self._deadline):
                raise WindowOutcomeUnknown("mssql_native.tds_budget_unknown")
            if deadline is not None:
                if type(deadline) not in (int, float) or not math.isfinite(deadline):
                    raise ValueError("mssql_native.tds_settlement_deadline")
                self._deadline = min(self._deadline, deadline)
            return self._deadline

    @property
    def identity(self) -> TdsProcessIdentity:
        if self.handle is None:
            raise WindowOutcomeUnknown("mssql_native.tds_reaping_unknown")
        return self.handle.identity

    def _close_resource(self, resource: _Resource) -> None:
        if resource.state == "CLOSED":
            return
        if resource.state != "OWNED":
            raise WindowOutcomeUnknown("mssql_native.tds_close_unknown")
        resource.state = "CLOSE_UNKNOWN"
        if resource.kind == "fd":
            self.descriptors = tuple(fd for fd in self.descriptors if fd != resource.value)
            os.close(resource.value)
        elif resource.kind == "cache":
            resource.value.cleanup()
        else:
            resource.value.close()
        resource.state = "CLOSED"

    def close_descriptor(self, descriptor: int) -> None:
        """Detach before close; ambiguous effects remain retained and never retried."""
        self.check_cleanup_owner()
        resource = next((r for r in self._resources if r.kind == "fd" and r.value == descriptor), None)
        if resource is None or resource.state != "OWNED":
            raise ValueError("mssql_native.tds_child_descriptor_unowned")
        self.check_owner()
        self._close_resource(resource)

    def close_socket(self, channel: Any) -> None:
        """Close the child-side launch socket once without registering its fd twice."""
        resource = next(r for r in self._resources if r.kind == "socket" and r.value is channel)
        self._close_resource(resource)

    def close_launch_descriptors(self, descriptors: tuple[int, ...], closer: Callable[[tuple[int, ...]], None]) -> None:
        """Preserve module-local batch-close seams without losing ambiguous tokens."""
        resources = [r for r in self._resources if r.kind == "fd" and r.value in descriptors and r.state == "OWNED"]
        for resource in resources:
            resource.state = "CLOSE_UNKNOWN"
        self.descriptors = tuple(fd for fd in self.descriptors if fd not in descriptors)
        closer(tuple(r.value for r in resources))
        for resource in resources:
            resource.state = "CLOSED"

    def _settle(
        self, *, deadline: float, kill: bool, current_deadline: Callable[[], float] | None = None
    ) -> TdsChildExit:
        self.check_cleanup_owner()
        if kill:
            deadline = self.capture_budget(deadline=deadline)
            self.poison()
        if self._settling or self._unknown:
            self.poison()
            raise WindowOutcomeUnknown("mssql_native.tds_reaping_unknown")
        if time.monotonic() >= deadline:
            raise ValueError("mssql_native.tds_settlement_deadline")
        if self.exit is not None:
            return self.exit
        if self.handle is None:
            raise WindowOutcomeUnknown("mssql_native.tds_reaping_unknown")
        self._settling = True
        try:
            if self._executor is not None and self._process_port.is_native_handle(self.handle):
                observed = self._process_port.settle_effective(
                    self.handle,
                    deadline=deadline,
                    direct_child=True,
                    kill=kill,
                    current_deadline=current_deadline or self.capture_budget,
                )
            else:
                method = self.handle.contain if kill else self.handle.wait
                observed = method(deadline=deadline, direct_child=True)
            if kill and time.monotonic() >= self.capture_budget():
                raise WindowOutcomeUnknown("mssql_native.tds_reaping_unknown")
            if not observed.reaped or observed.exit_code is None:
                raise WindowOutcomeUnknown("mssql_native.tds_reaping_unknown")
            self.exit = TdsChildExit(self.identity, observed.exit_code, True)
            self.process.returncode = observed.exit_code
            return self.exit
        except BaseException:
            if kill:
                self._unknown = True
            raise
        finally:
            self._settling = False

    def wait(self, *, deadline: float) -> TdsChildExit:
        return self._settle(deadline=deadline, kill=False)

    def terminate(self, *, deadline: float) -> TdsChildExit:
        return self._settle(deadline=deadline, kill=True)

    def close(self, *, descriptors_first: bool = False) -> None:
        """Close independently once after reap; any ambiguous ACK stays UNKNOWN."""
        self.check_cleanup_owner()
        if self.exit is None and self.process is not None:
            raise WindowOutcomeUnknown("mssql_native.tds_close_unsettled")
        self.poison()
        order = {
            "fd": 1,
            "socket": 1,
            "pidfd": 2 if descriptors_first else 0,
            "stdout": 3,
            "cache": 4,
            "acquisition": 5,
        }
        _close_all(
            tuple(partial(self._close_resource, r) for r in sorted(self._resources, key=lambda r: order[r.kind]))
        )
        self._closed = True


class TdsChildContainmentExecutor:
    """Dedicated schedule for the same custody engine; no separate pidfd effects."""

    process_port: TdsChildProcessPort = LINUX_TDS_CHILD_PROCESS

    def __init__(self, child: Any, identity: Any, deadline: float, allowance: float) -> None:
        self._initialize(child, identity, deadline, allowance)

    def _initialize(self, child: Any, identity: Any, deadline: float, allowance: float) -> None:
        self._resources = child if isinstance(child, TdsChildProcess) else TdsChildProcess(child, None, ())
        self._resources._process_port = self.process_port
        self.child, self.identity = self._resources.process, identity
        self.deadline, self.allowance = deadline, allowance
        self._condition = threading.Condition()
        self.ready = threading.Event()
        self.done = threading.Event()
        self.failed = False
        self._settlement = natural_settlement.TdsNaturalSettlementState()
        self._thread = threading.Thread(target=self._run, name="dpone-observe-containment", daemon=True)
        self._resources._executor = self._thread
        self._thread.start()

    @property
    def cleanup_deadline(self) -> float | None:
        return self._resources._deadline

    @property
    def exit(self) -> TdsChildExit | None:
        return self._resources.exit

    def request(self, deadline: float | None = None) -> float:
        """The first capture and every minimum feed the active syscall loop."""
        with self._condition:
            self._resources.poison()
            self._settlement.request_force()
            try:
                return self._resources.capture_budget(deadline=deadline, allowance=self.allowance)
            except BaseException:
                self.failed = True
                raise
            finally:
                self._condition.notify_all()

    def request_settlement(self, *, natural_deadline: float, containment_deadline: float | None = None) -> float:
        """Request one natural wait followed, if necessary, by containment."""
        command = natural_settlement.TdsNaturalSettlementCommand(natural_deadline, containment_deadline)
        with self._condition:
            self._settlement.admit(command)
            try:
                self._resources.poison()
                now = time.monotonic()
                plan = self._settlement.resolve(operation_deadline=self.deadline, allowance=self.allowance, now=now)
                captured = self._resources.capture_budget(deadline=plan.cleanup_deadline)
                self._condition.notify_all()
                return captured
            except BaseException as error:
                self._mark_unknown()
                if isinstance(error, natural_settlement.TdsNaturalSettlementPolicyError):
                    raise WindowOutcomeUnknown("mssql_native.tds_natural_settlement_unknown") from None
                raise

    def _mark_unknown(self) -> None:
        self._settlement.mark_unknown()
        self._resources.poison()
        self._resources._unknown = self.failed = True

    def _natural_ceiling(self) -> float:
        with self._condition:
            return self._settlement.natural_ceiling()

    def _run(self) -> None:
        try:
            self._resources.retain_handle(self.process_port.acquire(self.identity))
            self.ready.set()
            with self._condition:
                while not self._resources._budget_attempted:
                    remaining = self.deadline - time.monotonic()
                    if remaining <= 0:
                        self.request()
                        break
                    self._condition.wait(remaining)
                deadline = self._resources.capture_budget()
                plan = self._settlement.plan
                force = self._settlement.force_requested
                if plan is not None and not force:
                    try:
                        force = self._settlement.force_if_natural_expired(time.monotonic())
                    except BaseException:
                        self._mark_unknown()
                        raise
            if plan is not None and not force:
                try:
                    self._resources._settle(
                        deadline=plan.natural_deadline, kill=False, current_deadline=self._natural_ceiling
                    )
                except BaseException as error:
                    handle = self._resources.handle
                    if not self.process_port.permits_forced_transition(self._settlement, error, handle):
                        self._mark_unknown()
                        raise
                except BaseException:
                    self._mark_unknown()
                    raise
                else:
                    self._resources.close()
                    return
            self._resources.poison()
            self._resources.terminate(deadline=deadline)
            self._resources.close()
        except BaseException as error:
            self._resources.retain_acquisition(error)
            self.failed = True
        finally:
            # Events acknowledge executor progress, never containment or closure.
            for event in (self.ready, self.done):
                try:
                    event.set()
                except BaseException:
                    self.failed = True


def _existing_custody(process: Any, handle: Any | None, descriptors: tuple[int, ...]) -> TdsChildProcess | None:
    """Validate an explicit internal handoff, preserving legacy constructor calls."""
    if not isinstance(process, TdsChildProcess):
        return None
    if process.handle is not handle or process.descriptors != descriptors:
        raise ValueError("mssql_native.tds_custody_handoff_invalid")
    return process


from dpone.adapters.mssql_tds_unresolved_launch import (  # noqa: E402
    UnresolvedPythonTdsLaunch as UnresolvedPythonTdsLaunch,
)

# Public compatibility includes the original defining module for inspection.
UnresolvedPythonTdsLaunch.__module__ = __name__
