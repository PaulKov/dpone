"""Bounded journal waits without pretending to cancel thread-affine backend I/O.

A run owns its pool. Reservations survive timeout and blocked context teardown
until the actual actor thread terminates. Daemon threads do not establish
settlement or cancellation; unresolved records require newer-fence recovery.
Only the actor enters/exits the injected writer/client context manager.
"""

from __future__ import annotations

import math
import os
import queue
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Generic, TypeVar

from dpone.contracts.bounded_window import WindowOutcomeUnknown

Backend = TypeVar("Backend")
Snapshot = TypeVar("Snapshot")


def _remaining_seconds(deadline: float, clock: Callable[[], float]) -> float:
    """Validate and evaluate an absolute deadline in the injected clock domain."""
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ValueError("mssql_native.tds_journal_deadline_invalid")
    now = clock()
    if not math.isfinite(now):
        raise ValueError("mssql_native.tds_journal_clock_invalid")
    return max(0.0, deadline - now)


class TdsJournalActorUnknown(WindowOutcomeUnknown):
    """Unknown backend completion; gateway retains bounded close capability."""

    def __init__(self, gateway: _ActorCore[Any, Any] | None = None) -> None:
        self.gateway = gateway
        super().__init__("mssql_native.tds_journal_actor_unknown")


@dataclass
class _ActorCommand(Generic[Snapshot]):
    kind: str
    deadline: float
    event: object | None = None
    phase: object | None = None
    done: threading.Event = field(default_factory=threading.Event)
    snapshot: Snapshot | None = None
    failed: bool = False


class _ActorCore(Generic[Backend, Snapshot]):
    """Supervisor-thread gateway; only acknowledged snapshots are exposed.

    Construct through TdsActorPool.open. Timeout permanently disables new
    commands. close may still wait for actual teardown, but never removes a live
    actor from its pool or treats a late acknowledgement as fresh authority.
    """

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[Backend]],
        deadline: float,
        clock: Callable[[], float],
        snapshot_type: type[Snapshot],
    ) -> None:
        self._factory, self._clock = factory, clock
        self._snapshot_type = snapshot_type
        self._pid, self._owner = os.getpid(), threading.current_thread()
        self._stop = threading.Event()
        self._finished = threading.Event()
        self._teardown_failed = False
        self._queue: queue.Queue[_ActorCommand[Snapshot] | None] = queue.Queue(maxsize=1)
        self._snapshot: Snapshot | None = None
        self._inflight = False
        self._admitted = False
        self._ready: _ActorCommand[Snapshot] = _ActorCommand("initialize", deadline)
        self._thread = threading.Thread(target=self._run, name="dpone-tds-journal", daemon=True)

    def _owned(self) -> None:
        if self._pid != os.getpid() or self._owner is not threading.current_thread():
            raise TdsJournalActorUnknown(self)

    @property
    def snapshot(self) -> Snapshot:
        self._owned()
        if self._snapshot is None:
            raise TdsJournalActorUnknown(self)
        return self._snapshot

    def _remaining(self, deadline: float) -> float:
        return _remaining_seconds(deadline, self._clock)

    def _shutdown(self) -> None:
        self._stop.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass

    def _await(self, command: _ActorCommand[Snapshot]) -> Snapshot:
        try:
            if not command.done.wait(self._remaining(command.deadline)):
                raise TdsJournalActorUnknown(self)
            if self._remaining(command.deadline) <= 0 or self._stop.is_set() or command.failed:
                raise TdsJournalActorUnknown(self)
            if type(command.snapshot) is not self._snapshot_type:
                raise TdsJournalActorUnknown(self)
            self._snapshot = command.snapshot
            return command.snapshot
        except BaseException:
            self._shutdown()
            raise

    def _await_ready(self) -> None:
        self._await(self._ready)

    def _call(self, command: _ActorCommand[Snapshot]) -> Snapshot:
        self._owned()
        if self._stop.is_set() or self._inflight:
            self._shutdown()
            raise TdsJournalActorUnknown(self)
        if self._remaining(command.deadline) <= 0:
            self._shutdown()
            raise TdsJournalActorUnknown(self)
        self._inflight = True
        try:
            try:
                self._queue.put_nowait(command)
            except queue.Full:
                self._shutdown()
                raise TdsJournalActorUnknown(self) from None
            return self._await(command)
        finally:
            self._inflight = False

    def _initial(self, backend: Backend) -> Snapshot:
        raise NotImplementedError

    def _dispatch(self, backend: Backend, command: _ActorCommand[Snapshot]) -> Snapshot:
        raise NotImplementedError

    def close(self, *, deadline: float) -> None:
        self._owned()
        self._shutdown()
        self._wait_stopped(deadline)

    def _settled(self) -> bool:
        return self._finished.is_set() and not self._thread.is_alive()

    def _wait_stopped(self, deadline: float) -> None:
        # Thread.join is unsafe under SIGINT on affected CPython versions: it
        # may mark a still-running thread stopped. Completion is actor-owned.
        if not self._finished.wait(self._remaining(deadline)):
            raise TdsJournalActorUnknown(self)
        pause = threading.Event()
        while not self._settled():
            remaining = self._remaining(deadline)
            if remaining <= 0:
                raise TdsJournalActorUnknown(self)
            pause.wait(min(remaining, 0.001))
        if self._teardown_failed:
            raise TdsJournalActorUnknown(self)

    @contextmanager
    def _context(self) -> Iterator[Backend]:
        context = self._factory()
        writer = context.__enter__()
        try:
            yield writer
        finally:
            try:
                context.__exit__(*sys.exc_info())
            except BaseException:
                self._teardown_failed = True
                raise

    def _eligible(self, command: _ActorCommand[Snapshot]) -> bool:
        return not self._stop.is_set() and self._remaining(command.deadline) > 0

    def _run(self) -> None:
        command = self._ready
        try:
            if not self._eligible(command):
                raise TdsJournalActorUnknown()
            with self._context() as writer:
                if not self._eligible(command):
                    raise TdsJournalActorUnknown()
                command.snapshot = self._initial(writer)
                command.done.set()
                while not self._stop.is_set():
                    next_command = self._queue.get()
                    if next_command is None:
                        return
                    command = next_command
                    if not self._eligible(command):
                        raise TdsJournalActorUnknown()
                    result = self._dispatch(writer, command)
                    if not self._eligible(command):
                        raise TdsJournalActorUnknown()
                    command.snapshot = result
                    command.done.set()
        except BaseException:
            command.failed = True
            command.done.set()
            self._stop.set()
        finally:
            self._finished.set()


ActorT = TypeVar("ActorT", bound=_ActorCore[Any, Any])


class TdsActorPool:
    """Injected run-scoped capacity, shared by that run's supervisor threads.

    Stalled initialization, writes and teardown all occupy capacity. A different
    fencing token does not bypass this local bound. close bounds waiting only;
    callers retain this pool while any actor is still alive.
    """

    def __init__(self, *, capacity: int, clock: Callable[[], float] = monotonic) -> None:
        if type(capacity) is not int or not 1 <= capacity <= 1024:
            raise ValueError("mssql_native.tds_journal_capacity_invalid")
        self._capacity, self._clock = capacity, clock
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._actors: list[_ActorCore[Any, Any]] = []
        self._closed = False

    def _owned_process(self) -> None:
        if self._pid != os.getpid():
            raise TdsJournalActorUnknown()

    def assert_deadline(self, *, deadline: float) -> None:
        """Require completion before the deadline using this pool's actor clock."""
        self._owned_process()
        if _remaining_seconds(deadline, self._clock) <= 0:
            raise TdsJournalActorUnknown()

    def deadline_after(self, timeout: float) -> float:
        """Create an absolute deadline in the exact clock domain used by actors."""
        self._owned_process()
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("mssql_native.tds_journal_timeout_invalid")
        now = self._clock()
        if not math.isfinite(now):
            raise ValueError("mssql_native.tds_journal_clock_invalid")
        deadline = now + float(timeout)
        if not math.isfinite(deadline):
            raise ValueError("mssql_native.tds_journal_deadline_invalid")
        return deadline

    @property
    def live_count(self) -> int:
        self._owned_process()
        with self._lock:
            return sum(not actor._settled() for actor in self._actors)

    def open(
        self,
        build: Callable[[float, Callable[[], float]], ActorT],
        *,
        deadline: float,
    ) -> ActorT:
        """Admit one trusted allocation-only actor constructor before starting it.

        Composition chooses the actor kind. The constructor must not perform
        backend I/O, start threads or reenter this pool. Only the admitted actor
        enters the injected backend context. Constructor execution is trusted
        bounded work; this API does not claim to interrupt arbitrary callbacks.
        """
        self._owned_process()
        with self._lock:
            self._actors = [item for item in self._actors if not item._settled()]
            if self._closed or len(self._actors) >= self._capacity:
                raise TdsJournalActorUnknown()
            self.assert_deadline(deadline=deadline)
            actor = build(deadline, self._clock)
            if not isinstance(actor, _ActorCore):
                raise TdsJournalActorUnknown()
            actor._owned()
            if (
                actor._clock is not self._clock
                or actor._ready.deadline != deadline
                or actor._admitted
                or actor._thread.ident is not None
                or actor._thread.is_alive()
            ):
                raise TdsJournalActorUnknown()
            self.assert_deadline(deadline=deadline)
            actor._admitted = True
            self._actors.append(actor)
            try:
                actor._thread.start()
            except BaseException:
                # Thread creation may already have happened. Keep its reservation
                # until actor-owned completion proves actual thread settlement.
                actor._shutdown()
                raise TdsJournalActorUnknown(actor) from None
        actor._await_ready()
        return actor

    def close(self, *, deadline: float) -> None:
        self._owned_process()
        with self._lock:
            self._closed = True
            actors = tuple(self._actors)
        for actor in actors:
            actor._shutdown()
        failed = False
        for actor in actors:
            try:
                actor._wait_stopped(deadline)
            except WindowOutcomeUnknown:
                failed = True
        if failed:
            raise TdsJournalActorUnknown()
