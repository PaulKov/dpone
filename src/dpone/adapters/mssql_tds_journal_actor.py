"""Lifecycle and observation actors over shared deadline mechanics."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import get_args

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_tds_api import (
    TdsAttemptIdentity,
    TdsAttemptObservation,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsLifecycleEvent,
)
from dpone.ports.mssql_tds_journal import TdsAttemptObserver, TdsAttemptWriter


class TdsJournalActor(_ActorCore[TdsAttemptWriter, TdsAttemptSnapshot]):
    """Unchanged lifecycle gateway backed by the shared bounded actor mechanics."""

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[TdsAttemptWriter]],
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        super().__init__(factory, deadline, clock, TdsAttemptSnapshot)

    def _initial(self, backend: TdsAttemptWriter) -> TdsAttemptSnapshot:
        return backend.snapshot

    def _dispatch(self, backend: TdsAttemptWriter, command: _ActorCommand[TdsAttemptSnapshot]) -> TdsAttemptSnapshot:
        if command.kind == "assert":
            backend.assert_authority()
            return backend.snapshot
        if (
            command.kind == "advance"
            and type(command.event) in get_args(TdsLifecycleEvent)
            and type(command.phase) is TdsAttemptPhase
        ):
            # The closed runtime checks above narrow this private command envelope.
            from typing import cast

            return backend.advance(cast(TdsLifecycleEvent, command.event), expected_phase=command.phase)
        raise TdsJournalActorUnknown()

    def assert_authority(self, *, deadline: float) -> None:
        self._call(_ActorCommand("assert", deadline))

    def advance(
        self, event: TdsLifecycleEvent, *, expected_phase: TdsAttemptPhase, deadline: float
    ) -> TdsAttemptSnapshot:
        if type(event) not in get_args(TdsLifecycleEvent) or type(expected_phase) is not TdsAttemptPhase:
            raise ValueError("mssql_native.tds_journal_command_invalid")
        return self._call(_ActorCommand("advance", deadline, event, expected_phase))


class TdsAttemptObserverActor(_ActorCore[TdsAttemptObserver, TdsAttemptObservation]):
    """One exact lifecycle read on the actor thread, with no writer operations.

    Initialization reads once. Reading the local observation again neither calls
    the backend nor acquires authority; later recovery must CAS the exact saved
    snapshot. A failed read never becomes acknowledged absence.
    """

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[TdsAttemptObserver]],
        identity: TdsAttemptIdentity,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        if type(identity) is not TdsAttemptIdentity:
            raise ValueError("mssql_native.tds_observer_identity_invalid")
        self._identity = identity
        super().__init__(factory, deadline, clock, TdsAttemptObservation)

    @property
    def observation(self) -> TdsAttemptObservation:
        return self.snapshot

    def _initial(self, backend: TdsAttemptObserver) -> TdsAttemptObservation:
        observation = TdsAttemptObservation(backend.read(self._identity))
        if observation.snapshot is not None and observation.snapshot.state.identity != self._identity:
            raise TdsJournalActorUnknown()
        return observation

    def _dispatch(
        self, backend: TdsAttemptObserver, command: _ActorCommand[TdsAttemptObservation]
    ) -> TdsAttemptObservation:
        raise TdsJournalActorUnknown()
