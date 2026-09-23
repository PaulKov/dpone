"""Closed coordinator journal actor using shared deadline/teardown mechanics.

No independent pool or timeout engine is created here. The run-scoped pool owns
admission and retains live actors after timeout. This actor validates returned
state against the requested pure transition, not physical or SQL truth.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import get_args

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_tds_api import TdsAttemptOwnership, TdsDirectoryLimits, WindowLease
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorEvent,
    TdsCoordinatorIdentity,
    TdsCoordinatorPhase,
    TdsCoordinatorSnapshot,
    TdsCoordinatorState,
    advance_coordinator_state,
    take_over_coordinator_state,
)
from dpone.ports.mssql_tds_coordinator import (
    AdvanceCoordinator,
    AssertCoordinatorAuthority,
    CoordinatorInitialization,
    CoordinatorObservation,
    CoordinatorRequest,
    CreateCoordinator,
    ReadCoordinator,
    TakeOverCoordinator,
    TdsCoordinatorStore,
    TdsCoordinatorWriter,
)


def _expected_initial(
    request: CoordinatorInitialization,
) -> tuple[TdsCoordinatorIdentity, TdsCoordinatorState | None, int | None]:
    if type(request) not in get_args(CoordinatorInitialization) or type(request.limits) is not TdsDirectoryLimits:
        raise ValueError("mssql_native.tds_coordinator_initialization_invalid")
    if isinstance(request, TakeOverCoordinator):
        if type(request.observed) is not TdsCoordinatorSnapshot:
            raise ValueError("mssql_native.tds_coordinator_initialization_invalid")
        identity = request.observed.state.identity
    else:
        identity = request.identity
    if type(identity) is not TdsCoordinatorIdentity:
        raise ValueError("mssql_native.tds_coordinator_initialization_invalid")
    if isinstance(request, ReadCoordinator):
        return identity, None, None
    if type(request.lease) is not WindowLease or request.lease.target_id != identity.parent.target_key:
        raise ValueError("mssql_native.tds_coordinator_initialization_invalid")
    owner = TdsAttemptOwnership(request.lease.owner, request.lease.fence, request.supervisor_token)
    if isinstance(request, CreateCoordinator):
        return identity, TdsCoordinatorState(identity, owner, owner, TdsCoordinatorPhase.INTENT, 0), None
    return identity, take_over_coordinator_state(request.observed.state, owner), request.observed.revision


class TdsCoordinatorActor(_ActorCore[TdsCoordinatorStore, CoordinatorObservation]):
    """One explicit read/create/takeover initialization, with no mode upgrades."""

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[TdsCoordinatorStore]],
        initialization: CoordinatorInitialization,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        self._identity, self._expected, self._prior_revision = _expected_initial(initialization)
        self._initialization = initialization
        self._writer: TdsCoordinatorWriter | None = None
        self._current: TdsCoordinatorSnapshot | None = None
        super().__init__(factory, deadline, clock, CoordinatorObservation)

    @property
    def observation(self) -> CoordinatorObservation:
        """Only the last supervisor-acknowledged immutable observation."""
        return self.snapshot

    def _checked(self, snapshot: TdsCoordinatorSnapshot) -> TdsCoordinatorSnapshot:
        if type(snapshot) is not TdsCoordinatorSnapshot or snapshot.state.identity != self._identity:
            raise TdsJournalActorUnknown()
        return snapshot

    def _initial(self, backend: TdsCoordinatorStore) -> CoordinatorObservation:
        request = self._initialization
        if isinstance(request, ReadCoordinator):
            snapshot = backend.read(request.identity)
            return CoordinatorObservation(None if snapshot is None else self._checked(snapshot))
        if isinstance(request, CreateCoordinator):
            self._writer = backend.create(
                request.identity, request.limits, request.lease, supervisor_token=request.supervisor_token
            )
        else:
            self._writer = backend.take_over(
                request.observed, request.limits, request.lease, supervisor_token=request.supervisor_token
            )
        snapshot = self._checked(self._writer.snapshot)
        if snapshot.state != self._expected or (
            self._prior_revision is not None and snapshot.revision <= self._prior_revision
        ):
            raise TdsJournalActorUnknown()
        self._current = snapshot
        return CoordinatorObservation(snapshot)

    def execute(self, request: CoordinatorRequest, *, deadline: float) -> TdsCoordinatorSnapshot:
        """Submit one closed request; read mode cannot assert or mutate authority."""
        self._owned()
        if type(request) not in get_args(CoordinatorRequest) or isinstance(self._initialization, ReadCoordinator):
            raise ValueError("mssql_native.tds_coordinator_command_invalid")
        if isinstance(request, AdvanceCoordinator) and (
            type(request.event) not in get_args(TdsCoordinatorEvent)
            or type(request.expected_phase) is not TdsCoordinatorPhase
        ):
            raise ValueError("mssql_native.tds_coordinator_command_invalid")
        observed = self._call(_ActorCommand("coordinator", deadline, request))
        if observed.snapshot is None:
            raise TdsJournalActorUnknown(self)
        return observed.snapshot

    def _dispatch(
        self, backend: TdsCoordinatorStore, command: _ActorCommand[CoordinatorObservation]
    ) -> CoordinatorObservation:
        request, writer, previous = command.event, self._writer, self._current
        if (
            command.kind != "coordinator"
            or writer is None
            or previous is None
            or type(request) not in get_args(CoordinatorRequest)
        ):
            raise TdsJournalActorUnknown()
        if isinstance(request, AssertCoordinatorAuthority):
            writer.assert_authority()
            snapshot = self._checked(writer.snapshot)
            if snapshot != previous:
                raise TdsJournalActorUnknown()
        elif isinstance(request, AdvanceCoordinator):
            expected = advance_coordinator_state(previous.state, request.event, expected_phase=request.expected_phase)
            snapshot = self._checked(writer.advance(request.event, expected_phase=request.expected_phase))
            if (
                snapshot.state != expected
                or (expected == previous.state and snapshot != previous)
                or (expected != previous.state and snapshot.revision <= previous.revision)
            ):
                raise TdsJournalActorUnknown()
        else:
            raise TdsJournalActorUnknown()
        self._current = snapshot
        return CoordinatorObservation(snapshot)
