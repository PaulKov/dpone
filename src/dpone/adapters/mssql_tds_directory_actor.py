"""Closed directory gateway using the common deadline/teardown machinery."""

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import get_args

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_tds_directory import TdsDirectorySnapshot
from dpone.ports.mssql_tds_directory import (
    AssertDirectoryAuthority,
    AuthorizeDirectoryRetirement,
    CloseDirectoryAdmission,
    CreateDirectory,
    DirectoryInitialization,
    DirectoryObservation,
    DirectoryRequest,
    ReadDirectory,
    RecordDirectoryContainment,
    RecordDirectorySettlement,
    ReserveDirectoryOperation,
    ReserveDirectoryReconciliation,
    ResumeDirectory,
    SealDirectoryWork,
    TakeOverDirectory,
    TdsDirectoryStore,
    TdsDirectoryWriter,
)


class TdsDirectoryActor(_ActorCore[TdsDirectoryStore, DirectoryObservation]):
    """One explicit initialization mode; observation never upgrades to a writer."""

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[TdsDirectoryStore]],
        initialization: DirectoryInitialization,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        if type(initialization) not in get_args(DirectoryInitialization):
            raise ValueError("mssql_native.tds_directory_initialization_invalid")
        self._initialization = initialization
        self._writer: TdsDirectoryWriter | None = None
        super().__init__(factory, deadline, clock, DirectoryObservation)

    @property
    def observation(self) -> DirectoryObservation:
        return self.snapshot

    def _initial(self, backend: TdsDirectoryStore) -> DirectoryObservation:
        request = self._initialization
        if isinstance(request, ReadDirectory):
            return DirectoryObservation(backend.read(request.parent, request.limits))
        if isinstance(request, CreateDirectory):
            self._writer = backend.create(
                request.parent, request.limits, request.lease, supervisor_token=request.supervisor_token
            )
        elif isinstance(request, TakeOverDirectory):
            self._writer = backend.take_over(request.observed, request.lease, supervisor_token=request.supervisor_token)
        elif isinstance(request, ResumeDirectory):
            self._writer = backend.resume(request.claim, request.lease)
        else:
            raise TdsJournalActorUnknown()
        snapshot = self._writer.snapshot
        if type(snapshot) is not TdsDirectorySnapshot:
            raise TdsJournalActorUnknown()
        return DirectoryObservation(snapshot)

    def execute(self, request: DirectoryRequest, *, deadline: float) -> TdsDirectorySnapshot:
        self._owned()
        if type(request) not in get_args(DirectoryRequest) or isinstance(self._initialization, ReadDirectory):
            raise ValueError("mssql_native.tds_directory_command_invalid")
        observation = self._call(_ActorCommand("directory", deadline, request))
        if observation.snapshot is None:
            raise TdsJournalActorUnknown(self)
        return observation.snapshot

    def _dispatch(
        self, backend: TdsDirectoryStore, command: _ActorCommand[DirectoryObservation]
    ) -> DirectoryObservation:
        request, writer = command.event, self._writer
        if command.kind != "directory" or writer is None or type(request) not in get_args(DirectoryRequest):
            raise TdsJournalActorUnknown()
        if isinstance(request, AssertDirectoryAuthority):
            writer.assert_authority()
            result = writer.snapshot
        elif isinstance(request, ReserveDirectoryOperation):
            result = writer.reserve_operation(
                operation_id=request.operation_id, command=request.command, command_sha256=request.command_sha256
            )
        elif isinstance(request, ReserveDirectoryReconciliation):
            result = writer.reserve_reconciliation(
                operation_id=request.operation_id,
                command_sha256=request.command_sha256,
                reconciles_slot=request.reconciles_slot,
                containment=request.containment,
            )
        elif isinstance(request, RecordDirectoryContainment):
            result = writer.record_local_containment(request.index, request.proof)
        elif isinstance(request, RecordDirectorySettlement):
            result = writer.record_remote_settlement(request.index, request.proof)
        elif isinstance(request, SealDirectoryWork):
            result = writer.seal_work()
        elif isinstance(request, AuthorizeDirectoryRetirement):
            result = writer.authorize_retirement(request.authority)
        elif isinstance(request, CloseDirectoryAdmission):
            result = writer.close_admission()
        else:
            raise TdsJournalActorUnknown()
        if type(result) is not TdsDirectorySnapshot:
            raise TdsJournalActorUnknown()
        return DirectoryObservation(result)
