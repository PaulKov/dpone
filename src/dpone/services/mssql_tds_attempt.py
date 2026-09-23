from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from threading import current_thread
from typing import Any
from uuid import UUID

import dpone.contracts.mssql_tds_directory as directory_contract
import dpone.ports.mssql_tds_directory as directory_ports
import dpone.services.mssql_tds_attempt_departure_authority as departure_authority
from dpone.contracts.mssql_tds_suspension import TdsAttemptSuspension
from dpone.contracts.mssql_tds_worker import ParentAuthority
from dpone.ports.mssql_tds_directory import (
    AssertDirectoryAuthority,
    DirectoryObservation,
    ReserveDirectoryOperation,
    TdsDirectoryGateway,
)
from dpone.ports.mssql_tds_journal import TdsJournalGateway
from dpone.services.mssql_tds_attempt_retirement import (
    AttemptDepartureViewMixin,
    ShutdownCapability,
    TdsAttemptUnknown,
    close_tds_attempt_admission,
    complete_tds_attempt_retirement,
    contain_tds_verified_parent,
    reserve_tds_attempt_retirement,
    seal_tds_attempt_work,
    suspend_tds_attempt,
)
from dpone.services.mssql_tds_attempt_retirement import (
    assert_local_owner as _local,
)
from dpone.services.mssql_tds_attempt_retirement import (
    assert_retirement_descendant as _retirement_descendant,  # noqa: F401 - private compatibility export
)
from dpone.services.mssql_tds_attempt_retirement import (
    same_state_tree as _same_tree,
)
from dpone.services.mssql_tds_attempt_retirement import (
    validate_deadline as _deadline,
)
from dpone.services.mssql_tds_observe_settlement import ObserveSettlement
from dpone.services.mssql_tds_original_continuation import CreateSettlement, PreparationTransition
from dpone.services.mssql_tds_permission_grant_association import PermissionGrantAssociation
from dpone.services.mssql_tds_writer_contracts import (
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsCoordinatorCommand,
    TdsCoordinatorIdentity,
    TdsDirectorySnapshot,
    WindowContractError,
    reserve_operation,
    validate_original_record,
    validate_prepared_grant_binding,
    validate_prepared_verify_binding,
    validate_reservation_binding,
)

_ShutdownCapability = ShutdownCapability

_departure_binding = validate_reservation_binding


class TdsAttempt(AttemptDepartureViewMixin):
    def __init__(
        self,
        lifecycle: TdsJournalGateway,
        directory: TdsDirectoryGateway,
        *,
        _fresh_creation: bool = False,
        _composition_origin: object | None = None,
    ) -> None:
        if type(_fresh_creation) is not bool:
            raise ValueError("mssql_native.tds_attempt_fresh_creation_invalid")
        self._lifecycle, self._directory = lifecycle, directory
        self._pid, self._thread = os.getpid(), current_thread()
        self._busy = self._poisoned = self._closed = self._departure_asserting = False
        self._preparation: PreparationTransition | None = None
        self._prepared_origin: PreparationTransition | None = None
        self._create_settlement: CreateSettlement | None = None
        self._observe_settlement: ObserveSettlement | None = None
        self._observe_origin: object | None = None
        self._permission_grant_owner: PermissionGrantAssociation | None = None
        self._fresh_creation = _fresh_creation
        self._composition_origin = _composition_origin
        self._departure_helper_id = self._observe_helper_id = None  # type: UUID | None
        self._departure_baseline: (
            tuple[TdsAttemptSnapshot, TdsDirectorySnapshot, TdsCoordinatorIdentity, float] | None
        ) = None

    def _prepared_observe_settlement(self, helper_id: UUID, *, deadline: float) -> ObserveSettlement:
        self._owned()
        self._require_unpoisoned()
        if self._prepared_origin is None:
            raise WindowContractError("mssql_native.tds_attempt_prepared_origin_missing")
        return ObserveSettlement(self, self._prepared_origin, helper_id, deadline=deadline)

    def _assert_composition_origin(self, origin: object) -> None:
        self._owned()
        self._require_unpoisoned()
        if self._composition_origin is None or origin is not self._composition_origin:
            raise WindowContractError("mssql_native.tds_attempt_composition_origin_mismatch")

    @contextmanager
    def _create_departure_sequence(
        self, helper_id: UUID, create_identity: TdsCoordinatorIdentity, *, deadline: float
    ) -> Iterator[tuple[TdsAttemptSnapshot, TdsDirectorySnapshot, TdsAttemptUnknown]]:
        yield from departure_authority.create_departure_sequence(
            self,
            helper_id,
            create_identity,
            deadline=deadline,
            contract_error=WindowContractError,
            unknown_outcome=TdsAttemptUnknown,
            departure_binding=lambda *args: _departure_binding(*args),
        )

    def _assert_create_departure(
        self, helper_id: UUID, *, deadline: float
    ) -> tuple[TdsAttemptSnapshot, TdsDirectorySnapshot]:
        return departure_authority.assert_create_departure(
            self,
            helper_id,
            deadline=deadline,
            contract_error=WindowContractError,
            directory_authority=AssertDirectoryAuthority,
            local=lambda pid, thread: _local(pid, thread),
            departure_binding=lambda *args: _departure_binding(*args),
        )

    def _begin_observe(self, helper_id: UUID, identity: TdsCoordinatorIdentity, *, deadline: float) -> None:
        self._owned()
        self._require_unpoisoned()
        if self._permission_grant_owner is not None:
            raise WindowContractError("mssql_native.tds_attempt_permission_grant_active")
        if self._observe_helper_id is not None or self._departure_baseline is not None:
            raise WindowContractError("mssql_native.tds_attempt_observe_active")
        if type(helper_id) is not UUID or type(helper_id.int) is not int or not 0 < helper_id.int < 2**128:
            raise ValueError("mssql_native.tds_attempt_observe_invalid")
        _deadline(deadline)
        parent, directory = self.lifecycle, self.directory
        _departure_binding(parent, directory, identity, TdsCoordinatorCommand.OBSERVE)
        self._observe_helper_id = helper_id
        self._departure_baseline = (parent, directory, identity, deadline)
        self._assert_observe(helper_id, deadline=deadline)

    def _assert_observe(self, helper_id: UUID, *, deadline: float) -> tuple[TdsAttemptSnapshot, TdsDirectorySnapshot]:
        if self._observe_helper_id is None:
            raise WindowContractError("mssql_native.tds_attempt_observe_inactive")
        return self._assert_create_departure(helper_id, deadline=deadline)

    @contextmanager
    def _observe_sequence(
        self, helper_id: UUID, identity: TdsCoordinatorIdentity, *, deadline: float
    ) -> Iterator[None]:
        self._owned()
        self._require_unpoisoned()
        self._busy = True
        try:
            if self._departure_baseline is None or self._departure_baseline[2] != identity:
                raise WindowContractError("mssql_native.tds_attempt_observe_binding")
            self._assert_observe(helper_id, deadline=deadline)
            yield
            self._assert_observe(helper_id, deadline=deadline)
        except BaseException:
            self._poisoned = True
            raise
        finally:
            self._busy = False

    def _end_observe(self, helper_id: UUID) -> None:
        self._owned()
        if helper_id != self._observe_helper_id:
            raise WindowContractError("mssql_native.tds_attempt_observe_binding")
        self._observe_helper_id = None
        self._departure_baseline = None

    def _without_observe(self) -> None:
        if self._observe_helper_id is not None or self._preparation is not None:
            self._poisoned = True
            raise WindowContractError("mssql_native.tds_attempt_observe_active")

    def _owned(self) -> None:
        _local(self._pid, self._thread)
        if self._busy:
            self._poisoned = True
            raise WindowContractError("mssql_native.tds_attempt_reentrant")

    def _require_unpoisoned(self) -> None:
        if self._poisoned or self._closed:
            raise TdsAttemptUnknown((self._lifecycle, self._directory))

    @property
    def lifecycle(self) -> TdsAttemptSnapshot:
        self._owned()
        return self._lifecycle.snapshot

    @property
    def directory(self) -> TdsDirectorySnapshot:
        self._owned()
        snapshot = self._directory.observation.snapshot
        if snapshot is None:
            raise TdsAttemptUnknown((self._lifecycle, self._directory))
        return snapshot

    @contextmanager
    def _sequence(self, deadline: float) -> Iterator[None]:
        self._owned()
        if self._permission_grant_owner is not None:
            raise WindowContractError("mssql_native.tds_attempt_permission_grant_active")
        self._without_observe()
        if self._observe_settlement is not None and self._observe_settlement.complete is not True:
            self._poisoned = True
            raise WindowContractError("mssql_native.tds_attempt_observe_settlement_pending")
        _deadline(deadline)
        self._require_unpoisoned()
        self._busy = True
        try:
            self._lifecycle.assert_authority(deadline=deadline)
            self._require_unpoisoned()
            self._directory.execute(AssertDirectoryAuthority(), deadline=deadline)
            self._require_unpoisoned()
            yield
            self._require_unpoisoned()
        except BaseException:
            # Even rejected effects may invalidate an actor's writer authority.
            self._poisoned = True
            raise
        finally:
            self._busy = False

    def reserve_operation(
        self, operation_id: UUID, command: TdsCoordinatorCommand, command_sha256: str, *, deadline: float
    ) -> TdsDirectorySnapshot:
        if type(command) is not TdsCoordinatorCommand or command in (
            TdsCoordinatorCommand.GRANT,
            TdsCoordinatorCommand.RETIRE,
            TdsCoordinatorCommand.RECONCILE,
        ):
            raise ValueError("mssql_native.tds_attempt_ordinary_command_required")
        with self._sequence(deadline):
            return self._directory.execute(
                ReserveDirectoryOperation(operation_id, command, command_sha256), deadline=deadline
            )

    def _permission_grant(
        self, operation_id: UUID, command_sha256: str, implementation_sha256: str, *, deadline: float
    ) -> PermissionGrantAssociation:
        self._owned()
        self._require_unpoisoned()
        if self._permission_grant_owner is not None:
            raise WindowContractError("mssql_native.tds_attempt_permission_grant_active")
        owner = PermissionGrantAssociation(
            self,
            operation_id,
            (command_sha256, implementation_sha256),
            deadline,
            (
                TdsCoordinatorCommand.GRANT,
                TdsAttemptPhase.PREPARED,
                TdsCoordinatorIdentity,
                reserve_operation,
                ReserveDirectoryOperation,
                validate_original_record,
                validate_prepared_grant_binding,
                WindowContractError,
                validate_prepared_verify_binding,
                (directory_contract, directory_ports),
            ),
        )
        origin, settlement = self._prepared_origin, self._observe_settlement
        if type(origin) is not PreparationTransition or type(settlement) is not ObserveSettlement:
            raise WindowContractError("mssql_native.tds_permission_grant_origin_missing")
        expected = origin.expected
        current_parent = vars(self._lifecycle).get("_snapshot")
        observation = vars(self._directory).get("_snapshot")
        current_directory = observation.snapshot if type(observation) is DirectoryObservation else None
        identity, settled = origin.identity, (settlement.directory, settlement.local_ack, settlement.remote_ack)
        flags = (origin.failed, origin.cleaned, settlement.complete, settlement.pending, settlement.failed)
        if (
            type(expected) is not TdsAttemptSnapshot
            or type(current_parent) is not TdsAttemptSnapshot
            or type(current_directory) is not TdsDirectorySnapshot
            or type(identity) is not TdsCoordinatorIdentity
            or any(type(value) is not TdsDirectorySnapshot for value in settled)
            or any(type(value) is not bool for value in flags)
            or any(type(value) is not float for value in (origin.deadline, settlement.deadline))
            or type(identity.implementation_sha256) is not str
        ):
            raise WindowContractError("mssql_native.tds_permission_grant_prerequisite_invalid")
        if (
            not _same_tree(current_parent, expected)
            or not _same_tree(current_directory, settled[0])
            or not _same_tree(current_directory, settled[2])
            or flags != (False, True, True, False, False)
            or len(identity.implementation_sha256) != 64
            or any(c not in "0123456789abcdef" for c in identity.implementation_sha256)
            or expected.state.phase is not TdsAttemptPhase.PREPARED
            or implementation_sha256 != identity.implementation_sha256
            or not deadline <= min(origin.deadline, settlement.deadline)
        ):
            raise WindowContractError("mssql_native.tds_permission_grant_prerequisite_invalid")
        self._permission_grant_owner = owner
        owner._register()
        return owner

    def _permission_grant_call(self, owner: object, request: Any, pool: Any, *, deadline: float) -> object:
        self._owned()
        if owner is not self._permission_grant_owner:
            raise WindowContractError("mssql_native.tds_attempt_permission_grant_binding")
        _deadline(deadline)
        self._require_unpoisoned()
        self._busy = True
        try:
            self._lifecycle.assert_authority(deadline=deadline)
            observed = self._directory.execute(
                AssertDirectoryAuthority() if request is None else request, deadline=deadline
            )
            if pool is not None:
                pool.assert_deadline(deadline=deadline)
            return (self._lifecycle.snapshot, observed) if request is None else observed
        finally:
            self._busy = False

    def _permission_grant_unknown(self) -> BaseException:
        self._poisoned = True
        return TdsAttemptUnknown((self._lifecycle, self._directory))

    def seal_work(self, *, deadline: float) -> TdsDirectorySnapshot:
        return seal_tds_attempt_work(self, deadline=deadline)

    def reserve_retirement(self, operation_id: UUID, command_sha256: str, *, deadline: float) -> TdsDirectorySnapshot:
        return reserve_tds_attempt_retirement(self, operation_id, command_sha256, deadline=deadline)

    def contain_verified_parent(
        self, authority: ParentAuthority, proof_sha256: str, *, deadline: float
    ) -> TdsAttemptSnapshot:
        return contain_tds_verified_parent(self, authority, proof_sha256, deadline=deadline)

    def complete_retirement(
        self,
        operation_id: UUID,
        process_absence_sha256: str,
        object_absence_sha256: str,
        settlement_authority_sha256: str,
        remote_settlement_sha256: str,
        retired_sha256: str,
        *,
        deadline: float,
    ) -> tuple[TdsAttemptSnapshot, TdsDirectorySnapshot]:
        return complete_tds_attempt_retirement(
            self,
            operation_id,
            process_absence_sha256,
            object_absence_sha256,
            settlement_authority_sha256,
            remote_settlement_sha256,
            retired_sha256,
            deadline=deadline,
        )

    def close_admission(self, *, deadline: float) -> TdsDirectorySnapshot:
        return close_tds_attempt_admission(self, deadline=deadline)

    def suspend(self, *, deadline: float) -> TdsAttemptSuspension:
        return suspend_tds_attempt(self, deadline=deadline)

    def close(self, *, deadline: float) -> None:
        self._owned()
        self._without_observe()
        _deadline(deadline)
        self._closed = True
        self._busy = True
        try:
            TdsAttemptUnknown((self._lifecycle, self._directory)).close(deadline=deadline)
        finally:
            self._busy = False
