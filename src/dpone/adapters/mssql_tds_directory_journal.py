"""Fenced coordinator directory persistence, without SQL mutation authority.

The composition root serializes parent lifecycle and directory creation. The
WindowStore provides single-key CAS, not a transaction across those records.
Reading never acquires a writer; uncertain writes never reload and retry.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from threading import RLock, current_thread
from uuid import UUID

from dpone.contracts.mssql_tds_api import (
    DIRECTORY_OWNERSHIP_ENVELOPE_BYTES,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsAttemptState,
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowRecord,
    decode_directory_record,
    encode_directory_record,
    window_record_ack_matches,
)
from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand,
    TdsCoordinatorDirectory,
    TdsDirectoryLimits,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    authorize_retirement,
    close_admission,
    directory_key,
    initial_directory,
    record_local_containment,
    record_remote_settlement,
    reserve_operation,
    reserve_reconciliation,
    seal_work,
)
from dpone.ports.bounded_window import WindowStore
from dpone.ports.mssql_tds_journal import TdsAttemptObserver
from dpone.ports.mssql_tds_suspension import TdsAttemptResumeClaim


def _owner(parent: TdsAttemptIdentity, lease: WindowLease, supervisor_token: str) -> TdsAttemptOwnership:
    if lease.target_id != parent.target_key:
        raise WindowContractError("mssql_native.tds_directory_lease_target_mismatch")
    try:
        return TdsAttemptOwnership(lease.owner, lease.fence, supervisor_token)
    except ValueError:
        raise WindowContractError("mssql_native.tds_directory_owner_invalid") from None


def _persist(
    store: WindowStore,
    state: TdsCoordinatorDirectory,
    owner: TdsAttemptOwnership,
    revision: int | None,
    lease: WindowLease,
) -> TdsDirectorySnapshot:
    payload = encode_directory_record(state, owner).decode("utf-8")
    try:
        record = store.save(directory_key(state.parent), revision, payload, lease)
    except WindowContractError:
        raise
    except Exception:
        raise WindowOutcomeUnknown("mssql_native.tds_directory_write_unknown") from None
    if not window_record_ack_matches(record, revision=revision, payload=payload):
        raise WindowOutcomeUnknown("mssql_native.tds_directory_ack_unknown")
    return TdsDirectorySnapshot(state, owner, record.revision)


class TdsCoordinatorDirectoryJournal:
    """Create once or take over an exact snapshot under a strictly newer lease.

    Composition injects a fresh durable parent observer bound to this store
    context and actor thread. No caller snapshot replaces those reads.
    """

    def __init__(self, store: WindowStore, *, parent_observer: TdsAttemptObserver) -> None:
        self._store = store
        self._parent_observer = parent_observer

    def read(self, parent: TdsAttemptIdentity, limits: TdsDirectoryLimits) -> TdsDirectorySnapshot | None:
        try:
            record = self._store.load(directory_key(parent))
        except WindowContractError:
            raise
        except Exception:
            raise WindowOutcomeUnknown("mssql_native.tds_directory_read_unknown") from None
        if record is None:
            return None
        try:
            if type(record) is not WindowRecord or type(record.payload) is not str:
                raise ValueError("record_type")
            # Bound characters before allocating UTF-8; codec then bounds bytes.
            if len(record.payload) > limits.max_encoded_bytes + DIRECTORY_OWNERSHIP_ENVELOPE_BYTES:
                raise ValueError("record_size")
            return decode_directory_record(
                record.payload.encode("utf-8"), revision=record.revision, parent=parent, limits=limits
            )
        except (ValueError, TypeError, AttributeError, RecursionError):
            raise WindowContractError("mssql_native.tds_directory_record_invalid") from None

    def create(
        self, parent: TdsAttemptIdentity, limits: TdsDirectoryLimits, lease: WindowLease, *, supervisor_token: str
    ) -> _DirectoryWriter:
        owner = _owner(parent, lease, supervisor_token)
        observed = self._parent_observer.read(parent)
        if (
            observed is None
            or observed.state.identity != parent
            or observed.state.phase is not TdsAttemptPhase.CREATION_INTENT
            or observed.state.ownership != owner
        ):
            raise WindowContractError("mssql_native.tds_directory_parent_creation_required")
        state = initial_directory(parent, limits, schema_version=observed.state.schema_version)
        snapshot = _persist(self._store, state, owner, None, lease)
        return _DirectoryWriter(self._store, self._parent_observer, lease, snapshot, recovering=False)

    def take_over(
        self, observed: TdsDirectorySnapshot, lease: WindowLease, *, supervisor_token: str
    ) -> _DirectoryWriter:
        owner = _owner(observed.state.parent, lease, supervisor_token)
        if (
            owner.fence <= observed.ownership.fence
            or owner.supervisor_id == observed.ownership.supervisor_id
            or self.read(observed.state.parent, observed.state.limits) != observed
        ):
            raise WindowContractError("mssql_native.tds_directory_takeover_rejected")
        snapshot = _persist(self._store, observed.state, owner, observed.revision, lease)
        return _DirectoryWriter(self._store, self._parent_observer, lease, snapshot, recovering=True)

    def resume(self, claim: TdsAttemptResumeClaim, lease: WindowLease) -> _DirectoryWriter:
        """Consume one process-local suspension claim without changing ownership."""
        if type(claim) is not TdsAttemptResumeClaim:
            raise WindowContractError("mssql_native.tds_directory_resume_rejected")
        try:
            observed = claim.directory(lease)
        except ValueError as error:
            raise WindowContractError("mssql_native.tds_directory_resume_rejected") from error
        owner = observed.ownership
        parent = self._parent_observer.read(observed.state.parent)
        if (
            (owner.owner, owner.fence) != (lease.owner, lease.fence)
            or parent is None
            or parent.state.ownership != owner
            or self.read(observed.state.parent, observed.state.limits) != observed
        ):
            raise WindowContractError("mssql_native.tds_directory_resume_rejected")
        self._store.assert_lease(lease)
        return _DirectoryWriter(self._store, self._parent_observer, lease, observed, recovering=True)


class _DirectoryWriter:
    """Thread-confined capability with a closed set of durable transitions.

    Proof inputs must come from trusted local/SQL observers. This writer checks
    their binding and persists them; it cannot establish their physical truth.
    """

    def __init__(
        self,
        store: WindowStore,
        parent_observer: TdsAttemptObserver,
        lease: WindowLease,
        snapshot: TdsDirectorySnapshot,
        *,
        recovering: bool,
    ):
        self._store, self._lease, self._snapshot = store, lease, snapshot
        self._parent_observer = parent_observer
        self._recovering = recovering
        self._pid, self._thread = os.getpid(), current_thread()
        self._lock, self._poisoned = RLock(), False

    @property
    def snapshot(self) -> TdsDirectorySnapshot:
        return self._snapshot

    def _local_authority(self) -> None:
        if os.getpid() != self._pid:
            raise WindowContractError("mssql_native.tds_directory_process_mismatch")
        if current_thread() is not self._thread:
            raise WindowContractError("mssql_native.tds_directory_thread_mismatch")
        if self._poisoned:
            raise WindowOutcomeUnknown("mssql_native.tds_directory_writer_poisoned")

    def assert_authority(self) -> None:
        self._local_authority()
        with self._lock:
            try:
                self._store.assert_lease(self._lease)
                state = self._snapshot.state
                if (
                    TdsCoordinatorDirectoryJournal(self._store, parent_observer=self._parent_observer).read(
                        state.parent, state.limits
                    )
                    != self._snapshot
                ):
                    raise WindowContractError("mssql_native.tds_directory_revision_changed")
            except BaseException as error:
                self._poisoned = True
                if isinstance(error, Exception) and not isinstance(error, WindowContractError):
                    raise WindowOutcomeUnknown("mssql_native.tds_directory_authority_unknown") from None
                raise

    def _change(self, transition: Callable[[TdsCoordinatorDirectory], TdsCoordinatorDirectory]) -> TdsDirectorySnapshot:
        self._local_authority()
        with self._lock:
            self.assert_authority()
            try:
                state = transition(self._snapshot.state)
            except ValueError:
                raise WindowContractError("mssql_native.tds_directory_transition_invalid") from None
            if state == self._snapshot.state:
                return self._snapshot
            self._poisoned = True
            snapshot = _persist(self._store, state, self._snapshot.ownership, self._snapshot.revision, self._lease)
            self._snapshot = snapshot
            self._poisoned = False
            return snapshot

    def reserve_operation(
        self, *, operation_id: UUID, command: TdsCoordinatorCommand, command_sha256: str
    ) -> TdsDirectorySnapshot:
        self._local_authority()
        if self._recovering and command is not TdsCoordinatorCommand.RETIRE:
            raise WindowContractError("mssql_native.tds_directory_recovery_requires_settlement")
        return self._change(
            lambda state: reserve_operation(
                state,
                operation_id=operation_id,
                command=command,
                command_sha256=command_sha256,
                owner_fence=self._lease.fence,
            )
        )

    def reserve_reconciliation(
        self, *, operation_id: UUID, command_sha256: str, reconciles_slot: int, containment: TdsLocalContainment
    ) -> TdsDirectorySnapshot:
        return self._change(
            lambda state: reserve_reconciliation(
                state,
                operation_id=operation_id,
                command_sha256=command_sha256,
                owner_fence=self._lease.fence,
                reconciles_slot=reconciles_slot,
                containment=containment,
            )
        )

    def record_local_containment(self, index: int, proof: TdsLocalContainment) -> TdsDirectorySnapshot:
        return self._change(lambda state: record_local_containment(state, index, proof))

    def record_remote_settlement(self, index: int, proof: TdsRemoteSettlement) -> TdsDirectorySnapshot:
        return self._change(lambda state: record_remote_settlement(state, index, proof))

    def seal_work(self) -> TdsDirectorySnapshot:
        return self._change(seal_work)

    def authorize_retirement(self, authority: TdsAttemptState) -> TdsDirectorySnapshot:
        def observed_transition(state: TdsCoordinatorDirectory) -> TdsCoordinatorDirectory:
            try:
                observed = self._parent_observer.read(state.parent)
            except BaseException:
                self._poisoned = True
                raise
            if observed is None or observed.state != authority:
                raise WindowContractError("mssql_native.tds_directory_parent_authority_changed")
            return authorize_retirement(state, authority)

        return self._change(observed_transition)

    def close_admission(self) -> TdsDirectorySnapshot:
        return self._change(close_admission)
