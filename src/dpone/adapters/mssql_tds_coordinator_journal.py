"""Actor-owned coordinator persistence with exact acknowledgements and no SQL.

Directory admission and this single-key CAS are not a cross-record transaction.
Composition must serialize them; this synchronous primitive belongs on a bounded
actor, never a deadline-sensitive process supervisor. Missing child state does
not prove no launch, session or command, and cannot be recreated during recovery.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from threading import current_thread

from dpone.contracts.mssql_tds_api import (
    MAX_COORDINATOR_RECORD_BYTES,
    TdsAttemptOwnership,
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowRecord,
    decode_coordinator_state,
    encode_coordinator_state,
    window_record_ack_matches,
)
from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorFailed,
    CoordinatorLocalObserved,
    CoordinatorRemoteObserved,
    TdsCoordinatorEvent,
    TdsCoordinatorIdentity,
    TdsCoordinatorPhase,
    TdsCoordinatorSnapshot,
    TdsCoordinatorState,
    advance_coordinator_state,
    coordinator_key,
    initial_coordinator_state,
    take_over_coordinator_state,
)
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits, TdsDirectorySnapshot
from dpone.ports.bounded_window import WindowStore
from dpone.ports.mssql_tds_directory import TdsDirectoryObserver


def _owner(identity: TdsCoordinatorIdentity, lease: WindowLease, token: str) -> TdsAttemptOwnership:
    if type(identity) is not TdsCoordinatorIdentity or type(lease) is not WindowLease:
        raise WindowContractError("mssql_native.tds_coordinator_owner_invalid")
    if lease.target_id != identity.parent.target_key:
        raise WindowContractError("mssql_native.tds_coordinator_lease_mismatch")
    try:
        return TdsAttemptOwnership(lease.owner, lease.fence, token)
    except ValueError:
        raise WindowContractError("mssql_native.tds_coordinator_owner_invalid") from None


def _assert_lease(store: WindowStore, lease: WindowLease) -> None:
    try:
        store.assert_lease(lease)
    except WindowContractError:
        raise
    except Exception:
        raise WindowOutcomeUnknown("mssql_native.tds_coordinator_lease_unknown") from None


def _persist(
    store: WindowStore, state: TdsCoordinatorState, revision: int | None, payload: str, lease: WindowLease
) -> TdsCoordinatorSnapshot:
    try:
        record = store.save(coordinator_key(state.identity), revision, payload, lease)
    except WindowContractError:
        raise
    except Exception:
        raise WindowOutcomeUnknown("mssql_native.tds_coordinator_write_unknown") from None
    if not window_record_ack_matches(record, revision=revision, payload=payload):
        raise WindowOutcomeUnknown("mssql_native.tds_coordinator_ack_unknown")
    return TdsCoordinatorSnapshot(state, record.revision)


class TdsCoordinatorJournal:
    """Observe, create once from an original reservation, or take over exact state."""

    def __init__(self, store: WindowStore, directories: TdsDirectoryObserver) -> None:
        self._store, self._directories = store, directories

    def read(self, identity: TdsCoordinatorIdentity) -> TdsCoordinatorSnapshot | None:
        """Acknowledge a bounded exact observation; absence grants no launch proof."""
        try:
            record = self._store.load(coordinator_key(identity))
        except WindowContractError:
            raise
        except Exception:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_read_unknown") from None
        if record is None:
            return None
        try:
            if (
                type(record) is not WindowRecord
                or type(record.revision) is not int
                or not 1 <= record.revision <= 2**63 - 1
                or type(record.payload) is not str
                or len(record.payload) > MAX_COORDINATOR_RECORD_BYTES
            ):
                raise ValueError
            state = decode_coordinator_state(record.payload.encode("utf-8"), identity=identity)
            return TdsCoordinatorSnapshot(state, record.revision)
        except (ValueError, UnicodeError, RecursionError):
            raise WindowContractError("mssql_native.tds_coordinator_record_invalid") from None

    def _directory(
        self, identity: TdsCoordinatorIdentity, limits: TdsDirectoryLimits, owner: TdsAttemptOwnership
    ) -> TdsDirectorySnapshot:
        try:
            observed = self._directories.read(identity.parent, limits)
        except WindowContractError:
            raise
        except Exception:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_directory_unknown") from None
        if (
            type(observed) is not TdsDirectorySnapshot
            or observed.state.parent != identity.parent
            or observed.state.limits != limits
            or observed.ownership != owner
            or identity.slot_index >= len(observed.state.slots)
        ):
            raise WindowContractError("mssql_native.tds_coordinator_directory_mismatch")
        slot = observed.state.slots[identity.slot_index]
        if (slot.operation_id, slot.command, slot.command_sha256, slot.owner_fence) != (
            identity.operation_id,
            identity.command,
            identity.command_sha256,
            identity.original_fence,
        ):
            raise WindowContractError("mssql_native.tds_coordinator_reservation_mismatch")
        return observed

    def create(
        self, identity: TdsCoordinatorIdentity, limits: TdsDirectoryLimits, lease: WindowLease, *, supervisor_token: str
    ) -> _CoordinatorWriter:
        """Persist original intent only; an absent child after takeover is a gap."""
        owner = _owner(identity, lease, supervisor_token)
        _assert_lease(self._store, lease)
        directory = self._directory(identity, limits, owner)
        try:
            state = initial_coordinator_state(identity, directory.state, owner)
        except ValueError:
            raise WindowContractError("mssql_native.tds_coordinator_creation_rejected") from None
        payload = encode_coordinator_state(state).decode("utf-8")
        snapshot = _persist(self._store, state, None, payload, lease)
        return _CoordinatorWriter(self, lease, limits, snapshot)

    def take_over(
        self, observed: TdsCoordinatorSnapshot, limits: TdsDirectoryLimits, lease: WindowLease, *, supervisor_token: str
    ) -> _CoordinatorWriter:
        """CAS exact observed state; the recovered writer cannot resume execution."""
        if type(observed) is not TdsCoordinatorSnapshot:
            raise WindowContractError("mssql_native.tds_coordinator_observation_required")
        owner = _owner(observed.state.identity, lease, supervisor_token)
        try:
            state = take_over_coordinator_state(observed.state, owner)
        except ValueError:
            raise WindowContractError("mssql_native.tds_coordinator_takeover_rejected") from None
        _assert_lease(self._store, lease)
        self._directory(state.identity, limits, owner)
        if self.read(state.identity) != observed:
            raise WindowContractError("mssql_native.tds_coordinator_stale_takeover")
        payload = encode_coordinator_state(state).decode("utf-8")
        snapshot = _persist(self._store, state, observed.revision, payload, lease)
        return _CoordinatorWriter(self, lease, limits, snapshot)


class _CoordinatorWriter:
    """Single non-reentrant owner; uncertain calls preserve only last acknowledgement."""

    def __init__(
        self,
        journal: TdsCoordinatorJournal,
        lease: WindowLease,
        limits: TdsDirectoryLimits,
        snapshot: TdsCoordinatorSnapshot,
    ) -> None:
        self._journal, self._lease, self._limits, self._snapshot = journal, lease, limits, snapshot
        self._pid, self._thread = os.getpid(), current_thread()
        self._busy = self._poisoned = False

    def _local(self) -> None:
        if self._pid != os.getpid() or self._thread is not current_thread():
            raise WindowContractError("mssql_native.tds_coordinator_owner_mismatch")

    @property
    def snapshot(self) -> TdsCoordinatorSnapshot:
        self._local()
        return self._snapshot

    @contextmanager
    def _operation(self) -> Iterator[None]:
        self._local()
        if self._poisoned:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_writer_poisoned")
        if self._busy:
            raise WindowContractError("mssql_native.tds_coordinator_reentrant")
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    def _assert_current(self) -> TdsDirectorySnapshot:
        try:
            _assert_lease(self._journal._store, self._lease)
            state = self._snapshot.state
            if self._journal.read(state.identity) != self._snapshot:
                raise WindowContractError("mssql_native.tds_coordinator_revision_changed")
            return self._journal._directory(state.identity, self._limits, state.ownership)
        except BaseException:
            self._poisoned = True
            raise

    def assert_authority(self) -> None:
        """Check current journal/lease/directory bindings, never SQL exclusion."""
        with self._operation():
            self._assert_current()

    def advance(self, event: TdsCoordinatorEvent, *, expected_phase: TdsCoordinatorPhase) -> TdsCoordinatorSnapshot:
        """Commit one closed event; bad pure inputs cause no storage calls."""
        with self._operation():
            try:
                state = advance_coordinator_state(self._snapshot.state, event, expected_phase=expected_phase)
                payload = encode_coordinator_state(state).decode("utf-8")
            except ValueError:
                raise WindowContractError("mssql_native.tds_coordinator_transition_invalid") from None
            directory = self._assert_current()
            if type(event) not in (CoordinatorFailed, CoordinatorLocalObserved, CoordinatorRemoteObserved):
                try:
                    initial_coordinator_state(state.identity, directory.state, state.execution_owner)
                except ValueError:
                    self._poisoned = True
                    raise WindowContractError("mssql_native.tds_coordinator_reservation_changed") from None
            if state == self._snapshot.state:
                return self._snapshot
            self._poisoned = True
            snapshot = _persist(self._journal._store, state, self._snapshot.revision, payload, self._lease)
            self._snapshot = snapshot
            self._poisoned = False
            return snapshot
