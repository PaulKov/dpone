"""Fenced per-attempt CAS authority, separate from the parent chunk journal.

Recovery reads do not reacquire a writer. Only a newer fencing epoch may
take over an observed record, and that writer is restricted to reconciliation
and retirement. A failed CAS poisons the live writer even when its commit
acknowledgement may have been lost; reload-and-retry is never implicit.
"""

from __future__ import annotations

import hashlib
import os
from threading import RLock, current_thread

from dpone.contracts.mssql_tds_api import (
    Contained,
    ContainmentRequired,
    Retired,
    RetirementRequired,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsLifecycleEvent,
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowRecord,
    advance_state,
    canonical_json_bytes,
    decode_state,
    encode_state,
    initial_state,
    replace_ownership,
    window_record_ack_matches,
)
from dpone.ports.bounded_window import WindowStore
from dpone.ports.mssql_tds_journal import ParentRetirementRequired, TdsAttemptWriter
from dpone.ports.mssql_tds_suspension import TdsAttemptResumeClaim


def _key(identity: TdsAttemptIdentity) -> str:
    # Policy/build/file changes must locate and reject the original attempt.
    value = [identity.target_key, identity.run_id, identity.ordinal, identity.attempt]
    return "mssql-tds-attempt-v1/" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _lease_matches(identity: TdsAttemptIdentity, lease: WindowLease) -> None:
    if lease.target_id != identity.target_key:
        raise WindowContractError("mssql_native.tds_lease_target_mismatch")


def _persist(store: WindowStore, key: str, revision: int | None, payload: str, lease: WindowLease) -> WindowRecord:
    """Preserve known CAS/fence rejections; ambiguous storage replies forbid retry."""
    try:
        record = store.save(key, revision, payload, lease)
    except WindowContractError:
        raise
    except Exception as error:
        raise WindowOutcomeUnknown("mssql_native.tds_storage_outcome_unknown") from error
    if not window_record_ack_matches(record, revision=revision, payload=payload):
        raise WindowOutcomeUnknown("mssql_native.tds_storage_ack_unknown")
    return record


class TdsAttemptJournal:
    """Create a writer once, observe freely, or take over under a newer fence."""

    def __init__(self, store: WindowStore, *, backend: str = "mssql_python") -> None:
        if type(backend) is not str or backend not in {"mssql_python", "mssql_sqlclient"}:
            raise ValueError("mssql_native.tds_backend_invalid")
        self._store = store
        self._backend = backend

    def read(self, identity: TdsAttemptIdentity) -> TdsAttemptSnapshot | None:
        try:
            record = self._store.load(_key(identity))
        except WindowContractError:
            raise
        except Exception as error:
            raise WindowOutcomeUnknown("mssql_native.tds_storage_observation_unknown") from error
        if record is None:
            return None
        try:
            if len(record.payload) > 16384:
                raise ValueError("oversized record")
            state = decode_state(record.payload.encode("utf-8"))
        except (ValueError, UnicodeError, RecursionError) as error:
            raise WindowContractError("mssql_native.tds_invalid_lifecycle_record") from error
        if state.identity != identity:
            raise WindowContractError("mssql_native.tds_identity_changed")
        return TdsAttemptSnapshot(state, record.revision)

    def create(self, identity: TdsAttemptIdentity, lease: WindowLease, *, supervisor_token: str) -> TdsAttemptWriter:
        """Commit creation intent before CREATE; existing state is not a new writer."""
        _lease_matches(identity, lease)
        owner = TdsAttemptOwnership(lease.owner, lease.fence, supervisor_token)
        state = initial_state(identity, owner, backend=self._backend)
        record = _persist(self._store, _key(identity), None, encode_state(state).decode("utf-8"), lease)
        return _AttemptWriter(self._store, lease, TdsAttemptSnapshot(state, record.revision), recovering=False)

    def take_over(self, observed: TdsAttemptSnapshot, lease: WindowLease, *, supervisor_token: str) -> TdsAttemptWriter:
        """CAS the exact observed state; a stale/fabricated snapshot cannot replace it."""
        _lease_matches(observed.state.identity, lease)
        if self.read(observed.state.identity) != observed:
            raise WindowContractError("mssql_native.tds_stale_takeover")
        owner = TdsAttemptOwnership(lease.owner, lease.fence, supervisor_token)
        try:
            state = replace_ownership(observed.state, owner)
        except ValueError as error:
            raise WindowContractError("mssql_native.tds_invalid_takeover") from error
        record = _persist(
            self._store, _key(state.identity), observed.revision, encode_state(state).decode("utf-8"), lease
        )
        return _AttemptWriter(self._store, lease, TdsAttemptSnapshot(state, record.revision), recovering=True)

    def resume(self, claim: TdsAttemptResumeClaim, lease: WindowLease) -> TdsAttemptWriter:
        """Consume one process-local suspension claim without changing its fence."""
        if type(claim) is not TdsAttemptResumeClaim:
            raise WindowContractError("mssql_native.tds_resume_rejected")
        try:
            observed = claim.lifecycle(lease)
        except ValueError as error:
            raise WindowContractError("mssql_native.tds_resume_rejected") from error
        _lease_matches(observed.state.identity, lease)
        owner = observed.state.ownership
        if (owner.owner, owner.fence) != (lease.owner, lease.fence) or self.read(observed.state.identity) != observed:
            raise WindowContractError("mssql_native.tds_resume_rejected")
        self._store.assert_lease(lease)
        return _AttemptWriter(self._store, lease, observed, recovering=True)


class _AttemptWriter:
    """Process-local, thread-confined authority that can never recover itself."""

    def __init__(
        self, store: WindowStore, lease: WindowLease, snapshot: TdsAttemptSnapshot, *, recovering: bool
    ) -> None:
        self._store, self._lease, self._snapshot = store, lease, snapshot
        self._recovering = recovering
        self._thread, self._lock, self._poisoned = current_thread(), RLock(), False
        self._pid = os.getpid()

    @property
    def snapshot(self) -> TdsAttemptSnapshot:
        return self._snapshot

    def _local_authority(self) -> None:
        if os.getpid() != self._pid:
            raise WindowContractError("mssql_native.tds_supervisor_process_mismatch")
        if current_thread() is not self._thread:
            raise WindowContractError("mssql_native.tds_supervisor_thread_mismatch")
        if self._poisoned:
            raise WindowOutcomeUnknown("mssql_native.tds_writer_poisoned")

    def assert_authority(self) -> None:
        # Reject a fork before acquiring a lock inherited from another thread.
        self._local_authority()
        with self._lock:
            self._local_authority()
            try:
                self._store.assert_lease(self._lease)
                current = TdsAttemptJournal(self._store).read(self._snapshot.state.identity)
                if current != self._snapshot:
                    raise WindowContractError("mssql_native.tds_writer_revision_changed")
            except BaseException as error:
                self._poisoned = True
                if isinstance(error, Exception) and not isinstance(error, WindowContractError):
                    raise WindowOutcomeUnknown("mssql_native.tds_storage_observation_unknown") from error
                raise

    def advance(self, event: TdsLifecycleEvent, *, expected_phase: TdsAttemptPhase) -> TdsAttemptSnapshot:
        self._local_authority()
        with self._lock:
            self.assert_authority()
            if self._recovering and not isinstance(
                event, (ContainmentRequired, ParentRetirementRequired, Contained, RetirementRequired, Retired)
            ):
                raise WindowContractError("mssql_native.tds_recovery_requires_settlement")
            try:
                state = advance_state(self._snapshot.state, event, expected_phase=expected_phase)
            except ValueError as error:
                raise WindowContractError("mssql_native.tds_invalid_transition") from error
            try:
                record = _persist(
                    self._store,
                    _key(state.identity),
                    self._snapshot.revision,
                    encode_state(state).decode("utf-8"),
                    self._lease,
                )
                snapshot = TdsAttemptSnapshot(state, record.revision)
            except BaseException:
                self._poisoned = True
                raise
            self._snapshot = snapshot
            return snapshot
