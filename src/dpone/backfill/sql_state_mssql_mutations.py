"""Serializable MSSQL ledger mutations and pure state-transition policy."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Generic, TypeVar

from dpone.backfill.sql_state_mssql_invariants import (
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_RUNNING,
    CHUNK_STATUS_SUCCESS,
    BackfillChunkRecord,
    BackfillLedger,
    aware_utc,
    expired_iso,
    future_utc_iso,
    iso_later,
    merge_portable_scope_contract,
    merge_publication,
    merge_xmin_handoff,
    replace_chunk,
    require_campaign_identity,
    require_chunk_coordinates,
    require_current,
    require_unique_chunk_shape,
)
from dpone.backfill.sql_state_mssql_migration import chunks_for_compact_append
from dpone.backfill.sql_state_mssql_recovery import committed_chunk_revisions_match

_ResultT = TypeVar("_ResultT")
_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


@dataclass(frozen=True)
class MSSQLLedgerMutation(Generic[_ResultT]):
    """One candidate journal revision and its caller-facing result."""

    ledger: BackfillLedger | None
    chunks: tuple[BackfillChunkRecord, ...]
    result: _ResultT

    @classmethod
    def unchanged(cls, result: _ResultT) -> MSSQLLedgerMutation[_ResultT]:
        """Return a read-only transition which appends no journal rows."""

        return cls(ledger=None, chunks=(), result=result)


@dataclass(frozen=True)
class MSSQLCommittedMutation(Generic[_ResultT]):
    """Committed transition result plus its authoritative revision, if any."""

    ledger: BackfillLedger | None
    result: _ResultT


class MSSQLLedgerMutationCoordinator:
    """Execute latest-row ledger transitions in one SERIALIZABLE transaction."""

    def __init__(
        self,
        connector: Any,
        *,
        ensure_tables: Callable[[], None],
        load_for_update: Callable[[str, Any], BackfillLedger | None],
        load_chunks: Callable[[str, Any, tuple[int, ...]], tuple[BackfillChunkRecord, ...]],
        append_snapshot: Callable[[Any, BackfillLedger, tuple[BackfillChunkRecord, ...]], None],
    ) -> None:
        self._connector = connector
        self._ensure_tables = ensure_tables
        self._load_for_update = load_for_update
        self._load_chunks = load_chunks
        self._append_snapshot = append_snapshot

    def run(
        self,
        run_key: str,
        transition: Callable[[BackfillLedger | None], MSSQLLedgerMutation[_ResultT]],
        *,
        before_load: Callable[[Any], None] | None = None,
    ) -> MSSQLCommittedMutation[_ResultT]:
        """Lock, derive, append and commit one authoritative revision."""

        self._ensure_tables()
        begin = self._require_method("begin")
        commit = self._require_method("commit_transaction")
        rollback = self._require_method("rollback")
        begin()
        try:
            self._connector.execute_query("SET XACT_ABORT ON")
            self._connector.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            if before_load is not None:
                before_load(self._connector)
            current = self._load_for_update(run_key, self._connector)
            mutation = transition(current)
            candidate = mutation.ledger
            if candidate is not None:
                self._stamp(candidate, current=current)
                committed_chunks = chunks_for_compact_append(current, candidate, mutation.chunks)
                self._append_snapshot(self._connector, candidate, committed_chunks)
        except BaseException:
            rollback()
            raise

        try:
            commit()
        except BaseException as exc:
            self._close_after_unknown_commit()
            if candidate is None:
                return MSSQLCommittedMutation(ledger=None, result=mutation.result)
            recovered = self._load_for_update(run_key, self._connector)
            if recovered is None or recovered.campaign_jsonable() != candidate.campaign_jsonable():
                raise RuntimeError("mssql_backfill_state.commit_outcome_unknown") from exc
            if not committed_chunk_revisions_match(
                load_chunks=self._load_chunks,
                run_key=run_key,
                connector=self._connector,
                expected=committed_chunks,
            ):
                raise RuntimeError("mssql_backfill_state.commit_outcome_unknown") from exc
            candidate = recovered

        return MSSQLCommittedMutation(ledger=candidate, result=mutation.result)

    def _require_method(self, name: str) -> Callable[[], Any]:
        method = getattr(self._connector, name, None)
        if not callable(method):
            raise RuntimeError(f"mssql_backfill_state.connector_{name}_required")
        return method

    def _close_after_unknown_commit(self) -> None:
        close = getattr(self._connector, "close", None)
        if callable(close):
            close()

    @staticmethod
    def _stamp(candidate: BackfillLedger, *, current: BackfillLedger | None) -> None:
        now = datetime.now(_UTC).isoformat()
        candidate.created_at = candidate.created_at or (current.created_at if current is not None else None) or now
        candidate.updated_at = now


class MSSQLLedgerTransitionPolicy:
    """Derive append-only campaign and chunk revisions without connector I/O."""

    def save(
        self,
        current: BackfillLedger | None,
        *,
        incoming: BackfillLedger,
    ) -> MSSQLLedgerMutation[None]:
        if current is None:
            candidate = deepcopy(incoming)
            require_unique_chunk_shape(candidate)
            return MSSQLLedgerMutation(candidate, tuple(candidate.chunks), None)
        require_campaign_identity(current, incoming)
        candidate = deepcopy(current)
        if current.status != "cancel_requested":
            candidate.status = incoming.status
            candidate.cancel_reason = incoming.cancel_reason
            candidate.cancel_requested_by = incoming.cancel_requested_by
        candidate.xmin_handoff = merge_xmin_handoff(current.xmin_handoff, incoming.xmin_handoff)
        candidate.publication = merge_publication(current.publication, incoming.publication)
        candidate.portable_scope_column_contract = merge_portable_scope_contract(
            current.portable_scope_column_contract,
            incoming.portable_scope_column_contract,
        )
        return MSSQLLedgerMutation(candidate, (), None)

    def update_chunk(
        self,
        current: BackfillLedger | None,
        *,
        incoming: BackfillLedger,
        record: BackfillChunkRecord,
    ) -> MSSQLLedgerMutation[None]:
        current = require_current(current, incoming.run_key)
        require_campaign_identity(current, incoming)
        existing = current.chunk(record.index)
        require_chunk_coordinates(existing, record)
        if (
            current.status == "cancel_requested"
            and record.status == CHUNK_STATUS_RUNNING
            and (existing.status != CHUNK_STATUS_RUNNING or existing.lease_owner != record.lease_owner)
        ):
            raise RuntimeError("backfill campaign is cancelled")
        changed = deepcopy(record)
        changed.updated_at = _now_iso()
        candidate = deepcopy(current)
        replace_chunk(candidate, changed)
        return MSSQLLedgerMutation(candidate, (changed,), None)

    def acquire_chunk(
        self,
        current: BackfillLedger | None,
        *,
        index: int,
        owner: str,
        expiry: str,
    ) -> MSSQLLedgerMutation[bool]:
        current = require_current(current, "unknown")
        if current.status == "cancel_requested":
            return MSSQLLedgerMutation.unchanged(False)
        existing = current.chunk(index)
        if existing.status == CHUNK_STATUS_SUCCESS:
            return MSSQLLedgerMutation.unchanged(False)
        if existing.status == CHUNK_STATUS_RUNNING and not expired_iso(existing.lease_expires_at):
            return MSSQLLedgerMutation.unchanged(False)
        changed = deepcopy(existing)
        changed.status = CHUNK_STATUS_RUNNING
        changed.attempts += 1
        changed.lease_owner = owner
        changed.lease_expires_at = expiry
        changed.started_at = changed.started_at or _now_iso()
        changed.updated_at = _now_iso()
        changed.error = None
        candidate = deepcopy(current)
        replace_chunk(candidate, changed)
        return MSSQLLedgerMutation(candidate, (changed,), True)

    def renew_chunk(
        self,
        current: BackfillLedger | None,
        *,
        index: int,
        owner: str,
        expiry: str,
    ) -> MSSQLLedgerMutation[bool]:
        current = require_current(current, "unknown")
        existing = current.chunk(index)
        if (
            current.status == "cancel_requested"
            or existing.status != CHUNK_STATUS_RUNNING
            or existing.lease_owner != owner
            or expired_iso(existing.lease_expires_at)
        ):
            return MSSQLLedgerMutation.unchanged(False)
        if not iso_later(expiry, existing.lease_expires_at):
            return MSSQLLedgerMutation.unchanged(True)
        changed = deepcopy(existing)
        changed.lease_expires_at = expiry
        changed.updated_at = _now_iso()
        candidate = deepcopy(current)
        replace_chunk(candidate, changed)
        return MSSQLLedgerMutation(candidate, (changed,), True)

    def complete_chunk(
        self,
        current: BackfillLedger | None,
        *,
        record: BackfillChunkRecord,
        owner: str,
    ) -> MSSQLLedgerMutation[bool]:
        current = require_current(current, "unknown")
        existing = current.chunk(record.index)
        if existing.status != CHUNK_STATUS_RUNNING or existing.lease_owner != owner:
            return MSSQLLedgerMutation.unchanged(False)
        if record.status not in {CHUNK_STATUS_SUCCESS, CHUNK_STATUS_FAILED}:
            raise ValueError("completed backfill chunk must be success or failed")
        require_chunk_coordinates(existing, record)
        changed = deepcopy(record)
        changed.lease_owner = None
        changed.lease_expires_at = None
        changed.updated_at = _now_iso()
        candidate = deepcopy(current)
        replace_chunk(candidate, changed)
        return MSSQLLedgerMutation(candidate, (changed,), True)

    def recover_chunks(
        self,
        current: BackfillLedger | None,
        *,
        now: datetime,
    ) -> MSSQLLedgerMutation[tuple[int, ...]]:
        current = require_current(current, "unknown")
        candidate = deepcopy(current)
        changed: list[BackfillChunkRecord] = []
        for record in candidate.chunks:
            if record.status != CHUNK_STATUS_RUNNING or not expired_iso(record.lease_expires_at, now=now):
                continue
            record.status = CHUNK_STATUS_FAILED
            record.error = "lease_expired"
            record.lease_owner = None
            record.lease_expires_at = None
            record.updated_at = _now_iso()
            changed.append(record)
        if not changed:
            return MSSQLLedgerMutation.unchanged(())
        return MSSQLLedgerMutation(candidate, tuple(changed), tuple(record.index for record in changed))

    def cancel(
        self,
        current: BackfillLedger | None,
        *,
        reason: str,
        requested_by: str,
    ) -> MSSQLLedgerMutation[None]:
        current = require_current(current, "unknown")
        if current.status == "cancel_requested":
            return MSSQLLedgerMutation.unchanged(None)
        if _published(current):
            raise RuntimeError("backfill cancellation is closed after target publication")
        candidate = deepcopy(current)
        candidate.status = "cancel_requested"
        candidate.cancel_reason = reason
        candidate.cancel_requested_by = requested_by
        return MSSQLLedgerMutation(candidate, (), None)

    def acquire_campaign(
        self,
        current: BackfillLedger | None,
        *,
        owner: str,
        expiry: str,
    ) -> MSSQLLedgerMutation[bool]:
        current = require_current(current, "unknown")
        if current.status == "cancel_requested" and not _published(current):
            return MSSQLLedgerMutation.unchanged(False)
        candidate = deepcopy(current)
        if candidate.status == "cancel_requested":
            candidate.status = "active"
            candidate.cancel_reason = None
            candidate.cancel_requested_by = None
        orphaned_chunks: list[BackfillChunkRecord] = []
        for record in candidate.chunks:
            if record.status != CHUNK_STATUS_RUNNING:
                continue
            record.status = CHUNK_STATUS_FAILED
            record.error = "campaign_session_fence_orphaned"
            record.lease_owner = None
            record.lease_expires_at = None
            record.updated_at = _now_iso()
            orphaned_chunks.append(record)
        candidate.lock_owner = owner
        candidate.lock_expires_at = expiry
        return MSSQLLedgerMutation(candidate, tuple(orphaned_chunks), True)

    def renew_campaign(
        self,
        current: BackfillLedger | None,
        *,
        owner: str,
        expiry: str,
    ) -> MSSQLLedgerMutation[bool]:
        current = require_current(current, "unknown")
        if current.status == "cancel_requested" or current.lock_owner != owner:
            return MSSQLLedgerMutation.unchanged(False)
        if not iso_later(expiry, current.lock_expires_at):
            return MSSQLLedgerMutation.unchanged(True)
        candidate = deepcopy(current)
        candidate.lock_expires_at = expiry
        return MSSQLLedgerMutation(candidate, (), True)

    def release_campaign(
        self,
        current: BackfillLedger | None,
        *,
        owner: str,
    ) -> MSSQLLedgerMutation[bool]:
        current = require_current(current, "unknown")
        if current.lock_owner != owner:
            return MSSQLLedgerMutation.unchanged(False)
        candidate = deepcopy(current)
        candidate.lock_owner = None
        candidate.lock_expires_at = None
        return MSSQLLedgerMutation(candidate, (), True)


def _now_iso() -> str:
    return datetime.now(_UTC).isoformat()


def _published(ledger: BackfillLedger) -> bool:
    publication = ledger.publication
    return publication is not None and publication.phase == "published"


__all__ = [
    "MSSQLCommittedMutation",
    "MSSQLLedgerMutation",
    "MSSQLLedgerMutationCoordinator",
    "MSSQLLedgerTransitionPolicy",
    "aware_utc",
    "future_utc_iso",
]
