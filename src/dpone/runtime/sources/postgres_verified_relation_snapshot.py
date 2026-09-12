"""Linearizable ownership of one verified PostgreSQL relation snapshot."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from typing import Any

from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.sources.postgres_source_authority_types import SelectedPostgresSourceAuthority
from dpone.runtime.sources.postgres_verified_relation_observation import (
    PostgresGenericRelationQueryProfileV1,
    PostgresVerifiedRelationObservationError,
    execute_profile_item,
    observe_initial_witness,
    observe_relation_profile,
    require_active_witness,
    verify_physical_identity,
)
from dpone.runtime.sources.postgres_verified_relation_snapshot_cleanup import (
    PostgresSnapshotCleanupResultV1,
    cleanup_pinned_snapshot,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
    issue_repeatable_read_snapshot_lease,
)


class _VerifiedRelationSnapshotError(RuntimeError):
    """Runtime-local failure translated by the route-owned caller."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        recovery = {
            "snapshot_lease_mismatch": "retryable_source",
            "relation_lock_not_proven": "retryable_source",
            "catalog_observation_failed": "retryable_source",
            "snapshot_cleanup_failed": "retryable_source",
            "metadata_permission_denied": "operator_intervention",
            "source_authority_mismatch": "operator_intervention",
        }.get(reason, "operator_intervention")
        self.recovery = _SnapshotRecovery(recovery)
        super().__init__(reason)

    @property
    def __cause__(self) -> None:
        return None

    @__cause__.setter
    def __cause__(self, value: object) -> None:
        del value

    @property
    def __context__(self) -> None:
        return None

    @__context__.setter
    def __context__(self, value: object) -> None:
        del value


class _SnapshotRecovery(str):
    @property
    def value(self) -> str:
        return str(self)


def _route_error(reason: str) -> BaseException:
    """Keep generic snapshot ownership independent of route contracts."""

    return _VerifiedRelationSnapshotError(reason)


@dataclass(frozen=True, slots=True)
class PostgresVerifiedRelationSnapshotV1:
    connector: object
    physical_connection: object
    selected_source_authority: SelectedPostgresSourceAuthority
    query_profile: PostgresGenericRelationQueryProfileV1
    lifecycle: ExtractionLifecycleAuthority
    snapshot_lease: PostgresRepeatableReadSnapshotLease
    snapshot_token: str
    transaction_incarnation: str
    backend_pid: int

    def require_for(self, connector: object, *, allow_completed: bool = False) -> None:
        try:
            physical = getattr(connector, "connection")
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit):
            raise
        except Exception:
            raise _route_error("snapshot_lease_mismatch") from None
        if connector is not self.connector or physical is not self.physical_connection:
            raise _route_error("snapshot_lease_mismatch")
        lifecycle_receipt = self.lifecycle.receipt
        if lifecycle_receipt is None or (lifecycle_receipt.complete and not allow_completed):
            raise _route_error("snapshot_lease_mismatch")
        if not allow_completed:
            try:
                self.snapshot_lease.require_for(connector)
            except Exception:
                raise _route_error("snapshot_lease_mismatch") from None
        item = _profile_item(self.query_profile, "active_scope_revalidation")
        try:
            require_active_witness(
                connector,
                item,
                snapshot_token=self.snapshot_token,
                transaction_incarnation=self.transaction_incarnation,
            )
        except PostgresVerifiedRelationObservationError as error:
            raise _route_error(error.reason) from None
        if getattr(
            getattr(self.physical_connection, "info", None), "transaction_status", None
        ) is not _transaction_status("INTRANS"):
            raise _route_error("snapshot_lease_mismatch")


@dataclass(frozen=True, slots=True)
class PostgresSnapshotScopeTerminalReceiptV1:
    outcome: str
    cleanup_attempted: bool
    cleanup_succeeded: bool
    cleanup_error_reason: str | None
    connection_quarantined: bool


class PostgresVerifiedRelationSnapshotScopeV1:
    """Single terminal owner for the transaction and issuer reservation."""

    def __init__(self, verified: PostgresVerifiedRelationSnapshotV1, release: Any) -> None:
        self._verified = verified
        self._release = release
        self._condition = threading.Condition()
        self._state = "ACTIVE"
        self._receipt: PostgresSnapshotScopeTerminalReceiptV1 | None = None
        self._terminal_action: str | None = None
        self._terminal_contended = False

    @property
    def lifecycle(self) -> ExtractionLifecycleAuthority:
        return self._verified.lifecycle

    @property
    def terminal_receipt(self) -> PostgresSnapshotScopeTerminalReceiptV1 | None:
        # The immutable reference is published only after cleanup.  Reading it
        # must not contend with a waiter that is intentionally parked while
        # another caller owns the terminal transition.
        return self._receipt

    def require_active(self, connector: object) -> PostgresVerifiedRelationSnapshotV1:
        with self._condition:
            if self._state != "ACTIVE":
                raise _route_error("snapshot_lease_mismatch")
        self._verified.require_for(connector)
        return self._verified

    def complete_after_artifact_seal(self) -> PostgresSnapshotScopeTerminalReceiptV1:
        return self._terminate("completed", validate_complete=True, primary=None, action="complete")

    def abort_preserving(self, primary: BaseException) -> PostgresSnapshotScopeTerminalReceiptV1:
        if not isinstance(primary, BaseException):
            raise TypeError("primary must be an exception")
        return self._terminate("aborted", validate_complete=False, primary=primary, action="abort")

    def close_if_active(self) -> PostgresSnapshotScopeTerminalReceiptV1 | None:
        with self._condition:
            if self._state == "CLOSED":
                return (
                    self._receipt
                    if self._terminal_action == "close"
                    and self._receipt is not None
                    and self._receipt.cleanup_succeeded
                    else None
                )
        return self._terminate("aborted", validate_complete=False, primary=None, action="close")

    def _terminate(
        self,
        outcome: str,
        *,
        validate_complete: bool,
        primary: BaseException | None,
        action: str,
    ) -> PostgresSnapshotScopeTerminalReceiptV1:
        owns_transition = False
        waited_for_owner = False
        try:
            with self._condition:
                while self._state in {"COMPLETING", "ABORTING"}:
                    self._terminal_contended = True
                    waited_for_owner = True
                    self._condition.wait()
                if self._state == "CLOSED":
                    assert self._receipt is not None
                    if action == "complete" and self._receipt.outcome != "completed":
                        raise _route_error("snapshot_lease_mismatch")
                    if action == "abort" and primary is not None and waited_for_owner:
                        primary.__cause__ = primary.__context__ = None
                        raise primary
                    return self._receipt
                self._state = "COMPLETING" if outcome == "completed" else "ABORTING"
                self._terminal_action = action
                owns_transition = True
            if validate_complete:
                self._verified.require_for(self._verified.connector)
                self.lifecycle.complete()
                self._verified.require_for(self._verified.connector, allow_completed=True)
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit) as cancellation:
            if owns_transition and self.terminal_receipt is None:
                self._finish(self._settled_outcome())
            cancellation.__cause__ = cancellation.__context__ = None
            raise
        except BaseException as failure:
            if owns_transition and self.terminal_receipt is None:
                self._finish("aborted")
            if primary is not None:
                primary.__cause__ = primary.__context__ = None
                raise primary
            if getattr(failure, "reason", None) is not None:
                failure.__cause__ = failure.__context__ = None
                raise failure
            raise _route_error("snapshot_cleanup_failed") from None
        receipt, terminal_failure = self._finish(outcome)
        if primary is not None:
            if self._terminal_contended:
                primary.__cause__ = primary.__context__ = None
                raise primary
            return receipt
        if isinstance(terminal_failure, (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit)):
            terminal_failure.__cause__ = terminal_failure.__context__ = None
            raise terminal_failure
        if terminal_failure is not None and action != "close":
            raise _route_error("snapshot_cleanup_failed")
        if action == "close" and not receipt.cleanup_succeeded and not self._terminal_contended:
            raise _route_error("snapshot_cleanup_failed")
        return receipt

    def _settled_outcome(self) -> str:
        lifecycle_receipt = self.lifecycle.receipt
        return "completed" if lifecycle_receipt is not None and lifecycle_receipt.complete else "aborted"

    def _finish(
        self,
        outcome: str,
    ) -> tuple[PostgresSnapshotScopeTerminalReceiptV1, BaseException | None]:
        terminal_failure: BaseException | None = None
        try:
            result = cleanup_pinned_snapshot(self._verified.connector, self._verified.physical_connection)
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit) as cancellation:
            terminal_failure = cancellation
            connector = self._verified.connector
            quarantined = bool(getattr(connector, "quarantined", False) or getattr(connector, "_quarantined", False))
            result = PostgresSnapshotCleanupResultV1(False, quarantined)
        receipt = PostgresSnapshotScopeTerminalReceiptV1(
            outcome,
            True,
            result.succeeded,
            None if result.succeeded else "snapshot_cleanup_failed",
            result.connection_quarantined,
        )
        with self._condition:
            self._receipt = receipt
        try:
            self._release(self)
        except BaseException as release_failure:
            if terminal_failure is None:
                terminal_failure = release_failure
        finally:
            with self._condition:
                self._state = "CLOSED"
                self._condition.notify_all()
        return receipt, terminal_failure


class PostgresVerifiedRelationSnapshotIssuerV1:
    """Reserve and open exactly one verified relation scope at a time."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = "IDLE"
        self._scope: PostgresVerifiedRelationSnapshotScopeV1 | None = None

    @property
    def active(self) -> bool:
        with self._lock:
            return self._state != "IDLE"

    def open(
        self,
        *,
        connector: object,
        lifecycle: ExtractionLifecycleAuthority,
        selected_source_authority: SelectedPostgresSourceAuthority,
        query_profile: PostgresGenericRelationQueryProfileV1,
    ) -> PostgresVerifiedRelationSnapshotScopeV1:
        if (
            type(lifecycle) is not ExtractionLifecycleAuthority
            or type(selected_source_authority) is not SelectedPostgresSourceAuthority
            or type(query_profile) is not PostgresGenericRelationQueryProfileV1
        ):
            raise _route_error("exact_type_violation")
        with self._lock:
            if self._state != "IDLE":
                raise _route_error("snapshot_lease_mismatch")
            physical = getattr(connector, "connection")
            if getattr(getattr(physical, "info", None), "transaction_status", None) is not _transaction_status("IDLE"):
                raise _route_error("snapshot_lease_mismatch")
            self._state = "OPENING"
        try:
            getattr(connector, "begin")()
            for statement_id in ("set_transaction", "set_lock_timeout", "lock_relation"):
                execute_profile_item(connector, _profile_item(query_profile, statement_id))
            witness = observe_initial_witness(
                connector,
                _profile_item(query_profile, "initial_snapshot_witness"),
                selected_source_authority,
            )
            if selected_source_authority.version == 1:
                verify_physical_identity(
                    connector,
                    _profile_item(query_profile, "v1_physical_identity"),
                    selected_source_authority,
                )
            snapshot_lease = issue_repeatable_read_snapshot_lease(
                connector=connector,
                lifecycle=lifecycle,
                raw_snapshot_token=witness.snapshot_token,
                visible_horizon=witness.visible_horizon,
            )
            verified = PostgresVerifiedRelationSnapshotV1(
                connector,
                physical,
                selected_source_authority,
                query_profile,
                lifecycle,
                snapshot_lease,
                witness.snapshot_token,
                witness.transaction_incarnation,
                witness.backend_pid,
            )
            scope = PostgresVerifiedRelationSnapshotScopeV1(verified, self._release)
            with self._lock:
                self._scope = scope
                self._state = "ACTIVE"
            return scope
        except BaseException as original:
            if isinstance(original, PostgresVerifiedRelationObservationError):
                primary = _route_error(original.reason)
            elif isinstance(original, Exception):
                primary = _route_error("internal_invariant_violation")
            else:
                primary = original
            try:
                cleanup_pinned_snapshot(connector, physical)
            except BaseException:
                pass
            finally:
                self._reset()
            primary.__cause__ = primary.__context__ = None
            raise primary from None

    def _release(self, scope: PostgresVerifiedRelationSnapshotScopeV1) -> None:
        with self._lock:
            if self._scope is scope:
                self._scope = None
                self._state = "IDLE"

    def _reset(self) -> None:
        with self._lock:
            self._scope = None
            self._state = "IDLE"


def _profile_item(profile: PostgresGenericRelationQueryProfileV1, statement_id: str) -> Any:
    for item in profile.ordered_items:
        if item.statement_id == statement_id:
            return item
    raise _route_error("internal_invariant_violation")


def _transaction_status(name: str) -> object:
    """Load libpq transaction constants only when the R1 runtime is used."""

    from psycopg.pq import TransactionStatus

    return getattr(TransactionStatus, name)


__all__ = [
    "PostgresSnapshotScopeTerminalReceiptV1",
    "PostgresVerifiedRelationObservationError",
    "PostgresVerifiedRelationSnapshotIssuerV1",
    "PostgresVerifiedRelationSnapshotScopeV1",
    "PostgresVerifiedRelationSnapshotV1",
    "execute_profile_item",
    "observe_relation_profile",
]
