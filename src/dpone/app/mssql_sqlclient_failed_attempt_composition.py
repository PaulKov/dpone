"""Compose exact v2 failed-attempt observation and durable retirement."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Any, cast
from uuid import UUID

from dpone.adapters.mssql_sqlclient_failed_attempt_settlement import (
    WindowStoreSqlClientFailedSettlementJournal,
)
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedRetirementRequest,
    SqlClientFailedRetirementSubject,
    issue_failed_retirement_authorization,
)
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits, directory_key
from dpone.contracts.mssql_tds_worker import TdsAttemptError, TdsAttemptIdentity, TdsAttemptPhase
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.ports.bounded_window import WindowStore
from dpone.ports.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedRetirementReservation,
    SqlClientFailedRetirementTerminal,
)
from dpone.ports.mssql_tds_directory import TdsDirectoryObserver
from dpone.ports.mssql_tds_journal import TdsAttemptObserver
from dpone.services.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedAttemptSettlementService,
)

_INVALID = "mssql_native.sqlclient_failed_settlement_composition_invalid"
_UNKNOWN = "mssql_native.sqlclient_failed_settlement_unknown"
_TERMINAL = "mssql_native.sqlclient_terminal_input_or_policy"
_RETRYABLE = frozenset(
    {
        TdsAttemptError.CONNECTION,
        TdsAttemptError.DRIVER,
        TdsAttemptError.STARTUP_TIMEOUT,
        TdsAttemptError.OPERATION_TIMEOUT,
    }
)


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if is_dataclass(value):
        return {name: _plain(item) for name, item in asdict(cast(Any, value)).items()}
    if isinstance(value, dict):
        return {str(name): _plain(item) for name, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _snapshot_digest(value: object) -> str:
    return sha256(canonical_json_bytes(_plain(value))).hexdigest()


class DurableSqlClientFailedRetirementObserver:
    """Issue v2 authority only from exact contained durable observations."""

    def __init__(
        self,
        *,
        lease: WindowLease,
        lifecycle: TdsAttemptObserver,
        directories: TdsDirectoryObserver,
        directory_limits: TdsDirectoryLimits,
        resolve_attempt: Callable[[NativeChunkPlan, str], TdsAttemptIdentity],
        observe_stage: Callable[[TdsAttemptIdentity], SqlClientStageIdentity],
        observe_input_custody: Callable[[NativeChunkPlan, str], str],
    ) -> None:
        self._lease = lease
        self._lifecycle = lifecycle
        self._directories = directories
        self._limits = directory_limits
        self._resolve_attempt = resolve_attempt
        self._observe_stage = observe_stage
        self._observe_input_custody = observe_input_custody

    def observe(self, plan: NativeChunkPlan, attempt_id: str, lease: WindowLease) -> SqlClientFailedRetirementRequest:
        if lease != self._lease or type(attempt_id) is not str or not attempt_id:
            raise RuntimeError(_UNKNOWN)
        identity = self._resolve_attempt(plan, attempt_id)
        if type(identity) is not TdsAttemptIdentity or identity.target_key != lease.target_id:
            raise RuntimeError(_UNKNOWN)
        lifecycle = self._lifecycle.read(identity)
        directory = self._directories.read(identity, self._limits)
        if lifecycle is None or directory is None:
            raise RuntimeError(_UNKNOWN)
        state = lifecycle.state
        directory_state = directory.state
        if state.error not in _RETRYABLE:
            if type(state.error) is TdsAttemptError:
                raise RuntimeError(_TERMINAL)
            raise RuntimeError(_UNKNOWN)
        if (
            state.identity != identity
            or state.phase not in (TdsAttemptPhase.CONTAINED, TdsAttemptPhase.RETIREMENT_REQUIRED)
            or state.schema_version != 2
            or state.backend != "mssql_sqlclient"
            or state.object_identity is None
            or state.observation_sha256 is None
            or state.ownership.owner != lease.owner
            or state.ownership.fence != lease.fence
            or directory_state.parent != identity
            or directory.ownership.owner != lease.owner
            or directory.ownership.fence != lease.fence
            or (state.phase is TdsAttemptPhase.CONTAINED and directory_state.retirement_authority is not None)
            or (state.phase is TdsAttemptPhase.RETIREMENT_REQUIRED and directory_state.retirement_authority != state)
            or any(not slot.settled for slot in directory_state.slots)
        ):
            raise RuntimeError(_UNKNOWN)
        stage = self._observe_stage(identity)
        custody_sha256 = self._observe_input_custody(plan, attempt_id)
        if (
            type(stage) is not SqlClientStageIdentity
            or stage.object_id != state.object_identity.object_id
            or type(custody_sha256) is not str
        ):
            raise RuntimeError(_UNKNOWN)
        subject = SqlClientFailedRetirementSubject.bind(
            attempt=identity,
            stage=stage,
            object_identity=state.object_identity,
            expected_object_nonce=stage.object_nonce,
            input_custody_sha256=custody_sha256,
            error=state.error,
            observation_sha256=state.observation_sha256,
            lifecycle_revision=lifecycle.revision,
            lifecycle_state_sha256=_snapshot_digest(state),
            directory_key=directory_key(identity),
            directory_revision=directory.revision,
            directory_state_sha256=_snapshot_digest(directory_state),
            lease_owner=lease.owner,
            lease_fence=lease.fence,
        )
        return SqlClientFailedRetirementRequest(subject, issue_failed_retirement_authorization(subject))

    def operation_key(self, plan: NativeChunkPlan, attempt_id: str, lease: WindowLease) -> str:
        """Locate pending intent without observing mutable lifecycle state."""
        if lease != self._lease or type(attempt_id) is not str or not attempt_id:
            raise RuntimeError(_UNKNOWN)
        identity = self._resolve_attempt(plan, attempt_id)
        if type(identity) is not TdsAttemptIdentity or identity.target_key != lease.target_id:
            raise RuntimeError(_UNKNOWN)
        return sha256(
            b"dpone.sqlclient.failed-settlement-attempt.v2\0" + canonical_json_bytes(_plain(identity))
        ).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientFailedAttemptDeployment:
    """Lease-bound source-of-truth capabilities for failed settlement."""

    store: WindowStore
    lease: WindowLease
    lifecycle_observer: TdsAttemptObserver
    directory_observer: TdsDirectoryObserver
    directory_limits: TdsDirectoryLimits
    resolve_attempt: Callable[[NativeChunkPlan, str], TdsAttemptIdentity]
    observe_stage: Callable[[TdsAttemptIdentity], SqlClientStageIdentity]
    observe_input_custody: Callable[[NativeChunkPlan, str], str]
    reservation: SqlClientFailedRetirementReservation
    retirement: SqlClientFailedRetirementTerminal


def compose_sqlclient_failed_attempt_settlement(
    deployment: SqlClientFailedAttemptDeployment,
) -> SqlClientFailedAttemptSettlementService:
    """Bind observer, exact retirement engine and intent-before-effect journal."""
    if (
        type(deployment) is not SqlClientFailedAttemptDeployment
        or type(deployment.lease) is not WindowLease
        or type(deployment.directory_limits) is not TdsDirectoryLimits
        or any(
            not callable(value)
            for value in (
                deployment.resolve_attempt,
                deployment.observe_stage,
                deployment.observe_input_custody,
            )
        )
        or not callable(getattr(deployment.reservation, "reserve", None))
        or not callable(getattr(deployment.retirement, "observe_or_retire", None))
    ):
        raise ValueError(_INVALID)
    observer = DurableSqlClientFailedRetirementObserver(
        lease=deployment.lease,
        lifecycle=deployment.lifecycle_observer,
        directories=deployment.directory_observer,
        directory_limits=deployment.directory_limits,
        resolve_attempt=deployment.resolve_attempt,
        observe_stage=deployment.observe_stage,
        observe_input_custody=deployment.observe_input_custody,
    )
    return SqlClientFailedAttemptSettlementService(
        observer,
        deployment.reservation,
        deployment.retirement,
        WindowStoreSqlClientFailedSettlementJournal(deployment.store, deployment.lease),
    )


__all__ = (
    "DurableSqlClientFailedRetirementObserver",
    "SqlClientFailedAttemptDeployment",
    "compose_sqlclient_failed_attempt_settlement",
)
