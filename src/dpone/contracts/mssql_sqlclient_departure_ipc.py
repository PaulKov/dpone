"""Nonsecret helper intent and bindings, without process/SQL/ACK authentication."""

from dataclasses import dataclass, fields
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_create_departure import SqlClientCreateDeparture, validate_create_departure_inputs
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity
from dpone.contracts.mssql_tds_validation import _hash, _integer, deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsProcessIdentity

ERROR = "mssql_native.sqlclient_departure_ipc_invalid"


def _typed(value: Any, cls: type) -> None:
    """Preserve exact original scalar types while repeating a known constructor."""
    if type(value) is not cls:
        raise ValueError(ERROR)
    cls(**{f.name: getattr(value, f.name) for f in fields(cls)})


def _startup(value: TdsCoordinatorStartup) -> None:
    _typed(value, TdsCoordinatorStartup)
    _typed(value.process, TdsProcessIdentity)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDeparturePlan:
    """Declared launch inputs; helper source differs from historical CREATE source."""

    helper_id: UUID
    attempt: TdsAttemptIdentity
    ownership: TdsAttemptOwnership
    create_operation: TdsCoordinatorIdentity
    create_process: TdsProcessIdentity
    create_result_sha256: str
    create_local_exit_sha256: str
    original: TdsRemoteSessionIdentity
    database: TdsDatabaseObservation
    creator_admission: SqlClientObserverAdmission
    principal: SqlClientDatabasePrincipal
    implementation_sha256: str
    package_root: str
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    max_address_space_bytes: int
    schema: str = "dpone.sqlclient.departure-plan.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.departure-plan.v1":
                raise ValueError
            if type(self.helper_id) is not UUID or not self.helper_id.int:
                raise ValueError
            for value, cls in (
                (self.attempt, TdsAttemptIdentity),
                (self.ownership, TdsAttemptOwnership),
                (self.create_operation, TdsCoordinatorIdentity),
                (self.create_process, TdsProcessIdentity),
            ):
                _typed(value, cls)
            _typed(self.create_operation.parent, TdsAttemptIdentity)
            validate_create_departure_inputs(self.original, self.database, self.creator_admission, self.principal)
            for digest in (
                self.create_result_sha256,
                self.create_local_exit_sha256,
                self.implementation_sha256,
                self.admission_sha256,
            ):
                _hash(digest)
            deadline_nanoseconds(self.startup_deadline)
            deadline_nanoseconds(self.operation_deadline)
            _integer(self.max_address_space_bytes, 1)
            if (
                self.create_operation.command is not TdsCoordinatorCommand.CREATE
                or self.create_operation.parent != self.attempt
                or self.create_operation.original_fence != self.ownership.fence
                or self.attempt.database != self.database.name
                or self.startup_deadline > self.operation_deadline
            ):
                raise ValueError
            if (
                type(self.package_root) is not str
                or not self.package_root.startswith("/")
                or len(self.package_root.encode("utf-8")) > 4096
                or any(ord(c) < 32 or ord(c) == 127 for c in self.package_root)
            ):
                raise ValueError
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDepartureRequest:
    """Self-consistent startup still requires independently authenticated binding."""

    plan: SqlClientDeparturePlan
    startup: TdsCoordinatorStartup
    schema: str = "dpone.sqlclient.departure-request.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.departure-request.v1":
                raise ValueError
            _typed(self.plan, SqlClientDeparturePlan)
            _startup(self.startup)
            if (
                self.startup.implementation_sha256 != self.plan.implementation_sha256
                or self.startup.package_root != self.plan.package_root
                or self.startup.process.host_sha256 != self.plan.create_process.host_sha256
                or self.startup.process.boot_id != self.plan.create_process.boot_id
            ):
                raise ValueError
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDepartureResult:
    """Observed departure and exact request digest; no helper reap/ACK claim."""

    request_sha256: str
    departure: SqlClientCreateDeparture
    schema: str = "dpone.sqlclient.departure-result.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.departure-result.v1":
                raise ValueError
            _hash(self.request_sha256)
            _typed(self.departure, SqlClientCreateDeparture)
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError):
            raise ValueError(ERROR) from None


def validate_departure_request_binding(
    request: SqlClientDepartureRequest,
    *,
    startup: TdsCoordinatorStartup,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
) -> None:
    """Compare trusted original launch facts; incoming values cannot authenticate themselves."""
    try:
        _typed(request, SqlClientDepartureRequest)
        _startup(startup)
        _hash(admission_sha256)
        deadline_nanoseconds(startup_deadline)
        deadline_nanoseconds(operation_deadline)
        _integer(max_address_space_bytes, 1)
        if (
            startup_deadline > operation_deadline
            or request.startup != startup
            or request.plan.admission_sha256 != admission_sha256
            or request.plan.startup_deadline != startup_deadline
            or request.plan.operation_deadline != operation_deadline
            or request.plan.max_address_space_bytes != max_address_space_bytes
        ):
            raise ValueError
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError):
        raise ValueError(ERROR) from None
