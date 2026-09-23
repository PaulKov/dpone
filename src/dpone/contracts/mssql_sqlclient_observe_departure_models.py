"""Immutable original-OBSERVE departure records; they confer no execution authority.

The parent must retain the original operation, management principal, process and
artifact acknowledgements independently. No returned record replaces that custody.
"""

from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.mssql_sqlclient_create_departure import validate_create_departure_inputs
from dpone.contracts.mssql_sqlclient_create_departure_v2 import SqlClientCreateDepartureV2
from dpone.contracts.mssql_sqlclient_departure_ipc import _startup, _typed
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import _admission
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity
from dpone.contracts.mssql_tds_validation import _hash, _integer, deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsChildExit, TdsProcessIdentity

ERROR = "mssql_native.sqlclient_observe_departure_invalid"
FAILURES = (ValueError, TypeError, AttributeError, OverflowError, RecursionError, UnicodeError)


def _uuid(value: UUID) -> None:
    if type(value) is not UUID or type(value.int) is not int or not 0 < value.int < 2**128:
        raise ValueError(ERROR)


def _operation(value: TdsCoordinatorIdentity) -> None:
    _typed(value, TdsCoordinatorIdentity)
    _typed(value.parent, TdsAttemptIdentity)
    _uuid(value.operation_id)
    if value.command is not TdsCoordinatorCommand.OBSERVE:
        raise ValueError(ERROR)


def _observation(value: SqlClientCreateDepartureV2) -> None:
    """Validate raw UUID integers before any shared census projection or digest."""
    if type(value) is not SqlClientCreateDepartureV2:
        raise ValueError(ERROR)
    _uuid(value.original.connection_id)
    _uuid(value.database.database_guid)
    _uuid(value.observer.connection_id)
    for sample in value.samples:
        if sample.request is not None:
            _uuid(sample.request.connection_id)
    _typed(value, SqlClientCreateDepartureV2)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientObserveDeparturePlan:
    """Closed nonsecret expectations for a separate source-free verifier."""

    helper_id: UUID
    attempt: TdsAttemptIdentity
    ownership: TdsAttemptOwnership
    observe_operation: TdsCoordinatorIdentity
    observe_process: TdsProcessIdentity
    original_registration_artifact_sha256: str
    original_authority_artifact_sha256: str
    original_authority_sha256: str
    original_containment_artifact_sha256: str
    preparation_artifact_sha256: str
    original: TdsRemoteSessionIdentity
    database: TdsDatabaseObservation
    management_admission: SqlClientObserverAdmission
    principal: SqlClientDatabasePrincipal
    observer_admission: SqlClientObserverAdmission
    implementation_sha256: str
    package_root: str
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    max_address_space_bytes: int
    schema: str = "dpone.sqlclient.observe-departure-plan.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.observe-departure-plan.v1":
                raise ValueError
            _uuid(self.helper_id)
            _operation(self.observe_operation)
            for value, cls in (
                (self.attempt, TdsAttemptIdentity),
                (self.ownership, TdsAttemptOwnership),
                (self.observe_process, TdsProcessIdentity),
            ):
                _typed(value, cls)
            _admission(self.management_admission)
            _admission(self.observer_admission)
            validate_create_departure_inputs(self.original, self.database, self.management_admission, self.principal)
            _uuid(self.original.connection_id)
            _uuid(self.database.database_guid)
            for digest in (
                self.original_registration_artifact_sha256,
                self.original_authority_artifact_sha256,
                self.original_authority_sha256,
                self.original_containment_artifact_sha256,
                self.preparation_artifact_sha256,
                self.implementation_sha256,
                self.admission_sha256,
            ):
                _hash(digest)
            deadline_nanoseconds(self.startup_deadline)
            deadline_nanoseconds(self.operation_deadline)
            _integer(self.max_address_space_bytes, 1)
            if (
                self.observe_operation.parent != self.attempt
                or self.observe_operation.original_fence != self.ownership.fence
                or self.observe_operation.operation_id == self.helper_id
                or self.attempt.database != self.database.name
                or self.startup_deadline > self.operation_deadline
                or (self.management_admission.server, self.management_admission.database)
                != (self.observer_admission.server, self.observer_admission.database)
            ):
                raise ValueError
            if (
                type(self.package_root) is not str
                or not self.package_root.startswith("/")
                or len(self.package_root.encode("utf-8")) > 4096
                or any(ord(c) < 32 or ord(c) == 127 for c in self.package_root)
            ):
                raise ValueError
        except FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientObserveDepartureRequest:
    """Declared helper startup remains separately authenticated by the launcher."""

    plan: SqlClientObserveDeparturePlan
    startup: TdsCoordinatorStartup
    schema: str = "dpone.sqlclient.observe-departure-request.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.observe-departure-request.v1":
                raise ValueError
            _typed(self.plan, SqlClientObserveDeparturePlan)
            _startup(self.startup)
            if (
                self.startup.implementation_sha256 != self.plan.implementation_sha256
                or self.startup.package_root != self.plan.package_root
                or self.startup.process.host_sha256 != self.plan.observe_process.host_sha256
                or self.startup.process.boot_id != self.plan.observe_process.boot_id
                or self.startup.process == self.plan.observe_process
            ):
                raise ValueError
        except FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientObserveDepartureResult:
    """Same reviewed six-sample census value with an explicit OBSERVE wire schema."""

    request_sha256: str
    departure: SqlClientCreateDepartureV2
    schema: str = "dpone.sqlclient.observe-departure-result.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.observe-departure-result.v1":
                raise ValueError
            _hash(self.request_sha256)
            _observation(self.departure)
        except FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientObserveContainment:
    """Original reaped exit, including forced nonzero exit; no remote assertion."""

    attempt_sha256: str
    observe_operation: TdsCoordinatorIdentity
    registration_artifact_sha256: str
    authority_artifact_sha256: str
    original_authority_sha256: str
    preparation_artifact_sha256: str
    exit: TdsChildExit
    schema: str = "dpone.sqlclient.observe-containment.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.observe-containment.v1":
                raise ValueError
            _operation(self.observe_operation)
            _typed(self.exit, TdsChildExit)
            _typed(self.exit.identity, TdsProcessIdentity)
            for digest in (
                self.attempt_sha256,
                self.registration_artifact_sha256,
                self.authority_artifact_sha256,
                self.original_authority_sha256,
                self.preparation_artifact_sha256,
            ):
                _hash(digest)
            if self.exit.reaped is not True or self.attempt_sha256 != attempt_identity_digest(
                self.observe_operation.parent
            ):
                raise ValueError
        except FAILURES:
            raise ValueError(ERROR) from None


def validate_observe_departure_binding(
    request: SqlClientObserveDepartureRequest,
    *,
    startup: TdsCoordinatorStartup,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
    observer_admission: SqlClientObserverAdmission,
) -> None:
    """Compare separately retained launcher and verifier configuration facts."""
    try:
        _typed(request, SqlClientObserveDepartureRequest)
        _startup(startup)
        _admission(observer_admission)
        _hash(admission_sha256)
        deadline_nanoseconds(startup_deadline)
        deadline_nanoseconds(operation_deadline)
        _integer(max_address_space_bytes, 1)
        if (
            request.startup != startup
            or request.plan.admission_sha256 != admission_sha256
            or request.plan.startup_deadline != startup_deadline
            or request.plan.operation_deadline != operation_deadline
            or request.plan.max_address_space_bytes != max_address_space_bytes
            or request.plan.observer_admission != observer_admission
        ):
            raise ValueError
    except FAILURES:
        raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class SqlClientObserveContainmentReceipt:
    """Exact bounded containment byte metadata, not evidence of a write ACK."""

    operation_sha256: str
    relative_name: str
    payload_sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        _hash(self.operation_sha256)
        _hash(self.payload_sha256)
        _integer(self.byte_count, 1, 16384)
        expected = f"tds-sqlclient-observe-containment-{self.operation_sha256}-{self.payload_sha256}.json"
        if type(self.relative_name) is not str or self.relative_name != expected:
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientObserveContainmentObservation:
    """One subject and nullable last receipt; no persistence/close authority."""

    operation_sha256: str
    receipt: SqlClientObserveContainmentReceipt | None

    def __post_init__(self) -> None:
        _hash(self.operation_sha256)
        if self.receipt is not None:
            _typed(self.receipt, SqlClientObserveContainmentReceipt)
            if self.receipt.operation_sha256 != self.operation_sha256:
                raise ValueError(ERROR)
