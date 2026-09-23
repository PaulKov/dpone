"""Versioned helper intent with independently configured observer expectations.

These records establish consistency, never authenticated execution or caller
origin. V1 field inheritance is private validation reuse, not substitutability
at a v1 protocol boundary. No observation is projected into v1 counts.
"""

from dataclasses import dataclass, fields

from dpone.contracts.mssql_sqlclient_create_departure_v2 import SqlClientCreateDepartureV2
from dpone.contracts.mssql_sqlclient_departure_ipc import (
    ERROR,
    SqlClientDeparturePlan,
    SqlClientDepartureRequest,
    _startup,
    _typed,
    validate_departure_request_binding,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_observer_incarnation import _context_records
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_validation import _hash

_FAILURES = (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError)


def _admission(value: SqlClientObserverAdmission) -> None:
    """Validate original nested scalars before any constructor may copy them."""
    if type(value) is not SqlClientObserverAdmission:
        raise ValueError(ERROR)
    _context_records(value)
    _typed(value, SqlClientObserverAdmission)


def _common_plan(value: "SqlClientDeparturePlanV2") -> SqlClientDeparturePlan:
    """Validation-only common fields, preserving original objects and scalars."""
    return SqlClientDeparturePlan(
        **{f.name: getattr(value, f.name) for f in fields(SqlClientDeparturePlan) if f.name != "schema"}
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDeparturePlanV2(SqlClientDeparturePlan):
    """Required observer expectation; only server/database must match CREATE."""

    observer_admission: SqlClientObserverAdmission
    schema: str = "dpone.sqlclient.departure-plan.v2"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.departure-plan.v2":
                raise ValueError
            _admission(self.creator_admission)
            _admission(self.observer_admission)
            _common_plan(self)
            if (self.observer_admission.server, self.observer_admission.database) != (
                self.creator_admission.server,
                self.creator_admission.database,
            ):
                raise ValueError
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDepartureRequestV2:
    """Exact v2 plan and startup; independently retained bindings are required."""

    plan: SqlClientDeparturePlanV2
    startup: TdsCoordinatorStartup
    schema: str = "dpone.sqlclient.departure-request.v2"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.departure-request.v2":
                raise ValueError
            _typed(self.plan, SqlClientDeparturePlanV2)
            _startup(self.startup)
            SqlClientDepartureRequest(plan=_common_plan(self.plan), startup=self.startup)
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDepartureResultV2:
    """Full v2 observation bound to exact request bytes, without exit/ACK claims."""

    request_sha256: str
    departure: SqlClientCreateDepartureV2
    schema: str = "dpone.sqlclient.departure-result.v2"

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != "dpone.sqlclient.departure-result.v2":
                raise ValueError
            _hash(self.request_sha256)
            _typed(self.departure, SqlClientCreateDepartureV2)
        except _FAILURES:
            raise ValueError(ERROR) from None


def validate_departure_request_binding_v2(
    request: SqlClientDepartureRequestV2,
    *,
    startup: TdsCoordinatorStartup,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
) -> None:
    """Compare independently retained launch facts, usable by parent and helper."""
    try:
        _typed(request, SqlClientDepartureRequestV2)
        validate_departure_request_binding(
            SqlClientDepartureRequest(plan=_common_plan(request.plan), startup=request.startup),
            startup=startup,
            admission_sha256=admission_sha256,
            startup_deadline=startup_deadline,
            operation_deadline=operation_deadline,
            max_address_space_bytes=max_address_space_bytes,
        )
    except _FAILURES:
        raise ValueError(ERROR) from None


def validate_departure_observer_binding_v2(
    request: SqlClientDepartureRequestV2, *, observer_admission: SqlClientObserverAdmission
) -> None:
    """Parent-only comparison against its separately retained configured admission.

    Passing the received plan field back as the expectation establishes no
    independent binding. The parent must retain its validated immutable copy.
    """
    try:
        _typed(request, SqlClientDepartureRequestV2)
        _admission(observer_admission)
        if request.plan.observer_admission != observer_admission:
            raise ValueError
    except _FAILURES:
        raise ValueError(ERROR) from None
