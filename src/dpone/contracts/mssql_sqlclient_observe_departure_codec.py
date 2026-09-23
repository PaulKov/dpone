"""Closed canonical OBSERVE wire bodies, independent of private secret delivery.

Every encoder revalidates originals before projection. Result decoding requires
an independently held request and verifier admission; hashes authenticate no ACK.
"""

from dataclasses import asdict
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_create_departure_v2 import SqlClientCreateDepartureV2
from dpone.contracts.mssql_sqlclient_create_departure_v2_codec import _departure_body, _departure_from_body
from dpone.contracts.mssql_sqlclient_departure_ipc import _typed
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import _admission
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    ERROR,
    FAILURES,
    SqlClientObserveContainment,
    SqlClientObserveContainmentReceipt,
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
    _observation,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity, coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup, encode_startup
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_session import decode_session_identity, encode_session_identity
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsChildExit, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape, string_enum

PLAN_LIMIT = 65536
OBSERVATION_LIMIT = 16384
RESULT_LIMIT = 32768


def _bounded(payload: bytes, limit: int) -> None:
    if type(payload) is not bytes or not 0 < len(payload) <= limit:
        raise ValueError(ERROR)


def _operation_body(value: TdsCoordinatorIdentity) -> dict:
    return dict(asdict(value), operation_id=str(value.operation_id))


def _decode_operation(data: dict) -> TdsCoordinatorIdentity:
    data = record_shape(TdsCoordinatorIdentity, data)
    data["parent"] = construct_record(TdsAttemptIdentity, data["parent"])
    data["operation_id"] = canonical_uuid(data["operation_id"])
    data["command"] = string_enum(TdsCoordinatorCommand, data["command"])
    return TdsCoordinatorIdentity(**data)


def _decode_admission(data: dict) -> SqlClientObserverAdmission:
    data = record_shape(SqlClientObserverAdmission, data)
    for key, cls in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
    ):
        data[key] = construct_record(cls, data[key])
    return SqlClientObserverAdmission(**data)


def encode_observe_departure_plan(value: SqlClientObserveDeparturePlan) -> bytes:
    """Project only validated original operation and independently configured facts."""
    try:
        _typed(value, SqlClientObserveDeparturePlan)
        data = asdict(value)
        data.update(
            helper_id=str(value.helper_id),
            observe_operation=_operation_body(value.observe_operation),
            original=strict_json_object(encode_session_identity(value.original)),
            database=dict(asdict(value.database), database_guid=str(value.database.database_guid)),
        )
        payload = canonical_json_bytes(data)
        _bounded(payload, PLAN_LIMIT)
        return payload
    except FAILURES:
        raise ValueError(ERROR) from None


def decode_observe_departure_plan(payload: bytes) -> SqlClientObserveDeparturePlan:
    """Reject unknown fields and noncanonical scalars before any effect is possible."""
    try:
        _bounded(payload, PLAN_LIMIT)
        data = record_shape(SqlClientObserveDeparturePlan, strict_json_object(payload))
        data["helper_id"] = canonical_uuid(data["helper_id"])
        for key, cls in (
            ("attempt", TdsAttemptIdentity),
            ("ownership", TdsAttemptOwnership),
            ("observe_process", TdsProcessIdentity),
            ("principal", SqlClientDatabasePrincipal),
        ):
            data[key] = construct_record(cls, data[key])
        data["observe_operation"] = _decode_operation(data["observe_operation"])
        data["original"] = decode_session_identity(canonical_json_bytes(data["original"]))
        db = record_shape(TdsDatabaseObservation, data["database"])
        db["database_guid"] = canonical_uuid(db["database_guid"])
        data["database"] = TdsDatabaseObservation(**db)
        for key in ("management_admission", "observer_admission"):
            data[key] = _decode_admission(data[key])
        value = SqlClientObserveDeparturePlan(**data)
        if encode_observe_departure_plan(value) != payload:
            raise ValueError
        return value
    except FAILURES:
        raise ValueError(ERROR) from None


def encode_observe_departure_request(value: SqlClientObserveDepartureRequest) -> bytes:
    """The complete plan plus startup must fit the stricter request cap."""
    try:
        _typed(value, SqlClientObserveDepartureRequest)
        payload = canonical_json_bytes(
            dict(
                schema=value.schema,
                plan=strict_json_object(encode_observe_departure_plan(value.plan)),
                startup=strict_json_object(encode_startup(value.startup)),
            )
        )
        _bounded(payload, PLAN_LIMIT)
        return payload
    except FAILURES:
        raise ValueError(ERROR) from None


def decode_observe_departure_request(payload: bytes) -> SqlClientObserveDepartureRequest:
    """Decode only the nonsecret request; private credential schema is unsupported."""
    try:
        _bounded(payload, PLAN_LIMIT)
        data = record_shape(SqlClientObserveDepartureRequest, strict_json_object(payload))
        data["plan"] = decode_observe_departure_plan(canonical_json_bytes(data["plan"]))
        data["startup"] = decode_startup(canonical_json_bytes(data["startup"]))
        value = SqlClientObserveDepartureRequest(**data)
        if encode_observe_departure_request(value) != payload:
            raise ValueError
        return value
    except FAILURES:
        raise ValueError(ERROR) from None


def observe_departure_request_digest(request: SqlClientObserveDepartureRequest) -> str:
    """Hash exact canonical nonsecret request bytes, never a private envelope."""
    return sha256(encode_observe_departure_request(request)).hexdigest()


def encode_observe_departure_observation(value: SqlClientCreateDepartureV2) -> bytes:
    """Reuse the reviewed census body with the explicit OBSERVE outer schema."""
    try:
        _observation(value)
        payload = canonical_json_bytes(
            dict(schema="dpone.sqlclient.observe-departure-observation.v1", **_departure_body(value))
        )
        _bounded(payload, OBSERVATION_LIMIT)
        return payload
    except FAILURES:
        raise ValueError(ERROR) from None


def decode_observe_departure_observation(payload: bytes) -> SqlClientCreateDepartureV2:
    """Exact C/S/R/T/C/S types and census predicates remain shared with v2."""
    try:
        _bounded(payload, OBSERVATION_LIMIT)
        data = strict_json_object(payload)
        if data.pop("schema", None) != "dpone.sqlclient.observe-departure-observation.v1":
            raise ValueError
        value = _departure_from_body(data)
        if encode_observe_departure_observation(value) != payload:
            raise ValueError
        return value
    except FAILURES:
        raise ValueError(ERROR) from None


def make_observe_departure_result(
    request: SqlClientObserveDepartureRequest,
    departure: SqlClientCreateDepartureV2,
    *,
    observer_admission: SqlClientObserverAdmission,
) -> SqlClientObserveDepartureResult:
    """Bind original management facts and separately retained verifier expectation."""
    try:
        _typed(request, SqlClientObserveDepartureRequest)
        _observation(departure)
        _admission(observer_admission)
        plan = request.plan
        actual = departure.observer.authority
        if (
            (departure.original, departure.database, departure.admission, departure.principal)
            != (plan.original, plan.database, plan.management_admission, plan.principal)
            or observer_admission != plan.observer_admission
            or (actual.server, actual.database, actual.login, actual.transport)
            != (
                observer_admission.server,
                observer_admission.database,
                observer_admission.login,
                observer_admission.transport,
            )
        ):
            raise ValueError
        return SqlClientObserveDepartureResult(
            request_sha256=observe_departure_request_digest(request), departure=departure
        )
    except FAILURES:
        raise ValueError(ERROR) from None


def encode_observe_departure_result(
    value: SqlClientObserveDepartureResult,
    *,
    request: SqlClientObserveDepartureRequest,
    observer_admission: SqlClientObserverAdmission,
) -> bytes:
    """Result content cannot choose its own original request or verifier profile."""
    try:
        _typed(value, SqlClientObserveDepartureResult)
        if value != make_observe_departure_result(request, value.departure, observer_admission=observer_admission):
            raise ValueError
        payload = canonical_json_bytes(
            dict(
                schema=value.schema,
                request_sha256=value.request_sha256,
                departure=strict_json_object(encode_observe_departure_observation(value.departure)),
            )
        )
        _bounded(payload, RESULT_LIMIT)
        return payload
    except FAILURES:
        raise ValueError(ERROR) from None


def decode_observe_departure_result(
    payload: bytes, *, request: SqlClientObserveDepartureRequest, observer_admission: SqlClientObserverAdmission
) -> SqlClientObserveDepartureResult:
    """Decode under independently held expectations; shape is never origin proof."""
    try:
        _bounded(payload, RESULT_LIMIT)
        data = record_shape(SqlClientObserveDepartureResult, strict_json_object(payload))
        data["departure"] = decode_observe_departure_observation(canonical_json_bytes(data["departure"]))
        value = SqlClientObserveDepartureResult(**data)
        if encode_observe_departure_result(value, request=request, observer_admission=observer_admission) != payload:
            raise ValueError
        return value
    except FAILURES:
        raise ValueError(ERROR) from None


def encode_observe_containment(value: SqlClientObserveContainment, *, process: TdsProcessIdentity) -> bytes:
    """Require separately held original process; forced exit codes remain intact."""
    try:
        _typed(value, SqlClientObserveContainment)
        _typed(process, TdsProcessIdentity)
        if value.exit.identity != process:
            raise ValueError
        payload = canonical_json_bytes(dict(asdict(value), observe_operation=_operation_body(value.observe_operation)))
        _bounded(payload, 16384)
        return payload
    except FAILURES:
        raise ValueError(ERROR) from None


def decode_observe_containment(payload: bytes, *, process: TdsProcessIdentity) -> SqlClientObserveContainment:
    """Canonical original containment only, with no helper-success substitution."""
    try:
        _bounded(payload, 16384)
        data = record_shape(SqlClientObserveContainment, strict_json_object(payload))
        data["observe_operation"] = _decode_operation(data["observe_operation"])
        exit_data = record_shape(TdsChildExit, data["exit"])
        exit_data["identity"] = construct_record(TdsProcessIdentity, exit_data["identity"])
        data["exit"] = TdsChildExit(**exit_data)
        value = SqlClientObserveContainment(**data)
        if encode_observe_containment(value, process=process) != payload:
            raise ValueError
        return value
    except FAILURES:
        raise ValueError(ERROR) from None


def observe_containment_receipt(payload: bytes, *, process: TdsProcessIdentity) -> SqlClientObserveContainmentReceipt:
    """Describe canonical containment bytes only; persistence supplies the ACK."""
    value = decode_observe_containment(payload, process=process)
    operation = coordinator_identity_digest(value.observe_operation)
    digest = sha256(payload).hexdigest()
    return SqlClientObserveContainmentReceipt(
        operation, f"tds-sqlclient-observe-containment-{operation}-{digest}.json", digest, len(payload)
    )
