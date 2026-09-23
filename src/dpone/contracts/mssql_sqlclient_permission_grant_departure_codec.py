"""Canonical codecs and closed evidence validation for GRANT departure."""

from dataclasses import asdict
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_sqlclient_create_departure_v2_codec import (
    decode_create_departure_v2,
    encode_create_departure_v2,
)
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_sqlclient_grant_inventory import (
    SqlClientGrantPrincipal as SqlClientGrantPrincipal,
)
from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientPermissionRow
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
    session_authority_digest,
)
from dpone.contracts.mssql_sqlclient_observation import validate_catalog_admission as validate_catalog_admission
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from dpone.contracts.mssql_sqlclient_permission_grant import (
    decode_permission_grant_evidence,
    encode_permission_grant_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant_models import (
    ERROR,
    PermissionGrantDepartureEvidenceContext,
    SqlClientPermissionGrantDepartureCredentials,
    SqlClientPermissionGrantDeparturePlan,
    SqlClientPermissionGrantDepartureRequest,
    SqlClientPermissionGrantDepartureResult,
)
from dpone.contracts.mssql_sqlclient_permission_grant_models import (
    PermissionGrantDepartureCompletion as PermissionGrantDepartureCompletion,
)
from dpone.contracts.mssql_sqlclient_stage_identity import decode_stage_identity, encode_stage_identity
from dpone.contracts.mssql_sqlclient_stage_observation import SqlClientStageObservation
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, decode_startup, encode_startup
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds as deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape


def _admission(body: dict) -> SqlClientObserverAdmission:
    data = record_shape(SqlClientObserverAdmission, body)
    for name, cls in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
    ):
        data[name] = construct_record(cls, data[name])
    return SqlClientObserverAdmission(**data)


def encode_plan(value: SqlClientPermissionGrantDeparturePlan) -> bytes:
    if type(value) is not SqlClientPermissionGrantDeparturePlan:
        raise ValueError(ERROR)
    value.__post_init__()
    body = asdict(value)
    body.update(
        helper_id=str(value.helper_id),
        grant_evidence=strict_json_object(encode_permission_grant_evidence(value.grant_evidence)),
    )
    return canonical_json_bytes(body)


def decode_plan(payload: bytes) -> SqlClientPermissionGrantDeparturePlan:
    try:
        body = record_shape(SqlClientPermissionGrantDeparturePlan, strict_json_object(payload))
        body["helper_id"] = UUID(body["helper_id"])
        body["grant_evidence"] = decode_permission_grant_evidence(canonical_json_bytes(body["grant_evidence"]))
        for name in ("management_admission", "writer_admission"):
            body[name] = _admission(body[name])
        value = SqlClientPermissionGrantDeparturePlan(**body)
        if encode_plan(value) != payload:
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def encode_request(value: SqlClientPermissionGrantDepartureRequest) -> bytes:
    if type(value) is not SqlClientPermissionGrantDepartureRequest:
        raise ValueError(ERROR)
    value.__post_init__()
    return canonical_json_bytes(
        {
            "schema": value.schema,
            "plan": strict_json_object(encode_plan(value.plan)),
            "startup": strict_json_object(encode_startup(value.startup)),
        }
    )


def decode_request(payload: bytes) -> SqlClientPermissionGrantDepartureRequest:
    try:
        body = record_shape(SqlClientPermissionGrantDepartureRequest, strict_json_object(payload))
        body["plan"] = decode_plan(canonical_json_bytes(body["plan"]))
        body["startup"] = decode_startup(canonical_json_bytes(body["startup"]))
        value = SqlClientPermissionGrantDepartureRequest(**body)
        if encode_request(value) != payload:
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def _stage_body(value: SqlClientStageObservation) -> dict:
    return {
        "before": strict_json_object(encode_stage_identity(value.before)),
        "after": strict_json_object(encode_stage_identity(value.after)),
        "management_before": strict_json_object(encode_observer_incarnation(value.management_before)),
        "management_after": strict_json_object(encode_observer_incarnation(value.management_after)),
        "empty": value.empty,
        "metadata_permissions": list(value.metadata_permissions),
        "profile": value.profile,
    }


def _stage(body: dict) -> SqlClientStageObservation:
    data = record_shape(SqlClientStageObservation, body)
    for name in ("before", "after"):
        data[name] = decode_stage_identity(canonical_json_bytes(data[name]))
    for name in ("management_before", "management_after"):
        data[name] = decode_observer_incarnation(canonical_json_bytes(data[name]))
    data["metadata_permissions"] = tuple(data["metadata_permissions"])
    return SqlClientStageObservation(**data)


def validate_result(
    value: SqlClientPermissionGrantDepartureResult, request: SqlClientPermissionGrantDepartureRequest
) -> None:
    value.__post_init__()
    original = request.plan.grant_evidence
    permission_types = {"INSERT": "IN", "SELECT": "SL", "VIEW DEFINITION": "VW"}
    expected = tuple(
        (
            p.class_id,
            p.major_id,
            p.minor_id,
            p.grantee_id,
            p.grantor_id,
            permission_types[p.permission_name],
            p.permission_name,
            p.state,
        )
        for p in original.direct_permissions
    )
    actual = tuple(
        (
            p.class_id,
            p.major_id,
            p.minor_id,
            p.grantee_principal_id,
            p.grantor_principal_id,
            p.type,
            p.permission_name,
            p.state,
        )
        for p in value.direct_permissions
    )
    observers = (
        value.absence.observer,
        value.catalog_observer,
        value.stage.management_before,
        value.stage.management_after,
    )
    if (
        value.request_sha256 != sha256(encode_request(request)).hexdigest()
        or value.absence.original != original.authority.session
        or any(observer != observers[0] for observer in observers[1:])
        or observers[0].authority != value.absence.observer.authority
        or set(actual) != set(expected)
        or value.stage.before != original.request.stage
        or value.stage.after != original.request.stage
        or value.stage.empty != 1
    ):
        raise ValueError(ERROR)


def encode_result(
    value: SqlClientPermissionGrantDepartureResult, request: SqlClientPermissionGrantDepartureRequest
) -> bytes:
    validate_result(value, request)
    return canonical_json_bytes(
        {
            "schema": value.schema,
            "request_sha256": value.request_sha256,
            "absence": strict_json_object(encode_create_departure_v2(value.absence)),
            "catalog_observer": strict_json_object(encode_observer_incarnation(value.catalog_observer)),
            "direct_permissions": [asdict(row) for row in value.direct_permissions],
            "stage": _stage_body(value.stage),
        }
    )


def decode_result(
    payload: bytes, request: SqlClientPermissionGrantDepartureRequest
) -> SqlClientPermissionGrantDepartureResult:
    try:
        body = record_shape(SqlClientPermissionGrantDepartureResult, strict_json_object(payload))
        body["absence"] = decode_create_departure_v2(canonical_json_bytes(body["absence"]))
        body["catalog_observer"] = decode_observer_incarnation(canonical_json_bytes(body["catalog_observer"]))
        if type(body["direct_permissions"]) is not list:
            raise ValueError
        body["direct_permissions"] = tuple(
            SqlClientPermissionRow(**record_shape(SqlClientPermissionRow, row)) for row in body["direct_permissions"]
        )
        body["stage"] = _stage(body["stage"])
        value = SqlClientPermissionGrantDepartureResult(**body)
        if encode_result(value, request) != payload:
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def encode_credentials(value: SqlClientPermissionGrantDepartureCredentials) -> bytes:
    if type(value) is not SqlClientPermissionGrantDepartureCredentials:
        raise ValueError(ERROR)
    value.__post_init__()
    return canonical_json_bytes(
        {
            "schema": value.schema,
            "request": strict_json_object(encode_request(value.request)),
            "connection_material": asdict(value.connection_material),
            "session_nonce": value.session_nonce.hex(),
        }
    )


def decode_credentials(
    payload: bytes,
    *,
    startup: TdsCoordinatorStartup,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
) -> SqlClientPermissionGrantDepartureCredentials:
    try:
        body = record_shape(SqlClientPermissionGrantDepartureCredentials, strict_json_object(payload))
        body["request"] = decode_request(canonical_json_bytes(body["request"]))
        body["connection_material"] = construct_record(TdsConnectionMaterial, body["connection_material"])
        body["session_nonce"] = bytes.fromhex(body["session_nonce"])
        value = SqlClientPermissionGrantDepartureCredentials(**body)
        plan = value.request.plan
        if (
            value.request.startup != startup
            or plan.admission_sha256 != admission_sha256
            or plan.startup_deadline != startup_deadline
            or plan.operation_deadline != operation_deadline
            or plan.max_address_space_bytes != max_address_space_bytes
            or encode_credentials(value) != payload
        ):
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def evidence_payload(
    plan: SqlClientPermissionGrantDeparturePlan,
    kind: Kind,
    *,
    startup: TdsCoordinatorStartup | None,
    request: SqlClientPermissionGrantDepartureRequest | None,
    result: SqlClientPermissionGrantDepartureResult | None,
    local_exit: TdsChildExit | None,
    previous_sha256: tuple[str, ...],
) -> bytes:
    if type(plan) is not SqlClientPermissionGrantDeparturePlan or type(kind) is not Kind:
        raise ValueError(ERROR)
    for digest in previous_sha256:
        _hash(digest)
    body: dict[str, object] = {
        "schema": "dpone.sqlclient.permission-grant-departure-evidence.v1",
        "kind": kind.value,
        "helper_id": str(plan.helper_id),
        "attempt_sha256": attempt_identity_digest(plan.attempt),
        "previous_sha256": list(previous_sha256),
        "plan_sha256": sha256(encode_plan(plan)).hexdigest(),
    }
    if startup is not None:
        body["startup"] = strict_json_object(encode_startup(startup))
    # The retained-evidence runner binds the request before REGISTRATION so a
    # reentrant callback cannot replace it.  REGISTRATION still records only
    # startup identity; request authority first enters the durable chain at
    # CREDENTIAL_INTENT.
    if request is not None and kind not in (Kind.LAUNCH_INTENT, Kind.REGISTRATION):
        body["request_sha256"] = sha256(encode_request(request)).hexdigest()
    if result is not None:
        if request is None:
            raise ValueError(ERROR)
        body["result_sha256"] = sha256(encode_result(result, request)).hexdigest()
        body["authority_sha256"] = session_authority_digest(result.catalog_observer.authority).hex()
    if local_exit is not None:
        body["local_exit"] = asdict(local_exit)
    return canonical_json_bytes(body)


def evidence_subject(payload: bytes, kind: Kind, context: PermissionGrantDepartureEvidenceContext) -> tuple[UUID, str]:
    if type(context) is not PermissionGrantDepartureEvidenceContext:
        raise ValueError(ERROR)
    body, plan = strict_json_object(payload), context.plan
    required = {"schema", "kind", "helper_id", "attempt_sha256", "previous_sha256", "plan_sha256"}
    optional = {"startup", "request_sha256", "result_sha256", "authority_sha256", "local_exit"}
    expected = {
        Kind.LAUNCH_INTENT: set(),
        Kind.REGISTRATION: {"startup"},
        Kind.CREDENTIAL_INTENT: {"startup", "request_sha256"},
        Kind.RESULT: {"startup", "request_sha256", "result_sha256", "authority_sha256"},
        Kind.LOCAL_EXIT: {"startup", "request_sha256", "result_sha256", "authority_sha256", "local_exit"},
        Kind.EXCLUSION: {"startup", "request_sha256", "result_sha256", "authority_sha256", "local_exit"},
    }[kind]
    if (
        not required <= set(body)
        or not set(body) <= required | optional
        or body["schema"] != "dpone.sqlclient.permission-grant-departure-evidence.v1"
        or body["kind"] != kind.value
        or body["helper_id"] != str(plan.helper_id)
        or body["attempt_sha256"] != attempt_identity_digest(plan.attempt)
        or body["plan_sha256"] != sha256(encode_plan(plan)).hexdigest()
        or type(body["previous_sha256"]) is not list
        or len(body["previous_sha256"]) != list(Kind).index(kind)
        or set(body) - required != expected
    ):
        raise ValueError(ERROR)
    for digest in (
        *body["previous_sha256"],
        *(body[name] for name in expected & {"request_sha256", "result_sha256", "authority_sha256"}),
    ):
        _hash(digest)
    if "startup" in body:
        decode_startup(canonical_json_bytes(body["startup"]))
    return plan.helper_id, attempt_identity_digest(plan.attempt)
