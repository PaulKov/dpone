"""Canonical wire and evidence codecs for P9b restricted-writer departure."""

from dataclasses import asdict
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantPrincipal, SqlClientPermissionRow
from dpone.contracts.mssql_sqlclient_observation import session_authority_digest
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from dpone.contracts.mssql_sqlclient_permission_grant import (
    decode_permission_grant_evidence,
    encode_permission_grant_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
    _admission,
    _stage,
    _stage_body,
)
from dpone.contracts.mssql_sqlclient_restricted_session_departure_codec import (
    decode_restricted_session_departure,
    encode_restricted_session_departure,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import (
    ERROR,
    RestrictedWriterDepartureCredentials,
    RestrictedWriterDepartureEvidenceContext,
    RestrictedWriterDeparturePlan,
    RestrictedWriterDepartureRequest,
    RestrictedWriterDepartureResult,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import (
    RestrictedWriterDepartureCompletion as RestrictedWriterDepartureCompletion,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import (
    RestrictedWriterSettlementOperations as RestrictedWriterSettlementOperations,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_codec import (
    decode_verify_request,
    decode_verify_result,
    encode_verify_request,
    encode_verify_result,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup, encode_startup
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

# Two independently bounded 128 KiB stage identities, two 16 KiB management
# incarnations, one 16 KiB departure, one 16 KiB catalog incarnation, and the
# fixed principal/permission envelope fit below this closed 512 KiB frame.
RESULT_FRAME_LIMIT = 512 * 1024


def encode_plan(value: RestrictedWriterDeparturePlan) -> bytes:
    try:
        if type(value) is not RestrictedWriterDeparturePlan:
            raise ValueError
        value.__post_init__()
        return canonical_json_bytes(
            {
                "schema": value.schema,
                "helper_id": str(value.helper_id),
                "grant_evidence": strict_json_object(encode_permission_grant_evidence(value.grant_evidence)),
                "verify_request": strict_json_object(encode_verify_request(value.verify_request)),
                "verify_result": strict_json_object(encode_verify_result(value.verify_result)),
                "management_admission": asdict(value.management_admission),
                "writer_admission": asdict(value.writer_admission),
                "implementation_sha256": value.implementation_sha256,
                "package_root": value.package_root,
                "admission_sha256": value.admission_sha256,
                "startup_deadline": value.startup_deadline,
                "operation_deadline": value.operation_deadline,
                "max_address_space_bytes": value.max_address_space_bytes,
            }
        )
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def decode_plan(payload: bytes) -> RestrictedWriterDeparturePlan:
    try:
        body = record_shape(RestrictedWriterDeparturePlan, strict_json_object(payload))
        body["helper_id"] = UUID(body["helper_id"])
        body["grant_evidence"] = decode_permission_grant_evidence(canonical_json_bytes(body["grant_evidence"]))
        body["verify_request"] = decode_verify_request(canonical_json_bytes(body["verify_request"]))
        body["verify_result"] = decode_verify_result(canonical_json_bytes(body["verify_result"]))
        body["management_admission"] = _admission(body["management_admission"])
        body["writer_admission"] = _admission(body["writer_admission"])
        result = RestrictedWriterDeparturePlan(**body)
        if encode_plan(result) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def encode_request(value: RestrictedWriterDepartureRequest) -> bytes:
    try:
        if type(value) is not RestrictedWriterDepartureRequest:
            raise ValueError
        value.__post_init__()
        return canonical_json_bytes(
            {
                "schema": value.schema,
                "plan": strict_json_object(encode_plan(value.plan)),
                "startup": strict_json_object(encode_startup(value.startup)),
            }
        )
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def decode_request(payload: bytes) -> RestrictedWriterDepartureRequest:
    try:
        body = strict_json_object(payload)
        if set(body) != {"schema", "plan", "startup"}:
            raise ValueError
        result = RestrictedWriterDepartureRequest(
            plan=decode_plan(canonical_json_bytes(body["plan"])),
            startup=decode_startup(canonical_json_bytes(body["startup"])),
            schema=body["schema"],
        )
        if encode_request(result) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def validate_result(value: RestrictedWriterDepartureResult, request: RestrictedWriterDepartureRequest) -> None:
    value.__post_init__()
    request.__post_init__()
    original = request.plan.grant_evidence
    permission_types = {"INSERT": "IN", "SELECT": "SL", "VIEW DEFINITION": "VW"}
    expected = {
        (
            row.class_id,
            row.major_id,
            row.minor_id,
            row.grantee_id,
            row.grantor_id,
            permission_types[row.permission_name],
            row.permission_name,
            row.state,
        )
        for row in original.direct_permissions
    }
    actual = {
        (
            row.class_id,
            row.major_id,
            row.minor_id,
            row.grantee_principal_id,
            row.grantor_principal_id,
            row.type,
            row.permission_name,
            row.state,
        )
        for row in value.direct_permissions
    }
    observers = (
        value.absence.observer,
        value.catalog_observer,
        value.stage.management_before,
        value.stage.management_after,
    )
    if (
        value.request_sha256 != sha256(encode_request(request)).hexdigest()
        or value.absence.original != request.plan.verify_result.opening.session
        or any(observer != observers[0] for observer in observers[1:])
        or len(value.principals) != 2
        or value.principals.count(
            SqlClientGrantPrincipal(
                original.request.writer.principal_id,
                original.request.writer.name,
                original.request.writer.sid,
                "SQL_USER",
                "INSTANCE",
            )
        )
        != 1
        or sum(
            row.principal_id == 0
            and row.name == "public"
            and row.type_desc == "DATABASE_ROLE"
            and row.authentication_type_desc == "NONE"
            for row in value.principals
        )
        != 1
        or expected != actual
        or value.stage.before != request.plan.verify_request.stage
        or value.stage.after != request.plan.verify_request.stage
        or value.stage.empty != 1
        or value.row_count != 0
    ):
        raise ValueError(ERROR)


def encode_result(value: RestrictedWriterDepartureResult, request: RestrictedWriterDepartureRequest) -> bytes:
    validate_result(value, request)
    payload = canonical_json_bytes(
        {
            "schema": value.schema,
            "request_sha256": value.request_sha256,
            "absence": strict_json_object(encode_restricted_session_departure(value.absence)),
            "catalog_observer": strict_json_object(encode_observer_incarnation(value.catalog_observer)),
            "principals": [asdict(row) for row in value.principals],
            "direct_permissions": [asdict(row) for row in value.direct_permissions],
            "stage": _stage_body(value.stage),
            "row_count": value.row_count,
        }
    )
    if not 0 < len(payload) <= RESULT_FRAME_LIMIT:
        raise ValueError(ERROR)
    return payload


def decode_result(payload: bytes, request: RestrictedWriterDepartureRequest) -> RestrictedWriterDepartureResult:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= RESULT_FRAME_LIMIT:
            raise ValueError
        body = strict_json_object(payload)
        body = record_shape(RestrictedWriterDepartureResult, body)
        result = RestrictedWriterDepartureResult(
            request_sha256=body["request_sha256"],
            absence=decode_restricted_session_departure(canonical_json_bytes(body["absence"])),
            catalog_observer=decode_observer_incarnation(canonical_json_bytes(body["catalog_observer"])),
            principals=tuple(construct_record(SqlClientGrantPrincipal, row) for row in body["principals"]),
            direct_permissions=tuple(
                construct_record(SqlClientPermissionRow, row) for row in body["direct_permissions"]
            ),
            stage=_stage(body["stage"]),
            row_count=body["row_count"],
            schema=body["schema"],
        )
        if encode_result(result, request) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def encode_credentials(value: RestrictedWriterDepartureCredentials) -> bytes:
    if type(value) is not RestrictedWriterDepartureCredentials:
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


def decode_credentials(payload: bytes) -> RestrictedWriterDepartureCredentials:
    try:
        body = strict_json_object(payload)
        result = RestrictedWriterDepartureCredentials(
            request=decode_request(canonical_json_bytes(body["request"])),
            connection_material=construct_record(TdsConnectionMaterial, body["connection_material"]),
            session_nonce=bytes.fromhex(body["session_nonce"]),
            schema=body["schema"],
        )
        if encode_credentials(result) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def evidence_payload(
    plan: RestrictedWriterDeparturePlan,
    kind: Kind,
    *,
    startup=None,
    request: RestrictedWriterDepartureRequest | None = None,
    result: RestrictedWriterDepartureResult | None = None,
    local_exit: TdsChildExit | None = None,
    previous_sha256: tuple[str, ...] = (),
) -> bytes:
    for digest in previous_sha256:
        _hash(digest)
    body: dict[str, object] = {
        "schema": "dpone.sqlclient.restricted-writer-departure-evidence.v1",
        "kind": kind.value,
        "helper_id": str(plan.helper_id),
        "attempt_sha256": attempt_identity_digest(plan.attempt),
        "previous_sha256": list(previous_sha256),
        "plan_sha256": sha256(encode_plan(plan)).hexdigest(),
    }
    if startup is not None:
        body["startup"] = strict_json_object(encode_startup(startup))
    if request is not None:
        body["request_sha256"] = sha256(encode_request(request)).hexdigest()
    if result is not None:
        if request is None:
            raise ValueError(ERROR)
        body["result_sha256"] = sha256(encode_result(result, request)).hexdigest()
        body["authority_sha256"] = session_authority_digest(result.catalog_observer.authority).hex()
    if local_exit is not None:
        body["local_exit"] = asdict(local_exit)
    return canonical_json_bytes(body)


def evidence_subject(payload: bytes, kind: Kind, context: RestrictedWriterDepartureEvidenceContext):
    """Validate one exact P9b evidence payload and return its fixed subject."""
    try:
        if type(context) is not RestrictedWriterDepartureEvidenceContext or type(kind) is not Kind:
            raise ValueError
        context.__post_init__()
        body = strict_json_object(payload)
        index = tuple(Kind).index(kind)
        expected = {
            "schema",
            "kind",
            "helper_id",
            "attempt_sha256",
            "previous_sha256",
            "plan_sha256",
        }
        if index >= 1:
            expected.add("startup")
        if index >= 2:
            expected.add("request_sha256")
        if index >= 3:
            expected.update(("result_sha256", "authority_sha256"))
        if index >= 4:
            expected.add("local_exit")
        plan = context.plan
        previous = body.get("previous_sha256")
        if (
            set(body) != expected
            or body["schema"] != "dpone.sqlclient.restricted-writer-departure-evidence.v1"
            or body["kind"] != kind.value
            or body["helper_id"] != str(plan.helper_id)
            or body["attempt_sha256"] != attempt_identity_digest(plan.attempt)
            or body["plan_sha256"] != sha256(encode_plan(plan)).hexdigest()
            or type(previous) is not list
            or len(previous) != index
        ):
            raise ValueError
        for digest in previous:
            _hash(digest)
        if canonical_json_bytes(body) != payload:
            raise ValueError
        return plan.helper_id, attempt_identity_digest(plan.attempt)
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        raise ValueError(ERROR) from None


def remote_settlement_payload(completion) -> bytes:
    completion.__post_init__()
    return canonical_json_bytes(
        {
            "schema": "dpone.sqlclient.restricted-writer-remote-settlement.v1",
            "result_sha256": completion.receipts[3].payload_sha256,
            "local_exit_sha256": completion.receipts[4].payload_sha256,
            "exclusion_sha256": completion.receipts[5].payload_sha256,
            "authority_sha256": session_authority_digest(completion.result.catalog_observer.authority).hex(),
        }
    )
