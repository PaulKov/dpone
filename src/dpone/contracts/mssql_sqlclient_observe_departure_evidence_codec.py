"""Canonical payload codecs for OBSERVE departure evidence."""

from dataclasses import asdict
from hashlib import sha256
from typing import Any

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceReceipt,
    evidence_name,
    require_payload,
)
from dpone.contracts.mssql_sqlclient_departure_execution_evidence import (
    SqlClientDepartureLocalExit,
    decode_local_exit,
    encode_local_exit,
)
from dpone.contracts.mssql_sqlclient_departure_ipc import _typed
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import _admission
from dpone.contracts.mssql_sqlclient_departure_registration import (
    SqlClientDepartureRegistration,
    decode_registration,
    encode_registration,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission, session_authority_digest
from dpone.contracts.mssql_sqlclient_observe_departure import (
    ERROR,
    FAILURES,
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
)
from dpone.contracts.mssql_sqlclient_observe_departure_codec import (
    decode_observe_departure_plan,
    decode_observe_departure_request,
    decode_observe_departure_result,
    encode_observe_departure_plan,
    encode_observe_departure_request,
    encode_observe_departure_result,
)
from dpone.contracts.mssql_sqlclient_observe_departure_evidence_model import (
    SqlClientObserveDepartureCredentialIntent,
    SqlClientObserveDepartureExclusion,
    SqlClientObserveDepartureResultEvidence,
    _exclusion_binding,
    _subject,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, record_shape


def encode_observe_departure_launch_intent(
    plan: SqlClientObserveDeparturePlan, *, observer_admission: SqlClientObserverAdmission
) -> bytes:
    """Encode intent before spawn, without inventing a helper startup/request."""
    try:
        _typed(plan, SqlClientObserveDeparturePlan)
        _admission(observer_admission)
        if observer_admission != plan.observer_admission:
            raise ValueError
        payload = canonical_json_bytes(
            dict(
                schema="dpone.sqlclient.observe-departure-launch-intent-evidence.v1",
                plan=strict_json_object(encode_observe_departure_plan(plan)),
            )
        )
        require_payload(payload, Kind.LAUNCH_INTENT)
        return payload
    except FAILURES:
        raise ValueError(ERROR) from None


def decode_observe_departure_launch_intent(
    payload: bytes, *, plan: SqlClientObserveDeparturePlan, observer_admission: SqlClientObserverAdmission
) -> SqlClientObserveDeparturePlan:
    """Require canonical bytes matching the independently retained original plan."""
    try:
        expected = encode_observe_departure_launch_intent(plan, observer_admission=observer_admission)
        require_payload(payload, Kind.LAUNCH_INTENT)
        data = strict_json_object(payload)
        if data.pop("schema", None) != "dpone.sqlclient.observe-departure-launch-intent-evidence.v1" or set(data) != {
            "plan"
        }:
            raise ValueError
        value = decode_observe_departure_plan(canonical_json_bytes(data["plan"]))
        if value != plan or payload != expected:
            raise ValueError
        return value
    except FAILURES:
        raise ValueError(ERROR) from None


def observe_departure_launch_intent_receipt(
    payload: bytes, *, plan: SqlClientObserveDeparturePlan, observer_admission: SqlClientObserverAdmission
) -> SqlClientDepartureEvidenceReceipt:
    """Describe validated intent bytes; never claim an actor write ACK."""
    decode_observe_departure_launch_intent(payload, plan=plan, observer_admission=observer_admission)
    digest = sha256(payload).hexdigest()
    helper, attempt = plan.helper_id, attempt_identity_digest(plan.attempt)
    return SqlClientDepartureEvidenceReceipt(
        helper,
        attempt,
        Kind.LAUNCH_INTENT,
        evidence_name(helper, attempt, Kind.LAUNCH_INTENT, digest),
        digest,
        len(payload),
    )


def encode_observe_departure_evidence(
    value: Any, *, kind: Kind, request: SqlClientObserveDepartureRequest, observer_admission: SqlClientObserverAdmission
) -> bytes:
    """Closed kind dispatch under separately retained request and verifier facts."""
    try:
        _typed(request, SqlClientObserveDepartureRequest)
        _admission(observer_admission)
        if type(kind) is not Kind or observer_admission != request.plan.observer_admission:
            raise ValueError
        if kind is Kind.LAUNCH_INTENT:
            _typed(value, SqlClientObserveDeparturePlan)
            if value != request.plan:
                raise ValueError
            return encode_observe_departure_launch_intent(value, observer_admission=observer_admission)
        elif kind is Kind.REGISTRATION:
            _typed(value, SqlClientDepartureRegistration)
            _subject(value, request)
            if value.startup != request.startup or value.admission_sha256 != request.plan.admission_sha256:
                raise ValueError
            return encode_registration(value)
        elif kind is Kind.CREDENTIAL_INTENT:
            _typed(value, SqlClientObserveDepartureCredentialIntent)
            _subject(value, request)
            if value.request != request:
                raise ValueError
            body = dict(
                schema="dpone.sqlclient.observe-departure-credential-intent-evidence.v1",
                helper_id=str(value.helper_id),
                attempt_sha256=value.attempt_sha256,
                registration_sha256=value.registration_sha256,
                request=strict_json_object(encode_observe_departure_request(value.request)),
            )
        elif kind is Kind.RESULT:
            _typed(value, SqlClientObserveDepartureResultEvidence)
            _subject(value, request)
            body = dict(
                schema="dpone.sqlclient.observe-departure-result-evidence.v1",
                helper_id=str(value.helper_id),
                attempt_sha256=value.attempt_sha256,
                credential_intent_sha256=value.credential_intent_sha256,
                result=strict_json_object(
                    encode_observe_departure_result(
                        value.result, request=request, observer_admission=observer_admission
                    )
                ),
            )
        elif kind is Kind.LOCAL_EXIT:
            _typed(value, SqlClientDepartureLocalExit)
            _subject(value, request)
            if value.exit.identity != request.startup.process:
                raise ValueError
            return encode_local_exit(value)
        else:
            _typed(value, SqlClientObserveDepartureExclusion)
            _exclusion_binding(value, request)
            body = dict(
                asdict(value),
                schema="dpone.sqlclient.observe-departure-exclusion-evidence.v1",
                helper_id=str(value.helper_id),
            )
        payload = canonical_json_bytes(body)
        require_payload(payload, kind)
        return payload
    except FAILURES:
        raise ValueError(ERROR) from None


def decode_observe_departure_evidence(
    payload: bytes,
    *,
    kind: Kind,
    request: SqlClientObserveDepartureRequest,
    observer_admission: SqlClientObserverAdmission,
) -> Any:
    """Unknown or CREATE-specific outer schemas never enter OBSERVE dispatch."""
    try:
        require_payload(payload, kind)
        data = strict_json_object(payload)
        value: Any
        if kind is Kind.REGISTRATION:
            value = decode_registration(payload)
        elif kind is Kind.LOCAL_EXIT:
            value = decode_local_exit(payload)
        else:
            schema = data.pop("schema", None)
            if kind is Kind.LAUNCH_INTENT:
                _typed(request, SqlClientObserveDepartureRequest)
                value = decode_observe_departure_launch_intent(
                    payload, plan=request.plan, observer_admission=observer_admission
                )
            elif kind is Kind.CREDENTIAL_INTENT:
                if schema != "dpone.sqlclient.observe-departure-credential-intent-evidence.v1":
                    raise ValueError
                data = record_shape(SqlClientObserveDepartureCredentialIntent, data)
                data["helper_id"] = canonical_uuid(data["helper_id"])
                data["request"] = decode_observe_departure_request(canonical_json_bytes(data["request"]))
                value = SqlClientObserveDepartureCredentialIntent(**data)
            elif kind is Kind.RESULT:
                if schema != "dpone.sqlclient.observe-departure-result-evidence.v1":
                    raise ValueError
                data = record_shape(SqlClientObserveDepartureResultEvidence, data)
                data["helper_id"] = canonical_uuid(data["helper_id"])
                data["result"] = decode_observe_departure_result(
                    canonical_json_bytes(data["result"]), request=request, observer_admission=observer_admission
                )
                value = SqlClientObserveDepartureResultEvidence(**data)
            elif kind is Kind.EXCLUSION:
                if schema != "dpone.sqlclient.observe-departure-exclusion-evidence.v1":
                    raise ValueError
                data = record_shape(SqlClientObserveDepartureExclusion, data)
                data["helper_id"] = canonical_uuid(data["helper_id"])
                value = SqlClientObserveDepartureExclusion(**data)
            else:
                raise ValueError
        if (
            encode_observe_departure_evidence(value, kind=kind, request=request, observer_admission=observer_admission)
            != payload
        ):
            raise ValueError
        return value
    except FAILURES:
        raise ValueError(ERROR) from None


def observe_departure_evidence_receipt(
    payload: bytes,
    *,
    kind: Kind,
    request: SqlClientObserveDepartureRequest,
    observer_admission: SqlClientObserverAdmission,
) -> SqlClientDepartureEvidenceReceipt:
    """Describe exact validated bytes; this is not an actor write acknowledgement."""
    decode_observe_departure_evidence(payload, kind=kind, request=request, observer_admission=observer_admission)
    digest = sha256(payload).hexdigest()
    helper = request.plan.helper_id
    attempt = attempt_identity_digest(request.plan.attempt)
    return SqlClientDepartureEvidenceReceipt(
        helper, attempt, kind, evidence_name(helper, attempt, kind, digest), digest, len(payload)
    )


def validate_observe_exclusion_chain(
    value: SqlClientObserveDepartureExclusion,
    *,
    request: SqlClientObserveDepartureRequest,
    observer_admission: SqlClientObserverAdmission,
    payloads: tuple[bytes, ...],
) -> None:
    """Rebuild five supplied canonical predecessor links, without claiming their ACKs.

    The producer must supply its own original acknowledged bytes in phase order.
    This pure comparison cannot authenticate the origin of caller-authored bytes.
    """
    try:
        _typed(value, SqlClientObserveDepartureExclusion)
        _typed(request, SqlClientObserveDepartureRequest)
        _admission(observer_admission)
        if observer_admission != request.plan.observer_admission:
            raise ValueError
        _exclusion_binding(value, request)
        if type(payloads) is not tuple or len(payloads) != 5:
            raise ValueError
        kinds = (Kind.LAUNCH_INTENT, Kind.REGISTRATION, Kind.CREDENTIAL_INTENT, Kind.RESULT, Kind.LOCAL_EXIT)
        decoded = []
        hashes = []
        for kind, payload in zip(kinds, payloads, strict=True):
            decoded.append(
                decode_observe_departure_evidence(
                    payload, kind=kind, request=request, observer_admission=observer_admission
                )
            )
            hashes.append(sha256(payload).hexdigest())
        launch, registration, credential, result, local = decoded
        if (
            registration.launch_intent_sha256 != hashes[0]
            or credential.registration_sha256 != hashes[1]
            or result.credential_intent_sha256 != hashes[2]
            or local.registration_sha256 != hashes[1]
            or local.result_sha256 != hashes[3]
            or (
                value.launch_intent_sha256,
                value.registration_sha256,
                value.credential_intent_sha256,
                value.result_sha256,
                value.local_exit_sha256,
            )
            != tuple(hashes)
            or value.verifier_authority_sha256
            != session_authority_digest(result.result.departure.observer.authority).hex()
        ):
            raise ValueError
    except FAILURES:
        raise ValueError(ERROR) from None
