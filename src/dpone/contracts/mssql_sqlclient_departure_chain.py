"""Pure fixed six-record CREATE departure reconstruction; records are not ACKs.

Payload selectors preserve versioned wire contracts. The caller must still bind
actual original producers, acknowledged receipt objects, process reaping and EOF.
This module cannot execute a helper, persist evidence or settle a directory slot.
"""

from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_execution_evidence import (
    SqlClientDepartureExclusion,
    SqlClientDepartureLocalExit,
    SqlClientDepartureResultEvidence,
    encode_exclusion,
    encode_local_exit,
    encode_result_evidence,
)
from dpone.contracts.mssql_sqlclient_departure_ipc import (
    SqlClientDeparturePlan,
    SqlClientDepartureRequest,
    SqlClientDepartureResult,
    _startup,
    _typed,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import (
    SqlClientDeparturePlanV2,
    SqlClientDepartureRequestV2,
    SqlClientDepartureResultV2,
)
from dpone.contracts.mssql_sqlclient_departure_registration import (
    SqlClientDepartureCredentialIntent,
    SqlClientDepartureLaunchIntent,
    SqlClientDepartureRegistration,
    encode_credential_intent,
    encode_launch_intent,
    encode_registration,
)
from dpone.contracts.mssql_sqlclient_departure_versioned_payloads import (
    SqlClientDepartureCredentialIntentV2,
    SqlClientDepartureLaunchIntentV2,
    SqlClientDepartureResultEvidenceV2,
    encode_credential_intent_v2,
    encode_launch_intent_v2,
    encode_result_evidence_v2,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission, session_authority_digest
from dpone.contracts.mssql_sqlclient_observe_departure_codec import observe_departure_request_digest
from dpone.contracts.mssql_sqlclient_observe_departure_evidence import (
    SqlClientObserveDepartureCredentialIntent,
    SqlClientObserveDepartureExclusion,
    SqlClientObserveDepartureResultEvidence,
    encode_observe_departure_evidence,
    encode_observe_departure_launch_intent,
    validate_observe_exclusion_chain,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity


def departure_launch_payload(
    plan: SqlClientDeparturePlan | SqlClientDeparturePlanV2 | SqlClientObserveDeparturePlan,
    *,
    observer_admission: SqlClientObserverAdmission | None = None,
) -> bytes:
    if type(plan) is SqlClientObserveDeparturePlan:
        if observer_admission is None:
            raise ValueError("mssql_native.sqlclient_departure_version_invalid")
        return encode_observe_departure_launch_intent(plan, observer_admission=observer_admission)
    if type(plan) is SqlClientDeparturePlanV2:
        return encode_launch_intent_v2(SqlClientDepartureLaunchIntentV2(plan))
    if type(plan) is SqlClientDeparturePlan:
        return encode_launch_intent(SqlClientDepartureLaunchIntent(plan))
    raise ValueError("mssql_native.sqlclient_departure_version_invalid")


def departure_credential_payload(
    request: SqlClientDepartureRequest | SqlClientDepartureRequestV2 | SqlClientObserveDepartureRequest,
    registration_sha256: str,
    *,
    observer_admission: SqlClientObserverAdmission | None = None,
) -> bytes:
    if type(request) is SqlClientObserveDepartureRequest:
        _typed(request, SqlClientObserveDepartureRequest)
        if observer_admission is None:
            raise ValueError("mssql_native.sqlclient_departure_version_invalid")
        return encode_observe_departure_evidence(
            SqlClientObserveDepartureCredentialIntent(
                request.plan.helper_id, attempt_identity_digest(request.plan.attempt), registration_sha256, request
            ),
            kind=Kind.CREDENTIAL_INTENT,
            request=request,
            observer_admission=observer_admission,
        )
    subject = request.plan.helper_id, attempt_identity_digest(request.plan.attempt)
    if type(request) is SqlClientDepartureRequestV2:
        return encode_credential_intent_v2(SqlClientDepartureCredentialIntentV2(*subject, registration_sha256, request))
    if type(request) is SqlClientDepartureRequest:
        return encode_credential_intent(SqlClientDepartureCredentialIntent(*subject, registration_sha256, request))
    raise ValueError("mssql_native.sqlclient_departure_version_invalid")


def departure_result_payload(
    request: SqlClientDepartureRequest | SqlClientDepartureRequestV2 | SqlClientObserveDepartureRequest,
    result: SqlClientDepartureResult | SqlClientDepartureResultV2 | SqlClientObserveDepartureResult,
    credential_sha256: str,
    *,
    observer_admission: SqlClientObserverAdmission | None = None,
) -> bytes:
    if type(request) is SqlClientObserveDepartureRequest and type(result) is SqlClientObserveDepartureResult:
        _typed(request, SqlClientObserveDepartureRequest)
        if observer_admission is None:
            raise ValueError("mssql_native.sqlclient_departure_version_invalid")
        return encode_observe_departure_evidence(
            SqlClientObserveDepartureResultEvidence(
                request.plan.helper_id, attempt_identity_digest(request.plan.attempt), credential_sha256, result
            ),
            kind=Kind.RESULT,
            request=request,
            observer_admission=observer_admission,
        )
    subject = request.plan.helper_id, attempt_identity_digest(request.plan.attempt)
    if type(request) is SqlClientDepartureRequestV2 and type(result) is SqlClientDepartureResultV2:
        return encode_result_evidence_v2(
            SqlClientDepartureResultEvidenceV2(*subject, credential_sha256, result), request=request
        )
    if type(request) is SqlClientDepartureRequest and type(result) is SqlClientDepartureResult:
        return encode_result_evidence(
            SqlClientDepartureResultEvidence(*subject, credential_sha256, result), request=request
        )
    raise ValueError("mssql_native.sqlclient_departure_version_invalid")


def departure_registration_payload(
    plan: SqlClientDeparturePlan | SqlClientDeparturePlanV2 | SqlClientObserveDeparturePlan,
    startup: TdsCoordinatorStartup,
    launch_sha256: str,
) -> bytes:
    """The unchanged registration schema binds the admitted helper startup."""
    return encode_registration(
        SqlClientDepartureRegistration(
            plan.helper_id, attempt_identity_digest(plan.attempt), launch_sha256, startup, plan.admission_sha256
        )
    )


def departure_local_exit_payload(
    request: SqlClientDepartureRequest | SqlClientDepartureRequestV2 | SqlClientObserveDepartureRequest,
    local_exit: TdsChildExit,
    registration_sha256: str,
    result_sha256: str,
) -> bytes:
    """Only the successful helper exit belongs in this existing evidence kind."""
    return encode_local_exit(
        SqlClientDepartureLocalExit(
            request.plan.helper_id,
            attempt_identity_digest(request.plan.attempt),
            registration_sha256,
            result_sha256,
            local_exit,
        )
    )


def departure_exclusion_payload(
    plan: SqlClientDeparturePlan | SqlClientDeparturePlanV2, result_sha256: str, local_exit_sha256: str
) -> bytes:
    """Exact CREATE and helper links, without a claim that bytes were acknowledged."""
    return encode_exclusion(
        SqlClientDepartureExclusion(
            plan.helper_id,
            attempt_identity_digest(plan.attempt),
            coordinator_identity_digest(plan.create_operation),
            plan.create_result_sha256,
            plan.create_local_exit_sha256,
            result_sha256,
            local_exit_sha256,
        )
    )


def reconstruct_departure_chain(
    plan: SqlClientDeparturePlan | SqlClientDeparturePlanV2,
    request: SqlClientDepartureRequest | SqlClientDepartureRequestV2,
    startup: TdsCoordinatorStartup,
    result: SqlClientDepartureResult | SqlClientDepartureResultV2,
    local_exit: TdsChildExit,
) -> tuple[SqlClientDepartureEvidenceRecord, ...]:
    """Build fixed six canonical records from validated matching original versions."""
    if (type(plan), type(request), type(result)) not in (
        (SqlClientDeparturePlan, SqlClientDepartureRequest, SqlClientDepartureResult),
        (SqlClientDeparturePlanV2, SqlClientDepartureRequestV2, SqlClientDepartureResultV2),
    ):
        raise ValueError("mssql_native.sqlclient_departure_version_invalid")
    _typed(plan, type(plan))
    _typed(request, type(request))
    _typed(result, type(result))
    _startup(startup)
    _typed(local_exit, TdsChildExit)
    _typed(local_exit.identity, TdsProcessIdentity)
    if request.plan != plan or request.startup != startup or local_exit.identity != startup.process:
        raise ValueError("mssql_native.sqlclient_departure_chain_changed")
    subject = plan.helper_id, attempt_identity_digest(plan.attempt)
    launch = SqlClientDepartureEvidenceRecord(*subject, Kind.LAUNCH_INTENT, departure_launch_payload(plan))
    registration = SqlClientDepartureEvidenceRecord(
        *subject, Kind.REGISTRATION, departure_registration_payload(plan, startup, launch.receipt.payload_sha256)
    )
    credential = SqlClientDepartureEvidenceRecord(
        *subject, Kind.CREDENTIAL_INTENT, departure_credential_payload(request, registration.receipt.payload_sha256)
    )
    received = SqlClientDepartureEvidenceRecord(
        *subject, Kind.RESULT, departure_result_payload(request, result, credential.receipt.payload_sha256), request
    )
    exited = SqlClientDepartureEvidenceRecord(
        *subject,
        Kind.LOCAL_EXIT,
        departure_local_exit_payload(
            request, local_exit, registration.receipt.payload_sha256, received.receipt.payload_sha256
        ),
    )
    exclusion = SqlClientDepartureEvidenceRecord(
        *subject,
        Kind.EXCLUSION,
        departure_exclusion_payload(plan, received.receipt.payload_sha256, exited.receipt.payload_sha256),
    )
    return launch, registration, credential, received, exited, exclusion


def reconstruct_observe_departure_chain(
    plan: SqlClientObserveDeparturePlan,
    request: SqlClientObserveDepartureRequest,
    startup: TdsCoordinatorStartup,
    result: SqlClientObserveDepartureResult,
    local_exit: TdsChildExit,
    *,
    observer_admission: SqlClientObserverAdmission,
) -> tuple[SqlClientDepartureEvidenceRecord, ...]:
    """Predict all six OBSERVE records; real owner ACKs remain independently required."""
    _typed(plan, SqlClientObserveDeparturePlan)
    _typed(request, SqlClientObserveDepartureRequest)
    _typed(result, SqlClientObserveDepartureResult)
    _startup(startup)
    _typed(local_exit, TdsChildExit)
    _typed(local_exit.identity, TdsProcessIdentity)
    if request.plan != plan or request.startup != startup or local_exit.identity != startup.process:
        raise ValueError("mssql_native.sqlclient_departure_chain_changed")
    subject = plan.helper_id, attempt_identity_digest(plan.attempt)
    records: list[SqlClientDepartureEvidenceRecord] = []

    def put(kind: Kind, payload: bytes) -> str:
        record = SqlClientDepartureEvidenceRecord(
            *subject,
            kind,
            payload,
            observe_plan=plan if kind is Kind.LAUNCH_INTENT else None,
            observe_request=None if kind is Kind.LAUNCH_INTENT else request,
            observer_admission=observer_admission,
        )
        records.append(record)
        return record.receipt.payload_sha256

    launch = put(Kind.LAUNCH_INTENT, departure_launch_payload(plan, observer_admission=observer_admission))
    registration = put(Kind.REGISTRATION, departure_registration_payload(plan, startup, launch))
    credential = put(
        Kind.CREDENTIAL_INTENT,
        departure_credential_payload(request, registration, observer_admission=observer_admission),
    )
    received = put(
        Kind.RESULT, departure_result_payload(request, result, credential, observer_admission=observer_admission)
    )
    exited = put(Kind.LOCAL_EXIT, departure_local_exit_payload(request, local_exit, registration, received))
    exclusion = SqlClientObserveDepartureExclusion(
        *subject,
        coordinator_identity_digest(plan.observe_operation),
        plan.original_registration_artifact_sha256,
        plan.original_authority_artifact_sha256,
        plan.original_authority_sha256,
        plan.original_containment_artifact_sha256,
        plan.preparation_artifact_sha256,
        launch,
        registration,
        credential,
        observe_departure_request_digest(request),
        received,
        exited,
        session_authority_digest(result.departure.observer.authority).hex(),
    )
    validate_observe_exclusion_chain(
        exclusion,
        request=request,
        observer_admission=observer_admission,
        payloads=tuple(record.payload for record in records),
    )
    put(
        Kind.EXCLUSION,
        encode_observe_departure_evidence(
            exclusion, kind=Kind.EXCLUSION, request=request, observer_admission=observer_admission
        ),
    )
    return tuple(records)
