"""Closed six-kind helper evidence dispatch with validation before byte hashing."""

from dataclasses import dataclass, field
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    ERROR,
    SqlClientDepartureEvidenceReceipt,
    _require_subject,
    evidence_limit,
    evidence_name,
    require_payload,
)
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind as SqlClientDepartureEvidenceKind,
)
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceObservation as SqlClientDepartureEvidenceObservation,
)
from dpone.contracts.mssql_sqlclient_departure_execution_evidence import (
    decode_exclusion,
    decode_local_exit,
    decode_result_evidence,
)
from dpone.contracts.mssql_sqlclient_departure_ipc import SqlClientDepartureRequest
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import SqlClientDepartureRequestV2
from dpone.contracts.mssql_sqlclient_departure_registration import (
    decode_credential_intent,
    decode_launch_intent,
    decode_registration,
)
from dpone.contracts.mssql_sqlclient_departure_versioned_payloads import (
    decode_credential_intent_v2,
    decode_launch_intent_v2,
    decode_result_evidence_v2,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_observe_departure import (
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
)
from dpone.contracts.mssql_sqlclient_observe_departure_evidence import (
    decode_observe_departure_evidence,
    decode_observe_departure_launch_intent,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
    PermissionGrantDepartureEvidenceContext,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
    evidence_subject as permission_grant_evidence_subject,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import (
    RestrictedWriterDepartureEvidenceContext,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement_codec import (
    evidence_subject as restricted_writer_evidence_subject,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.strict_json import strict_json_object


@dataclass(frozen=True, slots=True)
class SqlClientDepartureEvidenceRecord:
    """Bounded canonical content, without producer/ACK/ordering authentication.

    CREATE RESULT retains its independently supplied nonsecret request. OBSERVE
    retains a plan before launch or the actual request later, plus admission.
    Payload and context are omitted from repr. Receipts describe exact wrapper
    bytes and cannot establish that a predecessor artifact was acknowledged.
    """

    helper_id: UUID
    attempt_sha256: str
    kind: Kind
    payload: bytes = field(repr=False)
    result_context: SqlClientDepartureRequest | SqlClientDepartureRequestV2 | None = field(default=None, repr=False)

    observe_plan: SqlClientObserveDeparturePlan | None = field(default=None, kw_only=True, repr=False)
    observe_request: SqlClientObserveDepartureRequest | None = field(default=None, kw_only=True, repr=False)
    observer_admission: SqlClientObserverAdmission | None = field(default=None, kw_only=True, repr=False)
    permission_grant_context: PermissionGrantDepartureEvidenceContext | None = field(
        default=None, kw_only=True, repr=False
    )
    restricted_writer_context: RestrictedWriterDepartureEvidenceContext | None = field(
        default=None, kw_only=True, repr=False
    )

    def _observe_subject(self) -> tuple[UUID, str]:
        """Validate held expectations; content does not authenticate their origin.

        Launch precedes startup. Later phases require the actual request; the
        parent separately authenticates the five predecessor ACKs for EXCLUSION.
        """
        if self.result_context is not None or type(self.observer_admission) is not SqlClientObserverAdmission:
            raise ValueError(ERROR)
        if self.kind is Kind.LAUNCH_INTENT:
            if type(self.observe_plan) is not SqlClientObserveDeparturePlan or self.observe_request is not None:
                raise ValueError(ERROR)
            plan = decode_observe_departure_launch_intent(
                self.payload, plan=self.observe_plan, observer_admission=self.observer_admission
            )
        else:
            if type(self.observe_request) is not SqlClientObserveDepartureRequest or self.observe_plan is not None:
                raise ValueError(ERROR)
            decode_observe_departure_evidence(
                self.payload, kind=self.kind, request=self.observe_request, observer_admission=self.observer_admission
            )
            plan = self.observe_request.plan
        return plan.helper_id, attempt_identity_digest(plan.attempt)

    def __post_init__(self) -> None:
        try:
            _require_subject(self.helper_id, self.attempt_sha256)
            evidence_limit(self.kind)
            require_payload(self.payload, self.kind)
            if self.restricted_writer_context is not None:
                if self.permission_grant_context is not None or any(
                    value is not None
                    for value in (self.result_context, self.observe_plan, self.observe_request, self.observer_admission)
                ):
                    raise ValueError
                subject = restricted_writer_evidence_subject(self.payload, self.kind, self.restricted_writer_context)
            elif self.permission_grant_context is not None:
                if any(
                    value is not None for value in (self.observe_plan, self.observe_request, self.observer_admission)
                ):
                    raise ValueError
                subject = permission_grant_evidence_subject(self.payload, self.kind, self.permission_grant_context)
            elif any(value is not None for value in (self.observe_plan, self.observe_request, self.observer_admission)):
                subject = self._observe_subject()
            elif self.kind is Kind.RESULT:
                if type(self.result_context) is SqlClientDepartureRequestV2:
                    result_v2 = decode_result_evidence_v2(self.payload, request=self.result_context)
                    subject = result_v2.helper_id, result_v2.attempt_sha256
                elif type(self.result_context) is SqlClientDepartureRequest:
                    result = decode_result_evidence(self.payload, request=self.result_context)
                    subject = result.helper_id, result.attempt_sha256
                else:
                    raise ValueError
            else:
                if self.result_context is not None:
                    raise ValueError
                if self.kind is Kind.LAUNCH_INTENT:
                    if (
                        strict_json_object(self.payload).get("schema")
                        == "dpone.sqlclient.departure-launch-intent-evidence.v2"
                    ):
                        launch_v2 = decode_launch_intent_v2(self.payload)
                        subject = launch_v2.plan.helper_id, attempt_identity_digest(launch_v2.plan.attempt)
                    else:
                        launch = decode_launch_intent(self.payload)
                        subject = launch.plan.helper_id, attempt_identity_digest(launch.plan.attempt)
                elif self.kind is Kind.REGISTRATION:
                    registration = decode_registration(self.payload)
                    subject = registration.helper_id, registration.attempt_sha256
                elif self.kind is Kind.CREDENTIAL_INTENT:
                    if (
                        strict_json_object(self.payload).get("schema")
                        == "dpone.sqlclient.departure-credential-intent-evidence.v2"
                    ):
                        credential_v2 = decode_credential_intent_v2(self.payload)
                        subject = credential_v2.helper_id, credential_v2.attempt_sha256
                    else:
                        credential = decode_credential_intent(self.payload)
                        subject = credential.helper_id, credential.attempt_sha256
                elif self.kind is Kind.LOCAL_EXIT:
                    local = decode_local_exit(self.payload)
                    subject = local.helper_id, local.attempt_sha256
                else:
                    exclusion = decode_exclusion(self.payload)
                    subject = exclusion.helper_id, exclusion.attempt_sha256
            if subject != (self.helper_id, self.attempt_sha256):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError, RecursionError, UnicodeError):
            raise ValueError(ERROR) from None

    @property
    def receipt(self) -> SqlClientDepartureEvidenceReceipt:
        """Repeat full validation before hashing exact canonical wrapper bytes."""
        self.__post_init__()
        digest = sha256(self.payload).hexdigest()
        return SqlClientDepartureEvidenceReceipt(
            self.helper_id,
            self.attempt_sha256,
            self.kind,
            evidence_name(self.helper_id, self.attempt_sha256, self.kind, digest),
            digest,
            len(self.payload),
        )
