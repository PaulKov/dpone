"""Six finite OBSERVE helper payloads; no writer, actor, registry or ACK authority.

Registration/local-exit reuse exact existing codecs. Every other schema is
explicitly OBSERVE. Receipt hashes describe bytes only, never their provenance.
"""

from dataclasses import dataclass, fields
from hashlib import sha256 as sha256
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    _require_subject,
)
from dpone.contracts.mssql_sqlclient_departure_ipc import _typed
from dpone.contracts.mssql_sqlclient_observation import session_authority_digest as session_authority_digest
from dpone.contracts.mssql_sqlclient_observe_departure import (
    ERROR,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
    _uuid,
)
from dpone.contracts.mssql_sqlclient_observe_departure_codec import (
    observe_departure_request_digest,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash


@dataclass(frozen=True, slots=True)
class SqlClientObserveDepartureCredentialIntent:
    """Only nonsecret request and the proposed predecessor artifact link."""

    helper_id: UUID
    attempt_sha256: str
    registration_sha256: str
    request: SqlClientObserveDepartureRequest

    def __post_init__(self) -> None:
        _uuid(self.helper_id)
        _require_subject(self.helper_id, self.attempt_sha256)
        _hash(self.registration_sha256)
        _typed(self.request, SqlClientObserveDepartureRequest)
        if (self.helper_id, self.attempt_sha256) != (
            self.request.plan.helper_id,
            attempt_identity_digest(self.request.plan.attempt),
        ):
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientObserveDepartureResultEvidence:
    """Observed result and proposed credential intent link; no successful exit."""

    helper_id: UUID
    attempt_sha256: str
    credential_intent_sha256: str
    result: SqlClientObserveDepartureResult

    def __post_init__(self) -> None:
        _uuid(self.helper_id)
        _require_subject(self.helper_id, self.attempt_sha256)
        _hash(self.credential_intent_sha256)
        _typed(self.result, SqlClientObserveDepartureResult)


@dataclass(frozen=True, slots=True)
class SqlClientObserveDepartureExclusion:
    """Closed certificate links; the owning composition must authenticate all ACKs."""

    helper_id: UUID
    attempt_sha256: str
    observe_operation_sha256: str
    original_registration_artifact_sha256: str
    original_authority_artifact_sha256: str
    original_authority_sha256: str
    original_containment_artifact_sha256: str
    preparation_artifact_sha256: str
    launch_intent_sha256: str
    registration_sha256: str
    credential_intent_sha256: str
    request_sha256: str
    result_sha256: str
    local_exit_sha256: str
    verifier_authority_sha256: str

    def __post_init__(self) -> None:
        _uuid(self.helper_id)
        _require_subject(self.helper_id, self.attempt_sha256)
        for f in fields(self):
            if f.name != "helper_id":
                _hash(getattr(self, f.name))


def _subject(value: Any, request: SqlClientObserveDepartureRequest) -> None:
    _uuid(value.helper_id)
    if (value.helper_id, value.attempt_sha256) != (
        request.plan.helper_id,
        attempt_identity_digest(request.plan.attempt),
    ):
        raise ValueError(ERROR)


def _exclusion_binding(value: SqlClientObserveDepartureExclusion, request: SqlClientObserveDepartureRequest) -> None:
    plan = request.plan
    _subject(value, request)
    for key in (
        "original_registration_artifact_sha256",
        "original_authority_artifact_sha256",
        "original_authority_sha256",
        "original_containment_artifact_sha256",
        "preparation_artifact_sha256",
    ):
        if getattr(value, key) != getattr(plan, key):
            raise ValueError(ERROR)
    if value.observe_operation_sha256 != coordinator_identity_digest(
        plan.observe_operation
    ) or value.request_sha256 != observe_departure_request_digest(request):
        raise ValueError(ERROR)
