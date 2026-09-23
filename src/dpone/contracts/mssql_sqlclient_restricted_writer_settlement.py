"""Closed contracts for independent settlement of one P9 VERIFY operation."""

from dataclasses import dataclass, field
from enum import StrEnum
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind,
    SqlClientDepartureEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantPrincipal, SqlClientPermissionRow
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission, session_authority_digest
from dpone.contracts.mssql_sqlclient_observation import validate_catalog_admission as validate_catalog_admission
from dpone.contracts.mssql_sqlclient_observer_incarnation import SqlClientObserverIncarnation
from dpone.contracts.mssql_sqlclient_permission_grant import SqlClientPermissionGrantEvidence
from dpone.contracts.mssql_sqlclient_restricted_session_departure import RestrictedSessionDeparture
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    SqlClientRestrictedWriterVerifyRequest,
    SqlClientRestrictedWriterVerifyResult,
    validate_verify_result,
)
from dpone.contracts.mssql_sqlclient_stage_observation import SqlClientStageObservation, quote_stage_identifier
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorLocalObserved,
    CoordinatorRemoteObserved,
    TdsCoordinatorLocalKind,
    TdsCoordinatorLocalObservation,
    TdsCoordinatorRemoteKind,
    TdsCoordinatorRemoteObservation,
    TdsCoordinatorSnapshot,
    advance_coordinator_state,
    coordinator_identity_digest,
)
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_directory import TdsDirectorySnapshot, TdsLocalContainment, TdsRemoteSettlement
from dpone.contracts.mssql_tds_directory_codec import process_identity_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash, _integer, deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity

ERROR = "mssql_native.sqlclient_restricted_writer_settlement_invalid"


def _exact(value: object, kind: type) -> None:
    if type(value) is not kind:
        raise ValueError(ERROR)
    value.__post_init__()  # type: ignore[attr-defined]


@dataclass(frozen=True, slots=True, kw_only=True)
class RestrictedWriterDeparturePlan:
    helper_id: UUID
    grant_evidence: SqlClientPermissionGrantEvidence
    verify_request: SqlClientRestrictedWriterVerifyRequest
    verify_result: SqlClientRestrictedWriterVerifyResult
    management_admission: SqlClientObserverAdmission
    writer_admission: SqlClientObserverAdmission
    implementation_sha256: str
    package_root: str
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    max_address_space_bytes: int
    schema: str = "dpone.sqlclient.restricted-writer-departure-plan.v1"

    def __post_init__(self) -> None:
        try:
            if type(self.helper_id) is not UUID or not self.helper_id.int:
                raise ValueError
            for value, kind in (
                (self.grant_evidence, SqlClientPermissionGrantEvidence),
                (self.verify_request, SqlClientRestrictedWriterVerifyRequest),
                (self.verify_result, SqlClientRestrictedWriterVerifyResult),
                (self.management_admission, SqlClientObserverAdmission),
                (self.writer_admission, SqlClientObserverAdmission),
            ):
                _exact(value, kind)
            validate_verify_result(self.verify_request, self.verify_result)
            for digest in (self.implementation_sha256, self.admission_sha256):
                _hash(digest)
            deadline_nanoseconds(self.startup_deadline)
            deadline_nanoseconds(self.operation_deadline)
            _integer(self.max_address_space_bytes, 1, 2**63 - 1)
            grant_request = self.grant_evidence.request
            if (
                self.schema != "dpone.sqlclient.restricted-writer-departure-plan.v1"
                or self.startup_deadline > self.operation_deadline
                or not self.package_root.startswith("/")
                or self.implementation_sha256 != self.verify_request.implementation_sha256
                or self.verify_request.parent != grant_request.parent
                or self.verify_request.stage != grant_request.stage
                or self.verify_request.writer != grant_request.writer
                or self.verify_request.writer_login != grant_request.writer_login
                or (self.management_admission.server, self.management_admission.database)
                != (self.writer_admission.server, self.writer_admission.database)
                or (self.management_admission.login, self.writer_admission.login)
                != (grant_request.management_login, grant_request.writer_login)
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None

    @property
    def attempt(self):
        return self.verify_request.parent


@dataclass(frozen=True, slots=True, kw_only=True)
class RestrictedWriterDepartureRequest:
    plan: RestrictedWriterDeparturePlan
    startup: TdsCoordinatorStartup
    schema: str = "dpone.sqlclient.restricted-writer-departure-request.v1"

    def __post_init__(self) -> None:
        try:
            _exact(self.plan, RestrictedWriterDeparturePlan)
            _exact(self.startup, TdsCoordinatorStartup)
            if self.schema != "dpone.sqlclient.restricted-writer-departure-request.v1" or (
                self.startup.implementation_sha256,
                self.startup.package_root,
            ) != (self.plan.implementation_sha256, self.plan.package_root):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class RestrictedWriterDepartureResult:
    request_sha256: str
    absence: RestrictedSessionDeparture
    catalog_observer: SqlClientObserverIncarnation
    principals: tuple[SqlClientGrantPrincipal, ...]
    direct_permissions: tuple
    stage: SqlClientStageObservation
    row_count: int
    schema: str = "dpone.sqlclient.restricted-writer-departure-result.v1"

    def __post_init__(self) -> None:
        try:
            _hash(self.request_sha256)
            for value, kind in (
                (self.absence, RestrictedSessionDeparture),
                (self.catalog_observer, SqlClientObserverIncarnation),
                (self.stage, SqlClientStageObservation),
            ):
                _exact(value, kind)
            if (
                self.schema != "dpone.sqlclient.restricted-writer-departure-result.v1"
                or type(self.principals) is not tuple
                or len(self.principals) != 2
                or type(self.direct_permissions) is not tuple
                or len(self.direct_permissions) != 3
                or type(self.row_count) is not int
                or self.row_count != 0
            ):
                raise ValueError
            for principal in self.principals:
                _exact(principal, SqlClientGrantPrincipal)
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class RestrictedWriterDepartureCompletion:
    request: RestrictedWriterDepartureRequest
    result: RestrictedWriterDepartureResult
    local_exit: TdsChildExit
    receipts: tuple[SqlClientDepartureEvidenceReceipt, ...]

    def __post_init__(self) -> None:
        try:
            _exact(self.request, RestrictedWriterDepartureRequest)
            _exact(self.result, RestrictedWriterDepartureResult)
            _exact(self.local_exit, TdsChildExit)
            if (
                self.local_exit.exit_code != 0
                or self.local_exit.reaped is not True
                or type(self.receipts) is not tuple
                or tuple(receipt.kind for receipt in self.receipts) != tuple(SqlClientDepartureEvidenceKind)
            ):
                raise ValueError
            subject = (self.request.plan.helper_id, attempt_identity_digest(self.request.plan.attempt))
            for receipt in self.receipts:
                _exact(receipt, SqlClientDepartureEvidenceReceipt)
                if (receipt.helper_id, receipt.attempt_sha256) != subject:
                    raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class RestrictedWriterDepartureEvidenceContext:
    """Exact plan retained only to validate the six generic departure ACKs."""

    plan: RestrictedWriterDeparturePlan

    def __post_init__(self) -> None:
        _exact(self.plan, RestrictedWriterDeparturePlan)


@dataclass(frozen=True, slots=True, kw_only=True)
class RestrictedWriterDepartureCredentials:
    request: RestrictedWriterDepartureRequest
    connection_material: TdsConnectionMaterial = field(repr=False)
    session_nonce: bytes = field(repr=False)
    schema: str = "dpone.sqlclient.restricted-writer-departure-credentials.v1"

    def __post_init__(self) -> None:
        try:
            _exact(self.request, RestrictedWriterDepartureRequest)
            _exact(self.connection_material, TdsConnectionMaterial)
            if (
                self.schema != "dpone.sqlclient.restricted-writer-departure-credentials.v1"
                or type(self.session_nonce) is not bytes
                or len(self.session_nonce) != 32
                or self.connection_material.database != self.request.plan.grant_evidence.authority.database.name
                or self.connection_material.username != self.request.plan.management_admission.login.name
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None


class RestrictedWriterSettlementEvidenceKind(StrEnum):
    REMOTE_SETTLEMENT = "remote_settlement"


@dataclass(frozen=True, slots=True)
class RestrictedWriterSettlementReceipt:
    attempt_sha256: str
    operation_sha256: str
    payload_sha256: str
    relative_name: str
    size: int

    def __post_init__(self) -> None:
        for value in (self.attempt_sha256, self.operation_sha256, self.payload_sha256):
            _hash(value)
        _integer(self.size, 1, 16384)
        expected = f"tds-restricted-writer-settlement-v1-{self.attempt_sha256}-{self.operation_sha256}-{self.payload_sha256}.json"
        if self.relative_name != expected:
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class RestrictedWriterSettlementRecord:
    attempt_sha256: str
    operation_sha256: str
    payload: bytes = field(repr=False)
    kind: RestrictedWriterSettlementEvidenceKind = RestrictedWriterSettlementEvidenceKind.REMOTE_SETTLEMENT

    def __post_init__(self) -> None:
        _hash(self.attempt_sha256)
        _hash(self.operation_sha256)
        if type(self.payload) is not bytes or not 0 < len(self.payload) <= 16384:
            raise ValueError(ERROR)

    @property
    def receipt(self) -> RestrictedWriterSettlementReceipt:
        self.__post_init__()
        digest = sha256(self.payload).hexdigest()
        return RestrictedWriterSettlementReceipt(
            self.attempt_sha256,
            self.operation_sha256,
            digest,
            f"tds-restricted-writer-settlement-v1-{self.attempt_sha256}-{self.operation_sha256}-{digest}.json",
            len(self.payload),
        )


class RestrictedWriterSettlementOperations:
    """Nominal P9b operations injected into contract-neutral outer layers."""

    request_type, plan_type = RestrictedWriterDepartureRequest, RestrictedWriterDeparturePlan
    completion_type, snapshot_type = RestrictedWriterDepartureCompletion, TdsCoordinatorSnapshot
    process_type, directory_type, record_type = (
        TdsProcessIdentity,
        TdsDirectorySnapshot,
        RestrictedWriterSettlementRecord,
    )
    validate_catalog_admission = staticmethod(validate_catalog_admission)
    principal_from_row = staticmethod(SqlClientGrantPrincipal.from_row)

    @staticmethod
    def is_public_principal(value):
        return (
            type(value) is SqlClientGrantPrincipal
            and value.principal_id == 0
            and value.name == "public"
            and value.type_desc == "DATABASE_ROLE"
            and value.authentication_type_desc == "NONE"
        )

    permission_from_row = staticmethod(lambda row: SqlClientPermissionRow(*row))
    quote_stage_identifier = staticmethod(quote_stage_identifier)
    deadline_nanoseconds = staticmethod(deadline_nanoseconds)

    def __init__(self, encode_request, validate_result, remote_payload) -> None:
        self.encode_request, self._validate_result, self._remote_payload = (
            encode_request,
            validate_result,
            remote_payload,
        )

    def plan(self, **values):
        return RestrictedWriterDeparturePlan(**values)

    def validate_request(self, value):
        _exact(value, RestrictedWriterDepartureRequest)

    def writer_principal(self, request):
        writer = request.writer
        return SqlClientGrantPrincipal(writer.principal_id, writer.name, writer.sid, "SQL_USER", "INSTANCE")

    def result(self, **values):
        return RestrictedWriterDepartureResult(**values)

    def validate_result(self, result, request):
        self._validate_result(result, request)

    def validate_record(self, value):
        _exact(value, RestrictedWriterSettlementRecord)

    def receipt(self, value):
        self.validate_record(value)
        return value.receipt

    def authority_digest(self, result):
        return session_authority_digest(result.catalog_observer.authority).hex()

    operation_digest = staticmethod(coordinator_identity_digest)
    attempt_digest = staticmethod(attempt_identity_digest)

    def settlement_record(self, attempt, operation, completion):
        return RestrictedWriterSettlementRecord(attempt, operation, self._remote_payload(completion))

    @staticmethod
    def expected_advance(state, event):
        return advance_coordinator_state(state, event, expected_phase=state.phase)

    @staticmethod
    def local_event(operation, process, authentication, evidence):
        return CoordinatorLocalObserved(
            TdsCoordinatorLocalObservation(
                operation, TdsCoordinatorLocalKind.CONTAINED, process, authentication, evidence
            )
        )

    @staticmethod
    def remote_event(operation, session, authority, evidence):
        return CoordinatorRemoteObserved(
            TdsCoordinatorRemoteObservation(operation, TdsCoordinatorRemoteKind.SETTLED, session, authority, evidence)
        )

    @staticmethod
    def local_proof(attempt, operation_id, process, evidence):
        return TdsLocalContainment(attempt, operation_id, process_identity_digest(process), evidence)

    @staticmethod
    def remote_proof(attempt, operation_id, authority, evidence):
        return TdsRemoteSettlement(attempt, operation_id, authority, evidence)
