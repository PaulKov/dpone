"""Immutable permission-grant and departure values shared by canonical codecs."""

from dataclasses import dataclass, field
from uuid import UUID

from dpone.contracts.mssql_sqlclient_create_departure_v2 import SqlClientCreateDepartureV2
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_observer_incarnation import SqlClientObserverIncarnation
from dpone.contracts.mssql_sqlclient_permission_grant import (
    SqlClientDirectPermission as SqlClientDirectPermission,
)
from dpone.contracts.mssql_sqlclient_permission_grant import (
    SqlClientPermissionGrantEvidence as SqlClientPermissionGrantEvidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant import (
    SqlClientPermissionGrantRequest as SqlClientPermissionGrantRequest,
)
from dpone.contracts.mssql_sqlclient_stage_observation import SqlClientStageObservation
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash, _integer, deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsChildExit

ERROR = "mssql_native.sqlclient_permission_grant_departure_invalid"


def _admission(value: object) -> SqlClientObserverAdmission:
    if type(value) is not SqlClientObserverAdmission:
        raise ValueError(ERROR)
    value.__post_init__()
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPermissionGrantDeparturePlan:
    helper_id: UUID
    grant_evidence: SqlClientPermissionGrantEvidence
    management_admission: SqlClientObserverAdmission
    writer_admission: SqlClientObserverAdmission
    implementation_sha256: str
    package_root: str
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    max_address_space_bytes: int
    schema: str = "dpone.sqlclient.permission-grant-departure-plan.v1"

    def __post_init__(self) -> None:
        try:
            if (
                type(self.helper_id) is not UUID
                or not self.helper_id.int
                or type(self.grant_evidence) is not SqlClientPermissionGrantEvidence
            ):
                raise ValueError
            self.grant_evidence.__post_init__()
            management, writer = _admission(self.management_admission), _admission(self.writer_admission)
            request, authority = self.grant_evidence.request, self.grant_evidence.authority
            for digest in (self.implementation_sha256, self.admission_sha256):
                _hash(digest)
            deadline_nanoseconds(self.startup_deadline)
            deadline_nanoseconds(self.operation_deadline)
            _integer(self.max_address_space_bytes, 1, 2**63 - 1)
            if (
                self.schema != "dpone.sqlclient.permission-grant-departure-plan.v1"
                or self.startup_deadline > self.operation_deadline
                or not self.package_root.startswith("/")
                or (management.server, management.database) != (writer.server, writer.database)
                or (management.login, writer.login) != (request.management_login, request.writer_login)
                or authority.session != self.grant_evidence.grant.session
                or authority.database.name != request.stage.database_name
                or self.implementation_sha256 != self.grant_evidence.operation.implementation_sha256
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None

    @property
    def attempt(self):
        return self.grant_evidence.request.parent


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPermissionGrantDepartureRequest:
    plan: SqlClientPermissionGrantDeparturePlan
    startup: TdsCoordinatorStartup
    schema: str = "dpone.sqlclient.permission-grant-departure-request.v1"

    def __post_init__(self) -> None:
        if (
            type(self.plan) is not SqlClientPermissionGrantDeparturePlan
            or type(self.startup) is not TdsCoordinatorStartup
        ):
            raise ValueError(ERROR)
        self.plan.__post_init__()
        self.startup.__post_init__()
        if self.schema != "dpone.sqlclient.permission-grant-departure-request.v1" or (
            self.startup.implementation_sha256,
            self.startup.package_root,
        ) != (self.plan.implementation_sha256, self.plan.package_root):
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPermissionGrantDepartureResult:
    request_sha256: str
    absence: SqlClientCreateDepartureV2
    catalog_observer: SqlClientObserverIncarnation
    direct_permissions: tuple
    stage: SqlClientStageObservation
    schema: str = "dpone.sqlclient.permission-grant-departure-result.v1"

    def __post_init__(self) -> None:
        _hash(self.request_sha256)
        if (
            self.schema != "dpone.sqlclient.permission-grant-departure-result.v1"
            or type(self.absence) is not SqlClientCreateDepartureV2
            or type(self.catalog_observer) is not SqlClientObserverIncarnation
            or type(self.direct_permissions) is not tuple
            or len(self.direct_permissions) != 3
            or type(self.stage) is not SqlClientStageObservation
        ):
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class PermissionGrantDepartureCompletion:
    request: SqlClientPermissionGrantDepartureRequest
    result: SqlClientPermissionGrantDepartureResult
    local_exit: TdsChildExit
    receipts: tuple[SqlClientDepartureEvidenceReceipt, ...]

    def __post_init__(self) -> None:
        try:
            self.request.__post_init__()
            self.result.__post_init__()
            self.local_exit.__post_init__()
            if (
                self.local_exit.exit_code != 0
                or self.local_exit.reaped is not True
                or type(self.receipts) is not tuple
                or len(self.receipts) != 6
                or tuple(value.kind for value in self.receipts) != tuple(Kind)
            ):
                raise ValueError
            for receipt in self.receipts:
                receipt.__post_init__()
                if (
                    receipt.helper_id != self.request.plan.helper_id
                    or receipt.attempt_sha256 != attempt_identity_digest(self.request.plan.attempt)
                ):
                    raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPermissionGrantDepartureCredentials:
    request: SqlClientPermissionGrantDepartureRequest
    connection_material: TdsConnectionMaterial = field(repr=False)
    session_nonce: bytes = field(repr=False)
    schema: str = "dpone.sqlclient.permission-grant-departure-credentials.v1"

    def __post_init__(self) -> None:
        if (
            self.schema != "dpone.sqlclient.permission-grant-departure-credentials.v1"
            or type(self.request) is not SqlClientPermissionGrantDepartureRequest
            or type(self.connection_material) is not TdsConnectionMaterial
            or type(self.session_nonce) is not bytes
            or len(self.session_nonce) != 32
        ):
            raise ValueError(ERROR)
        self.request.__post_init__()
        self.connection_material.__post_init__()
        if (
            self.connection_material.database != self.request.plan.grant_evidence.authority.database.name
            or self.connection_material.username != self.request.plan.management_admission.login.name
        ):
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class PermissionGrantDepartureEvidenceContext:
    plan: SqlClientPermissionGrantDeparturePlan

    def __post_init__(self) -> None:
        if type(self.plan) is not SqlClientPermissionGrantDeparturePlan:
            raise ValueError(ERROR)
        self.plan.__post_init__()
