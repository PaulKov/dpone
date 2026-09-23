"""One-session GRANT departure verifier using fixed reviewed readers."""

from hashlib import sha256
from time import monotonic_ns

from dpone.adapters.mssql_sqlclient_create_exclusion_v2 import SqlClientCreateExclusionObserverV2
from dpone.adapters.mssql_sqlclient_grant_catalog import SqlClientGrantCatalog
from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_sqlclient_observer_incarnation import parse_observer_incarnation_rows
from dpone.adapters.mssql_sqlclient_stage_catalog import SqlClientStageObserver
from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
    SqlClientGrantPrincipal,
    SqlClientPermissionGrantDepartureRequest,
    SqlClientPermissionGrantDepartureResult,
    SqlClientPermissionRow,
    deadline_nanoseconds,
    encode_request,
    validate_catalog_admission,
    validate_result,
)
from dpone.contracts.mssql_tds_api import TdsRemoteSessionIdentity

ERROR = "mssql_native.sqlclient_permission_grant_departure_unavailable"


class SqlClientPermissionGrantDepartureObserver:
    """Verify absence, grant persistence and stage identity on one SQL handle."""

    def __init__(self, connection: TdsSqlConnection, request: SqlClientPermissionGrantDepartureRequest) -> None:
        if type(connection) is not TdsSqlConnection or type(request) is not SqlClientPermissionGrantDepartureRequest:
            raise ValueError(ERROR)
        request.__post_init__()
        self._connection, self._request, self._used = connection, request, False

    def observe(self, session_nonce: bytes) -> SqlClientPermissionGrantDepartureResult:
        if self._used:
            raise RuntimeError(ERROR)
        self._used = True
        request, plan = self._request, self._request.plan
        evidence = plan.grant_evidence
        deadline, deadline_ns = plan.operation_deadline, deadline_nanoseconds(plan.operation_deadline)
        try:
            sql = TdsCoordinatorSql(
                self._connection,
                evidence.operation,
                evidence.grant.ownership,
                request.startup.process,
            )
            sql.acquire(session_nonce, deadline=deadline)
            if type(evidence.authority.session) is not TdsRemoteSessionIdentity:
                raise ValueError
            absence = SqlClientCreateExclusionObserverV2(
                self._connection.cursor,
                admission=plan.management_admission,
                observer_admission=plan.management_admission,
                operation_deadline_ns=deadline_ns,
                monotonic_ns=monotonic_ns,
            ).observe_departure_v2(
                original=evidence.authority.session,
                database=evidence.authority.database,
                principal=evidence.request.management.principal,
            )
            session = ObservationCursor(
                self._connection.cursor, deadline=deadline, check_deadline=lambda: sql.check_deadline(deadline=deadline)
            )
            catalog = SqlClientGrantCatalog._sharing(sql, deadline=deadline, session=session)
            rows = catalog.read_own_incarnation()
            incarnation = parse_observer_incarnation_rows(rows, admission=plan.management_admission)
            writer_rows = catalog.writer_admission(
                plan.writer_admission.database.database_id, plan.writer_admission.login.name
            )
            validate_catalog_admission(writer_rows, plan.writer_admission)
            principals = tuple(
                SqlClientGrantPrincipal.from_row(row)
                for row in catalog.principals(evidence.request.writer.name, bytes.fromhex(evidence.request.writer.sid))
            )
            expected_writer = SqlClientGrantPrincipal(
                evidence.request.writer.principal_id,
                evidence.request.writer.name,
                evidence.request.writer.sid,
                "SQL_USER",
                "INSTANCE",
            )
            public = tuple(
                value
                for value in principals
                if (
                    value.principal_id,
                    value.name,
                    value.type_desc,
                    value.authentication_type_desc,
                )
                == (0, "public", "DATABASE_ROLE", "NONE")
            )
            if principals.count(expected_writer) != 1 or len(public) != 1:
                raise ValueError(ERROR)
            expected_public = public[0]
            raw = tuple(
                SqlClientPermissionRow(*row)
                for row in catalog.permissions(expected_writer.principal_id, expected_public.principal_id, limit=4096)
            )
            direct = tuple(
                row
                for row in raw
                if row.class_id == 1
                and row.major_id == evidence.request.stage.object_id
                and row.grantee_principal_id == evidence.request.writer.principal_id
            )
            stage = SqlClientStageObserver._sharing(
                session, admission=plan.management_admission, operation_deadline_ns=deadline_ns
            ).observe(evidence.request.stage)
            result = SqlClientPermissionGrantDepartureResult(
                request_sha256=sha256(encode_request(request)).hexdigest(),
                absence=absence,
                catalog_observer=incarnation,
                direct_permissions=direct,
                stage=stage,
            )
            validate_result(result, request)
            return result
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            raise RuntimeError(ERROR) from None
