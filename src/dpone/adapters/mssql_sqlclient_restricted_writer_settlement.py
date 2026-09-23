"""One-session management observer for exact P9 restricted-writer settlement."""

from hashlib import sha256
from time import monotonic_ns
from typing import Any, Protocol, cast

from dpone.adapters.mssql_sqlclient_grant_catalog import SqlClientGrantCatalog
from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_sqlclient_observer_incarnation import parse_observer_incarnation_rows
from dpone.adapters.mssql_sqlclient_restricted_session_departure import (
    SqlClientRestrictedSessionDepartureObserver,
)
from dpone.adapters.mssql_sqlclient_stage_catalog import SqlClientStageObserver
from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql

ERROR = "mssql_native.sqlclient_restricted_writer_settlement_unavailable"


class SettlementAdapterOperations(Protocol):
    request_type: type

    def validate_request(self, value: object) -> None: ...
    def deadline_nanoseconds(self, value: float) -> int: ...
    def validate_catalog_admission(self, rows: object, admission: object) -> None: ...
    def principal_from_row(self, row: tuple) -> Any: ...
    def writer_principal(self, request: object) -> Any: ...
    def is_public_principal(self, value: object) -> bool: ...
    def permission_from_row(self, row: tuple) -> Any: ...
    def quote_stage_identifier(self, value: str) -> str: ...
    def encode_request(self, value: object) -> bytes: ...
    def result(self, **values: object) -> object: ...
    def validate_result(self, result: object, request: object) -> None: ...


class SqlClientRestrictedWriterSettlementObserver:
    """Prove exact session departure, catalog authority, stage identity and count."""

    def __init__(self, connection: TdsSqlConnection, request: object, operations: SettlementAdapterOperations) -> None:
        if type(connection) is not TdsSqlConnection or type(request) is not operations.request_type:
            raise ValueError(ERROR)
        operations.validate_request(request)
        self._connection, self._request, self._operations, self._used = connection, request, operations, False

    def observe(self, session_nonce: bytes) -> object:
        if self._used:
            raise RuntimeError(ERROR)
        self._used = True
        operations = self._operations
        if type(self._request) is not operations.request_type:
            raise RuntimeError(ERROR)
        operations.validate_request(self._request)
        request = cast(Any, self._request)
        plan = request.plan
        grant = plan.grant_evidence
        deadline = plan.operation_deadline
        deadline_ns = operations.deadline_nanoseconds(plan.operation_deadline)
        try:
            sql = TdsCoordinatorSql(self._connection, grant.operation, grant.grant.ownership, request.startup.process)
            sql.acquire(session_nonce, deadline=deadline)
            absence = SqlClientRestrictedSessionDepartureObserver(
                self._connection.cursor,
                admission=plan.writer_admission,
                observer_admission=plan.management_admission,
                operation_deadline_ns=deadline_ns,
                monotonic_ns=monotonic_ns,
            ).observe(
                original=plan.verify_result.opening.session,
                database=grant.authority.database,
                principal=grant.request.writer.principal,
            )
            session = ObservationCursor(
                self._connection.cursor,
                deadline=deadline,
                check_deadline=lambda: sql.check_deadline(deadline=deadline),
            )
            catalog = SqlClientGrantCatalog._sharing(sql, deadline=deadline, session=session)
            incarnation = parse_observer_incarnation_rows(
                catalog.read_own_incarnation(), admission=plan.management_admission
            )
            writer_rows = catalog.writer_admission(
                plan.writer_admission.database.database_id, plan.writer_admission.login.name
            )
            operations.validate_catalog_admission(writer_rows, plan.writer_admission)
            principals = tuple(
                operations.principal_from_row(row)
                for row in catalog.principals(
                    grant.request.writer.name,
                    bytes.fromhex(grant.request.writer.sid),
                )
            )
            writer = cast(Any, operations.writer_principal(grant.request))
            public_rows = tuple(value for value in principals if operations.is_public_principal(value))
            if principals.count(writer) != 1 or len(public_rows) != 1 or len(principals) != 2:
                raise ValueError(ERROR)
            public = cast(Any, public_rows[0])
            raw = tuple(
                operations.permission_from_row(row)
                for row in catalog.permissions(writer.principal_id, public.principal_id, limit=4096)
            )
            direct = tuple(
                row
                for row in cast(tuple[Any, ...], raw)
                if row.class_id == 1
                and row.major_id == plan.verify_request.stage.object_id
                and row.grantee_principal_id == writer.principal_id
            )
            target = (
                operations.quote_stage_identifier(plan.verify_request.stage.schema_name)
                + "."
                + operations.quote_stage_identifier(plan.verify_request.stage.table_name)
            )
            lease = session.begin()
            try:
                rows = session.rows(
                    lease,
                    "SELECT COUNT_BIG(*) FROM " + target + " WITH(READCOMMITTEDLOCK);",
                    (),
                    max_rows=1,
                    nextset_required=True,
                    detach_rows=False,
                )
                session.finish(lease)
            except BaseException:
                session.fail(lease)
                raise
            if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int or rows[0][0] != 0:
                raise ValueError(ERROR)
            stage = SqlClientStageObserver._sharing(
                session, admission=plan.management_admission, operation_deadline_ns=deadline_ns
            ).observe(plan.verify_request.stage)
            result = operations.result(
                request_sha256=sha256(operations.encode_request(request)).hexdigest(),
                absence=absence,
                catalog_observer=incarnation,
                principals=principals,
                direct_permissions=direct,
                stage=stage,
                row_count=rows[0][0],
            )
            operations.validate_result(result, request)
            return result
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            raise RuntimeError(ERROR) from None
