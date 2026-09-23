"""One concrete SQL session and cursor for finite retained OBSERVE reads.

The adapter acquires once after dual-envelope validation. It never reconnects,
reserves parent operations, executes supplied SQL, or writes parent evidence.
"""

import time

from dpone.adapters.mssql_sqlclient_grant_catalog import SqlClientGrantCatalog
from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_sqlclient_preparation_catalog import SqlClientPreparationCatalog
from dpone.adapters.mssql_sqlclient_stage_catalog import SqlClientStageObserver
from dpone.adapters.mssql_tds_coordinator_connection import TdsCoordinatorConnection, decode_connection_admission
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql
from dpone.contracts.mssql_sqlclient_observe import ERROR, SqlClientObserveRequest, observation_body
from dpone.contracts.mssql_sqlclient_observe_handshake import (
    SqlClientObserveCredentials,
    decode_credentials,
    encode_credentials,
)
from dpone.contracts.mssql_tds_api import (
    OPCODE_LIMITS,
    deadline_seconds,
    encode_authority,
    encode_preparation_rows,
    encode_rows,
    strict_json_object,
)
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup


class SqlClientObserveSession:
    """Own the actual connection; all commands use its one original SQL handle."""

    def __init__(
        self,
        request: SqlClientObserveRequest,
        credentials: SqlClientObserveCredentials,
        startup: TdsCoordinatorStartup,
        admission: bytes,
    ) -> None:
        build, profile = decode_connection_admission(admission)
        # Revalidate originals before even constructing a connection factory.
        encoded = encode_credentials(credentials, request)
        try:
            validated = decode_credentials(encoded, request, startup, profile)
        finally:
            del encoded
        self.request = request
        self.deadline = deadline_seconds(request.operation_deadline_ns)
        self._connection = TdsCoordinatorConnection(build, profile).connect(
            validated.connection_material, deadline=self.deadline
        )
        try:
            self._sql = TdsCoordinatorSql(
                self._connection, validated.identity, validated.execution_owner, validated.process
            )
            self._authority = self._sql.acquire(validated.session_nonce, deadline=self.deadline)
            self._session = ObservationCursor(
                self._sql.cursor, deadline=request.operation_deadline_ns, clock=time.monotonic_ns
            )
            self._catalog = SqlClientGrantCatalog._sharing(self._sql, deadline=self.deadline, session=self._session)
            self._observer = SqlClientStageObserver._sharing(
                self._session,
                admission=request.management_admission,
                operation_deadline_ns=request.operation_deadline_ns,
            )
        except BaseException:
            self._connection.close()
            raise
        finally:
            del validated

    def authority_payload(self) -> bytes:
        lease = self._session.begin()
        try:
            self._require_authority(lease)
            result = encode_authority(self._authority)
            self._session.current(lease)
            self._session.finish(lease)
            return result
        except BaseException:
            self._session.fail(lease)
            raise

    def _require_authority(self, lease: object) -> None:
        self._session.current(lease)
        self._sql.require_authority(deadline=self.deadline)
        self._session.current(lease)

    def dispatch(self, command: dict) -> list:
        lease = self._session.begin()
        try:
            self._require_authority(lease)
            result = _dispatch_within(self._catalog, self._observer, self.request, command, lease)
            self._require_authority(lease)
            self._session.finish(lease)
            return result
        except BaseException:
            self._session.fail(lease)
            raise

    def close(self) -> None:
        self._connection.close()


def _dispatch(
    catalog: SqlClientGrantCatalog, observer: SqlClientStageObserver, request: SqlClientObserveRequest, cmd: dict
) -> list:
    opcode, args = cmd["opcode"], cmd["arguments"]
    if opcode in OPCODE_LIMITS:
        session = catalog._session
        lease = session.begin()
        try:
            rows = SqlClientPreparationCatalog(session, request).read_within(lease, opcode)
            result = encode_preparation_rows(opcode, rows)
            session.finish(lease)
            return result
        except BaseException:
            session.fail(lease)
            raise
    if opcode == "GUARD":
        return [strict_json_object(encode_authority(catalog.guard()))]
    if opcode == "OBSERVE_SELECTED":
        return [observation_body(observer.observe(request.selected_stage), request)]
    if opcode == "OWN_INCARNATION":
        rows = catalog.read_own_incarnation()
    elif opcode == "WRITER_ADMISSION":
        rows = catalog.writer_admission(**args)
    elif opcode == "PRINCIPALS":
        rows = catalog.principals(args["name"], bytes.fromhex(args["sid"]))
    elif opcode == "PERMISSIONS":
        rows = catalog.permissions(**args)
    elif opcode == "MEMBER":
        rows = catalog.member(**args)
    elif opcode == "BATCH_MEMBERS":
        rows = catalog.members(tuple(args["object_ids"]))
    elif opcode == "OBJECT_PROPERTIES":
        rows = catalog.object_properties(**args)
    elif opcode == "BATCH_OBJECTS":
        rows = catalog.object_properties_batch(tuple(args["object_ids"]))
    elif opcode == "FEATURES":
        rows = catalog.features(**args)
    elif opcode == "BATCH_FEATURES":
        rows = catalog.features_batch(tuple(args["object_ids"]))
    elif opcode == "COLUMNS":
        rows = catalog.columns(**args)
    elif opcode == "BATCH_COLUMNS":
        rows = catalog.columns_batch(tuple(args["object_ids"]))
    else:
        raise ValueError(ERROR)
    return encode_rows(opcode, rows)


def _dispatch_within(
    catalog: SqlClientGrantCatalog,
    observer: SqlClientStageObserver,
    request: SqlClientObserveRequest,
    cmd: dict,
    lease: object,
) -> list:
    opcode, args = cmd["opcode"], cmd["arguments"]
    if opcode in OPCODE_LIMITS:
        rows = SqlClientPreparationCatalog(catalog._session, request).read_within(lease, opcode)
        return encode_preparation_rows(opcode, rows)
    if opcode == "GUARD":
        return [strict_json_object(encode_authority(catalog._guard_within(lease)))]
    if opcode == "OBSERVE_SELECTED":
        return [observation_body(observer._observe_within(lease, request.selected_stage), request)]
    return encode_rows(opcode, catalog._read_within(lease, opcode, args))
