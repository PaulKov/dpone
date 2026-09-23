"""One-connection P10f departure and typed stage-content observation.

The caller provides an independently admitted management connection and a
canonical native-row encoder. Synchronous driver calls require external process
containment. The adapter never reconnects, retries, mutates business data, or
publishes the stage.
"""

from collections.abc import Callable, Sequence
from hashlib import sha256
from typing import Any, Protocol

from dpone.adapters.mssql_sqlclient_create_exclusion_v2 import _partition
from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import (
    CONNECTIONS_SQL,
    REQUESTS_SQL,
    SESSIONS_SQL,
    TRANSACTIONS_SQL,
)
from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_sqlclient_observer_incarnation import capture_observer_incarnation
from dpone.adapters.mssql_sqlclient_stage_catalog import SqlClientStageObserver
from dpone.contracts.mssql_sqlclient_create_departure_v2 import (
    SqlClientDepartureSampleKind as Kind,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_tds_api import (
    SqlClientInputDescriptor,
    SqlClientStageContentExpectation,
    SqlClientStageIdentity,
    SqlClientWriterObservationRecord,
    SqlClientWriterSessionDeparture,
    SqlClientWriterSettlementObservation,
    TdsCreateType,
    observer_incarnation_digest,
    quote_stage_identifier,
)

_ERROR = "mssql_native.sqlclient_writer_settlement_unavailable"


class SettlementCursor(Protocol):
    def execute(self, sql: str, *parameters: Any) -> Any: ...
    def fetchone(self) -> Any: ...
    def nextset(self) -> Any: ...
    def close(self) -> Any: ...


def _target(stage: SqlClientStageIdentity) -> str:
    return quote_stage_identifier(stage.schema_name) + "." + quote_stage_identifier(stage.table_name)


def _layout_matches(stage: SqlClientStageIdentity, descriptor: SqlClientInputDescriptor) -> bool:
    declared = {
        TdsCreateType.BIGINT: "bigint",
        TdsCreateType.FLOAT53: "float(53)",
        TdsCreateType.NVARCHARMAX: "nvarchar(max)",
        TdsCreateType.DATETIME2_6: "datetime2(6)",
    }
    if len(stage.columns) != len(descriptor.columns):
        return False
    return all(
        observed.name == physical.name
        and observed.nullable is physical.nullable
        and physical.source_type == declared[observed.type] + (" nullable" if observed.nullable else "")
        for observed, physical in zip(stage.columns, descriptor.columns, strict=True)
    )


class SqlClientWriterSettlementObserver:
    """Observe departure and locked contents under one bounded SQL incarnation."""

    def __init__(
        self,
        cursor: SettlementCursor,
        *,
        management_admission: SqlClientObserverAdmission,
        operation_deadline_ns: int,
        operation_deadline: float,
        monotonic_ns: Callable[[], int],
        encode_row: Callable[[Sequence[Any]], bytes],
        finalize_digest: Callable[[int, int], str],
        close_connection: Callable[[], None],
    ) -> None:
        if (
            type(management_admission) is not SqlClientObserverAdmission
            or type(operation_deadline_ns) is not int
            or operation_deadline_ns < 1
            or type(operation_deadline) is not float
            or operation_deadline <= 0
            or not callable(encode_row)
            or not callable(finalize_digest)
            or not callable(close_connection)
        ):
            raise ValueError(_ERROR)
        management_admission.__post_init__()
        self._cursor = cursor
        self._admission = management_admission
        self._session = ObservationCursor(cursor, deadline=operation_deadline_ns, clock=monotonic_ns)
        self._stage = SqlClientStageObserver._sharing(
            self._session,
            admission=management_admission,
            operation_deadline_ns=operation_deadline_ns,
        )
        self._encode_row = encode_row
        self._finalize_digest = finalize_digest
        self._close_connection = close_connection
        self._operation_deadline = operation_deadline
        self._lease: object | None = None
        self._closed = False
        self._used = False

    def _current(self) -> None:
        self._session.current(self._lease)

    def _query(self, sql: str, *parameters: Any, limit: int = 3) -> list[Any]:
        return self._session.rows(
            self._lease,
            sql,
            parameters,
            max_rows=limit,
            nextset_required=True,
            detach_rows=True,
            current=self._current,
        )

    def _statement(self, sql: str) -> None:
        self._current()
        self._cursor.execute(sql)
        self._current()

    def _departure(
        self,
        record: SqlClientWriterObservationRecord,
        admission: SqlClientObserverAdmission,
        observer: Any,
    ) -> SqlClientWriterSessionDeparture:
        original = record.remote_session
        digest = observer_incarnation_digest(observer)
        old, own = str(original.connection_id), str(observer.connection_id)
        session, own_session = original.session_id, observer.session_id
        connection = (
            Kind.CONNECTIONS,
            CONNECTIONS_SQL,
            (old, old, session, own, own_session, observer.connect_time, old, old),
        )
        session_query = (Kind.SESSIONS, SESSIONS_SQL, (session, own_session, observer.login_time))
        queries = (
            connection,
            session_query,
            (Kind.REQUESTS, REQUESTS_SQL, (session, old, own, own_session, old)),
            (Kind.TRANSACTIONS, TRANSACTIONS_SQL, (session,)),
            connection,
            session_query,
        )
        samples = []
        for kind, sql, parameters in queries:
            before = capture_observer_incarnation(self._query, admission=self._admission)
            if observer_incarnation_digest(before) != digest:
                raise ValueError(_ERROR)
            rows = self._query(sql, *parameters)
            after = capture_observer_incarnation(self._query, admission=self._admission)
            if observer_incarnation_digest(after) != digest:
                raise ValueError(_ERROR)
            samples.append(_partition(rows, kind, digest, digest))
        return SqlClientWriterSessionDeparture(
            original=original,
            writer_admission=admission,
            writer_authority=record.authority,
            principal=record.resolved_database_principal,
            observer=observer,
            samples=tuple(samples),
        )

    def _contents(self, stage: SqlClientStageIdentity, expected_rows: int) -> tuple[int, str, int]:
        columns = ", ".join(quote_stage_identifier(column.name) for column in stage.columns)
        sql = "SELECT " + columns + " FROM " + _target(stage) + " WITH (HOLDLOCK, TABLOCK);"
        self._current()
        self._cursor.execute(sql)
        self._current()
        count, total = 0, 0
        while True:
            self._current()
            row = self._cursor.fetchone()
            self._current()
            if row is None:
                break
            count += 1
            if count > expected_rows:
                raise ValueError(_ERROR)
            payload = self._encode_row(tuple(row))
            if type(payload) is not bytes:
                raise ValueError(_ERROR)
            total = (total + int.from_bytes(sha256(payload).digest(), "big")) % (1 << 256)
        self._current()
        if self._cursor.nextset() not in (None, False):
            raise ValueError(_ERROR)
        self._current()
        digest = self._finalize_digest(count, total)
        if type(digest) is not str or len(digest) != 64:
            raise ValueError(_ERROR)
        return count, digest, total

    def observe(
        self,
        *,
        writer_observation: SqlClientWriterObservationRecord,
        writer_admission: SqlClientObserverAdmission,
        stage: SqlClientStageIdentity,
        input_descriptor: SqlClientInputDescriptor,
        expectation: SqlClientStageContentExpectation,
        operation_deadline: float,
    ) -> SqlClientWriterSettlementObservation:
        """Return evidence only after a serializable, locked catalog/content read."""
        if self._used or operation_deadline != self._operation_deadline:
            raise RuntimeError(_ERROR)
        self._used = True
        self._lease = self._session.begin()
        transaction = False
        try:
            for value, kind in (
                (writer_observation, SqlClientWriterObservationRecord),
                (writer_admission, SqlClientObserverAdmission),
                (stage, SqlClientStageIdentity),
                (input_descriptor, SqlClientInputDescriptor),
                (expectation, SqlClientStageContentExpectation),
            ):
                if type(value) is not kind:
                    raise ValueError(_ERROR)
                value.__post_init__()
            authority = writer_observation.authority
            if (
                writer_admission
                != SqlClientObserverAdmission(
                    authority.server, authority.database, authority.login, authority.transport
                )
                or writer_admission.login == self._admission.login
                or writer_admission.login.sid == self._admission.login.sid
                or not _layout_matches(stage, input_descriptor)
                or expectation.rows != input_descriptor.expected.rows
            ):
                raise ValueError(_ERROR)
            observer_before = capture_observer_incarnation(self._query, admission=self._admission)
            departure = self._departure(writer_observation, writer_admission, observer_before)
            self._statement("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;")
            transaction = True
            db = observer_before.authority.database
            before, permissions = self._stage.catalog_in_existing_session(
                stage,
                lease=self._lease,
                database_guid=db.database_guid,
                database_id=db.database_id,
                database_name=db.database_name,
            )
            count_row = self._query("SELECT COUNT_BIG(*) FROM " + _target(stage) + " WITH (HOLDLOCK, TABLOCK);")
            if len(count_row) != 1 or len(count_row[0]) != 1 or type(count_row[0][0]) is not int:
                raise ValueError(_ERROR)
            row_count, typed_digest, typed_sum = self._contents(stage, expectation.rows)
            after, final_permissions = self._stage.catalog_in_existing_session(
                stage,
                lease=self._lease,
                database_guid=db.database_guid,
                database_id=db.database_id,
                database_name=db.database_name,
            )
            if (
                permissions != final_permissions
                or count_row[0][0] != row_count
                or row_count != expectation.rows
                or typed_digest != expectation.typed_digest
            ):
                raise ValueError(_ERROR)
            self._statement("ROLLBACK TRANSACTION;")
            transaction = False
            observer_after = capture_observer_incarnation(self._query, admission=self._admission)
            result = SqlClientWriterSettlementObservation(
                departure=departure,
                stage_before=before,
                stage_after=after,
                observer_before=observer_before,
                observer_after=observer_after,
                row_count=row_count,
                typed_digest=typed_digest,
                typed_sum=typed_sum,
            )
            self._session.finish(self._lease)
            return result
        except BaseException as error:
            if transaction:
                try:
                    self._cursor.execute("IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;")
                except BaseException:
                    pass
            self._session.fail(self._lease)
            if not isinstance(error, Exception):
                raise
            raise RuntimeError(_ERROR) from None

    def close(self) -> None:
        """Close the externally owned connection exactly once."""
        if self._closed:
            raise RuntimeError(_ERROR)
        self._closed = True
        self._close_connection()


__all__ = ("SqlClientWriterSettlementObserver",)
