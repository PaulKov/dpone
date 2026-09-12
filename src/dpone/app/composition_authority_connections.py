"""Construct independent authority sessions without crossing credential endpoints."""

from __future__ import annotations

import json
import re
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

from dpone.adapters.composition_mssql_catalog import require_composition_mssql_schema
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION, require_control_schema
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.dbt_workspace_attempt import require_workspace_authority_connection_ref
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthoritySet
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.mssql_database_authority_support import bounded_authority_connection

if TYPE_CHECKING:
    from dpone.contracts.composition_activation import CompositionOccurrenceContext
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.sql_connection import SqlControlConnection


class _Inputs(Protocol):
    def resolve_connection(
        self, context: CompositionOccurrenceContext, connection_ref: str
    ) -> ResolvedBindingConnection: ...


class _ConnectorFactory(Protocol):
    def __call__(self, connection: ResolvedBindingConnection, *, autocommit: bool) -> Any: ...


_HEADER = (
    "SELECT @@TRANCOUNT,XACT_STATE(),CURRENT_TRANSACTION_ID(),d.database_id,d.name,"
    "LOWER(CONVERT(char(36),r.database_guid)),CONVERT(nvarchar(33),d.create_date,126),"
    "CASE WHEN SUSER_SID(ORIGINAL_LOGIN())=SUSER_SID() THEN 1 ELSE 0 END "
    "FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.database_id=DB_ID();"
)
_CH_BOUNDS = (
    " SETTINGS max_execution_time=10,max_result_rows=8193,max_result_bytes=8388608,result_overflow_mode='throw'"
)
_CH_FIXED = frozenset(
    {
        "SELECT database,name,toString(uuid),engine,create_table_query,dependencies_database,dependencies_table "
        "FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') ORDER BY database,name LIMIT 8193",
        "SELECT count() FROM system.clusters WHERE NOT is_local OR shard_num != 1 OR replica_num != 1",
        "SELECT count() FROM system.mutations WHERE NOT is_done",
        "SELECT count() FROM system.row_policies",
    }
)
_CH_LITERAL = r"'(?:[^'\\\x00]|\\[\\']){1,1024}'"
_CH_DYNAMIC = re.compile(
    r"(?:SELECT toString\(serverUUID\(\)\),name,toString\(uuid\),engine FROM system.databases WHERE name="
    + _CH_LITERAL
    + r" LIMIT 2|SELECT count\(\) FROM system.columns WHERE database="
    + _CH_LITERAL
    + r" AND default_kind != ''|SELECT count\(\) FROM system.data_skipping_indices WHERE database="
    + _CH_LITERAL
    + r")"
)


class CompositionAuthorityConnections:
    """Explicit app construction from exact locally verified runtime contexts.

    This does not enroll servers, grant privileges, issue writers or establish
    ACTIVE ownership. Target observers require externally provisioned SELECT on
    the authority row, complete control-database metadata visibility and access
    to its database-incarnation metadata. No method grants its own permissions.
    """

    def __init__(
        self,
        *,
        inputs: _Inputs,
        authority_connection_ref: str,
        control_schema: str = "dpone_control",
        connector_factory: _ConnectorFactory = ResolvedConnectorFactory.create,
    ) -> None:
        self._inputs = inputs
        self._reference = require_workspace_authority_connection_ref(authority_connection_ref)
        self._schema = require_control_schema(control_schema)
        self._factory = connector_factory

    def control_connection(self, context: CompositionOccurrenceContext) -> SqlControlConnection:
        """Return one independent verified controller connection; caller owns it.

        Verification ends with rollback and cursor close. The returned connection
        remains open for the caller's own protected transaction and readback.
        """
        connection, service, pin = self._authority(context)
        return self._open_verified(connection, service, pin, keep_connection=True)

    def require_mssql_target_service(
        self, target: ResolvedBindingConnection, expected_service_id: str, context: CompositionOccurrenceContext
    ) -> None:
        """Read the protected marker through the actual target host and principal.

        Only database is changed to the authority database. Host, port, driver,
        TLS and target credential material are retained; controller secrets are
        never forwarded to a workload endpoint. Both signed control-DB pins and
        service pins must agree before constructing this probe connection.
        """
        _, service, authority_pin = self._authority(context)
        try:
            target_service, authorities = self._mssql_authorities(target)
            target_pin = authorities.require(authority_pin.database_name, capability="target")
            if expected_service_id != service or target_service != service or target_pin != authority_pin:
                raise ValueError("target authority")
            probe = replace(target, credentials=replace(target.credentials, database=authority_pin.database_name))
        except Exception:
            raise CompositionAdmissionError("target_service_pin") from None
        self._open_verified(probe, service, authority_pin, keep_connection=False)

    def query_clickhouse(self, connection: ResolvedBindingConnection, statement: str) -> tuple[tuple[Any, ...], ...]:
        """Execute only fixed composition catalog SELECTs with bounded transport.

        This is not a free-SQL API. The injected protected enrollment reader must
        establish complete visibility of this binding's actual read principal.
        Native and HTTP clients use the same bounded immutable credential clone.
        """
        if (
            connection.descriptor is None
            or connection.descriptor.connection_type != "clickhouse"
            or not isinstance(statement, str)
            or len(statement) > 65536
        ):
            raise CompositionAdmissionError("clickhouse_catalog_statement")
        query = statement.removesuffix(_CH_BOUNDS)
        if query not in _CH_FIXED and _CH_DYNAMIC.fullmatch(query) is None:
            raise CompositionAdmissionError("clickhouse_catalog_statement")
        connector = None
        failure = None
        result = None
        try:
            credentials = connection.credentials
            settings = {
                **(credentials.settings or {}),
                "readonly": 1,
                "max_execution_time": 10,
                "max_result_rows": 8193,
                "max_result_bytes": 8 * 1024 * 1024,
                "result_overflow_mode": "throw",
                "skip_unavailable_shards": 0,
            }
            bounded = replace(
                connection,
                credentials=replace(
                    credentials,
                    connect_timeout=_timeout(credentials.connect_timeout),
                    send_receive_timeout=_timeout(credentials.send_receive_timeout),
                    settings=settings,
                ),
            )
            connector = self._factory(bounded, autocommit=True)
            rows = connector.get_records(query)
            if (
                not isinstance(rows, (tuple, list))
                or len(rows) > 8192
                or any(not isinstance(row, (tuple, list)) for row in rows)
            ):
                raise ValueError("row budget")
            if len(json.dumps(rows, allow_nan=False).encode()) > 8 * 1024 * 1024:
                raise ValueError("byte budget")
            result = tuple(tuple(_detach(value) for value in row) for row in rows)
        except Exception:
            failure = CompositionAdmissionError("clickhouse_catalog_query")
        finally:
            if connector is not None:
                try:
                    connector.close()
                except Exception:
                    failure = CompositionAdmissionError("clickhouse_catalog_cleanup")
        if failure is not None:
            raise failure from None
        assert result is not None
        return result

    def _authority(self, context):
        context.__post_init__()
        try:
            connection = self._inputs.resolve_connection(context, self._reference)
            service, authorities = self._mssql_authorities(connection)
            pin = authorities.require(connection.credentials.database, capability="target")
            return connection, service, pin
        except Exception:
            raise CompositionAdmissionError("authority_connection") from None

    @staticmethod
    def _mssql_authorities(connection):
        descriptor = connection.descriptor
        if descriptor is None or descriptor.connection_type != "mssql":
            raise ValueError("connector")
        service = descriptor.properties.get("composition_service_id")
        if type(service) is not str or str(UUID(service)) != service or UUID(service).int == 0:
            raise ValueError("service")
        database = connection.credentials.database
        if type(database) is not str or database != descriptor.properties.get("database"):
            raise ValueError("database")
        authorities = MssqlDatabaseAuthoritySet.from_connection_properties(descriptor.properties, capability="target")
        if any(pin.database_guid.int == 0 for pin in authorities.pins):
            raise ValueError("database incarnation")
        return service, authorities

    def _open_verified(self, connection, service, pin, *, keep_connection):
        connector = raw = cursor = None
        failure = None
        try:
            bounded = bounded_authority_connection(connection, connect_timeout_seconds=10)
            bounded = replace(
                bounded,
                credentials=replace(bounded.credentials, query_timeout=_timeout(bounded.credentials.query_timeout)),
            )
            connector = self._factory(bounded, autocommit=False)
            raw = connector.connection
            raw.autocommit = False
            cursor = raw.cursor()
            cursor.execute(
                "IF @@TRANCOUNT<>0 THROW 51000,'composition_observer_transaction',1; "
                "SET XACT_ABORT ON; SET NOCOUNT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;"
            )
            header = self._header(cursor, pin)
            self._marker(cursor, service)
            require_composition_mssql_schema(cursor, self._schema)
            self._marker(cursor, service)
            if self._header(cursor, pin) != header:
                raise ValueError("changed transaction")
        except Exception:
            failure = CompositionAdmissionError("authority_endpoint_observation")
        finally:
            for operation in (
                (raw.rollback if raw is not None else None),
                (cursor.close if cursor is not None else None),
            ):
                if operation is not None:
                    try:
                        operation()
                    except Exception:
                        failure = CompositionAdmissionError("authority_endpoint_cleanup")
            if connector is not None and (not keep_connection or failure is not None):
                try:
                    connector.close()
                except Exception:
                    failure = CompositionAdmissionError("authority_endpoint_cleanup")
        if failure is not None:
            raise failure from None
        assert raw is not None
        return raw

    @staticmethod
    def _header(cursor, pin):
        cursor.execute(_HEADER)
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if len(rows) != 1 or len(rows[0]) != 8:
            raise ValueError("database observation")
        value = rows[0]
        if (
            value[:2] != (1, 1)
            or any(type(value[index]) is not int for index in (0, 1, 2, 3, 7))
            or not 1 <= value[2] < 2**63
            or value[3:] != (pin.database_id, pin.database_name, str(pin.database_guid), pin.create_token, 1)
        ):
            raise ValueError("database incarnation")
        return value

    def _marker(self, cursor, service):
        cursor.execute(
            "SELECT TOP (2) singleton,schema_version,LOWER(CONVERT(char(36),service_id)) "
            f"FROM [{self._schema}].[composition_authority] WITH (HOLDLOCK);"
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if rows != ((1, COMPOSITION_MSSQL_SCHEMA_VERSION, service),) or any(
            type(value) is not int for value in rows[0][:2]
        ):
            raise ValueError("service marker")


def _timeout(value: object) -> int:
    return min(value, 10) if type(value) is int and value > 0 else 10


def _detach(value):
    if isinstance(value, (list, tuple)):
        return tuple(_detach(item) for item in value)
    if value is None or type(value) in (str, int, bool, float):
        return value
    raise ValueError("catalog scalar")
