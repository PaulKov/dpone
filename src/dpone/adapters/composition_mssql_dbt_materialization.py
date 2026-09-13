"""Read actual selected dbt tables/views and complete bounded MSSQL columns.

The app supplies verified source contracts and a fresh target connection with
its signed database pin. Its mandatory callback independently verifies protected
service/enrollment on that same connection. This adapter does not acquire writer
credentials, infer pins, install objects or commit. Data parity is separate.
"""

from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from dpone.adapters.dbapi_lifecycle import close, rollback
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_materialization import (
    MAX_COLUMNS,
    MAX_MATERIALIZATIONS,
    DbtMaterializationContract,
    actual_materialization_type,
    materialization_catalog_columns,
    materialization_observation_document,
    require_materialization_columns,
)
from dpone.contracts.composition_dbt_outcome import DbtCaptureError, DbtDispatchIntent, DbtOutcomeExpectation
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor

_actual_type = actual_materialization_type

ReadContracts = Callable[[CompositionAttemptIdentity], tuple[DbtMaterializationContract, ...]]
OpenTarget = Callable[
    [CompositionAttemptIdentity, DbtRelationWrite], tuple[SqlControlConnection, MssqlDatabaseAuthorityPin]
]
RequireTarget = Callable[
    [SqlControlConnection, CompositionAttemptIdentity, DbtRelationWrite, MssqlDatabaseAuthorityPin], None
]


class MssqlDbtMaterializationObserver:
    """Per-call source verification plus independent, transaction-stable catalog."""

    def __init__(
        self, *, read_contracts: ReadContracts, open_target: OpenTarget, require_target: RequireTarget
    ) -> None:
        self._read_contracts, self._open_target, self._require_target = read_contracts, open_target, require_target

    def __call__(
        self,
        attempt: CompositionAttemptIdentity,
        intent: DbtDispatchIntent,
        expectation: DbtOutcomeExpectation,
        invocation: str,
    ) -> bytes:
        """Return actual originals bound to this attempt and captured invocation."""
        if (
            type(attempt) is not CompositionAttemptIdentity
            or type(intent) is not DbtDispatchIntent
            or intent.attempt != attempt
        ):
            raise DbtCaptureError("materialization_attempt")
        attempt.__post_init__()
        intent.__post_init__()
        if (
            type(expectation) is not DbtOutcomeExpectation
            or type(invocation) is not str
            or not invocation
            or len(invocation) > 128
        ):
            raise DbtCaptureError("materialization_invocation")
        expectation.__post_init__()
        contracts = self._read_contracts(attempt)
        if (
            type(contracts) is not tuple
            or not 1 <= len(contracts) <= MAX_MATERIALIZATIONS
            or any(type(row) is not DbtMaterializationContract for row in contracts)
        ):
            raise DbtCaptureError("materialization_contracts")
        for contract in contracts:
            contract.__post_init__()
        projected = tuple(row.expectation for row in contracts)
        if projected != expectation.materializations or len({row[0] for row in projected}) != len(projected):
            raise DbtCaptureError("materialization_expectation")
        catalog = [self._observe(attempt, row) for row in contracts]
        # The reader reopens originals again, rather than reusing caller hashes.
        if self._read_contracts(attempt) != contracts:
            raise DbtCaptureError("materialization_source_changed")
        return materialization_observation_document(
            attempt_sha256=attempt.attempt_sha256,
            intent_sha256=intent.intent_sha256,
            invocation=invocation,
            projected=projected,
            catalog=catalog,
        )

    _require_columns = staticmethod(require_materialization_columns)

    def _observe(self, attempt: CompositionAttemptIdentity, contract: DbtMaterializationContract) -> dict[str, Any]:
        connection = cursor = None
        try:
            connection, pin = self._open_target(attempt, contract.write)
            if type(pin) is not MssqlDatabaseAuthorityPin or pin.database_name != contract.write.database:
                raise DbtCaptureError("materialization_database_pin")
            checked = MssqlDatabaseAuthorityPin.from_raw(
                pin.database_name,
                {
                    "database_id": pin.database_id,
                    "database_guid": str(pin.database_guid),
                    "create_token": pin.create_token,
                },
                capability="target",
            )
            if checked != pin or pin.database_id <= 4 or pin.database_guid.int == 0:
                raise DbtCaptureError("materialization_database_pin")
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(
                "SET NOCOUNT ON; SET XACT_ABORT ON; SET LOCK_TIMEOUT 10000; "
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; IF @@TRANCOUNT=0 BEGIN TRANSACTION;"
            )
            state = self._state(cursor, pin)
            self._require_target(connection, attempt, contract.write, pin)
            if connection.autocommit is not False or self._state(cursor, pin) != state:
                raise DbtCaptureError("materialization_transaction_changed")
            observed = self._catalog(cursor, contract)
            self._require_columns(contract, observed["columns"])
            self._require_target(connection, attempt, contract.write, pin)
            if (
                connection.autocommit is not False
                or self._state(cursor, pin) != state
                or self._catalog(cursor, contract) != observed
                or self._state(cursor, pin) != state
            ):
                raise DbtCaptureError("materialization_catalog_changed")
            connection.rollback()
            return {
                "unique_id": contract.write.resource_id,
                "write": asdict(contract.write),
                "declared_columns": [asdict(column) for column in contract.columns],
                "schema_sha256": contract.schema_sha256,
                "database_authority": {
                    "database_name": state[0],
                    "database_id": state[1],
                    "database_guid": state[2],
                    "create_token": state[3],
                },
                "transaction_id": state[6],
                **observed,
            }
        except DbtCaptureError:
            raise
        except Exception:
            raise DbtCaptureError("materialization_catalog_unavailable") from None
        finally:
            rollback(connection)
            close(cursor)
            close(connection)

    @staticmethod
    def _state(cursor: SqlControlCursor, pin: MssqlDatabaseAuthorityPin) -> tuple[Any, ...]:
        cursor.execute(
            "SELECT TOP (2) DB_NAME(),d.database_id,LOWER(CONVERT(char(36),r.database_guid)),"
            "CONVERT(nvarchar(33),d.create_date,126),@@TRANCOUNT,XACT_STATE(),CURRENT_TRANSACTION_ID(),"
            "HAS_PERMS_BY_NAME(DB_NAME(),N'DATABASE',N'VIEW DEFINITION'),"
            "CASE WHEN EXISTS (SELECT 1 FROM sys.database_permissions p JOIN sys.user_token u ON u.principal_id=p.grantee_principal_id "
            "WHERE p.state='D' AND p.permission_name IN ('CONTROL','VIEW DEFINITION','VIEW SECURITY DEFINITION','VIEW PERFORMANCE DEFINITION')) "
            "THEN 1 ELSE 0 END FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.database_id=DB_ID();"
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if (
            len(rows) != 1
            or len(rows[0]) != 9
            or rows[0][:4] != (pin.database_name, pin.database_id, str(pin.database_guid), pin.create_token)
            or any(type(value) is not int for value in rows[0][4:])
            or rows[0][4] < 1
            or rows[0][5] != 1
            or not 0 < rows[0][6] < 2**63
            or rows[0][7:] != (1, 0)
        ):
            raise DbtCaptureError("materialization_physical_visibility")
        return rows[0]

    @staticmethod
    def _catalog(cursor: SqlControlCursor, contract: DbtMaterializationContract) -> dict[str, Any]:
        write = contract.write
        cursor.execute(
            "SELECT TOP (2) o.object_id,s.name,o.name,CASE o.type WHEN 'U' THEN 'table' WHEN 'V' THEN 'view' ELSE 'unsupported' END,"
            "CONVERT(nvarchar(33),o.create_date,126),CONVERT(nvarchar(33),o.modify_date,126),HASHBYTES('SHA2_256',m.definition) "
            "FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id LEFT JOIN sys.sql_modules m ON m.object_id=o.object_id "
            "WHERE s.name=? AND o.name=?;",
            write.schema,
            write.relation,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if (
            len(rows) != 1
            or len(rows[0]) != 7
            or type(rows[0][0]) is not int
            or rows[0][0] <= 0
            or rows[0][1:4] != (write.schema, write.relation, contract.kind)
            or any(type(value) is not str or not value for value in rows[0][4:6])
            or (contract.kind == "view" and (type(rows[0][6]) is not bytes or len(rows[0][6]) != 32))
            or (contract.kind == "table" and rows[0][6] is not None)
        ):
            raise DbtCaptureError("materialization_relation")
        object_row = rows[0]
        cursor.execute(
            f"SELECT TOP ({MAX_COLUMNS + 1}) c.column_id,c.name,t.name,SCHEMA_NAME(ut.schema_id)+N'.'+ut.name,CONVERT(int,ut.is_user_defined),"
            "c.max_length,c.precision,c.scale,CONVERT(int,c.is_nullable),c.collation_name,CONVERT(int,c.is_computed),"
            "CONVERT(int,c.is_identity),CONVERT(int,c.is_hidden),c.generated_always_type,c.encryption_type "
            "FROM sys.columns c LEFT JOIN sys.types t ON t.user_type_id=c.system_type_id AND t.user_type_id=t.system_type_id "
            "JOIN sys.types ut ON ut.user_type_id=c.user_type_id WHERE c.object_id=? ORDER BY c.column_id;",
            object_row[0],
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        return materialization_catalog_columns(object_row, rows)
