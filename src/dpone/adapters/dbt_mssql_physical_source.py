"""Fresh-connection reader for the signed, caller-preserving source bridge."""

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_source_queries import ENTRY
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_source_identity import (
    PhysicalSourceIdentity,
    PhysicalSourceReadError,
    decode_physical_source_row,
    require_source_identity,
)
from dpone.contracts.dbt_mssql_physical_source_identity import _bytes as _bytes
from dpone.contracts.dbt_mssql_physical_source_identity import _uuid as _uuid
from dpone.contracts.dbt_mssql_physical_validation import require_physical_uuid
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class MssqlPhysicalSourceReader:
    """Read one current source identity with an actual METADATA or BUILD login.

    The injected factory selects the registered model database and supplies a
    fresh dedicated connection with finite connect/statement timeouts. It must
    not return an administrator or impersonating connection. Provisioning has
    already authenticated the registration and exact signed-module inventory.
    """

    def __init__(
        self, *, connection_factory: Callable[[], SqlControlConnection], registration: MssqlPhysicalRuntimeRegistration
    ) -> None:
        if type(registration) is not MssqlPhysicalRuntimeRegistration:
            raise ValueError("source reader requires an authenticated exact registration")
        registration.__post_init__()
        self._registration = registration
        self._schema = native_control_schema(registration.local_schema)
        self._connect = connection_factory

    def read(self, generation_id: str, executor_invocation_id: str) -> PhysicalSourceIdentity:
        """Commit a bounded read, close its connection and return validated facts.

        SQL/transport/commit failure returns no accepted observation. There is
        no mutation retry, receipt reconciliation or fallback caller. Locks end
        with this call; model mutation requires its own current admission checks.
        """
        require_physical_uuid(generation_id, "generation_id")
        require_physical_uuid(executor_invocation_id, "executor_invocation_id")
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            # ODBC can already have opened the fresh connection's transaction.
            # Own that transaction rather than nesting an unconditional BEGIN.
            cursor.execute(
                "IF @@TRANCOUNT=0 BEGIN TRANSACTION; "
                "IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 "
                "THROW 51420, 'DPONE_PHYSICAL_SOURCE_TRANSACTION_INVALID', 1;"
            )
            cursor.execute(
                f"EXEC [{self._schema}].[{ENTRY}] @registration_id=?, @generation=?, @expected_invocation=?",
                self._registration.registration_id,
                generation_id,
                executor_invocation_id,
            )
            facts = decode_physical_source_row(dbapi_lifecycle.row(cursor))
            if dbapi_lifecycle.row(cursor) is not None:
                raise PhysicalSourceReadError("source read returned extra facts")
            require_source_identity(facts, self._registration, generation_id, executor_invocation_id)
            connection.commit()
            return facts
        except Exception as error:
            dbapi_lifecycle.rollback(connection)
            raise PhysicalSourceReadError("source identity could not be observed") from error
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
