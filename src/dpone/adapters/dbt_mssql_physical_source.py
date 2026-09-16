"""Fresh-connection reader for the signed, caller-preserving source bridge."""

from collections.abc import Callable
from typing import cast
from uuid import UUID

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_source_queries import ENTRY
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import DatabasePrincipal
from dpone.contracts.dbt_mssql_physical_source_identity import PhysicalSourceIdentity, require_source_identity
from dpone.contracts.dbt_mssql_physical_validation import require_physical_uuid
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_identity import OriginalRef
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class PhysicalSourceReadError(RuntimeError):
    """No accepted source observation; no retry, mutation or release is implied."""


def _bytes(value: object, maximum: int) -> bytes:
    if type(value) not in {bytes, bytearray, memoryview}:
        raise PhysicalSourceReadError("source fact binary field has an invalid representation")
    result = bytes(cast(bytes | bytearray | memoryview, value))
    if not 0 < len(result) <= maximum:
        raise PhysicalSourceReadError("source fact binary field exceeds its bound")
    return result


def _uuid(value: object) -> str:
    return require_physical_uuid(str(value) if type(value) is UUID else value, "source UUID")


def _facts(row: tuple[object, ...] | None) -> PhysicalSourceIdentity:
    if row is None or len(row) != 14:
        raise PhysicalSourceReadError("source read requires exactly one closed fact row")
    return PhysicalSourceIdentity(
        wire_version=cast(int, row[0]),
        registration_id=_uuid(row[1]),
        registration_digest=_bytes(row[2], 71).decode("ascii"),
        generation_id=_uuid(row[3]),
        executor_invocation_id=_uuid(row[4]),
        guard_epoch=cast(int, row[5]),
        source_revision=cast(int, row[6]),
        reservation=OriginalRef(_bytes(row[7], 4096).decode("utf-8"), _bytes(row[8], 71).decode("ascii")),
        executor_payload=_bytes(row[9], 1048576),
        observed_model_principal=DatabasePrincipal(cast(int, row[10]), _bytes(row[11], 85).hex()),
        observed_control_principal=DatabasePrincipal(cast(int, row[12]), _bytes(row[13], 85).hex()),
    )


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
            cursor.execute("SET IMPLICIT_TRANSACTIONS OFF; BEGIN TRANSACTION")
            cursor.execute(
                f"EXEC [{self._schema}].[{ENTRY}] @registration_id=?, @generation=?, @expected_invocation=?",
                self._registration.registration_id,
                generation_id,
                executor_invocation_id,
            )
            facts = _facts(dbapi_lifecycle.row(cursor))
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
