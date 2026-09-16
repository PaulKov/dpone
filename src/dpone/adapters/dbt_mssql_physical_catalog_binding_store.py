"""Immutable companion binding writes and independent authenticated-expectation reads.

Typed arguments are not authority tokens. The application must authenticate the
selected retained policy/member and observe the pinned schema/module deployment.
This adapter proves only exact protected storage with its existing registration.
"""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_catalog_binding_schema import (
    TABLE,
    CatalogBindingStorageError,
    require_storage_registration,
    verify_binding_protection_sql,
    verify_binding_table_sql,
    verify_registration_context,
)
from dpone.contracts.dbt_mssql_physical_catalog_binding import (
    CatalogRegistrationBinding,
    catalog_binding_digest,
    decode_catalog_binding,
    encode_catalog_binding,
    require_catalog_binding_registration,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class MssqlPhysicalCatalogBindingStore:
    """Own one local transaction per attempt; never retry a mutation.

    The factory returns a fresh privileged model-database connection with finite
    connect/statement timeouts. Every attempt verifies both tables and DENYs,
    locks the registration first, then the companion UUID. Exact replay is read
    only; registration, schema, principals and programme cannot be repaired here.
    """

    def __init__(self, *, connection_factory: Callable[[], SqlControlConnection], local_schema: str) -> None:
        self._connect = connection_factory
        self._schema = native_control_schema(local_schema)

    def register(
        self, expected_registration: MssqlPhysicalRuntimeRegistration, binding: CatalogRegistrationBinding
    ) -> CatalogRegistrationBinding:
        """Insert absent or verify exact replay, then independently resolve once.

        Execute/commit uncertainty triggers that same single read-only resolution.
        Missing/conflicting/unreadable storage cannot produce success. There is no
        UPDATE, DELETE, new UUID, mutation retry or returned pre-commit observation.
        """
        expected = self._expected(expected_registration, binding)
        failure: Exception | None = None
        try:
            self._execute(expected_registration, expected, insert=True)
        except Exception as exc:
            failure = exc
        try:
            return self.resolve(expected_registration, binding)
        except Exception as exc:
            raise CatalogBindingStorageError("catalog binding outcome could not be independently verified") from (
                failure or exc
            )

    def resolve(
        self, expected_registration: MssqlPhysicalRuntimeRegistration, binding: CatalogRegistrationBinding
    ) -> CatalogRegistrationBinding:
        """Read only and require canonical binding plus complete registration equality."""
        expected = self._expected(expected_registration, binding)
        row = self._execute(expected_registration, expected, insert=False)
        if row is None or row != expected or type(row[2]) is not bytes:
            raise CatalogBindingStorageError("catalog binding is absent or differs from exact expected bytes")
        observed = decode_catalog_binding(row[2])
        require_catalog_binding_registration(observed, expected_registration)
        if observed != binding or catalog_binding_digest(observed).encode("ascii") != row[3]:
            raise CatalogBindingStorageError("catalog binding canonical payload or digest differs")
        return observed

    def _expected(
        self, registration: MssqlPhysicalRuntimeRegistration, binding: CatalogRegistrationBinding
    ) -> tuple[object, ...]:
        require_storage_registration(registration, self._schema)
        require_catalog_binding_registration(binding, registration)
        payload = encode_catalog_binding(binding)
        if not 1 <= len(payload) <= 1048576:
            raise ValueError("catalog binding payload exceeds storage budget")
        return (
            binding.registration_id,
            binding.registration_sha256.encode("ascii"),
            payload,
            catalog_binding_digest(binding).encode("ascii"),
        )

    def _execute(
        self, registration: MssqlPhysicalRuntimeRegistration, expected: tuple[object, ...], *, insert: bool
    ) -> tuple[object, ...] | None:
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            verify_registration_context(cursor, registration, self._schema, write=insert)
            cursor.execute(verify_binding_table_sql(self._schema))
            cursor.execute(verify_binding_protection_sql(self._schema, registration))
            lock = "UPDLOCK,HOLDLOCK" if insert else "HOLDLOCK"
            cursor.execute(
                f"SELECT registration_id,registration_digest,payload,binding_digest FROM [{self._schema}].[{TABLE}] WITH ({lock}) WHERE registration_id=?",
                expected[0],
            )
            row = dbapi_lifecycle.row(cursor)
            if dbapi_lifecycle.row(cursor) is not None:
                raise CatalogBindingStorageError("catalog binding returned multiple rows")
            if row is not None:
                if len(row) != 4:
                    raise CatalogBindingStorageError("catalog binding storage row has an invalid shape")
                row = (str(row[0]).lower(), *row[1:])
                if row != expected:
                    raise CatalogBindingStorageError("catalog binding UUID conflicts with existing bytes")
            elif insert:
                cursor.execute(
                    f"INSERT [{self._schema}].[{TABLE}] (registration_id,registration_digest,payload,binding_digest) VALUES (?,?,?,?)",
                    *expected,
                )
            connection.commit()
            return row
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
