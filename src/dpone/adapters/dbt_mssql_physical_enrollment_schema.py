"""Platform-owned enrollment deployment boundary.

Authenticated package/public-certificate bytes and an exclusive privileged DDL
window are external prerequisites. Runtime build/import paths must not provision
this boundary. Certificate private keys never enter this API. Deployment does not
certify effective signed permissions; both database layouts require live proof.
"""

from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from typing import TypedDict

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_enrollment_permissions import (
    ATTACH,
    CONTROL,
    ENROLL,
    OBSERVER,
    READ,
    C,
    E,
    EnrollmentModules,
    enrollment_permission_inventory,
)
from dpone.adapters.dbt_mssql_physical_enrollment_tables import TABLES, enrollment_tables, verify_enrollment_tables
from dpone.adapters.dbt_mssql_physical_source_schema import module_inventory_sql, principal_inventory_sql
from dpone.contracts.dbt_mssql_physical_catalog_binding import (
    CatalogRegistrationBinding,
    require_catalog_binding_registration,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.mssql_object_name import quote_mssql_identifier
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class _Coordinates(TypedDict):
    model_database: MssqlDatabaseAuthorityPin
    model_schema: str
    model_schema_id: int
    control_database: str
    control_schema: str
    enrollment_certificate_thumbprint: bytes


@dataclass(frozen=True, slots=True)
class EnrollmentDeploymentObservation:
    """Acknowledged expanded module hashes and observed certificate identities."""

    module_sha256: tuple[tuple[str, str], ...]
    source_sha256: tuple[tuple[str, str], ...]
    tables_sha256: str
    enrollment_certificate_thumbprint: bytes
    connection_certificate_thumbprint: bytes


class MssqlPhysicalEnrollmentSchemaProvisioner:
    """Retain authenticated inputs for the finite enrollment/session inventory.

    The injected factory must provide a fresh bounded CONTROL SERVER connection.
    Certificates E and C are distinct from each other and existing boundaries.
    Their identities must be observed from SQL Server, never guessed by hashing
    the public bytes. Missing package or public-certificate bytes fail before I/O.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        enrollment_sql: bytes,
        session_sql: bytes,
        discovery_sql: bytes,
        enrollment_certificate_public_bytes: bytes,
        connection_certificate_public_bytes: bytes,
    ) -> None:
        for value in (
            enrollment_sql,
            session_sql,
            discovery_sql,
            enrollment_certificate_public_bytes,
            connection_certificate_public_bytes,
        ):
            if type(value) is not bytes or not value:
                raise ValueError("authenticated package and public certificate bytes are required")
        if enrollment_certificate_public_bytes == connection_certificate_public_bytes:
            raise ValueError("enrollment and connection certificates must be distinct")
        self._connect = connection_factory
        self._enrollment = enrollment_sql
        self._session = session_sql
        self._discovery = discovery_sql
        self._enrollment_public = enrollment_certificate_public_bytes
        self._connection_public = connection_certificate_public_bytes

    def apply(
        self, registration: MssqlPhysicalRuntimeRegistration, binding: CatalogRegistrationBinding
    ) -> EnrollmentDeploymentObservation:
        """Install absent inventory or verify replay; drift and uncertain ACK reject."""
        require_catalog_binding_registration(binding, registration)
        if registration.local_schema != "dpone_physical" or binding.model_schema_owner_id != 1:
            raise ValueError("enrollment requires literal dpone_physical and dbo model schema owner")
        if type(registration.principals.observer) is not DedicatedObserver:
            raise ValueError("enrollment requires dedicated runtime mappings")
        from dpone.adapters.dbt_mssql_physical_discovery_schema import verify_catalog_binding_context
        from dpone.adapters.dbt_mssql_physical_enrollment_queries import enrollment_procedures
        from dpone.adapters.dbt_mssql_physical_session_queries import session_procedures

        connection = cursor = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET NOCOUNT ON; SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
            self._context(cursor, registration, "model")
            verify_catalog_binding_context(cursor, registration, binding)
            e_model = self._certificate(cursor, E, self._enrollment_public, private=True)
            c_model = self._certificate(cursor, C, self._connection_public, private=True)
            self._context(cursor, registration, "control")
            e_control = self._certificate(cursor, E, self._enrollment_public, private=True)
            cursor.execute("USE [master];")
            c_master = self._certificate(cursor, C, self._connection_public, private=False)
            if e_model != e_control or c_model != c_master or e_model == c_model:
                raise RuntimeError("enrollment certificate identities differ or overlap")
            coordinates = _Coordinates(
                model_database=registration.model_database,
                model_schema=binding.model_schema,
                model_schema_id=binding.model_schema_id,
                control_database=registration.control_database.database_name,
                control_schema=registration.control_schema,
                enrollment_certificate_thumbprint=e_model,
            )
            definitions = enrollment_procedures(
                enrollment_sql=self._enrollment, discovery_sql=self._discovery, **coordinates
            )
            sessions = session_procedures(
                enrollment_sql=self._enrollment,
                discovery_sql=self._discovery,
                session_sql=self._session,
                connection_certificate_thumbprint=c_model,
                **coordinates,
            )
            if set(definitions) != {ENROLL, READ, CONTROL} or set(sessions) != {ATTACH, OBSERVER}:
                raise ValueError("enrollment requires exactly five finite modules")
            definitions.update(sessions)
            modules = [
                ("model", "dpone_physical", name, C if name == OBSERVER else E, "SPVC")
                for name in (ENROLL, READ, ATTACH, OBSERVER)
            ]
            modules.append(("control", registration.control_schema, CONTROL, E, "CPVC"))
            objects = []
            for namespace, schema, name, _, _ in modules:
                self._context(cursor, registration, namespace)
                objects.append(self._exists(cursor, f"[{schema}].[{name}]"))
            self._context(cursor, registration, "model")
            objects.extend(self._exists(cursor, f"[dpone_physical].[{name}]") for name in TABLES)
            if any(objects) and not all(objects):
                raise RuntimeError("enrollment inventory is partial; repair is prohibited")
            create = not any(objects)
            self._runtime(cursor, registration, modules)
            self._inventories(cursor, registration, modules, create=create, before=True)
            if create:
                self._context(cursor, registration, "model")
                cursor.execute(enrollment_tables(enrollment_sql=self._enrollment))
                for namespace, schema, name, certificate, kind in modules:
                    self._context(cursor, registration, namespace)
                    cursor.execute(definitions[name])
                    cursor.execute(module_inventory_sql(schema, name, certificate, kind), definitions[name])
                    counter = "COUNTER " if kind == "CPVC" else ""
                    cursor.execute(
                        f"ADD {counter}SIGNATURE TO OBJECT::[{schema}].[{name}] BY CERTIFICATE [{certificate}];"
                    )
            self._inventories(cursor, registration, modules, create=create, before=False)
            for namespace, schema, name, certificate, kind in modules:
                self._context(cursor, registration, namespace)
                cursor.execute(module_inventory_sql(schema, name, certificate, kind), definitions[name])
            self._context(cursor, registration, "model")
            verify_enrollment_tables(cursor)
            self._runtime(cursor, registration, modules)
            connection.commit()
            return EnrollmentDeploymentObservation(
                tuple(
                    sorted(
                        (name, "sha256:" + sha256(body.encode("utf-16le")).hexdigest())
                        for name, body in definitions.items()
                    )
                ),
                tuple(
                    (name, "sha256:" + sha256(source).hexdigest())
                    for name, source in (
                        ("enrollment", self._enrollment),
                        ("session", self._session),
                        ("discovery", self._discovery),
                    )
                ),
                "sha256:" + sha256(enrollment_tables(enrollment_sql=self._enrollment).encode("utf-16le")).hexdigest(),
                e_model,
                c_model,
            )
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)

    @staticmethod
    def _one(cursor: SqlControlCursor) -> object:
        row = dbapi_lifecycle.row(cursor)
        if row is None or len(row) != 1 or dbapi_lifecycle.row(cursor) is not None:
            raise RuntimeError("enrollment inventory requires one scalar row")
        return row[0]

    @classmethod
    def _exists(cls, cursor: SqlControlCursor, name: str) -> bool:
        cursor.execute("SELECT OBJECT_ID(?)", name)
        observed = cls._one(cursor)
        if observed is not None and (type(observed) is not int or observed <= 0):
            raise RuntimeError("enrollment object identity is malformed")
        return observed is not None

    @staticmethod
    def _context(cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> None:
        pin = getattr(value, namespace + "_database")
        cursor.execute(f"USE {quote_mssql_identifier(pin.database_name)};")
        cursor.execute(
            """DECLARE @name nvarchar(128)=?,@id int=?,@created datetime2(7)=?,@guid uniqueidentifier=?;
IF ISNULL(HAS_PERMS_BY_NAME(NULL,NULL,'CONTROL SERVER'),0)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id
 WHERE d.database_id=DB_ID() AND d.database_id=@id AND CONVERT(varbinary(max),d.name)=CONVERT(varbinary(max),@name)
 AND DATALENGTH(d.name)=DATALENGTH(@name) AND CONVERT(datetime2(7),d.create_date)=@created AND r.database_guid=@guid
 AND d.is_trustworthy_on=0 AND d.is_db_chaining_on=0)
 OR NOT EXISTS (SELECT 1 FROM sys.configurations WHERE name='cross db ownership chaining' AND value_in_use=0)
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE grantee_principal_id=2 AND state IN ('G','W'))
 THROW 51630,'DPONE_ENROLLMENT_DATABASE_UNSAFE',1;""",
            pin.database_name,
            pin.database_id,
            pin.create_token,
            str(pin.database_guid),
        )
        schema = value.local_schema if namespace == "model" else value.control_schema
        cursor.execute(
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name=N'{schema}' AND principal_id=1) THROW 51630,'DPONE_ENROLLMENT_SCHEMA_UNSAFE',1;"
        )

    @classmethod
    def _certificate(cls, cursor: SqlControlCursor, name: str, public: bytes, *, private: bool) -> bytes:
        key = "AND pvt_key_encryption_type IN ('MK','PW')" if private else ""
        cursor.execute(
            f"""DECLARE @public varbinary(max)=?;
IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name=N'{name}' AND principal_id=1 {key}
 AND CERTENCODED(certificate_id)=@public AND DATALENGTH(CERTENCODED(certificate_id))=DATALENGTH(@public))
 OR (SELECT COUNT(*) FROM sys.certificates WHERE thumbprint=(SELECT thumbprint FROM sys.certificates WHERE name=N'{name}'))<>1
 THROW 51631,'DPONE_ENROLLMENT_CERTIFICATE_UNSAFE',1;""",
            public,
        )
        cursor.execute("SELECT thumbprint FROM sys.certificates WHERE name=?", name)
        thumbprint = cls._one(cursor)
        if type(thumbprint) is not bytes or len(thumbprint) != 20:
            raise RuntimeError("enrollment certificate thumbprint is malformed")
        return thumbprint

    def _runtime(
        self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, modules: EnrollmentModules
    ) -> dict[tuple[str, int], str]:
        assert isinstance(value.principals.observer, DedicatedObserver)
        mappings = (value.principals.metadata, value.principals.build, value.principals.observer.mapping)
        names = {}
        for namespace, schema, module, _, _ in modules:
            self._context(cursor, value, namespace)
            for ordinal, mapping in enumerate(mappings):
                principal = getattr(mapping, namespace)
                allowed = namespace == "model" and (
                    (ordinal == 0 and module in (ENROLL, READ)) or (ordinal == 1 and module == ATTACH)
                )
                cursor.execute(
                    principal_inventory_sql(schema, module, model=allowed),
                    principal.principal_id,
                    bytes.fromhex(principal.sid_hex),
                )
                name = self._one(cursor)
                if type(name) is not str:
                    raise RuntimeError("enrollment runtime principal name is malformed")
                names[(namespace, principal.principal_id)] = name
        return names

    def _inventories(
        self,
        cursor: SqlControlCursor,
        value: MssqlPhysicalRuntimeRegistration,
        modules: EnrollmentModules,
        *,
        create: bool,
        before: bool,
    ) -> None:
        enrollment_permission_inventory(
            cursor, value, modules, self._runtime(cursor, value, modules), create=create, before=before
        )
