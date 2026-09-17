"""Privileged deployment of the separate finite catalog permission boundary.

Before calling apply, the platform owner MUST authenticate the retained policy
projection and its model-schema selection against the registration's profile
reference and PLATFORM subject. DTO validation cannot establish that authority.
The owner also excludes concurrent privileged DDL and supplies a fresh CONTROL
SERVER connection with the pre-existing catalog certificate private key usable.
No key/password enters this API. Runtime and observer privileges are not widened.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_catalog_queries import ENTRY, catalog_procedure
from dpone.adapters.dbt_mssql_physical_source_schema import module_inventory_sql, principal_inventory_sql
from dpone.contracts.dbt_mssql_physical_registration import (
    MssqlPhysicalRuntimeRegistration,
    encode_physical_runtime_registration,
)
from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
from dpone.contracts.mssql_object_name import native_control_schema, quote_mssql_identifier
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


@dataclass(frozen=True, slots=True)
class CatalogDeploymentObservation:
    """Committed inventory observation, not profile authentication or admission."""

    model_schema: str
    model_schema_id: int
    model_schema_owner_id: int
    module_sha256: str
    certificate_thumbprint: bytes


class MssqlPhysicalCatalogSchemaProvisioner:
    """Install one exact signed module, or verify an existing deployment unchanged.

    A separate certificate grants database VIEW DEFINITION, SELECT on the system
    dependency view, and SELECT on the authenticated model schema. METADATA/BUILD
    receive only module EXECUTE. Existing definitions, signatures, grants or schema
    drift fail closed; the adapter never alters/replaces or repairs a deployment.
    Commit uncertainty produces no report and is never retried automatically.
    """

    _entry = ENTRY

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        catalog_sql: bytes,
        certificate_name: str,
        certificate_public_bytes: bytes,
        certificate_user: str,
    ) -> None:
        if type(catalog_sql) is not bytes or not catalog_sql:
            raise ValueError("authenticated catalog package bytes are required")
        if type(certificate_public_bytes) is not bytes or not certificate_public_bytes:
            raise ValueError("nonempty public certificate bytes are required")
        self._connect, self._sql, self._public = connection_factory, catalog_sql, certificate_public_bytes
        self._certificate = native_control_schema(certificate_name)
        self._user = native_control_schema(certificate_user)

    def apply(
        self,
        registration: MssqlPhysicalRuntimeRegistration,
        *,
        model_schema: str,
        model_schema_id: int,
        model_schema_owner_id: int,
    ) -> CatalogDeploymentObservation:
        """Install absent or verify existing exact inventory without repair."""
        return self._apply(
            registration,
            model_schema=model_schema,
            model_schema_id=model_schema_id,
            model_schema_owner_id=model_schema_owner_id,
            allow_create=True,
        )

    def verify(
        self,
        registration: MssqlPhysicalRuntimeRegistration,
        *,
        model_schema: str,
        model_schema_id: int,
        model_schema_owner_id: int,
    ) -> CatalogDeploymentObservation:
        """Read-only independent inventory observation; absence never installs."""
        return self._apply(
            registration,
            model_schema=model_schema,
            model_schema_id=model_schema_id,
            model_schema_owner_id=model_schema_owner_id,
            allow_create=False,
        )

    def _apply(
        self,
        registration: MssqlPhysicalRuntimeRegistration,
        *,
        model_schema: str,
        model_schema_id: int,
        model_schema_owner_id: int,
        allow_create: bool,
    ) -> CatalogDeploymentObservation:
        """Observe deployment only after external authentication and acknowledged commit.

        Schema selection must come from the authenticated retained policy; ID/owner
        come from its observed deployment, not arbitrary caller selection. The first cell requires dbo
        ownership (principal 1) of an existing dedicated model-data schema, and
        a dedicated observer. Shared observation needs a separately authenticated
        permission contract accounting for the new catalog EXECUTE grant.
        """
        encode_physical_runtime_registration(registration)
        if type(registration.principals.observer) is not DedicatedObserver:
            raise ValueError("catalog deployment first cell requires a dedicated observer")
        model_schema = native_control_schema(model_schema)
        reserved = {
            "dbo",
            "sys",
            "information_schema",
            registration.local_schema.lower(),
            registration.control_schema.lower(),
        }
        if model_schema.lower() in reserved:
            raise ValueError("model schema must be dedicated and distinct from control schemas")
        if type(model_schema_id) is not int or not 1 <= model_schema_id <= 2**31 - 1:
            raise ValueError("model schema ID must be a positive SQL int")
        if type(model_schema_owner_id) is not int or model_schema_owner_id != 1:
            raise ValueError("model schema requires dbo ownership")
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            self._context(cursor, registration, model_schema, model_schema_id)
            cursor.execute("SELECT thumbprint FROM sys.certificates WHERE name=?", self._certificate)
            thumbprint = self._one(cursor)
            if type(thumbprint) is not bytes or len(thumbprint) != 20:
                raise RuntimeError("catalog certificate thumbprint is malformed")
            definition = self._definition(
                registration,
                model_schema=model_schema,
                model_schema_id=model_schema_id,
                model_schema_owner_id=model_schema_owner_id,
                catalog_certificate_thumbprint=thumbprint,
            )
            schema = registration.local_schema
            cursor.execute("SELECT OBJECT_ID(?)", f"[{schema}].[{self._entry}]")
            existing = self._one(cursor) is not None
            if not existing and not allow_create:
                raise RuntimeError("catalog deployment is absent during read-only verification")
            names = self._runtime(cursor, registration, model_schema)
            self._signatures(cursor, schema, complete=existing)
            if not existing:
                cursor.execute(definition)
            cursor.execute(module_inventory_sql(schema, self._entry, self._certificate, "SPVC"), definition)
            self._certificate_user(cursor, model_schema, create=not existing)
            if not existing:
                cursor.execute(
                    f"ADD SIGNATURE TO OBJECT::[{schema}].[{self._entry}] BY CERTIFICATE [{self._certificate}];"
                )
                for name in names:
                    cursor.execute(
                        f"GRANT EXECUTE ON OBJECT::[{schema}].[{self._entry}] TO {quote_mssql_identifier(name)};"
                    )
            self._signatures(cursor, schema, complete=True)
            self._grants(cursor, registration)
            self._certificate_user(cursor, model_schema, create=False)
            self._runtime(cursor, registration, model_schema)
            cursor.execute(module_inventory_sql(schema, self._entry, self._certificate, "SPVC"), definition)
            connection.commit()
            return CatalogDeploymentObservation(
                model_schema,
                model_schema_id,
                model_schema_owner_id,
                sha256(definition.encode("utf-16le")).hexdigest(),
                thumbprint,
            )
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)

    def _definition(
        self,
        registration: MssqlPhysicalRuntimeRegistration,
        *,
        model_schema: str,
        model_schema_id: int,
        model_schema_owner_id: int,
        catalog_certificate_thumbprint: bytes,
    ) -> str:
        """Select immutable v1 rendering; v2 overrides this finite program seam."""
        return catalog_procedure(
            registration,
            catalog_sql=self._sql,
            model_schema=model_schema,
            model_schema_id=model_schema_id,
            model_schema_owner_id=model_schema_owner_id,
            catalog_certificate_thumbprint=catalog_certificate_thumbprint,
        )

    @staticmethod
    def _one(cursor: SqlControlCursor) -> object:
        row = dbapi_lifecycle.row(cursor)
        if row is None or len(row) != 1 or dbapi_lifecycle.row(cursor) is not None:
            raise RuntimeError("catalog inventory requires exactly one scalar row")
        return row[0]

    def _context(
        self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, schema: str, schema_id: int
    ) -> None:
        pin = value.model_database
        cursor.execute(f"USE {quote_mssql_identifier(pin.database_name)};")
        cursor.execute("SET NOCOUNT ON; SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
        cursor.execute("""IF @@TRANCOUNT=0 BEGIN TRANSACTION;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 THROW 51460,'DPONE_CATALOG_TRANSACTION_UNSAFE',1;""")
        cursor.execute(
            """DECLARE @name nvarchar(128)=?,@id int=?,@created datetime2(7)=?,@guid uniqueidentifier=?;
IF ISNULL(HAS_PERMS_BY_NAME(NULL,NULL,'CONTROL SERVER'),0)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id
 WHERE d.database_id=DB_ID() AND d.database_id=@id AND CONVERT(varbinary(max),d.name)=CONVERT(varbinary(max),@name)
 AND DATALENGTH(d.name)=DATALENGTH(@name) AND CONVERT(datetime2(7),d.create_date)=@created AND r.database_guid=@guid
 AND d.is_trustworthy_on=0 AND d.is_db_chaining_on=0)
 OR NOT EXISTS (SELECT 1 FROM sys.configurations WHERE name='cross db ownership chaining' AND value_in_use=0)
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE grantee_principal_id=2 AND state IN ('G','W'))
 THROW 51461,'DPONE_CATALOG_DATABASE_UNSAFE',1;""",
            pin.database_name,
            pin.database_id,
            pin.create_token,
            str(pin.database_guid),
        )
        cursor.execute(
            """DECLARE @name sysname=?,@id int=?,@local sysname=?,@control sysname=?;
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE schema_id=@id AND principal_id=1
 AND CONVERT(varbinary(max),name)=CONVERT(varbinary(max),@name) AND DATALENGTH(name)=DATALENGTH(@name))
 OR SCHEMA_ID(@local)=@id OR SCHEMA_ID(@control)=@id OR @id IN (1,3,4)
 OR NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name=@local AND principal_id=1)
 THROW 51462,'DPONE_CATALOG_SCHEMA_MISMATCH',1;""",
            schema,
            schema_id,
            value.local_schema,
            value.control_schema,
        )
        cursor.execute(
            f"""DECLARE @public varbinary(max)=?;
IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name=N'{self._certificate}' AND principal_id=1
 AND pvt_key_encryption_type IN ('MK','PW') AND CERTENCODED(certificate_id)=@public
 AND DATALENGTH(CERTENCODED(certificate_id))=DATALENGTH(@public))
 OR EXISTS (SELECT 1 FROM sys.server_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{self._certificate}'))
 THROW 51463,'DPONE_CATALOG_CERTIFICATE_MISMATCH',1;""",
            self._public,
        )

    def _runtime(self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, schema: str) -> list[str]:
        names = []
        mappings = [value.principals.metadata, value.principals.build]
        if isinstance(value.principals.observer, DedicatedObserver):
            mappings.append(value.principals.observer.mapping)
        for mapping in mappings:
            principal = mapping.model
            cursor.execute(
                principal_inventory_sql(value.local_schema, self._entry, model=True),
                principal.principal_id,
                bytes.fromhex(principal.sid_hex),
            )
            name = self._one(cursor)
            if type(name) is not str:
                raise RuntimeError("catalog runtime principal name is malformed")
            names.append(name)
            cursor.execute(
                f"""DECLARE @principal int=?,@login int;
SELECT @login=l.principal_id FROM sys.server_principals l JOIN sys.database_principals p ON p.sid=l.sid
 AND DATALENGTH(p.sid)=DATALENGTH(l.sid) WHERE p.principal_id=@principal;
IF EXISTS (SELECT 1 FROM sys.server_permissions WHERE state='D'
 AND grantee_principal_id IN (@login,(SELECT principal_id FROM sys.server_principals WHERE name='public' AND type='R'))
 AND permission_name IN ('VIEW ANY DEFINITION','VIEW ANY SECURITY DEFINITION','CONTROL SERVER'))
 OR EXISTS (SELECT 1 FROM sys.database_permissions p WHERE grantee_principal_id IN (@principal,0)
 AND ((state='D' AND permission_name IN ('VIEW DEFINITION','VIEW SECURITY DEFINITION','CONTROL') AND class IN (0,1,3))
 OR (state='D' AND permission_name IN ('SELECT','EXECUTE')
 AND (class=0 OR (class=3 AND major_id IN (SCHEMA_ID(N'{schema}'),SCHEMA_ID(N'{value.local_schema}'),SCHEMA_ID('sys')))
 OR (class=1 AND (major_id=OBJECT_ID('sys.sql_expression_dependencies') OR major_id IN
 (SELECT object_id FROM sys.objects WHERE schema_id=SCHEMA_ID(N'{schema}'))))))
 OR (class=3 AND major_id=SCHEMA_ID(N'{schema}'))))
 THROW 51464,'DPONE_CATALOG_VISIBILITY_UNSAFE',1;""",
                principal.principal_id,
            )
        return names[:2]

    def _signatures(self, cursor: SqlControlCursor, schema: str, *, complete: bool) -> None:
        expected = f"OBJECT_ID(N'[{schema}].[{self._entry}]')"
        missing = (
            f"OR NOT EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class=1 AND major_id={expected} AND crypt_type='SPVC' AND thumbprint=@thumbprint)"
            if complete
            else ""
        )
        cursor.execute(f"""DECLARE @thumbprint varbinary(64)=(SELECT thumbprint FROM sys.certificates WHERE name=N'{self._certificate}');
IF EXISTS (SELECT 1 FROM sys.crypt_properties WHERE thumbprint=@thumbprint
 AND (class<>1 OR {expected} IS NULL OR major_id<>{expected} OR crypt_type<>'SPVC'))
 OR EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class=1 AND major_id={expected}
 AND (thumbprint<>@thumbprint OR crypt_type<>'SPVC'))
 {missing} THROW 51465,'DPONE_CATALOG_SIGNATURE_MISMATCH',1;""")

    def _certificate_user(self, cursor: SqlControlCursor, schema: str, *, create: bool) -> None:
        expected = f"(0,0,0,N'VIEW DEFINITION',N'G'),(1,OBJECT_ID('sys.sql_expression_dependencies'),0,N'SELECT',N'G'),(3,SCHEMA_ID(N'{schema}'),0,N'SELECT',N'G')"
        actual = f"SELECT class,major_id,minor_id,permission_name,state FROM sys.database_permissions WHERE grantee_principal_id=DATABASE_PRINCIPAL_ID(N'{self._user}')"
        if create:
            cursor.execute(f"""IF DATABASE_PRINCIPAL_ID(N'{self._user}') IS NULL
BEGIN CREATE USER [{self._user}] FROM CERTIFICATE [{self._certificate}]; REVOKE CONNECT FROM [{self._user}]; END;""")
        cursor.execute(f"""DECLARE @user int=DATABASE_PRINCIPAL_ID(N'{self._user}');
IF @user IS NULL OR NOT EXISTS (SELECT 1 FROM sys.database_principals p JOIN sys.certificates c ON p.sid=c.sid
 AND DATALENGTH(p.sid)=DATALENGTH(c.sid) WHERE p.principal_id=@user AND p.type='C' AND c.name=N'{self._certificate}')
 OR EXISTS (SELECT 1 FROM sys.database_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{self._certificate}') AND principal_id<>@user)
 OR EXISTS (SELECT 1 FROM sys.database_role_members WHERE member_principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.schemas WHERE principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.objects WHERE principal_id=@user)
 OR EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {expected}) e(c,o,m,p,s))
 THROW 51466,'DPONE_CATALOG_CERTIFICATE_USER_UNSAFE',1;""")
        if create:
            cursor.execute(f"GRANT VIEW DEFINITION TO [{self._user}];")
            cursor.execute(f"GRANT SELECT ON OBJECT::sys.sql_expression_dependencies TO [{self._user}];")
            cursor.execute(f"GRANT SELECT ON SCHEMA::[{schema}] TO [{self._user}];")
        cursor.execute(f"""IF EXISTS (SELECT * FROM (VALUES {expected}) e(c,o,m,p,s) EXCEPT {actual})
 THROW 51466,'DPONE_CATALOG_CERTIFICATE_USER_UNSAFE',1;""")

    def _grants(self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration) -> None:
        principals = f"{value.principals.metadata.model.principal_id},{value.principals.build.model.principal_id}"
        cursor.execute(f"""IF (SELECT COUNT(*) FROM sys.database_permissions WHERE class=1
 AND major_id=OBJECT_ID(N'[{value.local_schema}].[{self._entry}]'))<>2
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE class=1 AND major_id=OBJECT_ID(N'[{value.local_schema}].[{self._entry}]')
 AND NOT (minor_id=0 AND state='G' AND permission_name='EXECUTE' AND grantee_principal_id IN ({principals})))
 THROW 51467,'DPONE_CATALOG_GRANT_MISMATCH',1;""")
