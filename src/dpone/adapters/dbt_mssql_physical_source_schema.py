"""Privileged provisioning of the finite caller-preserving source bridge.

The platform owner authenticates original policy/profile/toolchain/package inputs
and excludes concurrent privileged DDL before invoking this adapter. Those are
external preconditions, not assertions proven by registration JSON or callbacks.
Certificates and usable private keys must already exist in both databases. The
factory opens protected keys when necessary; no password/key material enters this
API. Runtime callers cannot use it. Successful installation is not model admission.
"""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_registration_store import MssqlPhysicalRegistrationStore
from dpone.adapters.dbt_mssql_physical_source_queries import ENTRY, HELPER, source_procedures
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import encode_physical_runtime_registration
from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
from dpone.contracts.mssql_object_name import native_control_schema, quote_mssql_identifier
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


def module_inventory_sql(schema: str, module: str, certificate: str, signature: str) -> str:
    """Check exact stored UTF-16 definition, dbo chain and no foreign signature.

    Bind the deterministic definition. Missing expected signature is allowed here
    only so installation can add it; final signature inventory is checked later.
    """
    schema, module, certificate = map(native_control_schema, (schema, module, certificate))
    if signature not in {"SPVC", "CPVC"}:
        raise ValueError("unsupported source signature")
    return f"""DECLARE @definition nvarchar(max)=?, @object int=OBJECT_ID(N'[{schema}].[{module}]');
IF NOT EXISTS (SELECT 1 FROM sys.procedures p JOIN sys.sql_modules m ON m.object_id=p.object_id
 JOIN sys.schemas s ON s.schema_id=p.schema_id WHERE p.object_id=@object AND p.type='P'
 AND s.principal_id=1 AND COALESCE(p.principal_id,s.principal_id)=1
 AND m.execute_as_principal_id IS NULL AND m.uses_ansi_nulls=1 AND m.uses_quoted_identifier=1
 AND m.is_schema_bound=0 AND m.is_recompiled=0
 AND CONVERT(varbinary(max),m.definition)=CONVERT(varbinary(max),@definition)
 AND DATALENGTH(m.definition)=DATALENGTH(@definition))
 OR EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class=1 AND major_id=@object
 AND (thumbprint<>(SELECT thumbprint FROM sys.certificates WHERE name=N'{certificate}')
 OR crypt_type<>'{signature}'))
 THROW 51440, 'DPONE_SOURCE_MODULE_INVENTORY_MISMATCH', 1;"""


def principal_inventory_sql(schema: str, module: str, *, model: bool) -> str:
    """Fail closed for the first SQL-login cell, including public covering grants.

    Existing finite native object grants remain possible. Database/server roles,
    ownership, impersonation and broad grants are rejected, not repaired. Bind the
    principal ID/SID; returns the observed name only after all checks succeed.
    """
    schema, module = map(native_control_schema, (schema, module))
    # The model entry may have exactly EXECUTE; helper has no runtime permission,
    # including DENY, since an explicit DENY also breaks the signed nested call.
    allowed = "AND NOT (state='G' AND permission_name='EXECUTE')" if model else ""
    return f"""DECLARE @principal int=?, @sid varbinary(85)=?, @login int, @name sysname;
SELECT @name=p.name,@login=l.principal_id FROM sys.database_principals p
 JOIN sys.server_principals l ON l.sid=p.sid AND DATALENGTH(l.sid)=DATALENGTH(p.sid)
 WHERE p.principal_id=@principal AND p.sid=@sid AND DATALENGTH(p.sid)=DATALENGTH(@sid)
 AND p.principal_id>4 AND p.type='S' AND p.authentication_type=1 AND l.type='S' AND l.is_disabled=0;
IF @name IS NULL OR @login IS NULL
 OR EXISTS (SELECT 1 FROM sys.database_role_members WHERE member_principal_id=@principal)
 OR EXISTS (SELECT 1 FROM sys.server_role_members WHERE member_principal_id=@login)
 OR EXISTS (SELECT 1 FROM sys.schemas WHERE principal_id=@principal)
 OR EXISTS (SELECT 1 FROM sys.objects WHERE principal_id=@principal)
 OR EXISTS (SELECT 1 FROM sys.databases WHERE owner_sid=@sid)
 OR EXISTS (SELECT 1 FROM sys.server_permissions WHERE grantee_principal_id IN
 (@login,(SELECT principal_id FROM sys.server_principals WHERE name='public' AND type='R'))
 AND state IN ('G','W') AND NOT
 ((class=100 AND permission_name IN ('CONNECT SQL','VIEW ANY DATABASE')) OR (class=105 AND permission_name='CONNECT')))
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE grantee_principal_id IN (@principal,0)
 AND state IN ('G','W') AND ((class=0 AND permission_name NOT IN ('CONNECT','VIEW DEFINITION',
 'VIEW ANY COLUMN ENCRYPTION KEY DEFINITION','VIEW ANY COLUMN MASTER KEY DEFINITION'))
 OR (class=3 AND major_id=SCHEMA_ID(N'{schema}')) OR class=4
 OR permission_name IN ('IMPERSONATE','CONTROL','ALTER','TAKE OWNERSHIP')))
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE grantee_principal_id IN (@principal,0)
 AND ((class=1 AND major_id=OBJECT_ID(N'[{schema}].[{module}]') {allowed})
 OR (state='D' AND permission_name IN ('EXECUTE','CONTROL') AND
 (class=0 OR (class=3 AND major_id=SCHEMA_ID(N'{schema}'))))))
 THROW 51441, 'DPONE_SOURCE_RUNTIME_PRINCIPAL_UNSAFE', 1;
SELECT @name;"""


class MssqlPhysicalSourceSchemaProvisioner:
    """Install exact modules/signatures, then insert authenticated registration.

    The injected fresh privileged connection selects M and sees complete server
    and database catalogs. Both databases reside on that instance. Provisioning
    switches only to literal registered database names and owns a single DDL
    transaction. Registration storage uses its own independently verified write
    and readback after the DDL commit. A failed/ambiguous DDL commit never starts
    registration; operator reconciliation is required before another attempt.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        admission_sql: bytes,
        certificate_name: str,
        certificate_public_bytes: bytes,
        certificate_user: str,
    ) -> None:
        if type(certificate_public_bytes) is not bytes or not certificate_public_bytes:
            raise ValueError("nonempty public certificate bytes are required")
        if type(admission_sql) is not bytes or not admission_sql:
            raise ValueError("authenticated admission package bytes are required")
        self._connect = connection_factory
        self._admission = admission_sql
        self._certificate = native_control_schema(certificate_name)
        self._public = certificate_public_bytes
        self._user = native_control_schema(certificate_user)

    def apply(self, value: MssqlPhysicalRuntimeRegistration) -> MssqlPhysicalRuntimeRegistration:
        """Reject drift without ALTER/replacement; exact replay may reuse objects.

        External authentication must precede this call. The returned registration
        proves storage readback only, never upstream authentication or runtime SQL
        permission qualification. No generic proof callback is accepted.
        """
        encode_physical_runtime_registration(value)
        definitions = source_procedures(
            admission_sql=self._admission,
            model_database=value.model_database.database_name,
            local_schema=value.local_schema,
            control_database=value.control_database.database_name,
            control_schema=value.control_schema,
        )
        if set(definitions) != {ENTRY, HELPER}:
            raise ValueError("source producer must return exactly the finite module pair")
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET NOCOUNT ON; SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
            for namespace in ("control", "model"):
                self._install(cursor, value, namespace, definitions)
            # Revisit both inventories after all changes, including same-DB cells.
            for namespace in ("control", "model"):
                self._verify(cursor, value, namespace, definitions)
            connection.commit()
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
        return MssqlPhysicalRegistrationStore(
            connection_factory=self._connect,
            local_schema=value.local_schema,
        ).register(value)

    def _context(self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> None:
        pin = getattr(value, namespace + "_database")
        cursor.execute(f"USE {quote_mssql_identifier(pin.database_name)};")
        cursor.execute(
            """DECLARE @name nvarchar(128)=?,@id int=?,@created datetime2(7)=?,@guid uniqueidentifier=?;
IF NOT EXISTS (SELECT 1 FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id
 WHERE d.database_id=DB_ID() AND d.database_id=@id AND CONVERT(varbinary(max),d.name)=CONVERT(varbinary(max),@name)
 AND DATALENGTH(d.name)=DATALENGTH(@name) AND CONVERT(datetime2(7),d.create_date)=@created AND r.database_guid=@guid
 AND d.is_trustworthy_on=0 AND d.is_db_chaining_on=0)
 OR NOT EXISTS (SELECT 1 FROM sys.configurations WHERE name='cross db ownership chaining' AND value_in_use=0)
 OR HAS_PERMS_BY_NAME(NULL,NULL,'CONTROL SERVER')<>1
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE grantee_principal_id=2 AND state IN ('G','W'))
 THROW 51442, 'DPONE_SOURCE_DATABASE_UNSAFE', 1;""",
            pin.database_name,
            pin.database_id,
            pin.create_token,
            str(pin.database_guid),
        )
        cursor.execute(
            f"""DECLARE @public varbinary(max)=?;
IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name=N'{self._certificate}'
 AND pvt_key_encryption_type IN ('MK','PW') AND principal_id=1
 AND CERTENCODED(certificate_id)=@public AND DATALENGTH(CERTENCODED(certificate_id))=DATALENGTH(@public))
 THROW 51443, 'DPONE_SOURCE_CERTIFICATE_MISMATCH', 1;""",
            self._public,
        )

    def _modules(self, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> list[tuple[str, str, str]]:
        selected = (
            [(value.local_schema, ENTRY, "SPVC")] if namespace == "model" else [(value.control_schema, HELPER, "CPVC")]
        )
        if value.model_database == value.control_database:
            return [(value.local_schema, ENTRY, "SPVC"), (value.control_schema, HELPER, "CPVC")]
        return selected

    def _signatures(
        self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str, *, complete: bool
    ) -> None:
        expected = ",".join(f"(OBJECT_ID(N'[{s}].[{m}]'),'{t}')" for s, m, t in self._modules(value, namespace))
        actual = (
            f"SELECT major_id,crypt_type FROM sys.crypt_properties WHERE class=1 AND thumbprint="
            f"(SELECT thumbprint FROM sys.certificates WHERE name=N'{self._certificate}')"
        )
        missing = f"OR EXISTS (SELECT * FROM (VALUES {expected}) e(id,kind) EXCEPT {actual})" if complete else ""
        cursor.execute(f"""IF EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {expected}) e(id,kind))
 OR EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class<>1 AND thumbprint=
 (SELECT thumbprint FROM sys.certificates WHERE name=N'{self._certificate}'))
{missing} THROW 51444, 'DPONE_SOURCE_SIGNATURE_INVENTORY_MISMATCH', 1;""")

    def _grants(self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> None:
        schema, module = (value.local_schema, ENTRY) if namespace == "model" else (value.control_schema, HELPER)
        principals = (
            f"{value.principals.metadata.model.principal_id},{value.principals.build.model.principal_id}"
            if namespace == "model"
            else f"DATABASE_PRINCIPAL_ID(N'{self._user}')"
        )
        expected = 2 if namespace == "model" else 1
        certificate_principal = (
            f"AND principal_id<>DATABASE_PRINCIPAL_ID(N'{self._user}')"
            if namespace == "control" or value.model_database == value.control_database
            else ""
        )
        cursor.execute(f"""IF (SELECT COUNT(*) FROM sys.database_permissions WHERE class=1
 AND major_id=OBJECT_ID(N'[{schema}].[{module}]'))<>{expected}
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE class=1
 AND major_id=OBJECT_ID(N'[{schema}].[{module}]') AND NOT
 (grantee_principal_id IN ({principals}) AND minor_id=0 AND state='G' AND permission_name='EXECUTE'))
 OR EXISTS (SELECT 1 FROM sys.objects WHERE schema_id=SCHEMA_ID(N'{schema}')
 AND COALESCE(principal_id,1)<>1)
 OR EXISTS (SELECT 1 FROM sys.server_principals WHERE sid=
 (SELECT thumbprint FROM sys.certificates WHERE name=N'{self._certificate}'))
 OR EXISTS (SELECT 1 FROM sys.database_principals WHERE sid=
 (SELECT thumbprint FROM sys.certificates WHERE name=N'{self._certificate}') {certificate_principal})
 THROW 51446, 'DPONE_SOURCE_GRANT_INVENTORY_MISMATCH', 1;""")

    def _names(self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> list[str]:
        schema, module = (value.local_schema, ENTRY) if namespace == "model" else (value.control_schema, HELPER)
        mappings = [value.principals.metadata, value.principals.build]
        if isinstance(value.principals.observer, DedicatedObserver):
            mappings.append(value.principals.observer.mapping)
        names = []
        for mapping in mappings:
            principal = getattr(mapping, namespace)
            cursor.execute(
                principal_inventory_sql(schema, module, model=namespace == "model"),
                principal.principal_id,
                bytes.fromhex(principal.sid_hex),
            )
            row = dbapi_lifecycle.row(cursor)
            if row is None or len(row) != 1 or type(row[0]) is not str or dbapi_lifecycle.row(cursor) is not None:
                raise RuntimeError("source principal inventory did not return exactly one name")
            names.append(row[0])
        return names[:2]

    def _certificate_user(self, cursor: SqlControlCursor, schema: str, *, create: bool) -> None:
        if create:
            cursor.execute(f"""IF DATABASE_PRINCIPAL_ID(N'{self._user}') IS NULL
 CREATE USER [{self._user}] FROM CERTIFICATE [{self._certificate}];""")
        cursor.execute(f"""DECLARE @user int=DATABASE_PRINCIPAL_ID(N'{self._user}');
IF @user IS NULL OR NOT EXISTS (SELECT 1 FROM sys.database_principals p JOIN sys.certificates c ON p.sid=c.thumbprint
 WHERE p.principal_id=@user AND p.type='C' AND c.name=N'{self._certificate}')
 OR EXISTS (SELECT 1 FROM sys.database_role_members WHERE member_principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.schemas WHERE principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.objects WHERE principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE grantee_principal_id=@user AND NOT
 (class=1 AND major_id=OBJECT_ID(N'[{schema}].[{HELPER}]') AND minor_id=0 AND permission_name='EXECUTE' AND state='G'))
 THROW 51445, 'DPONE_SOURCE_CERTIFICATE_USER_UNSAFE', 1;""")
        if create:
            cursor.execute(f"GRANT EXECUTE ON OBJECT::[{schema}].[{HELPER}] TO [{self._user}];")
        cursor.execute(f"""IF NOT EXISTS (SELECT 1 FROM sys.database_permissions
 WHERE grantee_principal_id=DATABASE_PRINCIPAL_ID(N'{self._user}') AND class=1
 AND major_id=OBJECT_ID(N'[{schema}].[{HELPER}]') AND minor_id=0 AND permission_name='EXECUTE' AND state='G')
 THROW 51445, 'DPONE_SOURCE_CERTIFICATE_USER_UNSAFE', 1;""")

    def _install(
        self,
        cursor: SqlControlCursor,
        value: MssqlPhysicalRuntimeRegistration,
        namespace: str,
        definitions: dict[str, str],
    ) -> None:
        self._context(cursor, value, namespace)
        self._signatures(cursor, value, namespace, complete=False)
        names = self._names(cursor, value, namespace)
        schema, module, kind = (
            (value.local_schema, ENTRY, "SPVC") if namespace == "model" else (value.control_schema, HELPER, "CPVC")
        )
        cursor.execute("SELECT OBJECT_ID(?)", f"[{schema}].[{module}]")
        row = dbapi_lifecycle.row(cursor)
        if row is None or len(row) != 1 or dbapi_lifecycle.row(cursor) is not None:
            raise RuntimeError("source module lookup did not return exactly one row")
        if row[0] is None:
            cursor.execute(definitions[module])
        cursor.execute(module_inventory_sql(schema, module, self._certificate, kind), definitions[module])
        counter = "COUNTER " if namespace == "control" else ""
        cursor.execute(f"""IF NOT EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class=1
 AND major_id=OBJECT_ID(N'[{schema}].[{module}]') AND crypt_type='{kind}'
 AND thumbprint=(SELECT thumbprint FROM sys.certificates WHERE name=N'{self._certificate}'))
 ADD {counter}SIGNATURE TO OBJECT::[{schema}].[{module}] BY CERTIFICATE [{self._certificate}];""")
        if namespace == "control":
            self._certificate_user(cursor, schema, create=True)
        else:
            for name in names:
                cursor.execute(f"GRANT EXECUTE ON OBJECT::[{schema}].[{module}] TO {quote_mssql_identifier(name)};")

    def _verify(
        self,
        cursor: SqlControlCursor,
        value: MssqlPhysicalRuntimeRegistration,
        namespace: str,
        definitions: dict[str, str],
    ) -> None:
        self._context(cursor, value, namespace)
        self._signatures(cursor, value, namespace, complete=True)
        self._names(cursor, value, namespace)
        self._grants(cursor, value, namespace)
        for schema, module, kind in self._modules(value, namespace):
            cursor.execute(module_inventory_sql(schema, module, self._certificate, kind), definitions[module])
        if namespace == "control":
            self._certificate_user(cursor, value.control_schema, create=False)
