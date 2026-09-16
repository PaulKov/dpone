"""Privileged finite deployment of the independent P-only discovery boundary.

The trusted platform authenticates retained policy/package inputs and excludes
concurrent privileged DDL. This adapter checks actual protected registration and
binding storage; a constructed binding is never treated as that proof. Existing
source/catalog certificates and registration rows are not changed. The factory
supplies one fresh CONTROL SERVER connection with usable pre-existing private
keys; no secret key material is accepted here.
"""

from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_catalog_binding_schema import (
    TABLE,
    verify_binding_protection_sql,
    verify_binding_table_sql,
    verify_registration_context,
)
from dpone.adapters.dbt_mssql_physical_discovery_queries import ENTRY, HELPER, discovery_procedures
from dpone.adapters.dbt_mssql_physical_source_schema import (
    MssqlPhysicalSourceSchemaProvisioner,
    module_inventory_sql,
    principal_inventory_sql,
)
from dpone.contracts.dbt_mssql_physical_catalog_binding import (
    CatalogRegistrationBinding,
    catalog_binding_digest,
    encode_catalog_binding,
    require_catalog_binding_registration,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
from dpone.contracts.mssql_object_name import quote_mssql_identifier
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


@dataclass(frozen=True, slots=True)
class DiscoveryDeploymentObservation:
    """Acknowledged deployment inventory; neither P ownership nor absence proof."""

    entry_sha256: str
    helper_sha256: str
    certificate_thumbprint: bytes


class MssqlPhysicalDiscoverySchemaProvisioner:
    """Install an absent finite pair or verify exact replay without repair.

    METADATA receives only model-entry EXECUTE. The separate certificate receives
    model database VIEW DEFINITION and control helper EXECUTE/VIEW DEFINITION.
    BUILD/observer receive no grants. One cross-database DDL transaction verifies
    all inventories before commit; uncertain commit never returns an observation.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        discovery_sql: bytes,
        certificate_name: str,
        certificate_public_bytes: bytes,
        certificate_user: str,
    ) -> None:
        self._boundary = _DiscoveryBoundary(
            connection_factory=connection_factory,
            admission_sql=discovery_sql,
            certificate_name=certificate_name,
            certificate_public_bytes=certificate_public_bytes,
            certificate_user=certificate_user,
        )

    def apply(
        self, registration: MssqlPhysicalRuntimeRegistration, binding: CatalogRegistrationBinding
    ) -> DiscoveryDeploymentObservation:
        """Verify retained storage and deployment; never create a registration."""
        return self._boundary.deploy(registration, binding)


class _DiscoveryBoundary(MssqlPhysicalSourceSchemaProvisioner):
    """Reuse finite database/certificate/signature verifiers through explicit hooks.

    Kept private and composed so the source provisioner's public registration
    mutation API is not exposed as discovery provisioning.
    """

    def deploy(
        self, value: MssqlPhysicalRuntimeRegistration, binding: CatalogRegistrationBinding
    ) -> DiscoveryDeploymentObservation:
        require_catalog_binding_registration(binding, value)
        if type(value.principals.observer) is not DedicatedObserver:
            raise ValueError("discovery requires dedicated observer mappings")
        connection = cursor = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET NOCOUNT ON; SET XACT_ABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;")
            self._context(cursor, value, "model")
            verify_catalog_binding_context(cursor, value, binding)
            thumbprints, existing = [], []
            for namespace in ("control", "model"):
                self._context(cursor, value, namespace)
                cursor.execute("SELECT thumbprint FROM sys.certificates WHERE name=?", self._certificate)
                thumbprint = self._one(cursor)
                if type(thumbprint) is not bytes or len(thumbprint) != 20:
                    raise RuntimeError("discovery certificate thumbprint is malformed")
                thumbprints.append(thumbprint)
                schema, module = self._location(value, namespace)
                cursor.execute("SELECT OBJECT_ID(?)", f"[{schema}].[{module}]")
                object_id = self._one(cursor)
                if object_id is not None and (type(object_id) is not int or object_id <= 0):
                    raise RuntimeError("discovery module object identity is malformed")
                existing.append(object_id is not None)
            if thumbprints[0] != thumbprints[1]:
                raise RuntimeError("discovery certificate thumbprints differ")
            if existing[0] != existing[1]:
                raise RuntimeError("discovery module pair is incomplete; no repair is permitted")
            definitions = discovery_procedures(
                discovery_sql=self._admission,
                model_database=value.model_database,
                local_schema=value.local_schema,
                control_database=value.control_database.database_name,
                control_schema=value.control_schema,
                model_schema=binding.model_schema,
                model_schema_id=binding.model_schema_id,
                model_schema_owner_id=binding.model_schema_owner_id,
                discovery_certificate_thumbprint=thumbprints[0],
            )
            if set(definitions) != {ENTRY, HELPER}:
                raise ValueError("discovery producer requires exactly the finite module pair")
            if not existing[0]:
                for namespace in ("control", "model"):
                    self._install(cursor, value, namespace, definitions)
                for namespace in ("control", "model"):
                    if namespace == "model" and value.model_database == value.control_database:
                        continue
                    self._context(cursor, value, namespace)
                    self._certificate_user(cursor, value, namespace, create=True)
            for namespace in ("control", "model"):
                self._verify(cursor, value, namespace, definitions)
            connection.commit()
            return DiscoveryDeploymentObservation(
                "sha256:" + sha256(definitions[ENTRY].encode("utf-16le")).hexdigest(),
                "sha256:" + sha256(definitions[HELPER].encode("utf-16le")).hexdigest(),
                thumbprints[0],
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
            raise RuntimeError("discovery inventory requires exactly one scalar row")
        return row[0]

    @staticmethod
    def _location(value: MssqlPhysicalRuntimeRegistration, namespace: str) -> tuple[str, str]:
        return (value.local_schema, ENTRY) if namespace == "model" else (value.control_schema, HELPER)

    def _modules(self, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> list[tuple[str, str, str]]:
        model, control = (value.local_schema, ENTRY, "SPVC"), (value.control_schema, HELPER, "CPVC")
        return (
            [model, control]
            if value.model_database == value.control_database
            else [model if namespace == "model" else control]
        )

    def _context(self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> None:
        super()._context(cursor, value, namespace)
        cursor.execute(f"""IF ISNULL(HAS_PERMS_BY_NAME(NULL,NULL,'CONTROL SERVER'),0)<>1
 OR EXISTS (SELECT 1 FROM sys.server_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{self._certificate}'))
 THROW 51490,'DPONE_DISCOVERY_CONTEXT_UNSAFE',1;""")

    def _names(self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> list[str]:
        schema, module = self._location(value, namespace)
        assert isinstance(value.principals.observer, DedicatedObserver)
        mappings = [value.principals.metadata, value.principals.build, value.principals.observer.mapping]
        names = []
        for ordinal, mapping in enumerate(mappings):
            principal = getattr(mapping, namespace)
            cursor.execute(
                principal_inventory_sql(schema, module, model=namespace == "model" and ordinal == 0),
                principal.principal_id,
                bytes.fromhex(principal.sid_hex),
            )
            name = self._one(cursor)
            if type(name) is not str:
                raise RuntimeError("discovery principal name is malformed")
            names.append(name)
            cursor.execute(
                """DECLARE @principal int=?,@login int;
SELECT @login=l.principal_id FROM sys.server_principals l JOIN sys.database_principals p ON p.sid=l.sid
 AND DATALENGTH(p.sid)=DATALENGTH(l.sid) WHERE p.principal_id=@principal;
IF EXISTS (SELECT 1 FROM sys.server_permissions WHERE state='D'
 AND grantee_principal_id IN (@login,(SELECT principal_id FROM sys.server_principals WHERE name='public' AND type='R'))
 AND permission_name IN ('VIEW ANY DEFINITION','VIEW ANY SECURITY DEFINITION','CONTROL SERVER'))
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE grantee_principal_id IN (@principal,0)
 AND state='D' AND class IN (0,1,3) AND permission_name IN ('VIEW DEFINITION','VIEW SECURITY DEFINITION','CONTROL'))
 THROW 51492,'DPONE_DISCOVERY_VISIBILITY_UNSAFE',1;""",
                principal.principal_id,
            )
        return names[:1]

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
        schema, module = self._location(value, namespace)
        kind, counter = ("SPVC", "") if namespace == "model" else ("CPVC", "COUNTER ")
        cursor.execute(definitions[module])
        cursor.execute(module_inventory_sql(schema, module, self._certificate, kind), definitions[module])
        cursor.execute(f"ADD {counter}SIGNATURE TO OBJECT::[{schema}].[{module}] BY CERTIFICATE [{self._certificate}];")
        if namespace == "model":
            cursor.execute(f"GRANT EXECUTE ON OBJECT::[{schema}].[{module}] TO {quote_mssql_identifier(names[0])};")

    def _certificate_user(
        self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str, *, create: bool
    ) -> None:
        same = value.model_database == value.control_database
        grants = []
        if namespace == "model" or same:
            grants.append(("(0,0,0,N'VIEW DEFINITION',N'G')", "VIEW DEFINITION"))
        if namespace == "control" or same:
            for permission in ("EXECUTE", "VIEW DEFINITION"):
                grants.append(
                    (
                        f"(1,OBJECT_ID(N'[{value.control_schema}].[{HELPER}]'),0,N'{permission}',N'G')",
                        f"{permission} ON OBJECT::[{value.control_schema}].[{HELPER}]",
                    )
                )
        expected = ",".join(row for row, _ in grants)
        actual = f"SELECT class,major_id,minor_id,permission_name,state FROM sys.database_permissions WHERE grantee_principal_id=DATABASE_PRINCIPAL_ID(N'{self._user}')"
        if create:
            cursor.execute(
                f"IF DATABASE_PRINCIPAL_ID(N'{self._user}') IS NULL BEGIN CREATE USER [{self._user}] FROM CERTIFICATE [{self._certificate}]; REVOKE CONNECT FROM [{self._user}]; END;"
            )
        cursor.execute(f"""DECLARE @user int=DATABASE_PRINCIPAL_ID(N'{self._user}');
IF @user IS NULL OR NOT EXISTS (SELECT 1 FROM sys.database_principals p JOIN sys.certificates c ON p.sid=c.sid
 AND DATALENGTH(p.sid)=DATALENGTH(c.sid) WHERE p.principal_id=@user AND p.type='C' AND c.name=N'{self._certificate}')
 OR EXISTS (SELECT 1 FROM sys.database_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{self._certificate}') AND principal_id<>@user)
 OR EXISTS (SELECT 1 FROM sys.database_role_members WHERE member_principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.schemas WHERE principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.objects WHERE principal_id=@user)
 OR EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {expected}) e(c,o,m,p,s))
 THROW 51493,'DPONE_DISCOVERY_CERTIFICATE_USER_UNSAFE',1;""")
        if create:
            for _, grant in grants:
                cursor.execute(f"GRANT {grant} TO [{self._user}];")
        cursor.execute(f"""IF EXISTS (SELECT * FROM (VALUES {expected}) e(c,o,m,p,s) EXCEPT {actual})
 THROW 51493,'DPONE_DISCOVERY_CERTIFICATE_USER_UNSAFE',1;""")

    def _grants(self, cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, namespace: str) -> None:
        schema, module = self._location(value, namespace)
        if namespace == "model":
            expected = f"({value.principals.metadata.model.principal_id},N'EXECUTE',N'G',0)"
        else:
            expected = f"(DATABASE_PRINCIPAL_ID(N'{self._user}'),N'EXECUTE',N'G',0),(DATABASE_PRINCIPAL_ID(N'{self._user}'),N'VIEW DEFINITION',N'G',0)"
        actual = f"SELECT grantee_principal_id,permission_name,state,minor_id FROM sys.database_permissions WHERE class=1 AND major_id=OBJECT_ID(N'[{schema}].[{module}]')"
        cursor.execute(f"""IF EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {expected}) e(u,p,s,m))
 OR EXISTS (SELECT * FROM (VALUES {expected}) e(u,p,s,m) EXCEPT {actual})
 OR EXISTS (SELECT 1 FROM sys.objects WHERE schema_id=SCHEMA_ID(N'{schema}') AND COALESCE(principal_id,1)<>1)
 THROW 51494,'DPONE_DISCOVERY_GRANT_INVENTORY_MISMATCH',1;""")


def verify_catalog_binding_context(
    cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, binding: CatalogRegistrationBinding
) -> None:
    """Verify protected registration/binding bytes and pinned schema/catalog body.

    The caller supplies a privileged pinned model-DB cursor and authenticated
    expectations; constructed arguments confer no authority. Registration
    preflight preserves its SET and conditional BEGIN behavior. Retained read
    locks belong to the caller's transaction; this helper neither commits nor
    closes it. It grants nothing and leaves all existing inventories unchanged.
    """
    verify_registration_context(cursor, value, value.local_schema, write=False)
    cursor.execute(verify_binding_table_sql(value.local_schema))
    cursor.execute(verify_binding_protection_sql(value.local_schema, value))
    cursor.execute(
        f"SELECT registration_digest,payload,binding_digest FROM [{value.local_schema}].[{TABLE}] WITH (HOLDLOCK) WHERE registration_id=?",
        value.registration_id,
    )
    row = dbapi_lifecycle.row(cursor)
    expected = (
        binding.registration_sha256.encode("ascii"),
        encode_catalog_binding(binding),
        catalog_binding_digest(binding).encode("ascii"),
    )
    if row != expected or dbapi_lifecycle.row(cursor) is not None:
        raise RuntimeError("discovery requires the exact protected catalog binding")
    cursor.execute(
        f"""DECLARE @schema sysname=?,@id int=?,@owner int=?,@hash varchar(71)=?;
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE schema_id=@id AND principal_id=@owner
 AND CONVERT(varbinary(max),name)=CONVERT(varbinary(max),@schema) AND DATALENGTH(name)=DATALENGTH(@schema))
 OR SCHEMA_ID(N'{value.local_schema}')=@id OR SCHEMA_ID(N'{value.control_schema}')=@id OR @id IN (1,3,4)
 OR NOT EXISTS (SELECT 1 FROM sys.procedures p JOIN sys.sql_modules m ON m.object_id=p.object_id
 JOIN sys.schemas s ON s.schema_id=p.schema_id WHERE p.object_id=OBJECT_ID(N'[{value.local_schema}].[physical_catalog_v2]',N'P')
 AND s.principal_id=1 AND COALESCE(p.principal_id,s.principal_id)=1 AND m.execute_as_principal_id IS NULL
 AND 'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',CONVERT(varbinary(max),m.definition)),2))=@hash)
 THROW 51491,'DPONE_DISCOVERY_BINDING_DEPLOYMENT_MISMATCH',1;""",
        binding.model_schema,
        binding.model_schema_id,
        binding.model_schema_owner_id,
        binding.catalog_module_sha256,
    )
