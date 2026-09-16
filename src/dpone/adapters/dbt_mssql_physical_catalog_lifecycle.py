"""Compose authenticated catalog policy, retained registration and v2 deployment.

This platform-only lifecycle performs no generation mutation or model admission.
Its installation steps are independently committed: failure can leave protected
storage or a module installed, but returns no accepted binding. Exact replay
verifies that state and never repairs drift or changes a registration UUID.
"""

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_catalog_binding_schema import MssqlPhysicalCatalogBindingSchemaProvisioner
from dpone.adapters.dbt_mssql_physical_catalog_binding_store import MssqlPhysicalCatalogBindingStore
from dpone.adapters.dbt_mssql_physical_catalog_policy import MssqlPhysicalCatalogPolicyReader
from dpone.adapters.dbt_mssql_physical_catalog_v2_schema import MssqlPhysicalCatalogV2SchemaProvisioner
from dpone.adapters.dbt_mssql_physical_registration_store import MssqlPhysicalRegistrationStore
from dpone.contracts.dbt_mssql_physical_catalog_binding import (
    CatalogRegistrationBinding,
    require_catalog_binding_registration,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.mssql_object_name import quote_mssql_identifier
from dpone.contracts.native_delivery import NativeOriginalsRefV1
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class MssqlPhysicalCatalogRegistrationLifecycle:
    """Trusted composition of real policy authentication and SQL boundaries.

    Supply fresh bounded privileged connections pinned to the model database,
    matching registration/binding stores and a distinct v2 certificate provisioner.
    The policy reader owns actual native verification; its returned DTO is never
    accepted as an input parameter in place of performing that verification.
    Exclude concurrent privileged DDL throughout provisioning and final readback.
    """

    def __init__(
        self,
        *,
        policy_reader: MssqlPhysicalCatalogPolicyReader,
        registration_store: MssqlPhysicalRegistrationStore,
        binding_schema: MssqlPhysicalCatalogBindingSchemaProvisioner,
        module_provisioner: MssqlPhysicalCatalogV2SchemaProvisioner,
        binding_store: MssqlPhysicalCatalogBindingStore,
        connection_factory: Callable[[], SqlControlConnection],
    ) -> None:
        if type(policy_reader) is not MssqlPhysicalCatalogPolicyReader:
            raise ValueError("catalog lifecycle requires the actual native policy reader")
        self._policy, self._registrations = policy_reader, registration_store
        self._schema, self._module, self._bindings = binding_schema, module_provisioner, binding_store
        self._connect = connection_factory

    def apply(
        self, refs: NativeOriginalsRefV1, *, registration: MssqlPhysicalRuntimeRegistration
    ) -> CatalogRegistrationBinding:
        """Authenticate before SQL writes; independently verify before success."""
        policy = self._policy.read(refs, registration=registration)
        retained = self._registrations.resolve(registration)
        if retained != registration:
            raise RuntimeError("catalog registration changed during independent readback")
        schema_id, owner = self._observe_schema(registration, policy.model_schema)
        self._schema.apply(registration)
        # Keep explicit keywords for the stable public provisioner contract.
        deployed = self._module.apply(
            registration,
            model_schema=policy.model_schema,
            model_schema_id=schema_id,
            model_schema_owner_id=owner,
        )
        binding = CatalogRegistrationBinding(
            policy.registration_id,
            policy.registration_sha256,
            policy.platform_subject,
            policy.trusted_profile,
            policy.profile_name,
            policy.workflow_id,
            policy.policy_member,
            policy.project_archive_sha256,
            policy.model_database_name,
            policy.model_schema,
            policy.resource_bounds,
            schema_id,
            owner,
            "sha256:" + deployed.module_sha256,
        )
        require_catalog_binding_registration(binding, retained)
        if self._bindings.register(registration, binding) != binding:
            raise RuntimeError("catalog binding write did not verify the exact expected record")
        final = self._module.verify(
            registration,
            model_schema=policy.model_schema,
            model_schema_id=schema_id,
            model_schema_owner_id=owner,
        )
        if final != deployed:
            raise RuntimeError("catalog module changed during independent readback")
        if self._bindings.resolve(registration, binding) != binding:
            raise RuntimeError("catalog binding changed during final readback")
        return binding

    def _observe_schema(self, registration: MssqlPhysicalRuntimeRegistration, schema: str) -> tuple[int, int]:
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(f"USE {quote_mssql_identifier(registration.model_database.database_name)};")
            cursor.execute(
                "SELECT schema_id,principal_id,name FROM sys.schemas "
                "WHERE CONVERT(varbinary(max),name)=CONVERT(varbinary(max),CONVERT(nvarchar(128),?)) "
                "AND DATALENGTH(name)=DATALENGTH(CONVERT(nvarchar(128),?))",
                schema,
                schema,
            )
            row = dbapi_lifecycle.row(cursor)
            if (
                row is None
                or len(row) != 3
                or dbapi_lifecycle.row(cursor) is not None
                or type(row[0]) is not int
                or row[0] <= 4
                or type(row[1]) is not int
                or row[1] != 1
                or row[2] != schema
            ):
                raise RuntimeError("catalog model schema is absent or outside the dedicated dbo-owned cell")
            connection.commit()
            return row[0], row[1]
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
