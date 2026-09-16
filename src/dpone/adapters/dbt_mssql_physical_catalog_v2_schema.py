"""Explicit v2 deployment using the shared exact finite certificate boundary.

Use a separate catalog certificate/user from v1. The same closed inventory rules
reject sharing a certificate between either module or any other signed object.
Policy authentication and immutable binding are composed by the lifecycle owner.
"""

from dpone.adapters.dbt_mssql_physical_catalog_schema import MssqlPhysicalCatalogSchemaProvisioner
from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import ENTRY, catalog_procedure_v2
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration


class MssqlPhysicalCatalogV2SchemaProvisioner(MssqlPhysicalCatalogSchemaProvisioner):
    """Install/verify one stable cohort module; registrations are separate rows."""

    _entry = ENTRY

    def _definition(
        self,
        registration: MssqlPhysicalRuntimeRegistration,
        *,
        model_schema: str,
        model_schema_id: int,
        model_schema_owner_id: int,
        catalog_certificate_thumbprint: bytes,
    ) -> str:
        return catalog_procedure_v2(
            catalog_sql=self._sql,
            local_schema=registration.local_schema,
            model_database=registration.model_database,
            model_schema=model_schema,
            model_schema_id=model_schema_id,
            model_schema_owner_id=model_schema_owner_id,
            catalog_certificate_thumbprint=catalog_certificate_thumbprint,
        )
