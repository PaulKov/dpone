"""Privileged installation of the immutable physical registration catalog.

Authentication, selected database/service pins and effective runtime permission
qualification are upstream provisioning preconditions, not created by this DDL.
"""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.contracts.dbt_mssql_physical_registration_values import DatabasePrincipal
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor

TABLE = "physical_runtime_registrations_v1"
# name, SQL type, catalog byte length; datetime2(7) has eight storage bytes.
COLUMNS: tuple[tuple[str, str, int], ...] = (
    ("registration_id", "uniqueidentifier", 16),
    ("payload", "varbinary(max)", -1),
    ("registration_digest", "varbinary(71)", 71),
    ("platform_subject", "varbinary(max)", -1),
    ("control_authority_locator", "varbinary(4096)", 4096),
    ("control_authority_digest", "varbinary(71)", 71),
    ("trusted_profile_locator", "varbinary(4096)", 4096),
    ("trusted_profile_digest", "varbinary(71)", 71),
    ("trusted_profile_subject", "varbinary(max)", -1),
    ("trusted_toolchain_locator", "varbinary(4096)", 4096),
    ("trusted_toolchain_digest", "varbinary(71)", 71),
    ("trusted_toolchain_subject", "varbinary(max)", -1),
    ("qualification_policy_id", "varbinary(4096)", 4096),
    ("control_connection_ref", "varbinary(4096)", 4096),
    ("model_connection_ref", "varbinary(4096)", 4096),
    ("service_authority_sha256", "varbinary(71)", 71),
    ("control_database_name", "nvarchar(128)", 256),
    ("control_database_id", "int", 4),
    ("control_database_create_token", "datetime2(7)", 8),
    ("control_database_guid", "uniqueidentifier", 16),
    ("model_database_name", "nvarchar(128)", 256),
    ("model_database_id", "int", 4),
    ("model_database_create_token", "datetime2(7)", 8),
    ("model_database_guid", "uniqueidentifier", 16),
    ("control_schema", "nvarchar(128)", 256),
    ("local_schema", "nvarchar(128)", 256),
    ("control_program_id", "varbinary(4096)", 4096),
    ("control_program_sha256", "varbinary(71)", 71),
    ("package_bundle_sha256", "varbinary(71)", 71),
    ("macro_authority_sha256", "varbinary(71)", 71),
    ("physical_policy", "varbinary(4096)", 4096),
    ("capacity_authority_locator", "varbinary(4096)", 4096),
    ("capacity_authority_digest", "varbinary(71)", 71),
    ("max_metadata_bytes", "int", 4),
    ("max_generation_bytes", "bigint", 8),
    ("max_catalog_rows", "int", 4),
    ("max_definition_utf16_bytes", "int", 4),
    ("max_dependency_rows", "int", 4),
    ("max_columns", "int", 4),
    ("metadata_control_principal_id", "int", 4),
    ("metadata_control_sid", "varbinary(85)", 85),
    ("metadata_model_principal_id", "int", 4),
    ("metadata_model_sid", "varbinary(85)", 85),
    ("build_control_principal_id", "int", 4),
    ("build_control_sid", "varbinary(85)", 85),
    ("build_model_principal_id", "int", 4),
    ("build_model_sid", "varbinary(85)", 85),
    ("observer_mode", "varbinary(32)", 32),
    ("observer_control_principal_id", "int", 4),
    ("observer_control_sid", "varbinary(85)", 85),
    ("observer_model_principal_id", "int", 4),
    ("observer_model_sid", "varbinary(85)", 85),
    ("observer_permission_contract_sha256", "varbinary(71)", 71),
)


def registration_table_sql(local_schema: str) -> str:
    """Render the frozen additive table; schema.sql is its checked package copy."""
    schema = native_control_schema(local_schema)
    columns = ",\n".join(f"    {name} {kind} NOT NULL" for name, kind, _ in COLUMNS)
    return f"""SET NOCOUNT ON; SET XACT_ABORT ON;
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name=N'{schema}' AND principal_id=1)
    THROW 51400, 'DPONE_PHYSICAL_REQUIRES_DBO_SCHEMA', 1;
IF OBJECT_ID(N'[{schema}].[{TABLE}]', N'U') IS NULL
CREATE TABLE [{schema}].[{TABLE}] (
{columns},
    PRIMARY KEY (registration_id),
    CHECK (DATALENGTH(payload)>=1 AND DATALENGTH(payload)<=1048576)
);
"""


def verify_registration_table_sql(local_schema: str) -> str:
    """Reject incompatible tables, including executable/hidden column behavior."""
    schema = native_control_schema(local_schema)
    values = ",".join(f"('{name}','{kind.split('(')[0]}',{length})" for name, kind, length in COLUMNS)
    return f"""DECLARE @object int=OBJECT_ID(N'[{schema}].[{TABLE}]',N'U');
IF @object IS NULL OR NOT EXISTS (
 SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
 WHERE t.object_id=@object AND COALESCE(t.principal_id,s.principal_id)=1
 AND s.principal_id=1 AND t.temporal_type=0 AND t.is_memory_optimized=0)
 OR EXISTS (SELECT 1 FROM sys.triggers WHERE parent_id=@object)
 OR EXISTS (SELECT 1 FROM sys.foreign_keys WHERE parent_object_id=@object OR referenced_object_id=@object)
 OR EXISTS (SELECT 1 FROM sys.database_permissions WHERE class=1 AND major_id=@object
 AND minor_id>0 AND state IN ('G','W'))
 OR EXISTS (SELECT 1 FROM sys.columns WHERE object_id=@object AND
 (is_nullable=1 OR is_computed=1 OR is_identity=1 OR default_object_id<>0
 OR user_type_id<>system_type_id OR is_sparse=1 OR is_column_set=1 OR generated_always_type<>0
 OR encryption_type IS NOT NULL OR (TYPE_NAME(system_type_id)='datetime2' AND scale<>7)))
 OR EXISTS (SELECT name,TYPE_NAME(user_type_id),max_length FROM sys.columns WHERE object_id=@object
 EXCEPT SELECT * FROM (VALUES {values}) e(name,kind,length))
 OR EXISTS (SELECT * FROM (VALUES {values}) e(name,kind,length)
 EXCEPT SELECT name,TYPE_NAME(user_type_id),max_length FROM sys.columns WHERE object_id=@object)
 OR (SELECT COUNT(*) FROM sys.indexes WHERE object_id=@object AND index_id>0)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=@object AND is_primary_key=1
 AND is_unique=1 AND is_disabled=0 AND has_filter=0 AND ignore_dup_key=0)
 OR (SELECT COUNT(*) FROM sys.index_columns WHERE object_id=@object AND key_ordinal>0)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.index_columns i JOIN sys.columns c
 ON c.object_id=i.object_id AND c.column_id=i.column_id
 WHERE i.object_id=@object AND i.key_ordinal=1 AND c.name='registration_id')
 OR (SELECT COUNT(*) FROM sys.check_constraints WHERE parent_object_id=@object)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id=@object
 AND is_disabled=0 AND is_not_trusted=0 AND is_not_for_replication=0
 AND LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(definition,' ',''),'(',''),')',''),'[',''),']',''))
 ='datalengthpayload>=1anddatalengthpayload<=1048576')
 THROW 51401, 'DPONE_PHYSICAL_REGISTRATION_SCHEMA_MISMATCH', 1;"""


def verify_model_principal(cursor: SqlControlCursor, principal: DatabasePrincipal) -> str:
    """Resolve exact database ID/SID to an ordinary user for quoted DDL only."""
    cursor.execute(
        "SELECT name FROM sys.database_principals WHERE principal_id=? AND sid=? "
        "AND DATALENGTH(sid)=? AND type IN ('S','U','E') AND principal_id>4 "
        "AND IS_ROLEMEMBER('db_owner',name)=0",
        principal.principal_id,
        bytes.fromhex(principal.sid_hex),
        len(bytes.fromhex(principal.sid_hex)),
    )
    row = dbapi_lifecycle.row(cursor)
    if row is None or len(row) != 1 or type(row[0]) is not str or dbapi_lifecycle.row(cursor) is not None:
        raise RuntimeError("physical registration principal is absent, changed or privileged")
    return row[0]


class MssqlPhysicalRegistrationSchemaMigration:
    """Install storage with privileged credentials, never with a runtime identity.

    Caller has authenticated the selected model database and runtime principals
    and excluded covering server permissions/impersonation. This storage migration
    neither authenticates source originals nor qualifies signed runtime modules.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        local_schema: str,
        runtime_principals: tuple[DatabasePrincipal, ...],
    ) -> None:
        self._schema = native_control_schema(local_schema)
        if not runtime_principals or any(type(value) is not DatabasePrincipal for value in runtime_principals):
            raise ValueError("existing runtime database principals are required")
        for principal in runtime_principals:
            principal.__post_init__()
        self._principals = runtime_principals
        self._connect = connection_factory

    def apply(self) -> None:
        """Create or verify without repairing retained objects, then apply DENYs."""
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            names = [verify_model_principal(cursor, value) for value in self._principals]
            cursor.execute(registration_table_sql(self._schema))
            cursor.execute(verify_registration_table_sql(self._schema))
            for name in names:
                quoted = "[" + name.replace("]", "]]") + "]"
                cursor.execute(
                    f"DENY SELECT, INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP ON "
                    f"OBJECT::[{self._schema}].[{TABLE}] TO {quoted}"
                )
                cursor.execute(f"DENY ALTER, TAKE OWNERSHIP ON SCHEMA::[{self._schema}] TO {quoted}")
            connection.commit()
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
