"""Protected companion storage, preserving the existing registration table.

No FK is installed: the unchanged registration verifier rejects inbound FKs.
The existing registration and every projection are instead locked and compared
in the same local transaction before binding writes. Original/policy and actual
module/schema deployment authentication belong to the application consumer.
"""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_registration_schema import (
    TABLE as REGISTRATION_TABLE,
)
from dpone.adapters.dbt_mssql_physical_registration_schema import (
    verify_model_principal,
    verify_registration_table_sql,
)
from dpone.adapters.dbt_mssql_physical_registration_store import (
    _preflight_sql,
    _select_columns,
    registration_columns,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import decode_physical_runtime_registration
from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
from dpone.contracts.mssql_object_name import native_control_schema, quote_mssql_identifier
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor

TABLE = "physical_catalog_bindings_v1"
COLUMNS = (
    ("registration_id", "uniqueidentifier", 16),
    ("registration_digest", "varbinary(71)", 71),
    ("payload", "varbinary(max)", -1),
    ("binding_digest", "varbinary(71)", 71),
)


class CatalogBindingStorageError(RuntimeError):
    """The immutable binding or its registration/protection was not proven."""


def binding_table_sql(local_schema: str) -> str:
    """Create only an absent four-column dbo-owned companion with no FK."""
    schema = native_control_schema(local_schema)
    columns = ",\n".join(f"{name} {kind} NOT NULL" for name, kind, _ in COLUMNS)
    return f"""IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name=N'{schema}' AND principal_id=1)
 THROW 51480,'DPONE_CATALOG_BINDING_SCHEMA_OWNER',1;
CREATE TABLE [{schema}].[{TABLE}] (
{columns},
PRIMARY KEY (registration_id),
CHECK (DATALENGTH(payload)>=1 AND DATALENGTH(payload)<=1048576));"""


def verify_binding_table_sql(local_schema: str) -> str:
    """Require the exact closed schema, PK and enabled trusted length check."""
    schema = native_control_schema(local_schema)
    values = ",".join(
        f"({i},'{name}','{kind.split('(')[0]}',{length})" for i, (name, kind, length) in enumerate(COLUMNS, 1)
    )
    actual = "SELECT column_id,name,TYPE_NAME(user_type_id),max_length FROM sys.columns WHERE object_id=@object"
    return f"""DECLARE @object int=OBJECT_ID(N'[{schema}].[{TABLE}]',N'U');
IF @object IS NULL OR NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
 WHERE t.object_id=@object AND s.principal_id=1 AND COALESCE(t.principal_id,s.principal_id)=1
 AND CONVERT(varbinary(max),s.name)=CONVERT(varbinary(max),N'{schema}')
 AND DATALENGTH(s.name)=DATALENGTH(N'{schema}')
 AND CONVERT(varbinary(max),t.name)=CONVERT(varbinary(max),N'{TABLE}')
 AND DATALENGTH(t.name)=DATALENGTH(N'{TABLE}')
 AND t.temporal_type=0 AND t.is_memory_optimized=0 AND t.is_filetable=0 AND t.is_replicated=0
 AND t.is_merge_published=0 AND t.is_tracked_by_cdc=0 AND t.is_remote_data_archive_enabled=0
 AND t.is_external=0 AND t.is_node=0 AND t.is_edge=0 AND t.ledger_type=0)
 OR EXISTS (SELECT 1 FROM sys.triggers WHERE parent_id=@object)
 OR EXISTS (SELECT 1 FROM sys.foreign_keys WHERE parent_object_id=@object OR referenced_object_id=@object)
 OR EXISTS (SELECT 1 FROM sys.columns WHERE object_id=@object AND
 (is_nullable=1 OR is_computed=1 OR is_identity=1 OR default_object_id<>0 OR rule_object_id<>0
 OR user_type_id<>system_type_id OR is_sparse=1 OR is_column_set=1 OR is_hidden=1
 OR is_rowguidcol=1 OR is_filestream=1 OR is_masked=1 OR generated_always_type<>0 OR encryption_type IS NOT NULL))
 OR EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {values}) e(ordinal,name,kind,length))
 OR EXISTS (SELECT * FROM (VALUES {values}) e(ordinal,name,kind,length) EXCEPT {actual})
 OR (SELECT COUNT(*) FROM sys.indexes WHERE object_id=@object)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=@object AND type=1 AND is_primary_key=1
 AND is_unique=1 AND is_disabled=0 AND is_hypothetical=0 AND has_filter=0 AND ignore_dup_key=0)
 OR (SELECT COUNT(*) FROM sys.index_columns WHERE object_id=@object AND key_ordinal>0)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.index_columns i JOIN sys.columns c ON c.object_id=i.object_id AND c.column_id=i.column_id
 WHERE i.object_id=@object AND i.key_ordinal=1 AND i.is_descending_key=0 AND c.name='registration_id')
 OR (SELECT COUNT(*) FROM sys.check_constraints WHERE parent_object_id=@object)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id=@object
 AND is_disabled=0 AND is_not_trusted=0 AND is_not_for_replication=0
 AND LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(definition,' ',''),'(',''),')',''),'[',''),']',''))
 ='datalengthpayload>=1anddatalengthpayload<=1048576')
 THROW 51481,'DPONE_CATALOG_BINDING_SCHEMA_MISMATCH',1;"""


def require_storage_registration(value: MssqlPhysicalRuntimeRegistration, local_schema: str) -> None:
    """Representation checks only; the caller supplies authenticated expectation."""
    registration_columns(value)
    if value.local_schema != local_schema:
        raise ValueError("registration local schema differs from binding storage")
    if not isinstance(value.principals.observer, DedicatedObserver):
        raise ValueError("catalog binding first cell requires a dedicated observer")


def verify_registration_context(
    cursor: SqlControlCursor, value: MssqlPhysicalRuntimeRegistration, local_schema: str, *, write: bool
) -> None:
    """Reuse frozen registration projections/preflight without a second connection.

    The private SQL helpers are deliberately shared read-only with the original
    registration store, so database/SID/DENY and projection semantics cannot drift.
    HOLDLOCK is retained until the caller settles the whole binding transaction.
    """
    require_storage_registration(value, local_schema)
    columns = registration_columns(value)
    cursor.execute(
        "SET NOCOUNT ON; SET XACT_ABORT ON; IF @@TRANCOUNT=0 BEGIN TRANSACTION; "
        "IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 THROW 51483,'DPONE_CATALOG_BINDING_TRANSACTION_UNSAFE',1;"
    )
    cursor.execute(verify_registration_table_sql(local_schema))
    cursor.execute(_preflight_sql(local_schema), *columns.values())
    lock = "UPDLOCK,HOLDLOCK" if write else "HOLDLOCK"
    cursor.execute(
        f"SELECT {_select_columns()} FROM [{local_schema}].[{REGISTRATION_TABLE}] WITH ({lock}) WHERE registration_id=?",
        value.registration_id,
    )
    row = dbapi_lifecycle.row(cursor)
    if row is None or len(row) != len(columns) or dbapi_lifecycle.row(cursor) is not None:
        raise CatalogBindingStorageError("existing registration is absent or ambiguous")
    row = (str(row[0]).lower(), *row[1:])
    if row != tuple(columns.values()) or type(row[1]) is not bytes:
        raise CatalogBindingStorageError("existing registration differs from complete expected identity")
    if decode_physical_runtime_registration(row[1]) != value:
        raise CatalogBindingStorageError("existing registration is not canonical")


def verify_binding_protection_sql(local_schema: str, value: MssqlPhysicalRuntimeRegistration) -> str:
    """Require exact direct object DENYs and existing schema mutation DENYs."""
    schema = native_control_schema(local_schema)
    require_storage_registration(value, schema)
    assert isinstance(value.principals.observer, DedicatedObserver)
    ids = [
        mapping.model.principal_id
        for mapping in (value.principals.metadata, value.principals.build, value.principals.observer.mapping)
    ]
    permissions = ("SELECT", "INSERT", "UPDATE", "DELETE", "ALTER", "TAKE OWNERSHIP")
    expected = ",".join(f"({principal},N'{permission}',N'D',0)" for principal in ids for permission in permissions)
    actual = f"SELECT grantee_principal_id,permission_name,state,minor_id FROM sys.database_permissions WHERE class=1 AND major_id=OBJECT_ID(N'[{schema}].[{TABLE}]')"
    schema_expected = ",".join(
        f"({principal},N'{permission}')" for principal in ids for permission in ("ALTER", "TAKE OWNERSHIP")
    )
    return f"""IF EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {expected}) e(principal,permission,state,minor))
 OR EXISTS (SELECT * FROM (VALUES {expected}) e(principal,permission,state,minor) EXCEPT {actual})
 OR EXISTS (SELECT * FROM (VALUES {schema_expected}) e(principal,permission)
 EXCEPT SELECT grantee_principal_id,permission_name FROM sys.database_permissions
 WHERE class=3 AND major_id=SCHEMA_ID(N'{schema}') AND state='D')
 THROW 51482,'DPONE_CATALOG_BINDING_PROTECTION_MISMATCH',1;"""


class MssqlPhysicalCatalogBindingSchemaProvisioner:
    """Install once for a stable authenticated registration/principal cohort.

    The factory supplies a fresh privileged model-DB connection with bounded
    connect/statement timeouts. No covering authority or original authentication
    is created here. Existing tables or missing DENYs are never repaired.
    """

    def __init__(self, *, connection_factory: Callable[[], SqlControlConnection], local_schema: str) -> None:
        self._connect = connection_factory
        self._schema = native_control_schema(local_schema)

    def apply(self, expected_registration: MssqlPhysicalRuntimeRegistration) -> None:
        """Create absent companion, or verify exact replay, in one transaction."""
        require_storage_registration(expected_registration, self._schema)
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            verify_registration_context(cursor, expected_registration, self._schema, write=True)
            cursor.execute("SELECT OBJECT_ID(?)", f"[{self._schema}].[{TABLE}]")
            row = dbapi_lifecycle.row(cursor)
            if row is None or len(row) != 1 or dbapi_lifecycle.row(cursor) is not None:
                raise CatalogBindingStorageError("binding table lookup must return one scalar row")
            if row[0] is None:
                cursor.execute(binding_table_sql(self._schema))
                observer = expected_registration.principals.observer
                assert isinstance(observer, DedicatedObserver)
                for mapping in (
                    expected_registration.principals.metadata,
                    expected_registration.principals.build,
                    observer.mapping,
                ):
                    name = verify_model_principal(cursor, mapping.model)
                    cursor.execute(
                        f"DENY SELECT, INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP ON OBJECT::[{self._schema}].[{TABLE}] TO {quote_mssql_identifier(name)};"
                    )
            cursor.execute(verify_binding_table_sql(self._schema))
            cursor.execute(verify_binding_protection_sql(self._schema, expected_registration))
            connection.commit()
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
