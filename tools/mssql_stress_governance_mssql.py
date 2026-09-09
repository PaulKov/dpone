"""Disposable MSSQL catalogs and authority bindings for stress certification.

These helpers are explicit operator composition. They may provision only the
caller-provided disposable vendor coordinates; production runtime never calls
them implicitly.
"""

from __future__ import annotations

import uuid
from typing import Any

from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.state.mssql_database_authority import MssqlDatabaseAuthorityVerifier


def ensure_target_identity_registry(target: Any, *, database: str) -> None:
    current_database = target.get_records("SELECT CONVERT(sysname, DB_NAME())")
    if current_database != [(database,)]:
        raise AssertionError("target identity fixture opened the wrong database")
    exists = target.get_records(f"SELECT OBJECT_ID(N'[{database}].[dbo].[dpone_target_identity]', N'U')")[0][0]
    if exists is not None:
        return
    target.execute_query(
        f"""
        CREATE TABLE [{database}].[dbo].[dpone_target_identity] (
            [binding_id] uniqueidentifier NOT NULL,
            [schema_name] nvarchar(128) COLLATE DATABASE_DEFAULT NOT NULL,
            [table_name] nvarchar(128) COLLATE DATABASE_DEFAULT NOT NULL,
            [created_at_utc] datetime2(7) NOT NULL
                CONSTRAINT [df_dpone_target_identity_created] DEFAULT SYSUTCDATETIME(),
            CONSTRAINT [pk_dpone_target_identity] PRIMARY KEY CLUSTERED ([binding_id]),
            CONSTRAINT [uq_dpone_target_identity_name]
                UNIQUE NONCLUSTERED ([schema_name], [table_name])
        )
        """
    )
    target.execute_query(
        """
        CREATE TRIGGER [dbo].[trg_dpone_target_identity_immutable]
        ON [dbo].[dpone_target_identity]
        INSTEAD OF UPDATE, DELETE
        AS
            THROW 51000, 'DPONE_TARGET_IDENTITY_IMMUTABLE', 1;
        """
    )


def _resolved_database_connection(master: Any, database: str) -> ResolvedBindingConnection:
    rows = master.get_records(
        """
        SELECT d.database_id, CONVERT(nvarchar(33), d.create_date, 126) AS create_token,
               CONVERT(nvarchar(36), r.database_guid) AS database_guid
        FROM sys.databases AS d
        INNER JOIN sys.database_recovery_status AS r ON r.database_id = d.database_id
        WHERE d.name = ?
        """,
        (database,),
        as_dict=True,
    )
    if len(rows) != 1:
        raise AssertionError(f"database authority fixture unavailable: {database}")
    row = rows[0]
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(database=database),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor(
            connection_type="mssql",
            properties={
                "database": database,
                "database_authorities": {
                    database: {
                        "database_id": int(row["database_id"]),
                        "create_token": str(row["create_token"]),
                        "database_guid": str(row["database_guid"]).lower(),
                    }
                },
            },
        ),
    )


def bind_factual_mssql_database_authority(
    storage: Any,
    master: Any,
    *,
    target_database: str,
    staging_database: str,
    state_database: str,
    master_connector_factory: Any | None = None,
    staging_connector_factory: Any | None = None,
) -> None:
    """Bind exact disposable database facts through the production verifier."""

    master_factory = master_connector_factory or (lambda _connection: mssql_connector_for_database(master, "master"))
    staging_factory = staging_connector_factory or (
        lambda _connection, database: _verified_database_connector(master, database)
    )
    storage.bind_database_authority(
        MssqlDatabaseAuthorityVerifier.from_connections(
            target_connection=_resolved_database_connection(master, target_database),
            state_connection=_resolved_database_connection(master, state_database),
            target_database=target_database,
            staging_database=staging_database,
            state_database=state_database,
            master_connector_factory=master_factory,
            staging_connector_factory=staging_factory,
        )
    )


def _verified_database_connector(template: Any, database: str) -> Any:
    """Clone a vendor connector and assert its requested session binding."""

    connector = mssql_connector_for_database(template, database)
    try:
        rows = connector.get_records("SELECT CONVERT(sysname, DB_NAME())")
        if rows != [(database,)]:
            raise AssertionError("staging authority fixture opened the wrong database")
    except BaseException:
        connector.close()
        raise
    return connector


def mssql_connector_for_database(template: Any, database: str) -> MSSQLConnector:
    """Clone reviewed connection coordinates without consulting test modules."""

    return MSSQLConnector(
        host=str(template.host),
        port=int(template.port),
        database=database,
        user=template.user,
        password=template.password,
        driver=str(template.driver),
        encrypt=str(template.encrypt),
        trust_server_certificate=str(template.trust_server_certificate),
        connect_timeout=int(template.connect_timeout),
        query_timeout=int(template.query_timeout),
        autocommit=bool(template.autocommit),
        application_name=str(template.application_name),
        bcp_path=str(template.bcp_path),
        odbc_options=dict(template.odbc_options),
    )


def bind_target_identity(target: Any, *, database: str, schema: str, table: str) -> None:
    rows = target.get_records(
        f"SELECT [binding_id] FROM [{database}].[dbo].[dpone_target_identity] "
        "WHERE [schema_name] = ? AND [table_name] = ?",
        (schema, table),
    )
    if len(rows) > 1:
        raise AssertionError("target identity fixture contains an ambiguous binding")
    if rows:
        return
    target.execute_query(
        f"INSERT INTO [{database}].[dbo].[dpone_target_identity] "
        "([binding_id], [schema_name], [table_name]) VALUES (?, ?, ?)",
        (str(uuid.uuid4()), schema, table),
    )


__all__ = [
    "bind_factual_mssql_database_authority",
    "bind_target_identity",
    "ensure_target_identity_registry",
    "mssql_connector_for_database",
]
