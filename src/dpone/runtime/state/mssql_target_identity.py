"""Target-local registry contract for one canonical MSSQL relation identity."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from dpone.runtime.state.mssql_target_identity_contract import (
    TARGET_IDENTITY_REGISTRY_CONTRACT,
    TARGET_IDENTITY_REGISTRY_INTEGRITY,
    TARGET_IDENTITY_REGISTRY_SCHEMA,
    TARGET_IDENTITY_REGISTRY_TABLE,
    require_target_identity_registry_contract,
)
from dpone.runtime.state.mssql_target_identity_models import MssqlPhysicalTargetIdentity

if TYPE_CHECKING:
    from dpone.runtime.state.mssql_route_preflight import MssqlSessionIdentity


class MssqlPhysicalTargetIdentityError(RuntimeError):
    """Raised when the target registry cannot prove one durable binding."""


def resolve_mssql_physical_target_identity(
    connector: Any,
    *,
    session: MssqlSessionIdentity,
    database: str,
    schema: str,
    table: str,
) -> MssqlPhysicalTargetIdentity:
    """Resolve an immutable registry row before any PostgreSQL read."""

    requested = tuple(str(value or "").strip() for value in (database, schema, table))
    if any(not value for value in requested):
        raise MssqlPhysicalTargetIdentityError("mssql_physical_target_coordinates_incomplete")
    requested_database, requested_schema, requested_table = requested
    database_row = _one_row(
        connector.get_records(
            """
            SELECT database_id, name AS database_name, collation_name,
                   CONVERT(nvarchar(33), create_date, 126) AS database_create_token
            FROM sys.databases
            WHERE database_id = DB_ID(?)
            """,
            (requested_database,),
            as_dict=True,
        ),
        "mssql_physical_target_database_unavailable",
    )
    actual_database = _required_text(database_row.get("database_name"), "database_name")
    database_collation = _required_text(database_row.get("collation_name"), "database_collation")
    create_token = _catalog_token(database_row.get("database_create_token"))
    require_target_identity_registry_contract(
        connector,
        database=actual_database,
        database_collation=database_collation,
    )

    qualified_registry = _qualified(
        connector,
        actual_database,
        TARGET_IDENTITY_REGISTRY_SCHEMA,
        TARGET_IDENTITY_REGISTRY_TABLE,
    )
    quoted_database = connector.quote_identifier(actual_database)
    rows = connector.get_records(
        f"""
        SELECT r.binding_id, r.schema_name, r.table_name,
               o.object_id, o.type AS object_type
        FROM {qualified_registry} AS r WITH (HOLDLOCK)
        INNER JOIN {quoted_database}.sys.schemas AS s ON s.name = r.schema_name
        OUTER APPLY (
            SELECT candidate.object_id, candidate.type
            FROM {quoted_database}.sys.objects AS candidate
            WHERE candidate.schema_id = s.schema_id
              AND candidate.parent_object_id = 0
              AND candidate.name = r.table_name
        ) AS o
        WHERE r.schema_name = CONVERT(nvarchar(128), ?)
          AND r.table_name = CONVERT(nvarchar(128), ?)
        """,
        (requested_schema, requested_table),
        as_dict=True,
    )
    row = _one_row(rows, "mssql_physical_target_registry_binding_missing_or_ambiguous")
    object_id = _optional_positive_int(row.get("object_id"), "object_id")
    if object_id is not None and str(row.get("object_type") or "").strip().upper() != "U":
        raise MssqlPhysicalTargetIdentityError("mssql_physical_target_is_not_table")
    try:
        binding_id = uuid.UUID(str(row.get("binding_id") or ""))
    except (ValueError, AttributeError) as exc:
        raise MssqlPhysicalTargetIdentityError("mssql_physical_target_binding_id_invalid") from exc
    return MssqlPhysicalTargetIdentity(
        server_name=session.server_name,
        machine_name=session.machine_name,
        instance_name=session.instance_name,
        replica_name=session.replica_name,
        database_name=actual_database,
        database_create_token=create_token,
        binding_id=binding_id,
        schema_name=_required_text(row.get("schema_name"), "schema_name"),
        table_name=_required_text(row.get("table_name"), "table_name"),
        object_id=object_id,
    )


def assert_mssql_physical_target_identity(
    connector: Any,
    *,
    database: str,
    schema: str,
    table: str,
    expected: bytes,
) -> None:
    """Re-resolve the immutable binding under the target transaction lock."""

    from dpone.runtime.state.mssql_route_preflight import MssqlSessionIdentity

    actual = resolve_mssql_physical_target_identity(
        connector,
        session=MssqlSessionIdentity.read(connector),
        database=database,
        schema=schema,
        table=table,
    )
    if actual.digest != expected:
        raise MssqlPhysicalTargetIdentityError("mssql_physical_target_binding_changed")


def _qualified(connector: Any, database: str, schema: str, table: str) -> str:
    try:
        return str(connector.qualified_name(schema, table, database=database))
    except TypeError:
        return str(connector.qualified_name(f"{database}.{schema}", table))


def _one_row(rows: Any, error: str) -> dict[str, Any]:
    values = list(rows or ())
    if len(values) != 1 or not isinstance(values[0], dict):
        raise MssqlPhysicalTargetIdentityError(error)
    return values[0]


def _required_text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise MssqlPhysicalTargetIdentityError(f"mssql_physical_target_{field}_missing")
    return result


def _catalog_token(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return _required_text(value, "database_create_token")


def _optional_positive_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise MssqlPhysicalTargetIdentityError(f"mssql_physical_target_{field}_invalid") from exc
    if result <= 0:
        raise MssqlPhysicalTargetIdentityError(f"mssql_physical_target_{field}_invalid")
    return result


__all__ = [
    "MssqlPhysicalTargetIdentity",
    "MssqlPhysicalTargetIdentityError",
    "TARGET_IDENTITY_REGISTRY_CONTRACT",
    "TARGET_IDENTITY_REGISTRY_INTEGRITY",
    "TARGET_IDENTITY_REGISTRY_SCHEMA",
    "TARGET_IDENTITY_REGISTRY_TABLE",
    "assert_mssql_physical_target_identity",
    "resolve_mssql_physical_target_identity",
]
