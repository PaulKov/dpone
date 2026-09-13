"""Observe signed database incarnations and the protected service marker in place."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION, require_control_schema
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.strict_json import canonical_json_bytes


def require_mssql_connection_identity(
    connection: Any,
    *,
    pins: tuple[MssqlDatabaseAuthorityPin, ...],
    control_database: str,
    control_schema: str,
    service_id: str,
) -> bytes:
    """Verify on the caller's existing transaction; never open a substitute session.

    The caller obtains pins from its verified descriptor, including control
    database continuity. This observer neither learns pins nor commits the read.
    """
    if not pins or control_schema != require_control_schema(control_schema):
        raise CompositionAdmissionError("mssql_connection_identity")
    cursor = connection.cursor()
    try:
        for pin in pins:
            cursor.execute(
                "SELECT d.name,d.database_id,LOWER(CONVERT(char(36),r.database_guid)),"
                "CONVERT(nvarchar(33),d.create_date,126),d.state "
                "FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.name=?;",
                pin.database_name,
            )
            if tuple(tuple(row) for row in cursor.fetchall()) != (
                (pin.database_name, pin.database_id, str(pin.database_guid), pin.create_token, 0),
            ):
                raise CompositionAdmissionError("mssql_connection_database")
        cursor.execute(
            "SELECT TOP (2) singleton,schema_version,LOWER(CONVERT(char(36),service_id)) FROM "
            f"{_quote(control_database)}.{_quote(control_schema)}.[composition_authority] WITH (HOLDLOCK);"
        )
        if tuple(tuple(row) for row in cursor.fetchall()) != ((1, COMPOSITION_MSSQL_SCHEMA_VERSION, service_id),):
            raise CompositionAdmissionError("mssql_connection_service")
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-mssql-connection-observation.v1",
                "service_id": service_id,
                "control_database": control_database,
                "control_schema": control_schema,
                "schema_version": COMPOSITION_MSSQL_SCHEMA_VERSION,
                "databases": [{**asdict(pin), "database_guid": str(pin.database_guid)} for pin in pins],
            }
        )
    finally:
        cursor.close()


def _quote(value: str) -> str:
    if type(value) is not str or not value or len(value) > 128 or "\x00" in value:
        raise CompositionAdmissionError("mssql_connection_identifier")
    return "[" + value.replace("]", "]]") + "]"
