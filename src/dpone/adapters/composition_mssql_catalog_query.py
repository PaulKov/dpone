"""Exact bounded query primitive for MSSQL composition catalog audits."""

from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.ports.sql_connection import SqlControlCursor

Rows = tuple[tuple[object, ...], ...]


def require_exact_catalog_rows(
    cursor: SqlControlCursor, part: str, query: str, expected: Rows, *parameters: object
) -> None:
    """Require exact row values and Python types from a bounded projection."""
    cursor.execute(f"SELECT TOP ({len(expected) + 1}) /* composition_schema:{part} */ " + query, *parameters)
    actual = tuple(tuple(row) for row in cursor.fetchall())
    if actual != expected or any(
        type(value) is not type(wanted)
        for row, expected_row in zip(actual, expected, strict=True)
        for value, wanted in zip(row, expected_row, strict=True)
    ):
        raise CompositionAdmissionError("control_schema_" + part)
