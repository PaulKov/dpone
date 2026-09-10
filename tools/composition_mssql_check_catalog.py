"""Capture controlled synthetic CHECK originals and render a pinned reference.

Only the isolated SQL fixture calls capture after installing its frozen DDL.
The returned observation belongs in that run's original artifact. Integration
verifies the run/source/archive identities before rendering this source module.
Neither function installs DDL, adopts a runtime database or certifies a route.
"""

from __future__ import annotations

from hashlib import sha256

from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
from dpone.adapters.composition_mssql_gate_schema import render_composition_mssql_login_gate
from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES, CompositionTable, require_control_schema
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema
from dpone.ports.sql_connection import SqlControlCursor

_SCHEMA = "dpone.composition-mssql-check-catalog.v1"
_GATE_SCHEMA = "dpone.composition-mssql-gate-check-catalog.v1"


def _ddl_digest(*, gate: bool) -> str:
    ddl = (
        render_composition_mssql_login_gate(control_database="dpone_control")
        if gate
        else render_composition_mssql_schema()
    )
    return "sha256:" + sha256(ddl.encode("utf-8")).hexdigest()


def capture_check_catalog(cursor: SqlControlCursor, control_schema: str) -> dict[str, object]:
    """Read exact stored expressions from one newly installed synthetic schema.

    Row and document bounds reject unexpected data without truncating a passing
    reference. The fixture owns connection/installation and artifact retention.
    Captured flags remain observations until render_reference validates them.
    """
    return _capture(cursor, control_schema, COMPOSITION_TABLES, _SCHEMA, _ddl_digest(gate=False))


def capture_gate_check_catalog(cursor: SqlControlCursor, control_schema: str) -> dict[str, object]:
    """Retain the same exact originals for the separately installed four gate tables."""
    return _capture(cursor, control_schema, COMPOSITION_GATE_TABLES, _GATE_SCHEMA, _ddl_digest(gate=True))


def _capture(
    cursor: SqlControlCursor,
    control_schema: str,
    tables: tuple[CompositionTable, ...],
    observation_schema: str,
    ddl_digest: str,
) -> dict[str, object]:
    schema = require_control_schema(control_schema)
    checks: list[list[object]] = []
    for table in tables:
        cursor.execute(
            f"SELECT TOP ({len(table.checks) + 1}) c.name,"
            "CASE WHEN DATALENGTH(c.definition) BETWEEN 2 AND 131072 THEN c.definition END,"
            "c.parent_column_id,CONVERT(int,c.is_disabled),CONVERT(int,c.is_not_trusted),"
            "CONVERT(int,c.is_not_for_replication),CONVERT(int,c.uses_database_collation),"
            "CONVERT(int,c.is_system_named) FROM sys.check_constraints c "
            "WHERE c.parent_object_id=OBJECT_ID(?,N'U') ORDER BY c.name COLLATE Latin1_General_100_BIN2;",
            f"[{schema}].[composition_{table.name}]",
        )
        checks.extend([table.name, *tuple(value)] for value in cursor.fetchall())
    return {
        "schema": observation_schema,
        "ddl_sha256": ddl_digest,
        "checks": checks,
    }


def render_reference(observation: object) -> str:
    """Emit only a complete reference for this exact canonical DDL producer.

    SQL's stored expression bytes are preserved without whitespace, identifier,
    literal or parenthesis normalization. This function grants no installation
    authority; original CI provenance must be retained with the generated file.
    """
    return _render(observation, COMPOSITION_TABLES, _SCHEMA, _ddl_digest(gate=False))


def render_gate_reference(observation: object) -> str:
    """Generate only the complete four-table gate reference from its original capture."""
    return _render(observation, COMPOSITION_GATE_TABLES, _GATE_SCHEMA, _ddl_digest(gate=True))


def _render(
    observation: object,
    tables: tuple[CompositionTable, ...],
    observation_schema: str,
    digest: str,
) -> str:
    if type(observation) is not dict or set(observation) != {"schema", "ddl_sha256", "checks"}:
        raise ValueError("check_catalog_shape")
    expected = {check.name: table.name for table in tables for check in table.checks}
    rows = observation["checks"]
    if observation["schema"] != observation_schema or observation["ddl_sha256"] != digest:
        raise ValueError("check_catalog_ddl")
    if type(rows) is not list or len(rows) != len(expected):
        raise ValueError("check_catalog_inventory")
    definitions = {}
    for row in rows:
        if type(row) is not list or len(row) != 9:
            raise ValueError("check_catalog_row")
        table, name, expression, *flags = row
        if (
            type(name) is not str
            or name in definitions
            or expected.get(name) != table
            or type(expression) is not str
            or not 1 <= len(expression) <= 65536
            or any(type(value) is not int or value != 0 for value in flags)
        ):
            raise ValueError("check_catalog_original")
        definitions[name] = expression
    entries = "\n".join(f"    {name!r}: {expression!r}," for name, expression in sorted(definitions.items()))
    return (
        '"""Generated from the controlled SQL CHECK round trip; do not hand edit."""\n\n'
        f"CHECK_DDL_SHA256: str = {digest!r}\nCHECK_DEFINITIONS: dict[str, str] = {{\n{entries}\n}}\n"
    )
