"""Capture controlled synthetic CHECK originals and render a pinned reference.

Only the isolated SQL fixture calls capture after installing its frozen DDL.
The returned observation belongs in that run's original artifact. Integration
verifies the run/source/archive identities before rendering this source module.
Neither function installs DDL, adopts a runtime database or certifies a route.
"""

from __future__ import annotations

import re
import subprocess
import sys
from hashlib import sha256
from pathlib import Path

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


def render_reference(observation: object, context: object) -> str:
    """Emit only a complete reference for this exact canonical DDL producer.

    SQL's stored expression bytes are preserved without whitespace, identifier,
    literal or parenthesis normalization. This function grants no installation
    authority; original CI provenance must bind the mandatory context to this
    capture and be retained with the generated file.
    """
    return _render(observation, context, COMPOSITION_TABLES, _SCHEMA, _ddl_digest(gate=False))


def render_gate_reference(observation: object, context: object) -> str:
    """Generate only the complete four-table gate reference from its original capture."""
    return _render(observation, context, COMPOSITION_GATE_TABLES, _GATE_SCHEMA, _ddl_digest(gate=True))


def _render(
    observation: object,
    context: object,
    tables: tuple[CompositionTable, ...],
    observation_schema: str,
    digest: str,
) -> str:
    if type(observation) is not dict or set(observation) != {"schema", "ddl_sha256", "checks"}:
        raise ValueError("check_catalog_shape")
    collation = _context_collation(context)
    expected = {check.name: table.name for table in tables for check in table.checks}
    rows = observation["checks"]
    if observation["schema"] != observation_schema or observation["ddl_sha256"] != digest:
        raise ValueError("check_catalog_ddl")
    if type(rows) is not list or len(rows) != len(expected):
        raise ValueError("check_catalog_inventory")
    columns = {table.name: len(table.columns) for table in tables}
    definitions = {}
    metadata = {}
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
            or type(flags[0]) is not int
            or not 0 <= flags[0] <= columns[table]
            or type(flags[4]) is not int
            or flags[4] not in (0, 1)
            or any(type(flags[index]) is not int or flags[index] != 0 for index in (1, 2, 3, 5))
        ):
            raise ValueError("check_catalog_original")
        try:
            if len(expression.encode("utf-16le")) > 131072:
                raise ValueError("check_catalog_original")
        except UnicodeError:
            raise ValueError("check_catalog_original") from None
        definitions[name] = expression
        metadata[name] = (flags[0], flags[4])
    entries = "\n".join(f"    {name!r}: {expression!r}," for name, expression in sorted(definitions.items()))
    bindings = "\n".join(f"    {name!r}: {value!r}," for name, value in sorted(metadata.items()))
    return _format_reference(
        '"""Generated from the controlled SQL CHECK round trip; do not hand edit."""\n\n'
        f"CHECK_DDL_SHA256: str = {digest!r}\n"
        f"CHECK_DATABASE_COLLATION: str = {collation!r}\n"
        f"CHECK_DEFINITIONS: dict[str, str] = {{\n{entries}\n}}\n"
        f"CHECK_METADATA: dict[str, tuple[int, int]] = {{\n{bindings}\n}}\n"
    )


def _context_collation(context: object) -> str:
    """Validate documentary context; external provenance authenticates the pair."""
    if type(context) is not dict or set(context) != {
        "database",
        "compatibility_level",
        "options_mask",
        "product_version",
        "product_version_status",
        "session_id",
        "database_collation",
    }:
        raise ValueError("check_catalog_context")
    for key in ("database", "database_collation"):
        if type(context[key]) is not str or re.fullmatch(r"[A-Za-z0-9_]{1,128}", context[key]) is None:
            raise ValueError("check_catalog_context")
    for key, minimum, maximum in (
        ("compatibility_level", 1, 255),
        ("options_mask", 0, 2147483647),
        ("session_id", 1, 32767),
    ):
        if type(context[key]) is not int or not minimum <= context[key] <= maximum:
            raise ValueError("check_catalog_context")
    version = context["product_version"]
    if (
        type(version) is not str
        or len(version) > 128
        or re.fullmatch(r"[0-9]+(?:\.[0-9]+){1,3}", version) is None
        or type(context["product_version_status"]) is not str
        or context["product_version_status"] != "OBSERVED"
    ):
        raise ValueError("check_catalog_context")
    return context["database_collation"]


def _format_reference(source: str) -> str:
    """Use this checkout's locked formatter without writing a reference or cache."""
    root = Path(__file__).resolve().parents[1]
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "ruff",
                "format",
                "--no-cache",
                "--config",
                str(root / "pyproject.toml"),
                "--stdin-filename",
                "composition_check_reference.py",
                "-",
            ],
            input=source,
            encoding="utf-8",
            capture_output=True,
            timeout=10,
            check=True,
            cwd=root,
        )
        if not result.stdout:
            raise ValueError("check_catalog_formatter")
        return result.stdout
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise ValueError("check_catalog_formatter") from None
