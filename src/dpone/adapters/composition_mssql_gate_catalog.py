"""Complete optional gate group required by issuance and retained MSSQL proofs.

Base-only trust/owner operations do not call this wrapper. The immutable issued
principal anchors the dependency even if every optional object has disappeared.
CHECK originals are pinned separately from core DDL by their controlled producer.
"""

from hashlib import sha256

from dpone.adapters import composition_mssql_gate_check_definitions as reference
from dpone.adapters.composition_mssql_catalog import (
    inspect_composition_table,
    require_composition_catalog_visibility,
)
from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
from dpone.adapters.composition_mssql_gate_schema import gate_table_trigger, render_composition_mssql_login_gate
from dpone.adapters.composition_mssql_layout import require_control_schema
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.ports.sql_connection import SqlControlCursor


def require_composition_mssql_gate_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Audit every fixed gate table and module; LOGON policy is checked by its owner."""
    try:
        schema = require_control_schema(control_schema)
        names = {check.name for table in COMPOSITION_GATE_TABLES for check in table.checks}
        values = reference.CHECK_DEFINITIONS
        digest = (
            "sha256:"
            + sha256(render_composition_mssql_login_gate(control_database="dpone_control").encode("utf-8")).hexdigest()
        )
        if (
            type(values) is not dict
            or set(values) != names
            or reference.CHECK_DDL_SHA256 != digest
            or any(type(value) is not str or not value for value in values.values())
        ):
            raise CompositionAdmissionError("login_gate_schema_reference")
        require_composition_catalog_visibility(cursor, schema)
        for table in COMPOSITION_GATE_TABLES:
            inspect_composition_table(cursor, schema, table, dict(values), gate_table_trigger(schema, table.name))
    except CompositionAdmissionError:
        raise
    except Exception:
        raise CompositionAdmissionError("login_gate_schema_unavailable") from None
