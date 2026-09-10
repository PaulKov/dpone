"""Explicit offline catalog observations for adapter boundary tests.

These strings/rows model provisioned prerequisites and never certify SQL. Real
catalog inspection and transaction guards still execute in adapter tests; live
producer references remain untouched outside the owning monkeypatch fixture.
"""

import re
from hashlib import sha256

from dpone.adapters import composition_mssql_check_definitions as core_reference
from dpone.adapters import composition_mssql_gate_check_definitions as gate_reference
from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
from dpone.adapters.composition_mssql_gate_schema import gate_table_trigger, render_composition_mssql_login_gate
from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema
from tests.test_composition_mssql_catalog import expected_rows


def install_offline_catalog_references(monkeypatch):
    """Pin named fake definitions only for the duration of one offline test."""
    for reference, tables, ddl in (
        (core_reference, COMPOSITION_TABLES, render_composition_mssql_schema()),
        (
            gate_reference,
            COMPOSITION_GATE_TABLES,
            render_composition_mssql_login_gate(control_database="dpone_control"),
        ),
    ):
        monkeypatch.setattr(
            reference,
            "CHECK_DEFINITIONS",
            {check.name: "OFFLINE BOUNDARY " + check.name for table in tables for check in table.checks},
        )
        monkeypatch.setattr(reference, "CHECK_DDL_SHA256", "sha256:" + sha256(ddl.encode()).hexdigest())


def catalog_observation(sql, parameters):
    """Return complete independent fixed projections, never a count-only shortcut."""
    match = re.match(r"SELECT TOP \((\d+)\) /\* composition_schema:(\w+) \*/ ", sql)
    if match is None:
        return None
    schema = "dpone_control"
    rows = expected_rows(schema)
    rows.update(
        expected_rows(
            schema,
            tables=COMPOSITION_GATE_TABLES,
            definitions=gate_reference.CHECK_DEFINITIONS,
            trigger_for=gate_table_trigger,
        )
    )
    return list(rows[parameters[-1], match[2]])
