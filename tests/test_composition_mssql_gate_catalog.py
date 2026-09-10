"""Every optional gate association is mandatory once MSSQL authority is issued."""

from hashlib import sha256

import pytest

from dpone.adapters import composition_mssql_gate_check_definitions as reference
from dpone.adapters.composition_mssql_gate_catalog import require_composition_mssql_gate_schema
from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
from dpone.adapters.composition_mssql_gate_schema import gate_table_trigger, render_composition_mssql_login_gate
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.test_composition_mssql_catalog import Cursor, expected_rows


@pytest.fixture
def gate_catalog(monkeypatch):
    definitions = {
        check.name: "OFFLINE GATE EXPRESSION " + check.name
        for table in COMPOSITION_GATE_TABLES
        for check in table.checks
    }
    monkeypatch.setattr(reference, "CHECK_DEFINITIONS", definitions)
    monkeypatch.setattr(
        reference,
        "CHECK_DDL_SHA256",
        "sha256:" + sha256(render_composition_mssql_login_gate(control_database="dpone_control").encode()).hexdigest(),
    )
    rows = expected_rows(tables=COMPOSITION_GATE_TABLES, definitions=definitions, trigger_for=gate_table_trigger)
    return Cursor(rows)


def test_complete_gate_catalog_keeps_exact_existing_update_delete_events(gate_catalog):
    require_composition_mssql_gate_schema(gate_catalog, "control")
    assert len(gate_catalog.calls) == 1 + 4 * 9
    for table in COMPOSITION_GATE_TABLES:
        key = f"[control].[composition_{table.name}]"
        events = gate_catalog.rows[key, "events"]
        assert tuple(row[1] for row in events) == ("DELETE", "UPDATE")
    columns = gate_catalog.rows["[control].[composition_login_gates]", "columns"]
    assert [(r[1], r[2], r[3], r[4], r[19]) for r in columns[2:4]] == [
        ("login_sid", "binary", 16, None, 1),
        ("login_name", "nvarchar", 256, "Latin1_General_100_BIN2", 1),
    ]


@pytest.mark.parametrize("table", [table.name for table in COMPOSITION_GATE_TABLES])
@pytest.mark.parametrize("part", ["table", "columns", "keys", "foreign_keys", "objects", "triggers", "events"])
def test_missing_part_of_any_gate_table_never_becomes_an_optional_pass(gate_catalog, table, part):
    gate_catalog.rows[f"[control].[composition_{table}]", part] = ()
    with pytest.raises(CompositionAdmissionError, match="control_schema_" + part):
        require_composition_mssql_gate_schema(gate_catalog, "control")


@pytest.mark.parametrize(
    "part", ["table", "columns", "keys", "foreign_keys", "checks", "objects", "triggers", "events", "row_security"]
)
def test_unexpected_catalog_row_is_not_ignored(gate_catalog, part):
    key = ("[control].[composition_login_gates]", part)
    gate_catalog.rows[key] += (("unexpected",),)
    with pytest.raises(CompositionAdmissionError, match="control_schema_" + part):
        require_composition_mssql_gate_schema(gate_catalog, "control")


def test_missing_entire_gate_group_cannot_relax_dependency(gate_catalog):
    gate_catalog.rows = {("control", "visibility"): (("control", 1, 1, 0),)}
    with pytest.raises(CompositionAdmissionError):
        require_composition_mssql_gate_schema(gate_catalog, "control")


def test_stale_gate_check_reference_rejects_before_queries(gate_catalog, monkeypatch):
    monkeypatch.setattr(reference, "CHECK_DDL_SHA256", "sha256:" + "0" * 64)
    with pytest.raises(CompositionAdmissionError, match="login_gate_schema_reference"):
        require_composition_mssql_gate_schema(gate_catalog, "control")
    assert gate_catalog.calls == []


def test_no_check_managed_schema_table_still_rejects_added_check(gate_catalog):
    key = ("[control].[composition_mssql_managed_schemas]", "checks")
    gate_catalog.rows[key] = (("unexpected",),)
    with pytest.raises(CompositionAdmissionError, match="control_schema_checks"):
        require_composition_mssql_gate_schema(gate_catalog, "control")
