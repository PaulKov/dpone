"""Explicit offline catalog observations for adapter boundary tests.

These strings/rows model provisioned prerequisites and never certify SQL. Real
catalog inspection and transaction guards still execute in adapter tests; live
producer references remain untouched outside the owning monkeypatch fixture.
"""

import re
from copy import deepcopy
from hashlib import sha256

from dpone.adapters import composition_mssql_check_definitions as core_reference
from dpone.adapters import composition_mssql_gate_check_definitions as gate_reference
from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
from dpone.adapters.composition_mssql_gate_schema import gate_table_trigger, render_composition_mssql_login_gate
from dpone.adapters.composition_mssql_invariants import composition_invariant_trigger_sql
from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema


def expected_rows(schema="control", *, tables=COMPOSITION_TABLES, definitions=None, metadata=None, trigger_for=None):
    """Independent projected values from the frozen physical layout."""
    rows: dict[tuple[str, str], tuple[tuple[object, ...], ...]] = {
        (schema, "visibility"): ((schema, 1, 1, 0),),
        (schema, "legacy"): (),
        ("database", "collation"): (("Latin1_General_100_BIN2",),),
    }
    definitions = core_reference.CHECK_DEFINITIONS if definitions is None else definitions
    metadata = core_reference.CHECK_METADATA if metadata is None else metadata
    for table in tables:
        name = f"[{schema}].[composition_{table.name}]"
        rows[name, "table"] = ((schema, "composition_" + table.name, 1, 1, *([0] * 17), None, None),)
        rows[name, "columns"] = tuple(
            (
                index,
                column.name,
                column.sql_type,
                column.length,
                column.collation,
                int(column.nullable),
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                0,
                0,
                0,
                int(column.sql_type in {"varchar", "nvarchar", "binary", "varbinary"}),
            )
            for index, column in enumerate(table.columns, 1)
        )
        rows[name, "keys"] = tuple(
            (
                key.name,
                column,
                index,
                index,
                0,
                0,
                int(key.primary),
                1,
                int(not key.primary),
                1 if key.primary else 2,
                0,
                0,
                0,
                None,
                0,
                key.name,
                "PRIMARY_KEY_CONSTRAINT" if key.primary else "UNIQUE_CONSTRAINT",
            )
            for key in sorted(table.keys, key=lambda value: value.name)
            for index, column in enumerate(key.columns, 1)
        )
        rows[name, "foreign_keys"] = tuple(
            (foreign.name, schema, index, column, schema, "composition_" + foreign.target, target, 0, 0, 0, 0, 0, 0)
            for foreign in sorted(table.foreign_keys, key=lambda value: value.name)
            for index, (column, target) in enumerate(zip(foreign.columns, foreign.target_columns, strict=True), 1)
        )
        rows[name, "checks"] = tuple(
            (
                check.name,
                schema,
                metadata[check.name][0],
                definitions[check.name],
                len(definitions[check.name].encode("utf-16le")),
                0,
                0,
                0,
                metadata[check.name][1],
                0,
            )
            for check in sorted(table.checks, key=lambda value: value.name)
        )
        module = trigger_for(schema, table.name) if trigger_for else None
        trigger = module.name if module else "composition_" + table.name + "_invariant"
        objects = [(check.name, "CHECK_CONSTRAINT", schema, 0) for check in table.checks]
        objects += [
            (key.name, "PRIMARY_KEY_CONSTRAINT" if key.primary else "UNIQUE_CONSTRAINT", schema, 0)
            for key in table.keys
        ]
        objects += [(foreign.name, "FOREIGN_KEY_CONSTRAINT", schema, 0) for foreign in table.foreign_keys]
        rows[name, "objects"] = tuple(sorted((*objects, (trigger, "SQL_TRIGGER", schema, 0))))
        raw = (module.definition if module else composition_invariant_trigger_sql(schema, table.name)).encode(
            "utf-16le"
        )
        rows[name, "triggers"] = (
            (trigger, schema, "SQL_TRIGGER", 1, 0, 0, 0, 0, 1, 1, None, 0, 0, sha256(raw).digest(), len(raw)),
        )
        rows[name, "events"] = tuple(
            (trigger, event, 0, 0) for event in (module.events if module else ("DELETE", "INSERT", "UPDATE"))
        )
        rows[name, "row_security"] = ()
    return rows


class Cursor:
    def __init__(self, rows):
        self.rows, self.calls = deepcopy(rows), []

    def execute(self, sql, *parameters):
        match = re.match(r"SELECT TOP \((\d+)\) /\* composition_schema:(\w+) \*/ ", sql)
        assert match, "only bounded catalog SELECTs are allowed"
        self.key = (parameters[-1] if parameters else "database", match[2])
        self.calls.append((sql, parameters, int(match[1]), self.key))
        return self

    def fetchall(self):
        return self.rows[self.key]

    def fetchone(self):
        raise AssertionError("complete bounded projections are required")

    def close(self):
        raise AssertionError("the caller owns the cursor")


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
        monkeypatch.setattr(
            reference, "CHECK_METADATA", {check.name: (0, 0) for table in tables for check in table.checks}
        )
        monkeypatch.setattr(reference, "CHECK_DATABASE_COLLATION", "Latin1_General_100_BIN2")
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
            metadata=gate_reference.CHECK_METADATA,
            trigger_for=gate_table_trigger,
        )
    )
    return list(rows[parameters[-1] if parameters else "database", match[2]])
