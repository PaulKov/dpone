"""Gate storage migration preserves the existing synchronous login barrier."""

from hashlib import sha256

from dpone.adapters.composition_mssql_gate_schema import (
    login_trigger_sql,
    render_composition_mssql_login_gate,
)
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema


def test_gate_and_evidence_use_shared_operation_without_changing_login_barrier():
    sql = render_composition_mssql_login_gate(control_database="control")
    assert "attempt_sha256" not in sql
    assert "composition_attempts" not in sql
    assert "FOREIGN KEY ([operation_key], [operation_family])" in sql
    assert "REFERENCES [dpone_control].[composition_operations] ([operation_key], [operation_family])" in sql
    assert "DATALENGTH([operation_family])=9" in sql
    assert "i.operation_key = d.operation_key" in sql
    assert sql.count("CREATE TABLE ") == 4
    assert login_trigger_sql("control", "dpone_control") in sql


def test_gate_unicode_identifiers_keep_byte_lengths_and_binary_collation():
    sql = render_composition_mssql_login_gate(control_database="control")
    assert "[login_sid] binary(16) NOT NULL" in sql
    for name in ("login_name", "database_name", "writer_role", "schema_name"):
        assert f"[{name}] nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL" in sql
    assert "[database_create_token] nvarchar(33) COLLATE Latin1_General_100_BIN2 NOT NULL" in sql


def test_all_gate_constraints_are_named_and_immutable_modules_retain_events():
    from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
    from dpone.adapters.composition_mssql_gate_schema import gate_table_trigger

    sql = render_composition_mssql_login_gate(control_database="control")
    for table in COMPOSITION_GATE_TABLES:
        for constraint in (*table.keys, *table.foreign_keys, *table.checks):
            assert f"CONSTRAINT [{constraint.name}]" in sql
        trigger = gate_table_trigger("dpone_control", table.name)
        assert trigger.events == ("DELETE", "UPDATE")
        assert trigger.definition in sql
        assert "AFTER UPDATE, DELETE" in trigger.definition


def test_shared_renderer_extension_preserves_frozen_core_and_logon_bytes():
    assert sha256(render_composition_mssql_schema().encode()).hexdigest() == (
        "4ba5f294030e7c2da8cb867adfbb2fcba45ba87df623fa24f9f7d86e858ae990"
    )
    assert sha256(login_trigger_sql("control", "dpone_control").encode()).hexdigest() == (
        "70216f7740069cd9e383a21f57a1c5dccfa5ed284e2de68cdf0299eee59ac3f7"
    )
