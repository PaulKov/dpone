"""Offline catalog projections; fabricated CHECK text is never live SQL proof."""

from copy import deepcopy
from hashlib import sha256

import pytest

from dpone.adapters import composition_mssql_catalog as catalog
from dpone.adapters import composition_mssql_check_definitions as reference
from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES, LEGACY_COMPOSITION_OBJECTS
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.composition_mssql_catalog_helpers import Cursor as Cursor
from tests.composition_mssql_catalog_helpers import expected_rows as expected_rows


@pytest.fixture
def verified_reference(monkeypatch):
    values = {
        check.name: "OFFLINE FIXTURE ONLY: " + check.name for table in COMPOSITION_TABLES for check in table.checks
    }
    monkeypatch.setattr(reference, "CHECK_DEFINITIONS", values)
    monkeypatch.setattr(reference, "CHECK_METADATA", {name: (0, 0) for name in values})
    monkeypatch.setattr(reference, "CHECK_DATABASE_COLLATION", "Latin1_General_100_BIN2")
    monkeypatch.setattr(
        reference, "CHECK_DDL_SHA256", "sha256:" + sha256(render_composition_mssql_schema().encode()).hexdigest()
    )
    return values


def test_empty_reference_fails_before_catalog_reads(monkeypatch):
    monkeypatch.setattr(reference, "CHECK_DEFINITIONS", {})
    cursor = Cursor({})
    with pytest.raises(CompositionAdmissionError, match="control_schema_reference"):
        catalog.require_composition_mssql_schema(cursor, "control")
    assert cursor.calls == []


@pytest.mark.parametrize("schema", ["control", "Other_1"])
def test_exact_complete_shape_and_row_bounds(schema, verified_reference):
    rows = expected_rows(schema)
    cursor = Cursor(rows)
    catalog.require_composition_mssql_schema(cursor, schema)
    assert len(cursor.calls) == len(rows) == 75
    assert all(limit == len(rows[key]) + 1 for _, _, limit, key in cursor.calls)
    assert all("COMMIT" not in sql and "ROLLBACK" not in sql for sql, _, _, _ in cursor.calls)


@pytest.mark.parametrize("schema", ["", "a.b", "private;SELECT", "x" * 129, None])
def test_invalid_schema_is_sanitized_before_io(schema, verified_reference):
    cursor = Cursor({})
    with pytest.raises(CompositionAdmissionError, match="control_schema$") as caught:
        catalog.require_composition_mssql_schema(cursor, schema)
    assert "private" not in str(caught.value) and cursor.calls == []


@pytest.mark.parametrize("drift", ["missing", "extra", "empty", "type", "pin"])
def test_unverified_reference_cannot_be_adopted(drift, verified_reference, monkeypatch):
    values = dict(verified_reference)
    first = next(iter(values))
    if drift == "missing":
        del values[first]
    elif drift == "extra":
        values["extra"] = "private"
    elif drift in {"empty", "type"}:
        values[first] = "" if drift == "empty" else None
    else:
        monkeypatch.setattr(reference, "CHECK_DDL_SHA256", "sha256:" + "0" * 64)
    monkeypatch.setattr(reference, "CHECK_DEFINITIONS", values)
    with pytest.raises(CompositionAdmissionError, match="control_schema_reference"):
        catalog.require_composition_mssql_schema(Cursor({}), "control")


@pytest.mark.parametrize(
    "part", ["table", "columns", "keys", "checks", "foreign_keys", "objects", "triggers", "events"]
)
@pytest.mark.parametrize("drift", ["missing", "extra", "changed"])
def test_complete_projection_drift_rejects(part, drift, verified_reference):
    rows = expected_rows()
    key = ("[control].[composition_domains]", part)
    original = rows[key]
    assert original
    rows[key] = original[1:] if drift == "missing" else (*original, ("extra",)) if drift == "extra" else (("changed",),)
    with pytest.raises(CompositionAdmissionError, match="control_schema_" + part):
        catalog.require_composition_mssql_schema(Cursor(rows), "control")


@pytest.mark.parametrize(
    "part,width",
    [
        ("table", 23),
        ("columns", 20),
        ("keys", 17),
        ("foreign_keys", 13),
        ("checks", 10),
        ("triggers", 15),
        ("events", 4),
    ],
)
def test_each_catalog_field_is_compared(part, width, verified_reference):
    original = expected_rows()
    key = ("[control].[composition_domains]", part)
    assert len(original[key][0]) == width
    for index in range(width):
        rows = deepcopy(original)
        row = list(rows[key][0])
        row[index] = "changed" if type(row[index]) is not str else row[index] + "changed"
        rows[key] = (tuple(row), *rows[key][1:])
        with pytest.raises(CompositionAdmissionError, match="control_schema_" + part):
            catalog.require_composition_mssql_schema(Cursor(rows), "control")


@pytest.mark.parametrize("legacy", LEGACY_COMPOSITION_OBJECTS)
def test_legacy_names_reject_without_a_table_type_filter(legacy, verified_reference):
    rows = expected_rows()
    rows["control", "legacy"] = (("composition_" + legacy,),)
    cursor = Cursor(rows)
    with pytest.raises(CompositionAdmissionError, match="control_schema_legacy"):
        catalog.require_composition_mssql_schema(cursor, "control")
    assert "sys.objects" in cursor.calls[-1][0] and "N'U'" not in cursor.calls[-1][0]


@pytest.mark.parametrize("stage", ["execute", "fetchall"])
def test_driver_failures_never_leak_originals(stage, verified_reference, monkeypatch):
    cursor = Cursor(expected_rows())

    def fail(*args):
        raise RuntimeError("private-driver-canary")

    monkeypatch.setattr(cursor, stage, fail)
    with pytest.raises(CompositionAdmissionError, match="control_schema_unavailable") as caught:
        catalog.require_composition_mssql_schema(cursor, "control")
    assert "private-driver-canary" not in str(caught.value) and caught.value.__suppress_context__


@pytest.mark.parametrize("permission", [0, None, True, 1.0])
@pytest.mark.parametrize("scope", [1, 2])
def test_visibility_requires_an_actual_integer_permission(scope, permission, verified_reference):
    rows = expected_rows()
    visibility = ["control", 1, 1, 0]
    visibility[scope] = permission
    rows["control", "visibility"] = (tuple(visibility),)
    with pytest.raises(CompositionAdmissionError, match="control_schema_visibility"):
        catalog.require_composition_mssql_schema(Cursor(rows), "control")


def test_any_row_security_predicate_rejects(verified_reference):
    rows = expected_rows()
    rows["[control].[composition_domains]", "row_security"] = ((100,),)
    with pytest.raises(CompositionAdmissionError, match="control_schema_row_security"):
        catalog.require_composition_mssql_schema(Cursor(rows), "control")


@pytest.mark.parametrize("drift", ["whitespace", "oversize", "collation_dependency"])
def test_check_text_is_exact_and_oversize_projection_cannot_pass(drift, verified_reference):
    rows = expected_rows()
    key = ("[control].[composition_domains]", "checks")
    row = list(rows[key][0])
    if drift == "whitespace":
        row[3] = str(row[3]) + " "
        row[4] = len(row[3].encode("utf-16le"))
    elif drift == "oversize":
        row[3], row[4] = None, 2147483647
    else:
        row[8] = 1
    rows[key] = (tuple(row), *rows[key][1:])
    with pytest.raises(CompositionAdmissionError, match="control_schema_checks"):
        catalog.require_composition_mssql_schema(Cursor(rows), "control")


def test_sql_projections_bound_all_potential_module_bodies(verified_reference):
    cursor = Cursor(expected_rows())
    catalog.require_composition_mssql_schema(cursor, "control")
    queries = {key[1]: (sql, parameters) for sql, parameters, _, key in cursor.calls}
    assert "DATALENGTH(i.filter_definition)" in queries["keys"][0]
    assert "CASE WHEN DATALENGTH(c.definition)<=? THEN c.definition END" in queries["checks"][0]
    assert isinstance(queries["checks"][1][0], int) and queries["checks"][1][0] > 0
    assert "HASHBYTES('SHA2_256',m.definition),DATALENGTH(m.definition)" in queries["triggers"][0]


def test_masking_uses_its_actual_catalog_view(verified_reference):
    cursor = Cursor(expected_rows())
    catalog.require_composition_mssql_schema(cursor, "control")
    query = next(sql for sql, _, _, key in cursor.calls if key[1] == "columns")
    assert "sys.masked_columns m" in query
    assert "m.object_id=c.object_id AND m.column_id=c.column_id AND m.is_masked=1" in query
    assert "c.is_masked" not in query


def test_cross_schema_policy_metadata_requires_database_visibility(verified_reference):
    cursor = Cursor(expected_rows())
    catalog.require_composition_mssql_schema(cursor, "control")
    query = cursor.calls[0][0]
    assert "HAS_PERMS_BY_NAME(DB_NAME(),N'DATABASE',N'VIEW DEFINITION')" in query
    assert "sys.user_token" in query and "sys.database_permissions" in query


@pytest.mark.parametrize("part", ["columns", "keys", "checks", "foreign_keys", "objects", "events"])
def test_complete_order_and_duplicate_rows_are_not_sets(part, verified_reference):
    original = expected_rows()
    key = ("[control].[composition_operations]", part)
    assert len(original[key]) > 1
    for replacement in (tuple(reversed(original[key])), (original[key][0],) * len(original[key])):
        rows = deepcopy(original)
        rows[key] = replacement
        with pytest.raises(CompositionAdmissionError, match="control_schema_" + part):
            catalog.require_composition_mssql_schema(Cursor(rows), "control")


@pytest.mark.parametrize("denied", [1, None, False, 0.0])
def test_narrower_metadata_denials_or_unknown_observations_reject(denied, verified_reference):
    rows = expected_rows()
    rows["control", "visibility"] = (("control", 1, 1, denied),)
    with pytest.raises(CompositionAdmissionError, match="control_schema_visibility"):
        catalog.require_composition_mssql_schema(Cursor(rows), "control")


def test_current_database_collation_must_match_pin(verified_reference, monkeypatch):
    metadata = {name: (0, 0) for name in verified_reference}
    monkeypatch.setattr(reference, "CHECK_METADATA", metadata, raising=False)
    monkeypatch.setattr(reference, "CHECK_DATABASE_COLLATION", "Latin1_General_100_BIN2", raising=False)
    rows = expected_rows()
    rows["database", "collation"] = (("SQL_Latin1_General_CP1_CI_AS",),)
    with pytest.raises(CompositionAdmissionError, match="control_schema_collation"):
        catalog.require_composition_mssql_schema(Cursor(rows), "control")


def test_missing_metadata_rejects_before_catalog_reads(verified_reference, monkeypatch):
    monkeypatch.setattr(reference, "CHECK_METADATA", {}, raising=False)
    monkeypatch.setattr(reference, "CHECK_DATABASE_COLLATION", "Latin1_General_100_BIN2", raising=False)
    cursor = Cursor({})
    with pytest.raises(CompositionAdmissionError, match="control_schema_reference"):
        catalog.require_composition_mssql_schema(cursor, "control")
    assert cursor.calls == []


@pytest.mark.parametrize(
    "value", [None, [], (0,), (0, 0, 0), (True, 0), (0, False), (1.0, 0), (-1, 0), (99, 0), (0, 2)]
)
def test_malformed_metadata_never_reaches_the_catalog(value, verified_reference, monkeypatch):
    metadata = dict(reference.CHECK_METADATA)
    metadata["ck_c_authority_singleton"] = value
    monkeypatch.setattr(reference, "CHECK_METADATA", metadata)
    cursor = Cursor({})
    with pytest.raises(CompositionAdmissionError, match="control_schema_reference$"):
        catalog.require_composition_mssql_schema(cursor, "control")
    assert cursor.calls == []


@pytest.mark.parametrize("damage", ["missing", "extra", "nonmapping"])
def test_metadata_inventory_is_closed(damage, verified_reference, monkeypatch):
    metadata = dict(reference.CHECK_METADATA)
    if damage == "missing":
        metadata.pop("ck_c_authority_singleton")
    elif damage == "extra":
        metadata["another_table_check"] = (0, 0)
    monkeypatch.setattr(reference, "CHECK_METADATA", list(metadata) if damage == "nonmapping" else metadata)
    cursor = Cursor({})
    with pytest.raises(CompositionAdmissionError, match="control_schema_reference$"):
        catalog.require_composition_mssql_schema(cursor, "control")
    assert cursor.calls == []


@pytest.mark.parametrize("value", [None, "", True, "unknown collation", "x" * 129])
def test_malformed_collation_pin_rejects_before_reads(value, verified_reference, monkeypatch):
    monkeypatch.setattr(reference, "CHECK_DATABASE_COLLATION", value)
    cursor = Cursor({})
    with pytest.raises(CompositionAdmissionError, match="control_schema_reference$"):
        catalog.require_composition_mssql_schema(cursor, "control")
    assert cursor.calls == []


@pytest.mark.parametrize("field,value", [(2, 2), (8, 0), (5, 1), (6, 1), (7, 1), (9, 1), (2, True), (8, True)])
def test_nonzero_captured_binding_is_exact_and_unsafe_flags_remain_zero(field, value, verified_reference, monkeypatch):
    metadata = dict(reference.CHECK_METADATA)
    metadata["ck_c_authority_singleton"] = (1, 1)
    monkeypatch.setattr(reference, "CHECK_METADATA", metadata)
    rows = expected_rows()
    catalog.require_composition_mssql_schema(Cursor(rows), "control")
    key = ("[control].[composition_authority]", "checks")
    row = list(rows[key][0])
    row[field] = value
    rows[key] = (tuple(row), *rows[key][1:])
    with pytest.raises(CompositionAdmissionError, match="control_schema_checks$"):
        catalog.require_composition_mssql_schema(Cursor(rows), "control")


@pytest.mark.parametrize("expression", ["😀" * 32769, "\ud800"], ids=["utf16_overflow", "invalid_utf16"])
def test_impossible_reference_expression_rejects_before_reads(expression, verified_reference, monkeypatch):
    definitions = dict(verified_reference)
    definitions["ck_c_authority_singleton"] = expression
    monkeypatch.setattr(reference, "CHECK_DEFINITIONS", definitions)
    cursor = Cursor({})
    with pytest.raises(CompositionAdmissionError, match="control_schema_reference$"):
        catalog.require_composition_mssql_schema(cursor, "control")
    assert cursor.calls == []
