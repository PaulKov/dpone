"""Exact reusable namespace statements; no SQL authority or live certification."""

from hashlib import sha256
from pathlib import Path

import pytest

from dpone.adapters.dbt_mssql_physical_namespace_queries import physical_namespace_sql
from tests.test_dbt_mssql_physical_discovery_queries import procedures

SOURCE = Path("packages/dbt-dpone/control/sqlserver/physical-v1/discovery.sql").read_bytes()


def test_discovery_expansion_preserves_pre_extraction_signed_module_bytes():
    # Obtained from the complete pre-extraction producer at afb972e6, not from
    # two paths through the new shared renderer. Changes require an explicit
    # signed-module compatibility decision rather than automatic regeneration.
    expected = {
        "physical_discover_absent_v1": "60adb615f6abd06023409c2b93708d48f83741db0c2e3c4f63edd0aaf7cc3cbb",
        "physical_control_require_owner_v1": "ec444d75517c198f0312952be462d038c848b4f34e86066f110278002eb59229",
    }
    assert {name: sha256(sql.encode("utf-8")).hexdigest() for name, sql in procedures().items()} == expected


def render(source=SOURCE, **changes):
    return physical_namespace_sql(
        discovery_sql=source,
        **({"model_schema": "analytics", "model_schema_id": 8, "control_schema": "native_control"} | changes),
    )


def test_shared_fragments_are_exact_substrings_without_authority_or_result():
    inputs, assertion = render()
    entry = procedures()["physical_discover_absent_v1"]
    assert entry.count(inputs) == entry.count(assertion) == 1
    assert "DECLARE @objects TABLE" in inputs
    assert "FROM sys.objects" in assertion and "COLLATE CATALOG_DEFAULT" in assertion
    assert "VIEW SECURITY DEFINITION" in assertion and "sys.filegroups" in assertion
    assert "physical_control_require_owner_v1" not in inputs + assertion
    assert "SELECT CONVERT(smallint,1),@registration_id" not in inputs + assertion
    assert "{{" not in inputs + assertion


@pytest.mark.parametrize(
    "anchor",
    [
        b"DECLARE @objects TABLE(",
        b"DECLARE @guard_epoch bigint,@control_id int,@control_sid varbinary(85);",
        b"IF ISNULL(HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),0)<>1",
        b"SELECT CONVERT(smallint,1),@registration_id,@registration_digest,",
    ],
)
def test_missing_or_duplicate_anchor_rejects(anchor):
    for source in (SOURCE.replace(anchor, b"", 1), SOURCE + anchor):
        with pytest.raises(ValueError):
            render(source)


def test_reversed_spans_reject():
    start = b"DECLARE @objects TABLE("
    end = b"DECLARE @guard_epoch bigint,@control_id int,@control_sid varbinary(85);"
    source = SOURCE.replace(start, b"__TEMP__", 1).replace(end, start, 1).replace(b"__TEMP__", end, 1)
    with pytest.raises(ValueError):
        render(source)


@pytest.mark.parametrize(
    "marker", ["OBJECT_SHAPE", "OBJECT_CANONICAL", "MODEL_SCHEMA", "MODEL_SCHEMA_ID", "CONTROL_SCHEMA"]
)
def test_missing_fragment_marker_rejects(marker):
    with pytest.raises(ValueError):
        render(SOURCE.replace(("{{" + marker + "}}").encode(), b""))


def test_unknown_fragment_marker_rejects():
    with pytest.raises(ValueError):
        render(SOURCE.replace(b"DECLARE @objects TABLE(", b"DECLARE @objects TABLE({{UNEXPECTED}}"))


@pytest.mark.parametrize(
    "changes",
    [{"model_schema_id": True}, {"model_schema_id": 0}, {"control_schema": "bad;sql"}, {"model_schema": "bad\x00"}],
)
def test_invalid_coordinates_reject(changes):
    with pytest.raises(ValueError):
        render(**changes)


def test_unicode_model_schema_is_preserved_as_escaped_literal():
    _, assertion = render(model_schema="аналитика's]")
    assert "N'аналитика''s]'" in assertion


@pytest.mark.parametrize(
    "marker", ["OBJECT_SHAPE", "OBJECT_CANONICAL", "MODEL_SCHEMA", "MODEL_SCHEMA_ID", "CONTROL_SCHEMA"]
)
def test_duplicate_fragment_marker_rejects(marker):
    encoded = ("{{" + marker + "}}").encode()
    with pytest.raises(ValueError):
        render(SOURCE.replace(encoded, encoded + encoded))


@pytest.mark.parametrize("schema", ["dbo", "SYS", "information_schema", "NATIVE_CONTROL"])
def test_reserved_model_schema_rejects(schema):
    with pytest.raises(ValueError):
        render(model_schema=schema)
