"""Catalog admission rejects drift; no SQL in this suite provisions a server."""

from importlib import import_module

import pytest

from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding


def api(name="admission"):
    if name == "ddl":
        return import_module("dpone.adapters.mssql_publication_catalog_ddl")
    return import_module(f"dpone.runtime.state.mssql_publication_{name}")


BINDING = PublicationAuthorityBinding("mssql", "metadata", "Example_System", "dbo", "example", "test")
SLOT = "dpone_cluster_publication_authority"
EVENTS = "dpone_cluster_publication_events"


def columns():
    common = [
        ("slot_key", "char", 64, 0, "Latin1_General_100_BIN2", 0),
        ("binding_digest", "binary", 32, 0, None, 0),
        ("revision", "bigint", 8, 0, None, 0),
        ("operation_id", "nvarchar", 256, 0, "Latin1_General_100_BIN2", 0),
        ("phase", "varchar", 32, 0, "Latin1_General_100_BIN2", 0),
        ("payload", "varbinary", -1, 0, None, 0),
        ("payload_sha256", "binary", 32, 0, None, 0),
        ("write_id", "uniqueidentifier", 16, 0, None, 0),
    ]
    extra = [
        ("previous_sha256", "binary", 32, 1, None, 0),
        ("origin", "varchar", 32, 0, "Latin1_General_100_BIN2", 0),
        ("provenance", "varbinary", -1, 0, None, 0),
        ("provenance_sha256", "binary", 32, 0, None, 0),
        ("created_utc", "datetime2", 8, 0, None, 7),
    ]
    return [
        (table, i, *row)
        for table, fields in [(SLOT, common), (EVENTS, common + extra)]
        for i, row in enumerate(fields, 1)
    ]


class Catalog:
    def __init__(self):
        self.calls = []
        self.column_rows = columns()
        self.index_rows = [
            (SLOT, "pk_publication_authority", 1, 1, 0, 0, 0, 1, "slot_key", 1),
            (EVENTS, "pk_publication_events", 1, 1, 0, 0, 0, 1, "slot_key", 1),
            (EVENTS, "pk_publication_events", 1, 1, 0, 0, 0, 2, "revision", 1),
            (EVENTS, "uq_publication_write", 1, 0, 0, 0, 0, 1, "write_id", 2),
        ]
        self.trigger_rows = None
        self.other_rows = []

    def get_records(self, sql, params):
        assert sql.lstrip().upper().startswith("SELECT")
        assert params == ("dbo", SLOT, EVENTS)
        self.calls.append(sql)
        if sql.startswith("SELECT t.name,c.column_id"):
            return self.column_rows
        if sql.startswith("SELECT t.name,i.name"):
            return self.index_rows
        if sql.startswith("SELECT t.name,tr.name"):
            return (
                self.trigger_rows
                if self.trigger_rows is not None
                else [
                    (
                        EVENTS,
                        "dpone_publication_events_immutable",
                        0,
                        0,
                        api("ddl").immutable_event_trigger(schema="dbo"),
                    )
                ]
            )
        if "sys.objects" in sql:
            return self.other_rows
        raise AssertionError("unrecognised read-only catalog query")


def test_valid_catalog_is_read_only_admitted():
    catalog = Catalog()
    api().require_publication_catalog(catalog, binding=BINDING)
    assert len(catalog.calls) == 4


@pytest.mark.parametrize(
    "change",
    [
        "absent",
        "column",
        "nullable",
        "collation",
        "scale",
        "extra_index",
        "disabled_index",
        "filtered_index",
        "wrong_key",
        "missing_trigger",
        "altered_trigger",
        "disabled_trigger",
        "extra_object",
    ],
)
def test_catalog_drift_blocks_before_any_runtime_write(change):
    catalog = Catalog()
    if change == "absent":
        catalog.column_rows = []
    elif change in {"column", "nullable", "collation", "scale"}:
        row = list(catalog.column_rows[0])
        position, value = {"column": (3, "varchar"), "nullable": (5, 1), "collation": (6, "wrong"), "scale": (7, 1)}[
            change
        ]
        row[position] = value
        catalog.column_rows[0] = tuple(row)
    elif change == "extra_index":
        catalog.index_rows.append((SLOT, "unreviewed", 1, 0, 0, 0, 0, 1, "revision", 2))
    elif change in {"disabled_index", "filtered_index", "wrong_key"}:
        row = list(catalog.index_rows[0])
        position, value = {"disabled_index": (4, 1), "filtered_index": (5, 1), "wrong_key": (8, "payload")}[change]
        row[position] = value
        catalog.index_rows[0] = tuple(row)
    elif change == "missing_trigger":
        catalog.trigger_rows = []
    elif change in {"altered_trigger", "disabled_trigger"}:
        catalog.trigger_rows = [
            (
                EVENTS,
                "dpone_publication_events_immutable",
                int(change == "disabled_trigger"),
                0,
                "CREATE TRIGGER untrusted"
                if change == "altered_trigger"
                else api("ddl").immutable_event_trigger(schema="dbo"),
            )
        ]
    else:
        catalog.other_rows = [(EVENTS, "unexpected_default", "D")]
    with pytest.raises(ValueError, match="publication catalog"):
        api().require_publication_catalog(catalog, binding=BINDING)


def test_driver_error_is_redacted():
    class Broken:
        def get_records(self, sql, params):
            raise RuntimeError("synthetic-secret")

    with pytest.raises(ValueError, match="publication catalog") as error:
        api().require_publication_catalog(Broken(), binding=BINDING)
    assert "synthetic-secret" not in str(error.value)


@pytest.mark.parametrize("database,schema", [("x];DROP", "dbo"), ("Example_System", "dbo.x"), ("", "dbo")])
def test_ddl_rejects_sql_fragments(database, schema):
    with pytest.raises(ValueError):
        api("ddl").render_publication_catalog_ddl(database=database, schema=schema)


def test_ddl_is_an_explicit_non_idempotent_plan_not_runtime_bootstrap():
    sql = api("ddl").render_publication_catalog_ddl(database="Example_System", schema="dbo")
    assert "USE [Example_System]" in sql
    assert "CREATE TABLE [dbo].[dpone_cluster_publication_authority]" in sql
    assert "CREATE TABLE [dbo].[dpone_cluster_publication_events]" in sql
    assert "PRIMARY KEY CLUSTERED ([slot_key], [revision])" in sql
    assert "UNIQUE ([write_id])" in sql
    assert "AFTER UPDATE, DELETE" in sql
    assert "THROW" in sql
    assert "IF NOT EXISTS" not in sql  # incompatible pre-existing objects must not be blessed
    assert "DROP" not in sql and "TRUNCATE" not in sql
