"""Opt-in primitive renderer DDL proof, not native admission or route certification.

Parent runner supplies DPONE_RUN_PHYSICAL_RENDERING_LIVE=1 and synthetic
DPONE_NATIVE_SQL_TEST_HOST/PASSWORD. Four cases own four fresh databases. Use a
private --junitxml path and -o junit_family=xunit1 to retain observed engine/driver
metadata through record_property. No fixture credentials or DSN are recorded.
"""

import importlib
import json
import os
import time
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

import pytest

from dpone.adapters.dbt_mssql_physical_rendering import render_candidate_create, render_candidate_layout
from dpone.contracts.dbt_mssql_physical import (
    AbsentPredecessor,
    PhysicalFilegroup,
    PhysicalModelPlan,
    PhysicalModelSpec,
    PhysicalRelation,
)
from dpone.contracts.mssql_object_name import quote_mssql_identifier
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.contracts.native_identity import OriginalRef
from tests.support.dbt_mssql_physical_rendering_live import RenderingFixture

LAYOUTS = ("rowstore_none", "rowstore_row", "rowstore_page", "columnstore")
pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_PHYSICAL_RENDERING_LIVE") != "1", reason="isolated SQL2022 rendering proof disabled"
    ),
]


@pytest.fixture
def sql_fixture():
    for setting in ("DPONE_NATIVE_SQL_TEST_HOST", "DPONE_NATIVE_SQL_TEST_PASSWORD"):
        if not os.environ.get(setting):
            pytest.fail("enabled rendering proof requires synthetic fixture setting: " + setting)
    fixture = RenderingFixture(importlib.import_module("pyodbc"))
    try:
        fixture.install()
        yield fixture
    finally:
        fixture.cleanup()


def _plan(fixture, layout):
    return PhysicalModelPlan(
        str(uuid4()),
        PhysicalModelSpec(
            model_unique_id="model.synthetic.rendering",
            source_graph_sha256="sha256:" + sha256(b"primitive renderer fixture graph").hexdigest(),
            relation=PhysicalRelation(fixture.database, fixture.schema, "never_published_target"),
            columns=(
                MssqlCatalogColumn("ид]", "int", False),
                MssqlCatalogColumn("меткаΩ", "nvarchar(30)", True, fixture.collation),
                MssqlCatalogColumn("сумма", "decimal(12,3)", False),
                MssqlCatalogColumn("момент", "datetime2(7)", True),
            ),
            layout=layout,
            filegroup=PhysicalFilegroup(fixture.filegroup_id, fixture.filegroup_name),
            resource_bounds=OriginalRef(
                "tests/primitive-renderer", "sha256:" + sha256(b"synthetic limits").hexdigest()
            ),
        ),
        AbsentPredecessor(),
    )


def _assert_layout(fixture, plan, object_id, layout):
    columnstore = layout == "columnstore"
    index_id = 1 if columnstore else 0
    index_type = 5 if columnstore else 0
    index_name = plan.columnstore_index_name if columnstore else None
    assert fixture.query(
        "SELECT index_id,name,type,is_unique,is_primary_key,is_unique_constraint,is_disabled,is_hypothetical,has_filter,data_space_id "
        "FROM sys.indexes WHERE object_id=? ORDER BY index_id",
        object_id,
    ) == [(index_id, index_name, index_type, False, False, False, False, False, False, fixture.filegroup_id)]
    compression = {
        "rowstore_none": (0, "NONE"),
        "rowstore_row": (1, "ROW"),
        "rowstore_page": (2, "PAGE"),
        "columnstore": (3, "COLUMNSTORE"),
    }[layout]
    assert fixture.query(
        "SELECT p.index_id,p.partition_number,p.data_compression,p.data_compression_desc,d.data_space_id,d.name,d.type "
        "FROM sys.partitions p JOIN sys.indexes i ON i.object_id=p.object_id AND i.index_id=p.index_id "
        "JOIN sys.data_spaces d ON d.data_space_id=i.data_space_id WHERE p.object_id=? ORDER BY p.index_id,p.partition_number",
        object_id,
    ) == [(index_id, 1, *compression, fixture.filegroup_id, fixture.filegroup_name, "FG")]
    index_columns = fixture.query(
        "SELECT column_id,key_ordinal,is_descending_key,is_included_column,partition_ordinal,column_store_order_ordinal "
        "FROM sys.index_columns WHERE object_id=? ORDER BY column_id",
        object_id,
    )
    assert index_columns == ([(column, 0, False, True, 0, 0) for column in range(1, 5)] if columnstore else [])


@pytest.mark.parametrize("layout", LAYOUTS)
def test_live_rendered_candidate_ddl_preserves_exact_columns_layout_and_duplicates(
    sql_fixture, layout, record_property
):
    fixture = sql_fixture
    plan = _plan(fixture, layout)
    create_sql = render_candidate_create(plan)
    layout_sql = render_candidate_layout(plan)
    fixture.query(create_sql)
    objects = fixture.query(
        "SELECT object_id FROM sys.tables WHERE schema_id=SCHEMA_ID(?) AND name=?", fixture.schema, plan.candidate_name
    )
    assert len(objects) == 1
    object_id = objects[0][0]
    expected_columns = [
        (1, "ид]", "int", 4, 10, 0, False, None, 56, 56),
        (2, "меткаΩ", "nvarchar", 60, 0, 0, True, fixture.collation, 231, 231),
        (3, "сумма", "decimal", 9, 12, 3, False, None, 106, 106),
        (4, "момент", "datetime2", 8, 27, 7, True, None, 42, 42),
    ]
    column_sql = (
        "SELECT c.column_id,c.name,t.name,c.max_length,c.precision,c.scale,c.is_nullable,c.collation_name,c.user_type_id,c.system_type_id "
        "FROM sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id WHERE c.object_id=? ORDER BY c.column_id"
    )
    assert fixture.query(column_sql, object_id) == expected_columns
    _assert_layout(fixture, plan, object_id, "rowstore_none")
    relation = ".".join(
        quote_mssql_identifier(name) for name in (fixture.database, fixture.schema, plan.candidate_name)
    )
    columns = ",".join(quote_mssql_identifier(column.name) for column in plan.spec.columns)
    duplicate = (7, "повтор] Ω", Decimal("12.340"), "2026-09-16T01:02:03.1234567")
    fixture.query(f"INSERT INTO {relation} ({columns}) VALUES (?,?,?,?),(?,?,?,?);", *duplicate, *duplicate)
    assert fixture.query(f"SELECT COUNT_BIG(*) FROM {relation}") == [(2,)]
    if layout_sql is not None:
        fixture.query(layout_sql)
    assert fixture.query(column_sql, object_id) == expected_columns
    _assert_layout(fixture, plan, object_id, layout)
    assert fixture.query(
        f"SELECT [ид]]],[меткаΩ],[сумма],CONVERT(char(27),[момент],126) FROM {relation} ORDER BY [ид]]]"
    ) == [duplicate, duplicate]
    assert fixture.query(
        "SELECT OBJECT_ID(?)",
        ".".join(quote_mssql_identifier(name) for name in (fixture.schema, plan.spec.relation.table)),
    ) == [(None,)]
    record_property(
        "physical_rendering_evidence",
        json.dumps(
            {
                **fixture.evidence,
                "layout": layout,
                "model_plan_sha256": plan.model_plan_sha256,
                "create_sql_utf16_sha256": sha256(create_sql.encode("utf-16le")).hexdigest(),
                "layout_sql_utf16_sha256": None
                if layout_sql is None
                else sha256(layout_sql.encode("utf-16le")).hexdigest(),
                "filegroup_id": fixture.filegroup_id,
                "filegroup_name": fixture.filegroup_name,
                "collation": fixture.collation,
                "duplicate_rows_after_layout": 2,
                "elapsed_seconds": round(time.monotonic() - fixture.started, 3),
            },
            sort_keys=True,
        ),
    )
