"""INSERT metadata delegates to the existing native normalization authorities."""

from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_object_name import quote_mssql_identifier
from dpone.runtime.sinks.mssql_native_prepared_insert import build_prepared_insert
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import (
    MssqlNativeGeneratedColumn,
    MssqlNativeLineageProjection,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import ResolvedMssqlNativeSchema
from dpone.runtime.sinks.strategies.mssql.mssql_native_staging import MssqlNativeStagingNormalizer


def projection(*, lineage_on, hash_on, unique_key=None, started=None):
    config = SimpleNamespace(
        options={
            "lineage": {
                "preset": "hierarchical",
                "features": {"run_identity": True, "operations": True, "diagnostics": True},
            }
            if lineage_on
            else False,
            "__dpone_load_identity": {"run_id": "run'雪", "load_id": "load'雪"},
            "source_type": "clickhouse",
        },
        source_schema="source'雪",
        source_table="table'😀",
        unique_key=unique_key,
    )
    lineage = MssqlNativeLineageProjection.resolve(
        config, SimpleNamespace(extraction_started_at=started or datetime(2026, 1, 1, tzinfo=UTC))
    )
    schema = (("n", "int"), ("value", "nvarchar(40)"))
    types = dict(schema)
    generated = [MssqlNativeGeneratedColumn("__dpone__deleted_at", "datetime2(7)", True, "CONVERT(datetime2(7), NULL)")]
    if hash_on:
        generated.append(MssqlNativeGeneratedColumn("__dpone__row_hash", "varchar(64)", False, "NULL"))
    types.update((column.name, column.target_type) for column in generated)
    resolved = ResolvedMssqlNativeSchema(
        types,
        dict(schema),
        {"n": False, "value": True, **{column.name: column.nullable for column in generated}},
        {"value": "Latin1_General_100_BIN2"},
        {name: name for name, _ in schema},
        tuple(generated),
    ).with_lineage(lineage)
    return schema, resolved, lineage


def build(schema, resolved, lineage):
    return build_prepared_insert(
        target_sql="[db].[stage].[prepared]",
        source_sql="SELECT [n], [value] FROM [db].[stage].[a] UNION ALL SELECT [n], [value] FROM [db].[stage].[b]",
        business_schema=schema,
        resolved=resolved,
        lineage=lineage,
        quote_identifier=quote_mssql_identifier,
    )


@pytest.mark.parametrize("lineage_on", [False, True])
@pytest.mark.parametrize("hash_on", [False, True])
@pytest.mark.parametrize("unique_key", [None, ("n",)])
def test_insert_expressions_match_existing_direct_bcp_update(lineage_on, hash_on, unique_key):
    schema, resolved, lineage = projection(lineage_on=lineage_on, hash_on=hash_on, unique_key=unique_key)
    before = (dict(resolved.types), dict(resolved.nullability), dict(resolved.collations))
    sql = build(schema, resolved, lineage)
    queries = []
    connector = SimpleNamespace(quote_identifier=quote_mssql_identifier, execute_query=queries.append)
    strategy = SimpleNamespace(connector=connector, staging_manager=None, _staging_name=lambda stage: "[prepared]")
    MssqlNativeStagingNormalizer(strategy)._project_authoritative_metadata(
        SimpleNamespace(columns=resolved.ordered_target_names), resolved, lineage
    )
    if queries:
        # The existing direct BCP path continues to project metadata by UPDATE.
        assignments = queries[0].removeprefix("UPDATE n SET ").split(" FROM [prepared] AS n")[0]
        # Split on assignment boundaries only; canonical expressions contain commas.
        import re

        parts = re.split(r", (?=\[__dpone__\w+\] = )", assignments)
        for assignment in parts:
            name, value = assignment.split(" = ", 1)
            assert f"{value.replace('n.[', 'r.[')} AS {name}" in sql
    else:
        assert not lineage_on and not hash_on
    assert "CONVERT(datetime2(7), NULL) AS [__dpone__deleted_at]" in sql
    for column in resolved.generated_columns:
        assert f"r.{quote_mssql_identifier(column.name)}" not in sql
    assert before == (resolved.types, resolved.nullability, resolved.collations)
    assert sql.startswith(
        "INSERT INTO [db].[stage].[prepared] ("
        + ", ".join(map(quote_mssql_identifier, resolved.ordered_target_names))
        + ") SELECT "
    )
    assert "UPDATE" not in sql and "SYSUTCDATETIME" not in sql


def test_lifecycle_identity_literals_and_generated_nulls_are_authoritative():
    started = datetime(2026, 1, 1, 3, 4, 5, 123456, tzinfo=timezone(timedelta(hours=3)))
    schema, resolved, lineage = projection(lineage_on=True, hash_on=True, started=started)
    sql = build(schema, resolved, lineage)
    assert "N'run''雪' AS [__dpone__run_id]" in sql
    assert "N'load''雪' AS [__dpone__load_id]" in sql
    literal = "CONVERT(datetime2(7), '2026-01-01T00:04:05.123456', 126)"
    assert f"{literal} AS [__dpone__loaded_at]" in sql
    assert f"{literal} AS [__dpone__extracted_at]" in sql
    for name in ("parent_row_id", "list_index", "op", "meta"):
        assert f"NULL AS [__dpone__{name}]" in sql
    assert "N'v1|clickhouse|source''雪|table''😀;'" in sql
    assert "HASHBYTES('SHA2_256'" in sql


def test_wire_row_hash_is_recomputed_even_if_source_supplies_forged_metadata():
    schema, resolved, lineage = projection(lineage_on=False, hash_on=False)
    resolved.types["__dpone__row_hash"] = "varchar(64)"
    resolved.wire_to_target["__dpone__row_hash"] = "__dpone__row_hash"
    sql = build(schema, resolved, lineage)
    assert "AS [__dpone__row_hash]" in sql
    assert "r.[__dpone__row_hash]" not in sql
    assert "HASHBYTES('SHA2_256'" in sql
