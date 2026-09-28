"""Schema evolution must not remap types already projected by the sink."""

import pytest

from dpone.readiness.schema_evolution_ddl import add_column_sql, alter_column_sql


@pytest.mark.parametrize("render", [add_column_sql, alter_column_sql])
@pytest.mark.parametrize(
    "dtype",
    ["Nullable(Date)", "Nullable(String)", "UInt64", "Int8", "Decimal(18, 4)", "DateTime64(3, 'UTC')"],
)
def test_preserves_projected_physical_type(render, dtype: str) -> None:
    sql = render("clickhouse", "`target_fact`", "event_value", dtype, nullable=dtype.startswith("Nullable("))
    assert sql.endswith("`event_value` " + dtype)


@pytest.mark.parametrize("dtype", ["String; DROP TABLE example", "Nullable(String)--", "Date /*comment*/"])
def test_rejects_statement_tokens(dtype: str) -> None:
    with pytest.raises(ValueError, match="Unsafe ClickHouse"):
        alter_column_sql("clickhouse", "`target_fact`", "event_value", dtype, nullable=True)
