from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig
from dpone.runtime.sinks.clickhouse_sql_mixin import ClickHouseSqlMixin
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypeMapper

MSSQL_CLICKHOUSE_NULLABILITY_TYPES: tuple[str, ...] = (
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "decimal(18,4)",
    "numeric(38,9)",
    "money",
    "smallmoney",
    "float",
    "real",
    "bit",
    "uniqueidentifier",
    "date",
    "datetime",
    "datetime2(7)",
    "smalldatetime",
    "datetimeoffset(7)",
    "time(7)",
    "nvarchar(510)",
    "varchar(max)",
    "varbinary(max)",
    "rowversion",
    "timestamp",
)


def _mssql_type_with_nullability(source_type: str, *, nullable: bool) -> str:
    return f"{source_type} nullable" if nullable else source_type


def _clickhouse_type_with_nullability(clickhouse_type: str, *, nullable: bool) -> str:
    if nullable and not clickhouse_type.startswith("Nullable("):
        return f"Nullable({clickhouse_type})"
    if not nullable and clickhouse_type.startswith("Nullable(") and clickhouse_type.endswith(")"):
        return clickhouse_type[len("Nullable(") : -1]
    return clickhouse_type


def _clickhouse_type_is_nullable(clickhouse_type: str) -> bool:
    return clickhouse_type.startswith("Nullable(")


def test_mssql_clickhouse_type_decision_exposes_generic_target_type_alias() -> None:
    decision = MssqlClickHouseTypeMapper().resolve_column("value", "int nullable")

    assert decision.target_type == decision.clickhouse_type


@pytest.mark.parametrize("source_type", MSSQL_CLICKHOUSE_NULLABILITY_TYPES)
@pytest.mark.parametrize("source_nullable", [False, True])
def test_mssql_clickhouse_auto_schema_inference_preserves_source_nullability(
    source_type: str,
    source_nullable: bool,
) -> None:
    mapper = MssqlClickHouseTypeMapper()
    base_type = mapper.resolve_column("value", source_type).clickhouse_type

    decision = mapper.resolve_column(
        "value",
        _mssql_type_with_nullability(source_type, nullable=source_nullable),
    )

    assert decision.clickhouse_type == _clickhouse_type_with_nullability(base_type, nullable=source_nullable)
    assert _clickhouse_type_is_nullable(decision.clickhouse_type) is source_nullable


@pytest.mark.parametrize("source_type", MSSQL_CLICKHOUSE_NULLABILITY_TYPES)
@pytest.mark.parametrize("source_nullable", [False, True])
@pytest.mark.parametrize("target_nullable", [False, True])
def test_clickhouse_runtime_physical_contract_can_force_target_nullability_for_mssql_types(
    source_type: str,
    source_nullable: bool,
    target_nullable: bool,
) -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class Sink(ClickHouseSqlMixin):
        def __init__(self) -> None:
            self.connector = FakeConnector()
            self.logger = SimpleNamespace()

    base_type = MssqlClickHouseTypeMapper().resolve_column("value", source_type).clickhouse_type
    target_type = _clickhouse_type_with_nullability(base_type, nullable=target_nullable)
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "physical_design": {
                "columns": {
                    "value": {
                        "target_type": {
                            "clickhouse": target_type,
                        }
                    }
                }
            }
        },
    )
    sink = Sink()

    sink._create_table(
        load_config,
        [("value", _mssql_type_with_nullability(source_type, nullable=source_nullable))],
        if_not_exists=True,
    )

    assert f"`value` {target_type}" in sink.connector.queries[-1]


@pytest.mark.parametrize("source_type", MSSQL_CLICKHOUSE_NULLABILITY_TYPES)
def test_clickhouse_runtime_nullability_policy_can_infer_non_nullable_targets(source_type: str) -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class Sink(ClickHouseSqlMixin):
        def __init__(self) -> None:
            self.connector = FakeConnector()
            self.logger = SimpleNamespace()

    base_type = MssqlClickHouseTypeMapper().resolve_column("value", source_type).clickhouse_type
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "nullability": {
                            "mode": "non_nullable_by_default",
                            "null_handling": "default",
                        }
                    }
                }
            }
        },
    )
    sink = Sink()

    sink._create_table(
        load_config,
        [("value", _mssql_type_with_nullability(source_type, nullable=True))],
        if_not_exists=True,
    )

    assert f"`value` {base_type}" in sink.connector.queries[-1]
    assert "Nullable(" not in sink.connector.queries[-1]


def test_clickhouse_runtime_nullability_policy_supports_per_column_preserve_override() -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class Sink(ClickHouseSqlMixin):
        def __init__(self) -> None:
            self.connector = FakeConnector()
            self.logger = SimpleNamespace()

    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "nullability": {
                            "mode": "non_nullable_by_default",
                            "columns": {
                                "comment": {"mode": "preserve_source"},
                            },
                        }
                    }
                }
            }
        },
    )
    sink = Sink()

    sink._create_table(
        load_config,
        [("amount", "int nullable"), ("comment", "nvarchar(510) nullable")],
        if_not_exists=True,
    )

    assert "`amount` Int32" in sink.connector.queries[-1]
    assert "`comment` Nullable(String)" in sink.connector.queries[-1]


def test_clickhouse_runtime_explicit_target_type_wins_over_nullability_policy() -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class Sink(ClickHouseSqlMixin):
        def __init__(self) -> None:
            self.connector = FakeConnector()
            self.logger = SimpleNamespace()

    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "physical_design": {
                "columns": {
                    "value": {
                        "target_type": {
                            "clickhouse": "Nullable(Decimal(18,4))",
                        }
                    }
                },
                "storage": {
                    "clickhouse": {
                        "nullability": {
                            "mode": "non_nullable_by_default",
                        }
                    }
                },
            }
        },
    )
    sink = Sink()

    sink._create_table(
        load_config,
        [("value", "decimal(18,4) nullable")],
        if_not_exists=True,
    )

    assert "`value` Nullable(Decimal(18,4))" in sink.connector.queries[-1]
