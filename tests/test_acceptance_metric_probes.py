from __future__ import annotations

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.governance.acceptance_metrics import (
    AcceptanceMetricConfigurationError,
    AcceptanceMetricRequest,
)
from dpone.runtime.governance.clickhouse_acceptance_metrics import ClickHouseAcceptanceMetricProbe
from dpone.runtime.governance.mssql_acceptance_metrics import MssqlAcceptanceMetricProbe
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.clickhouse import ClickHouseSource


def test_acceptance_metric_request_preserves_legacy_positional_field_order() -> None:
    request = AcceptanceMetricRequest(
        "target",
        ("id",),
        "landing.orders",
        "landing",
        "orders",
        None,
        None,
        True,
        ("id",),
        ("id",),
    )

    assert request.schema == "landing"
    assert request.table == "orders"
    assert request.include_row_count is True
    assert request.null_count_columns == ("id",)
    assert request.distinct_count_columns == ("id",)
    assert request.database is None


def test_clickhouse_acceptance_metric_probe_collects_row_null_and_distinct_counts() -> None:
    connector = _Connector()
    probe = ClickHouseAcceptanceMetricProbe(connector)

    snapshot = probe.collect(
        AcceptanceMetricRequest(
            side="source",
            columns=("id", "name"),
            dataset_identity="DWH_Tech.work",
            schema="DWH_Tech",
            table="work",
            include_row_count=True,
            null_count_columns=("id", "name"),
            distinct_count_columns=("id", "name"),
        )
    )

    assert snapshot.row_count == 3
    assert snapshot.null_counts == {"id": 0, "name": 1}
    assert snapshot.distinct_counts == {"id": 3, "name": 2}
    assert snapshot.dataset == "DWH_Tech.work"
    query = connector.queries[0]
    assert "count() AS `row_count`" in query
    assert "countIf(isNull(`name`)) AS `null__name`" in query
    assert "uniqExact(`id`) AS `distinct__id`" in query
    assert "FROM `DWH_Tech`.`work`" in query


def test_clickhouse_acceptance_metric_probe_uses_query_without_leaking_sql_in_snapshot() -> None:
    connector = _Connector()
    probe = ClickHouseAcceptanceMetricProbe(connector)

    snapshot = probe.collect(
        AcceptanceMetricRequest(
            side="source",
            columns=("id",),
            dataset_identity="source_query:abc",
            sql="SELECT id FROM secret_table",
            sql_hash="abc",
            include_row_count=True,
            null_count_columns=("id",),
            distinct_count_columns=(),
        )
    )

    assert "secret_table" in connector.queries[0]
    assert snapshot.dataset == "source_query:abc"
    assert "secret_table" not in str(snapshot.to_jsonable())


def test_clickhouse_source_and_sink_expose_acceptance_metric_probe() -> None:
    connector = _Connector()

    source = ClickHouseSource(connector, logger=_Logger())
    sink = ClickHouseSink(connector, logger=_Logger())

    assert isinstance(source.acceptance_metric_probe, ClickHouseAcceptanceMetricProbe)
    assert isinstance(sink.acceptance_metric_probe, ClickHouseAcceptanceMetricProbe)


def test_mssql_acceptance_metric_probe_collects_target_metrics_from_explicit_database() -> None:
    connector = _MssqlConnector()
    probe = MssqlAcceptanceMetricProbe(connector)

    snapshot = probe.collect(
        AcceptanceMetricRequest(
            side="target",
            columns=("id", "name"),
            dataset_identity="DWH_Dev.ch.marketing__web",
            database="DWH_Dev",
            schema="ch",
            table="marketing__web",
            include_row_count=True,
            null_count_columns=("id", "name"),
            distinct_count_columns=("id", "name"),
        )
    )

    assert snapshot.row_count == 3
    assert snapshot.null_counts == {"id": 0, "name": 1}
    assert snapshot.distinct_counts == {"id": 3, "name": 2}
    assert snapshot.dataset == "DWH_Dev.ch.marketing__web"
    query = connector.queries[0]
    assert "COUNT_BIG(*) AS [row_count]" in query
    assert "COALESCE(SUM(CASE WHEN [name] IS NULL" in query
    assert "COUNT_BIG(DISTINCT [id]) AS [distinct__0]" in query
    assert "FROM [DWH_Dev].[ch].[marketing__web]" in query
    assert "dwh_example" not in query


def test_mssql_sink_exposes_acceptance_metric_probe() -> None:
    connector = _MssqlConnector()

    sink = MSSQLSink(connector, logger=_Logger())

    assert isinstance(sink.acceptance_metric_probe, MssqlAcceptanceMetricProbe)


@pytest.mark.parametrize("database", ("DWH_Dev", "dwh_example"))
def test_mssql_acceptance_normalizes_compact_database_schema(database: str) -> None:
    connector = _MssqlConnector()
    probe = MssqlAcceptanceMetricProbe(connector)

    probe.collect(
        AcceptanceMetricRequest(
            side="target",
            columns=("id",),
            dataset_identity=f"{database}.ch.marketing__web",
            database=database,
            schema=f"{database}.ch",
            table="marketing__web",
        )
    )

    assert f"FROM [{database}].[ch].[marketing__web]" in connector.queries[0]


def test_mssql_acceptance_rejects_missing_or_mismatched_database_authority() -> None:
    probe = MssqlAcceptanceMetricProbe(_MssqlConnector())

    with pytest.raises(ValueError, match="target_acceptance_metric_database_required"):
        probe.collect(
            AcceptanceMetricRequest(
                side="target",
                columns=("id",),
                dataset_identity="ch.marketing__web",
                schema="ch",
                table="marketing__web",
            )
        )
    with pytest.raises(ValueError, match="Unsafe MSSQL schema identifier"):
        probe.collect(
            AcceptanceMetricRequest(
                side="target",
                columns=("id",),
                dataset_identity="DWH_Dev.ch.marketing__web",
                database="DWH_Dev",
                schema="dwh_example.ch",
                table="marketing__web",
            )
        )


def test_mssql_sink_rejects_target_acceptance_without_database_before_other_preflight() -> None:
    sink = MSSQLSink(_MssqlConnector(), logger=_Logger())
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="marketing",
        source_table="web",
        target_schema="ch",
        target_table="marketing__web",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "source_type": "clickhouse",
            "sink_type": "mssql",
            "quality": {
                "acceptance": {
                    "enabled": True,
                    "mode": "required",
                    "capture": {"source": False, "staged": False, "target": True},
                    "checks": {"row_count": True},
                }
            },
        },
    )

    with pytest.raises(AcceptanceMetricConfigurationError) as raised:
        sink.preflight_before_extract(load_config=config)

    assert raised.value.field_path == "quality.acceptance.capture.target"


class _Connector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_records(self, query, params=None, as_dict=False):  # noqa: ANN001
        del params
        self.queries.append(str(query))
        assert as_dict is True
        return [
            {
                "row_count": 3,
                "null__id": 0,
                "null__name": 1,
                "distinct__id": 3,
                "distinct__name": 2,
            }
        ]


class _MssqlConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value.replace(']', ']]')}]"

    @staticmethod
    def qualified_name(schema: str, table: str, *, database: str | None = None) -> str:
        parts = [part for part in (database, schema, table) if part]
        return ".".join(f"[{part}]" for part in parts)

    def get_records(self, query, params=None, as_dict=False):  # noqa: ANN001
        del params
        self.queries.append(str(query))
        assert as_dict is True
        return [
            {
                "row_count": 3,
                "null__0": 0,
                "null__1": 1,
                "distinct__0": 3,
                "distinct__1": 2,
            }
        ]


class _Logger:
    def log_etl_progress(self, *args, **kwargs):  # noqa: ANN002, ANN003
        del args, kwargs
