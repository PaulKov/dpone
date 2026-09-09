from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from dpone.runtime.connectors.clickhouse_partitioning import ClickHousePartitionService
from dpone.runtime.connectors.clickhouse_query_ops import ClickHouseQueryOps
from dpone.runtime.sinks.strategies.bigquery.partition_validation_service import BigQueryPartitionValidationService
from dpone.runtime.support.data_type_mapper import (
    CanonicalType,
    _BigQueryDialect,
    _ClickHouseTypeParser,
    _PgTypeParser,
    _PythonTypeParser,
)


class FakeLogger:
    def __init__(self) -> None:
        self.progress: list[tuple[str, dict]] = []
        self.errors: list[tuple[str, dict]] = []
        self.validations: list[dict] = []

    def log_etl_progress(self, event: str, payload: dict) -> None:
        self.progress.append((event, payload))

    def log_etl_error(self, message: str, payload: dict) -> None:
        self.errors.append((message, payload))

    def _validate_single_partition(self, **kwargs) -> bool:
        self.validations.append(kwargs)
        return kwargs["target_count"] >= 0


class FakeClickHouseConnector:
    __class_name_marker__ = "ClickHouseConnector"

    def __init__(self, rows: list[dict] | None = None, counts: dict[str, int] | None = None) -> None:
        self.logger = FakeLogger()
        self.rows = rows or []
        self.counts = counts or {}
        self.queries: list[tuple[str, dict | None]] = []

    @property
    def __class__(self):  # type: ignore[override]
        return type("ClickHouseConnector", (), {})

    def get_records(self, query: str, params: dict | None = None, as_dict: bool = False):
        self.queries.append((query, params))
        return self.rows

    def count_rows_by_date(
        self,
        *,
        schema: str,
        table: str,
        date_column: str,
        partition_date: str,
        partition_by: str,
    ) -> int:
        return self.counts.get(partition_date, 0)


def test_data_type_parsers_map_postgres_clickhouse_and_python_values_to_canonical_types() -> None:
    pg = _PgTypeParser()
    clickhouse = _ClickHouseTypeParser()
    python = _PythonTypeParser()

    assert pg.parse("numeric(10,2)").base is CanonicalType.NUMERIC
    assert pg.parse("varchar(255)").base is CanonicalType.STRING
    assert pg.parse("int[]").base is CanonicalType.ARRAY
    assert pg.parse("int[]").element.base is CanonicalType.INTEGER
    assert pg.parse("mystery").base is CanonicalType.UNKNOWN

    assert clickhouse.parse("Nullable(LowCardinality(String))").base is CanonicalType.STRING
    assert clickhouse.parse("Array(UInt64)").base is CanonicalType.ARRAY
    assert clickhouse.parse("Array(UInt64)").element.base is CanonicalType.INTEGER
    assert clickhouse.parse("FixedString(16)").base is CanonicalType.BYTES
    assert clickhouse.parse("DateTime64(3, 'UTC')").base is CanonicalType.TIMESTAMP

    assert _BigQueryDialect.to_type(clickhouse.parse("Array(String)")) == "RECORD<list ARRAY<STRUCT<item STRING>>>"
    assert _BigQueryDialect.to_type(clickhouse.parse("FixedString(16)")) == "BYTES"

    assert python.parse_value(True).base is CanonicalType.BOOLEAN
    assert python.parse_value(1).base is CanonicalType.INTEGER
    assert python.parse_value("2024-01-02T03:04:05Z").base is CanonicalType.TIMESTAMP
    assert python.parse_value("Sun, 07 Nov 2021 04:49:18 +0300").base is CanonicalType.TIMESTAMP
    assert python.parse_value({"a": 1}).base is CanonicalType.JSON
    assert python.parse_value([1, 2]).base is CanonicalType.ARRAY

    parsed = python.parse_timestamp("Sun, 07 Nov 2021 04:49:18 +0300")
    assert parsed == datetime(2021, 11, 7, 4, 49, 18)
    assert python.parse_timestamp("not-a-date") is None


def test_clickhouse_partition_service_generates_day_month_and_year_predicates() -> None:
    service = ClickHousePartitionService(FakeClickHouseConnector())

    assert service.generate_date_partitions(
        "2024-01-01",
        "2024-01-03",
        "day",
        "event_date",
        current_date=date(2024, 1, 3),
        last_value="2024-01-03 12:00:00",
    ) == [
        ("2024-01-01", "toDate(`event_date`) = '2024-01-01'"),
        ("2024-01-02", "toDate(`event_date`) = '2024-01-02'"),
        ("2024-01-03", "toDate(`event_date`) = '2024-01-03' AND `event_date` > '2024-01-03 12:00:00'"),
    ]
    assert service.generate_date_partitions(date(2024, 1, 15), date(2024, 3, 1), "month", "event_date") == [
        ("2024-01-01", "toYYYYMM(`event_date`) = 202401"),
        ("2024-02-01", "toYYYYMM(`event_date`) = 202402"),
        ("2024-03-01", "toYYYYMM(`event_date`) = 202403"),
    ]
    assert service.generate_date_partitions(datetime(2023, 5, 1), datetime(2024, 1, 1), "year", "event_date") == [
        ("2023-01-01", "toYear(`event_date`) = 2023"),
        ("2024-01-01", "toYear(`event_date`) = 2024"),
    ]


def test_clickhouse_partition_service_discovers_nonempty_partitions_and_counts_rows() -> None:
    connector = FakeClickHouseConnector(
        rows=[
            {"partition_date": "2024-01-01"},
            {"partition_date": datetime(2024, 2, 2, 3, 4)},
            {"partition_date": date(2024, 3, 3)},
        ]
    )
    service = ClickHousePartitionService(connector)

    assert service.discover_nonempty_partitions(
        "raw",
        "events",
        "event_date",
        "2024-01-01",
        "2024-03-31",
        "month",
    ) == {"2024-01-01", "2024-02-01", "2024-03-01"}
    assert connector.logger.progress[0][0] == "CH_PARTITION_DISCOVERY"
    assert connector.logger.progress[-1][0] == "CH_PARTITION_DISCOVERY_COMPLETE"

    connector.rows = [{"cnt": 42}]
    assert service.count_rows_by_date("raw", "events", "event_date", "2024-01-01") == 42
    connector.rows = []
    assert service.count_rows_by_date("raw", "events", "event_date", "2024-01-01") == 0


def test_clickhouse_query_ops_coerces_csv_values_and_streams_batches(tmp_path: Path) -> None:
    executed: list[tuple[str, object]] = []

    class FakeConnection:
        def execute(self, query, params=None, with_column_types=False, settings=None):
            del settings
            if "system.columns" in str(query):
                return [
                    ("id", "UInt64"),
                    ("amount", "Nullable(Decimal(10,2))"),
                    ("ratio", "Float64"),
                    ("name", "LowCardinality(String)"),
                ]
            if with_column_types:
                return ([(1, "alice"), (2, "bob")], [("id", "UInt64"), ("name", "String")])
            copied_params = list(params) if isinstance(params, list) else params
            executed.append((query, copied_params))
            return [(99,)]

        def execute_iter(self, query, params=None, settings=None):
            del settings
            yield (1, "alice")
            yield (2, "bob")
            yield (3, "carol")

    connector = SimpleNamespace(
        logger=FakeLogger(),
        database="default",
        connection=FakeConnection(),
        print_query=lambda query, params=None: None,
    )
    ops = ClickHouseQueryOps(connector)
    csv_file = tmp_path / "rows.csv"
    csv_file.write_text("id,amount,ratio,name\n1,10.50,0.25,Alice\n2,,1.5,Bob\n", encoding="utf-8")

    assert ops.get_records("SELECT 1", as_dict=True) == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    assert ops.query_max_column("raw.events", "id") == 99
    assert list(ops.get_records_streaming("SELECT id, name FROM raw.events", batch_size=2, as_dict=False)) == [
        [(1, "alice"), (2, "bob")],
        [(3, "carol")],
    ]
    assert ClickHouseQueryOps._unwrap_clickhouse_type("Nullable(LowCardinality(String))") == ("String", True)
    assert ops._coerce_csv_value("10.50", "Nullable(Decimal(10,2))") == Decimal("10.50")
    assert ops._coerce_csv_value("", "Nullable(Int64)") is None

    ops.import_from_csv("raw", "events", str(csv_file), batch_rows=1)

    insert_batches = [params for query, params in executed if str(query).startswith("INSERT INTO raw.events")]
    assert insert_batches == [
        [(1, Decimal("10.50"), 0.25, "Alice")],
        [(2, None, 1.5, "Bob")],
    ]
    assert ops.build_select_query("raw", "events", ["id", "name"], limit=10, offset=20) == (
        "SELECT `id`, `name` FROM `raw`.`events` LIMIT 10 OFFSET 20"
    )


def test_bigquery_partition_validation_uses_artifact_rows_or_estimates_from_clickhouse_counts() -> None:
    logger = FakeLogger()
    service = BigQueryPartitionValidationService(logger)
    source_connector = FakeClickHouseConnector(counts={"2024-01-01": 10, "2024-01-02": 30})
    load_config = SimpleNamespace(
        source_schema="raw",
        source_table="events",
        options={
            "_source_connector": source_connector,
            "date_column": "event_date",
            "partition_by": "day",
        },
    )
    payload = SimpleNamespace(
        artifact=SimpleNamespace(
            new_partitions=["2024-01-01", "2024-01-02"],
            lookback_partitions=[],
            _partition_rows={},
        )
    )

    result = service.validate_clickhouse(load_config, payload, inserted=80)

    assert result == {
        "2024-01-01": {"target_count": 20, "match": True},
        "2024-01-02": {"target_count": 60, "match": True},
    }
    assert [item["target_count"] for item in logger.validations] == [20, 60]

    payload.artifact._partition_rows = {"2024-01-01": 7}
    payload.artifact.new_partitions = ["2024-01-01"]
    assert service.validate_clickhouse(load_config, payload, inserted=80) == {
        "2024-01-01": {"target_count": 7, "match": True}
    }

    load_config.options["_source_connector"] = object()
    assert service.validate_clickhouse(load_config, payload, inserted=80) == {}
