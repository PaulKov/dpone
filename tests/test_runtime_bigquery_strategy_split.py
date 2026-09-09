from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

from dpone.runtime.sinks.strategies.bigquery.bigquery_base import BigQueryStrategyBase
from dpone.runtime.sinks.strategies.bigquery.dml_helper import BigQueryDmlHelper
from dpone.runtime.sinks.strategies.bigquery.exchange_logger import BigQueryExchangeLogger
from dpone.runtime.sinks.strategies.bigquery.partition_validation_service import BigQueryPartitionValidationService
from dpone.runtime.sinks.strategies.bigquery.target_table_manager import BigQueryTargetTableManager
from dpone.runtime.sinks.strategies.progress import ProgressEvent, StrategyProgressLogger


class StubLogger:
    def __init__(self):
        self.progress = []
        self.samples = []
        self.messages = []
        self.partition_checks = []

    def log_etl_progress(self, event, payload):
        self.progress.append((event, payload))

    def log_data_sample(self, kind, rows, max_rows):
        self.samples.append((kind, rows, max_rows))

    def log_etl_error(self, message, payload):
        self.messages.append(("error", message, payload))

    def warning(self, message):
        self.messages.append(("warning", message))

    def info(self, message):
        self.messages.append(("info", message))

    def _validate_single_partition(self, **kwargs):
        self.partition_checks.append(kwargs)
        return True


class StubJob:
    def __init__(self, inserted=0, updated=0, deleted=0):
        self._properties = {
            "statistics": {
                "query": {
                    "dmlStats": {
                        "insertedRowCount": inserted,
                        "updatedRowCount": updated,
                        "deletedRowCount": deleted,
                    }
                }
            }
        }

    def result(self):
        return None


class StubConnection:
    def __init__(self, inserted=0, updated=0, deleted=0):
        self.job = StubJob(inserted=inserted, updated=updated, deleted=deleted)
        self.queries = []

    def query(self, query, job_config=None):
        self.queries.append((query, job_config))
        return self.job


@dataclass
class StubConnector:
    project_id: str = "demo-proj"
    connection: StubConnection = field(default_factory=StubConnection)
    records: list = field(default_factory=list)
    executed: list = field(default_factory=list)

    def _build_query_config(self, params=None):
        return {"params": params}

    def execute_query(self, query):
        self.executed.append(query)
        return 0

    def get_records(self, query):
        self.executed.append(("get_records", query))
        return list(self.records)


class ClickHouseConnector:
    def __init__(self, counts):
        self.counts = counts

    def count_rows_by_date(self, schema, table, date_column, partition_date, partition_by):
        del schema, table, date_column, partition_by
        return self.counts.get(partition_date, 0)


class DummyStrategy(BigQueryStrategyBase):
    def load(self, load_config, payload):
        raise NotImplementedError


def test_bigquery_base_import_and_unique_key_condition_without_google_deps() -> None:
    strategy = DummyStrategy(connector=StubConnector(), logger=StubLogger())

    assert strategy._build_unique_key_condition("s", "t", ["id", "dt"]) == "s.`id` = t.`id` AND s.`dt` = t.`dt`"


def test_dml_helper_executes_and_counts_dml_rows() -> None:
    connector = StubConnector(connection=StubConnection(inserted=2, updated=3, deleted=4))
    helper = BigQueryDmlHelper(
        connector=connector,
        logger=StubLogger(),
        include_technical_columns=lambda _cfg: True,
        get_fq_table=lambda schema, table: f"`demo.{schema}.{table}`",
    )

    affected = helper.execute_dml_query("DELETE FROM t WHERE TRUE")

    assert affected == 9
    assert connector.connection.queries[0][0] == "DELETE FROM t WHERE TRUE"


def test_target_table_manager_create_table_from_staging_isolated_from_google_specific_bits() -> None:
    connector = StubConnector(records=[{"row_count": 7}])
    manager = BigQueryTargetTableManager(
        connector=connector,
        logger=StubLogger(),
        include_technical_columns=lambda _cfg: True,
    )
    ensured = []
    manager.ensure_technical_columns = lambda cfg: ensured.append(cfg.target_table)  # type: ignore[method-assign]
    cfg = SimpleNamespace(target_schema="landing", target_table="orders", staging_schema="stg")
    payload = SimpleNamespace(schema=[("id", "int64"), ("attrs", "json")])

    inserted = manager.create_table_from_staging(
        cfg,
        "orders__tmp",
        payload,
        build_select_with_json_parse=lambda schema, include_technical_columns=True, **_kwargs: (
            ", ".join(f"`{col}`" for col, _ in schema),
            ", ".join(f"`{col}`" for col, _ in schema),
        ),
    )

    assert inserted == 7
    assert ensured == ["orders__tmp"]
    assert connector.executed[0].startswith("\n        CREATE TABLE `demo-proj.landing.orders__tmp` AS")


def test_partition_validation_service_uses_partition_rows_when_present() -> None:
    logger = StubLogger()
    service = BigQueryPartitionValidationService(logger)
    source_connector = ClickHouseConnector({"2026-03-01": 10})
    cfg = SimpleNamespace(
        options={"_source_connector": source_connector, "date_column": "created_at", "partition_by": "day"},
        source_schema="public",
        source_table="events",
    )
    payload = SimpleNamespace(
        artifact=SimpleNamespace(new_partitions=["2026-03-01"], _partition_rows={"2026-03-01": 11})
    )

    result = service.validate_clickhouse(cfg, payload, inserted=99)

    assert result == {"2026-03-01": {"target_count": 11, "match": True}}
    assert logger.partition_checks[0]["target_count"] == 11


def test_exchange_logger_records_structured_events() -> None:
    logger = StubLogger()
    exchange_logger = BigQueryExchangeLogger(logger)

    exchange_logger.log_exchange_start("`demo.landing.orders`")
    exchange_logger.log_exchange_complete("`demo.landing.orders`", 12)

    assert logger.progress == [
        (
            "BQ_EXCHANGE_START",
            {"Target": "`demo.landing.orders`", "Mode": "Exchange Pattern (атомарная замена)"},
        ),
        (
            "BQ_EXCHANGE_COMPLETE",
            {"Target": "`demo.landing.orders`", "Inserted": 12, "Mode": "Exchange Pattern"},
        ),
    ]


def test_strategy_progress_logger_standardizes_event_transport() -> None:
    logger = StubLogger()
    progress = StrategyProgressLogger(logger)

    progress.emit(ProgressEvent(code="SINK_STAGE", payload={"Rows": 10}))
    progress.emit("SINK_DONE", {"Rows": 20})

    assert logger.progress == [
        ("SINK_STAGE", {"Rows": 10}),
        ("SINK_DONE", {"Rows": 20}),
    ]
