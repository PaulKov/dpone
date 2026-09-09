from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.schema_evolution import SchemaEvolutionService
from dpone.runtime.sinks.clickhouse_nullability_policy import ClickHouseNullInsertPolicy
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.type_system.source_sink.provenance import SourceColumnProvenance


def _load_config(options: dict) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options,
    )


def _defaulting_options() -> dict:
    return {
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
    }


def test_clickhouse_null_insert_policy_emits_clickhouse_native_defaulting_settings() -> None:
    policy = ClickHouseNullInsertPolicy.from_options(_defaulting_options())

    assert policy.merge_format_settings({}) == {"input_format_null_as_default": 1}
    assert policy.driver_settings() == {"input_format_null_as_default": True}
    assert policy.insert_select_settings_clause() == " SETTINGS insert_null_as_default=1"


def test_clickhouse_null_insert_policy_keeps_fail_fast_without_defaulting_settings() -> None:
    policy = ClickHouseNullInsertPolicy.from_options(
        {
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "nullability": {
                            "mode": "non_nullable_by_default",
                            "null_handling": "fail_fast",
                        }
                    }
                }
            }
        }
    )

    assert policy.merge_format_settings({"max_threads": 4}) == {"max_threads": 4}
    assert policy.driver_settings() == {}
    assert policy.insert_select_settings_clause() == ""


def test_clickhouse_null_insert_policy_rejects_conflicting_direct_settings() -> None:
    policy = ClickHouseNullInsertPolicy.from_options(_defaulting_options())

    with pytest.raises(ValueError, match="input_format_null_as_default"):
        policy.merge_format_settings({"input_format_null_as_default": 0})


def test_clickhouse_null_insert_policy_validates_per_column_fail_fast_before_defaulting() -> None:
    policy = ClickHouseNullInsertPolicy.from_options(
        {
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "nullability": {
                            "mode": "non_nullable_by_default",
                            "null_handling": "default",
                            "columns": {
                                "amount": {"null_handling": "fail_fast"},
                            },
                        }
                    }
                }
            }
        }
    )

    with pytest.raises(ValueError, match="amount"):
        policy.validate_rows(["id", "amount"], [(1, None)])


def test_clickhouse_row_insert_passes_driver_defaulting_setting() -> None:
    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[tuple[object, ...]], dict]] = []

        def execute(self, query: str, rows: list[tuple[object, ...]], **kwargs) -> None:
            self.calls.append((query, rows, kwargs))

    class Sink:
        def __init__(self) -> None:
            self.connector = SimpleNamespace(connection=Connection())

        def _table(self, load_config: LoadConfig) -> str:
            return f"`{load_config.target_schema}`.`{load_config.target_table}`"

    sink = Sink()
    service = ClickHousePayloadIngestionService(sink, sink_factory=lambda connector: Sink())

    inserted = service.execute_insert(_load_config(_defaulting_options()), ["amount"], [(None,)])

    assert inserted == 1
    assert sink.connector.connection.calls[0][2]["settings"] == {"input_format_null_as_default": True}


def test_clickhouse_insert_select_adds_insert_null_as_default_setting() -> None:
    class Connector:
        host = "127.0.0.1"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        secure = False

        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> int:
            self.queries.append(query)
            return 0

        def get_records(self, query: str) -> list[tuple[int]]:
            self.queries.append(query)
            return [(1,)]

    connector = Connector()
    sink = ClickHouseSink(connector)
    target = _load_config(_defaulting_options())
    source = replace(_load_config(_defaulting_options()), target_table="orders_staging")

    inserted = sink._insert_from_table(source, target)

    assert inserted == 1
    assert "SETTINGS insert_null_as_default=1 SELECT" in connector.queries[0]


def test_clickhouse_bulk_runner_receives_null_defaulting_insert_setting(tmp_path) -> None:
    class Connector:
        host = "127.0.0.1"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        secure = False

        def execute_query(self, query: str) -> int:
            del query
            return 0

        def get_records(self, query: str) -> list[tuple[int]]:
            del query
            return [(1,)]

    class Runner:
        calls: list[object] = []

        def __init__(self, credentials, options) -> None:
            del credentials
            self.options = options

        def insert_file(self, table: str, columns: list[str], input_path: str) -> None:
            del table, columns, input_path
            self.calls.append(self.options)

    file_path = tmp_path / "orders.tsv"
    file_path.write_text("1\t\\N\n", encoding="utf-8")
    artifact = FileExportArtifact(str(file_path), ["id", "amount"], format="mssql-delimited", estimated_rows=1)
    options = _defaulting_options()
    options["clickhouse_bulk"] = {"mode": "client"}

    result = ClickHouseSink(Connector(), client_runner_cls=Runner).load(
        _load_config(options),
        LoadPayload(artifact=artifact, schema=[("id", "int"), ("amount", "int nullable")]),
    )

    assert result.inserted_rows == 1
    assert Runner.calls[0].settings["input_format_null_as_default"] == 1


def test_schema_evolution_compares_exact_clickhouse_physical_nullability() -> None:
    class Connector:
        host = "127.0.0.1"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        secure = False

        def __init__(self) -> None:
            self.mutations: list[str] = []

        def get_records(self, query: str) -> list[tuple[object, ...]]:
            if query.startswith("EXISTS TABLE"):
                return [(1,)]
            if "FROM system.columns" in query:
                return [("id", "Int32"), ("business_date", "Date")]
            raise AssertionError(query)

        def execute_query(self, query: str) -> int:
            self.mutations.append(query)
            return 0

    connector = Connector()
    options = {
        **_defaulting_options(),
        "source_type": "mssql",
        "sink_type": "clickhouse",
    }
    payload = LoadPayload(
        artifact=SimpleNamespace(),
        schema=(("id", "int"), ("business_date", "date")),
        relation_metadata=(
            SourceColumnProvenance(name="id", declared_type="int", nullable=True),
            SourceColumnProvenance(name="business_date", declared_type="date", nullable=True),
        ),
    )

    prepared = SchemaEvolutionService().prepare_payload(
        _load_config(options),
        ClickHouseSink(connector),
        payload,
    )

    assert prepared is payload
    assert connector.mutations == []
