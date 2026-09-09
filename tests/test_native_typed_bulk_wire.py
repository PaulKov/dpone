from __future__ import annotations

from types import SimpleNamespace

from dpone.config import LoadConfig
from dpone.runtime.bulk_wire import BulkWirePlanner, BulkWirePolicy
from dpone.runtime.connectors.clickhouse_http_bulk import ClickHouseHttpOptions
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.clickhouse_staging_decoder import ClickHouseStagingDecoder
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest


class ClickHouseConnector:
    host = "localhost"
    port = 9000
    database = "default"
    user = "default"
    password = ""
    secure = False
    compression = True
    connect_timeout = 10
    send_receive_timeout = 3600
    settings = {}
    application_name = "test"


class MssqlConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.options: list[object] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
        del output_path
        self.queries.append(query)
        self.options.append(options)
        return 2


class Logger:
    def log_etl_progress(self, *_args, **_kwargs) -> None:
        pass


def _load_config(options: dict) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options=options,
    )


def test_bulk_wire_policy_defaults_to_auto_typed_raw() -> None:
    policy = BulkWirePolicy.from_options({"native_transfer": {"wire": {"mode": "typed_raw"}}})

    assert policy.mode == "typed_raw"
    assert policy.text_safety == "prove_or_transcode"
    assert policy.delimiter_profile == "auto"
    assert policy.null_policy == "sidecar"


def test_http_clickhouse_no_longer_forces_mssql_source_encoded_tsv() -> None:
    factory = MSSQLQueryoutArtifactFactory(MssqlConnector(), Logger(), sink_connector=ClickHouseConnector())

    assert not factory.should_encode_for_clickhouse_direct(
        _load_config(
            {
                "native_transfer": {"wire": {"mode": "typed_raw"}},
                "clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"},
            }
        )
    )


def test_source_encoded_mode_keeps_legacy_clickhouse_tsv_projection() -> None:
    factory = MSSQLQueryoutArtifactFactory(MssqlConnector(), Logger(), sink_connector=ClickHouseConnector())

    assert factory.should_encode_for_clickhouse_direct(
        _load_config(
            {
                "native_transfer": {"wire": {"mode": "source_encoded"}},
                "clickhouse_bulk": {"mode": "http"},
            }
        )
    )


def test_auto_wire_preserves_legacy_source_encoded_until_typed_contract_requested() -> None:
    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int")],
        source_options={"native_transfer": {"wire": {"mode": "auto"}}},
        sink_options={"clickhouse_bulk": {"mode": "http"}},
    )

    assert contract.selected_route == "source_encoded_direct_tsv"


def test_typed_raw_mssql_queryout_does_not_emit_replace_projection(tmp_path) -> None:
    connector = MssqlConnector()
    factory = MSSQLQueryoutArtifactFactory(connector, Logger(), sink_connector=ClickHouseConnector())
    config = _load_config(
        {
            "runtime": {"storage": {"work_dir": str(tmp_path)}},
            "native_transfer": {"wire": {"mode": "typed_raw"}},
            "clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"},
        }
    )

    artifact = factory.artifact_for_query(
        config,
        "SELECT [id], [name] FROM [dbo].[orders]",
        [("id", "int"), ("name", "varchar(100)")],
    )

    assert artifact.format == "mssql-delimited"
    assert "REPLACE(" not in connector.queries[0]
    assert "SELECT [id], [name] FROM [dbo].[orders]" == connector.queries[0]
    assert getattr(artifact, "bulk_wire_contract").selected_route == "typed_raw_direct"


def test_partitioned_typed_raw_queryout_keeps_bulk_wire_contract_per_file(tmp_path) -> None:
    connector = MssqlConnector()
    factory = MSSQLQueryoutArtifactFactory(connector, Logger(), sink_connector=ClickHouseConnector())
    config = _load_config(
        {
            "runtime_storage": {"work_dir": str(tmp_path)},
            "partitioning": {
                "column": "id",
                "bounds": {"lower": 0, "upper": 10},
                "num_partitions": 2,
            },
            "native_transfer": {
                "execution": {"mode": "batch"},
                "wire": {"mode": "typed_raw", "delimiter_profile": "ascii_control"},
            },
            "clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"},
        }
    )

    artifact = factory.artifact_for_query(
        config,
        "SELECT [id], [name] FROM [dbo].[orders]",
        [("id", "int"), ("name", "varchar(100)")],
    )

    assert len(artifact.partitions) == 2
    assert all(
        getattr(partition, "bulk_wire_contract").selected_route == "typed_raw_direct"
        for partition in artifact.partitions
    )
    assert all("REPLACE(" not in query for query in connector.queries)
    assert all("[id] >=" in query for query in connector.queries)
    assert connector.options[0].field_terminator == "\x1f"
    assert connector.options[0].row_terminator == "\x1e\n"


def test_clickhouse_http_runner_uses_custom_separated_contract_options() -> None:
    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int"), ("name", "varchar(100)")],
        source_options={"native_transfer": {"wire": {"mode": "typed_raw", "delimiter_profile": "ascii_control"}}},
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"}},
    )

    options = ClickHouseHttpOptions.from_bulk_wire_contract(contract)

    assert options.input_format == "CustomSeparated"
    assert options.settings["format_custom_field_delimiter"] == "\x1f"
    assert options.settings["format_custom_row_after_delimiter"] == "\x1e"
    assert options.settings["format_custom_row_between_delimiter"] == "\n"
    assert options.settings["format_custom_escaping_rule"] == "CSV"


def test_clickhouse_staging_decoder_reports_bulk_wire_evidence() -> None:
    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int"), ("name", "varchar(100)")],
        source_options={"native_transfer": {"wire": {"mode": "typed_raw"}}},
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"}},
    )
    artifact = SimpleNamespace(bulk_wire_contract=contract)
    payload = LoadPayload(artifact=artifact, schema=[("id", "int"), ("name", "varchar(100)")])

    decoder = ClickHouseStagingDecoder(
        connector=SimpleNamespace(execute_query=lambda _query: None),
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        create_staging_table=lambda cfg, _schema: cfg,
        map_type=lambda dtype, *_args: "Int32" if dtype == "int" else "String",
    )

    evidence = decoder.bulk_wire_evidence(_load_config({}), payload)

    assert evidence["schema_version"] == "dpone.native_transfer.bulk_wire.v1"
    assert evidence["selected_route"] == "typed_raw_direct"
    assert evidence["mssql_source_escaping"] is False


def test_clickhouse_bulk_load_uses_contract_input_format(tmp_path) -> None:
    class FakeRunner:
        calls: list[ClickHouseHttpOptions] = []

        def __init__(self, _credentials, options):
            self.options = options

        def insert_file(self, *_args):
            self.calls.append(self.options)
            return None

    class FakeConnector(ClickHouseConnector):
        def execute_query(self, _query: str) -> int:
            return 0

        def get_records(self, _query: str):
            return [(1,)]

    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int")],
        source_options={"native_transfer": {"wire": {"mode": "typed_raw", "delimiter_profile": "ascii_control"}}},
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"}},
    )
    export_path = tmp_path / "orders.bcp"
    export_path.write_text("1\x1e\n", encoding="utf-8")
    artifact = FileExportArtifact(str(export_path), ["id"], format="mssql-delimited", estimated_rows=1)
    artifact.bulk_wire_contract = contract
    config = _load_config({"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"}})

    sink = ClickHouseSink(FakeConnector(), http_runner_cls=FakeRunner)
    sink._insert_file_with_http(config, artifact, [("id", "int")])

    assert FakeRunner.calls[0].input_format == "CustomSeparated"


def test_strategy_native_transfer_plan_names_typed_wire_fast_path() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_table="dbo.orders",
            target_table="landing.orders",
            strategy="full_refresh",
            source_options={
                "columns": [{"name": "id", "type": "int"}],
                "native_transfer": {"wire": {"mode": "typed_raw"}},
            },
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"}},
        )
    )

    assert plan.fast_path_id == "mssql_bcp_queryout_to_clickhouse_typed_wire"
    assert plan.ingest_method == "clickhouse_typed_staging"
    assert plan.native_ingest_settings["bulk_wire"]["selected_route"] == "typed_raw_direct"
