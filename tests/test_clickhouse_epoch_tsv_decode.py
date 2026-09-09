from __future__ import annotations

from types import SimpleNamespace

from dpone.config import LoadConfig
from dpone.runtime.connectors.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.clickhouse_sql_mixin import ClickHouseSqlMixin
from dpone.runtime.sinks.clickhouse_staging_decoder import ClickHouseStagingDecoder
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


class FakeConnector:
    host = "localhost"
    port = 9000
    database = "default"
    user = "default"
    password = ""
    secure = False

    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute_query(self, query: str) -> None:
        self.queries.append(query)

    def get_records(self, query: str):
        self.queries.append(query)
        return [(0,)]


def _load_config(options: dict | None = None) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="events",
        target_schema="landing",
        target_table="events",
        options=options or {},
    )


def _decoder(connector: FakeConnector, decoded_config: LoadConfig) -> ClickHouseStagingDecoder:
    return ClickHouseStagingDecoder(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        create_staging_table=lambda _cfg, _schema: decoded_config,
        map_type=ClickHouseSqlMixin._map_type_for_config,
    )


def test_epoch_naive_timestamps_use_integer_wire_staging_types() -> None:
    connector = FakeConnector()
    decoder = _decoder(connector, _load_config())
    payload = LoadPayload(
        artifact=SimpleNamespace(bulk_text_codec=ClickHouseTabSeparatedCodec()),
        schema=[
            ("legacy_at", "datetime"),
            ("created_at", "datetime2(7)"),
            ("rounded_at", "smalldatetime"),
            ("maybe_at", "datetime2(7) nullable"),
            ("note", "nvarchar(max)"),
        ],
    )

    staging_schema = decoder.staging_schema(_load_config(), payload)

    assert staging_schema.clickhouse_types is True
    assert staging_schema.columns == [
        ("legacy_at", "Int64"),
        ("created_at", "Int64"),
        ("rounded_at", "Int64"),
        ("maybe_at", "Nullable(Int64)"),
        ("note", "String"),
    ]


def test_epoch_naive_timestamp_decode_uses_clickhouse_datetime64_conversion() -> None:
    connector = FakeConnector()
    staging_config = _load_config()
    staging_config.target_table = "events__raw"
    decoded_config = _load_config()
    decoded_config.target_table = "events__decoded"
    decoder = _decoder(connector, decoded_config)
    payload = LoadPayload(
        artifact=SimpleNamespace(bulk_text_codec=ClickHouseTabSeparatedCodec()),
        schema=[("created_at", "datetime2(7)"), ("maybe_at", "datetime2(7) nullable")],
    )

    decoder.prepare(_load_config(), staging_config, payload)

    query = connector.queries[-1]
    assert "fromUnixTimestamp64Nano((toInt64(`created_at`) * 100), 'UTC')" in query
    assert "CAST(fromUnixTimestamp64Nano((toInt64(`created_at`) * 100), 'UTC') AS DateTime64(7))" in query
    assert "if(isNull(`maybe_at`), NULL" in query
    assert "AS `maybe_at`" in query


def test_text_temporal_transfer_keeps_datetime64_wire_staging_type() -> None:
    policy = MssqlClickHouseTypePolicy.from_config({"temporal": {"naive_timestamp": {"transfer_encoding": "text"}}})
    payload = LoadPayload(
        artifact=SimpleNamespace(bulk_text_codec=ClickHouseTabSeparatedCodec(policy)),
        schema=[("created_at", "datetime2(7)")],
    )
    decoder = _decoder(FakeConnector(), _load_config())

    staging_schema = decoder.staging_schema(_load_config(), payload)

    assert staging_schema.columns == [("created_at", "DateTime64(7)")]


def test_string_marker_decode_still_runs_for_clickhouse_tsv_codec() -> None:
    connector = FakeConnector()
    staging_config = _load_config()
    staging_config.target_table = "events__raw"
    decoded_config = _load_config()
    decoded_config.target_table = "events__decoded"
    payload = LoadPayload(
        artifact=SimpleNamespace(bulk_text_codec=ClickHouseTabSeparatedCodec()),
        schema=[("note", "nvarchar(max)")],
    )

    _decoder(connector, decoded_config).prepare(_load_config(), staging_config, payload)

    query = connector.queries[-1]
    assert "replace(if(`note` = '__dpone__tsv__empty', '', `note`)" in query
    assert "__dpone__tsv__prefix" in query


def test_clickhouse_sink_creates_raw_integer_staging_for_epoch_tsv_payload() -> None:
    connector = FakeConnector()
    sink = ClickHouseSink(connector)
    payload = LoadPayload(
        artifact=SimpleNamespace(bulk_text_codec=ClickHouseTabSeparatedCodec()),
        schema=[("created_at", "datetime2(7)"), ("note", "nvarchar(max)")],
    )

    sink._create_payload_staging_table(_load_config(), payload)

    ddl = connector.queries[-1]
    assert "`created_at` Int64" in ddl
    assert "`note` String" in ddl
