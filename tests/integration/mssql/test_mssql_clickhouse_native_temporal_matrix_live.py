from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

import pytest
from tests.integration.mssql.mssql_clickhouse_live_support import open_mssql_connector

from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.connectors.clickhouse_http_bulk import (
    ClickHouseHttpBulkRunner,
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
)
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.native_acceleration import NativeAccelerationRegistry
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder
from dpone.runtime.support.temporal_fidelity import TemporalFidelityPolicy
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
]


def test_bcp_native_temporal_targets_match_python_reference_and_required_acceleration(
    tmp_path: Path,
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    mssql = open_mssql_connector()
    schema_name = f"native_temporal_{uuid.uuid4().hex[:8]}"
    source_table = "events"
    reference_table = f"native_temporal_reference_{uuid.uuid4().hex[:8]}"
    accelerated_table = f"native_temporal_accelerated_{uuid.uuid4().hex[:8]}"
    bcp_path = tmp_path / "native-temporal.bcp"
    accelerated_bcp_path = tmp_path / "native-temporal-accelerated.bcp"
    source_schema = _source_schema()
    target_schema = _target_schema()
    try:
        _prepare_mssql(mssql, schema_name, source_table)
        copied = mssql.bcp_queryout(
            f"SELECT {', '.join(f'[{name}]' for name, _ in source_schema)} "
            f"FROM [{schema_name}].[{source_table}] ORDER BY [event_id]",
            str(bcp_path),
            options=BcpOptions(
                bcp_path=mssql.bcp_path,
                file_format="native",
                batch_size=100,
                packet_size=16_384,
                timeout_seconds=120,
                trust_server_certificate=True,
            ),
        )
        assert copied == 4
        shutil.copyfile(bcp_path, accelerated_bcp_path)

        _create_clickhouse_target(clickhouse_connector, clickhouse_settings.database, reference_table)
        _create_clickhouse_target(clickhouse_connector, clickhouse_settings.database, accelerated_table)
        reference_stream = _transcode(
            bcp_path,
            source_schema,
            target_schema,
            acceleration_mode="auto",
            registry=NativeAccelerationRegistry(module_loader=lambda: None),
        )
        accelerated_stream = _transcode(
            accelerated_bcp_path,
            source_schema,
            target_schema,
            acceleration_mode="required",
            registry=NativeAccelerationRegistry(),
        )
        runner = ClickHouseHttpBulkRunner(
            ClickHouseHttpCredentials(
                host=clickhouse_settings.host,
                port=int(os.getenv("DPONE_IT_CH_HTTP_PORT", "8123")),
                database=clickhouse_settings.database,
                user=clickhouse_settings.user,
                password=clickhouse_settings.password,
                secure=clickhouse_settings.secure,
            ),
            ClickHouseHttpOptions(input_format="Native"),
        )
        columns = [name for name, _ in target_schema]
        runner.insert_stream(reference_table, columns, reference_stream.iter_bytes())
        runner.insert_stream(accelerated_table, columns, accelerated_stream.iter_bytes())

        reference_rows = _read_temporal_rows(clickhouse_connector, clickhouse_settings.database, reference_table)
        accelerated_rows = _read_temporal_rows(clickhouse_connector, clickhouse_settings.database, accelerated_table)

        assert reference_rows == accelerated_rows == _expected_rows()
        assert reference_stream.native_acceleration_evidence.requested_mode == "auto"
        assert reference_stream.native_acceleration_evidence.selected_backend == "python_reference"
        assert reference_stream.native_acceleration_evidence.rows == 4
        assert accelerated_stream.native_acceleration_evidence.requested_mode == "required"
        assert accelerated_stream.native_acceleration_evidence.selected_backend == "native_accelerated"
        assert accelerated_stream.native_acceleration_evidence.fallback_reason is None
        assert accelerated_stream.native_acceleration_evidence.rows == 4
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{reference_table}`")
        clickhouse_connector.execute_query(
            f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{accelerated_table}`"
        )
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema_name}].[{source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema_name}]")
        mssql.close()


def _source_schema() -> list[tuple[str, str]]:
    return [
        ("event_id", "int"),
        ("date_as_date", "date nullable"),
        ("date_as_date32", "date nullable"),
        ("datetime2_as_datetime", "datetime2(7) nullable"),
        ("datetime2_as_datetime64", "datetime2(7) nullable"),
        ("legacy_datetime", "datetime nullable"),
        ("legacy_smalldatetime", "smalldatetime nullable"),
        ("time_as_string", "time(7) nullable"),
        ("time_as_seconds", "time(7) nullable"),
        ("offset_as_datetime64", "datetimeoffset(7) nullable"),
        ("offset_as_string", "datetimeoffset(7) nullable"),
        ("fixed_ascii", "char(4) nullable"),
    ]


def _target_schema() -> list[tuple[str, str]]:
    return [
        ("event_id", "Int32"),
        ("date_as_date", "Nullable(Date)"),
        ("date_as_date32", "Nullable(Date32)"),
        ("datetime2_as_datetime", "Nullable(DateTime)"),
        ("datetime2_as_datetime64", "Nullable(DateTime64(7))"),
        ("legacy_datetime", "Nullable(DateTime)"),
        ("legacy_smalldatetime", "Nullable(DateTime64(0))"),
        ("time_as_string", "Nullable(String)"),
        ("time_as_seconds", "Nullable(UInt32)"),
        ("offset_as_datetime64", "Nullable(DateTime64(7, 'UTC'))"),
        ("offset_as_string", "Nullable(String)"),
        ("fixed_ascii", "Nullable(FixedString(4))"),
    ]


def _prepare_mssql(mssql, schema_name: str, source_table: str) -> None:
    mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema_name}]')")
    mssql.execute_query(
        f"""
        CREATE TABLE [{schema_name}].[{source_table}] (
            [event_id] int NOT NULL PRIMARY KEY,
            [date_as_date] date NULL,
            [date_as_date32] date NULL,
            [datetime2_as_datetime] datetime2(7) NULL,
            [datetime2_as_datetime64] datetime2(7) NULL,
            [legacy_datetime] datetime NULL,
            [legacy_smalldatetime] smalldatetime NULL,
            [time_as_string] time(7) NULL,
            [time_as_seconds] time(7) NULL,
            [offset_as_datetime64] datetimeoffset(7) NULL,
            [offset_as_string] datetimeoffset(7) NULL,
            [fixed_ascii] char(4) NULL
        )
        """
    )
    mssql.execute_query(
        f"""
        INSERT INTO [{schema_name}].[{source_table}] VALUES
        (
            1,
            CAST('1970-01-01' AS date),
            CAST('1900-01-01' AS date),
            CAST('1970-01-01T00:00:00.0000000' AS datetime2(7)),
            CAST('1900-01-01T00:00:00.0000000' AS datetime2(7)),
            CAST('1970-01-01T00:00:00.000' AS datetime),
            CAST('1900-01-01T00:00:00' AS smalldatetime),
            NULL, NULL, NULL, NULL, NULL
        ),
        (
            2,
            CAST('2149-06-06' AS date),
            CAST('2299-12-31' AS date),
            CAST('2106-02-07T06:28:15.9999999' AS datetime2(7)),
            CAST('2299-12-31T23:59:59.9999999' AS datetime2(7)),
            CAST('2106-02-07T06:28:15.000' AS datetime),
            CAST('2079-06-06T23:59:00' AS smalldatetime),
            NULL, NULL, NULL, NULL, NULL
        ),
        (
            3,
            CAST('2026-08-09' AS date),
            CAST('2026-08-09' AS date),
            CAST('2026-08-09T12:34:56.9876543' AS datetime2(7)),
            CAST('2026-08-09T12:34:56.1234567' AS datetime2(7)),
            CAST('2026-08-09T12:34:56.123' AS datetime),
            CAST('2026-08-09T12:35:00' AS smalldatetime),
            CAST('12:34:56.1234567' AS time(7)),
            CAST('12:34:56.1234567' AS time(7)),
            CAST('2026-08-09T15:34:56.7654321+03:00' AS datetimeoffset(7)),
            CAST('2026-08-09T15:34:56.7654321+03:00' AS datetimeoffset(7)),
            CAST('xy' AS char(4))
        ),
        (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )


def _create_clickhouse_target(clickhouse, database: str, table: str) -> None:
    definitions = ", ".join(f"`{name}` {dtype}" for name, dtype in _target_schema())
    clickhouse.execute_query(
        f"CREATE TABLE `{database}`.`{table}` ({definitions}) ENGINE = MergeTree ORDER BY event_id"
    )


def _transcode(
    bcp_path: Path,
    source_schema: list[tuple[str, str]],
    target_schema: list[tuple[str, str]],
    *,
    acceleration_mode: str,
    registry: NativeAccelerationRegistry,
):
    source_options = {
        "native_transfer": {
            "wire": {
                "mode": "typed_binary",
                "source_native_format": "bcp_native",
                "binary_format": "native",
                "acceleration": {"mode": acceleration_mode},
            }
        }
    }
    artifact = SourceNativeArtifact(
        bcp_path,
        columns=[name for name, _ in source_schema],
        estimated_rows=2,
        native_wire_contract=build_mssql_bcp_native_contract(
            schema=source_schema,
            query="SELECT governed temporal fixture",
            target_format="Native",
        ),
        bulk_wire_contract=BulkWirePlanner().plan(
            source_type="mssql",
            sink_type="clickhouse",
            schema=source_schema,
            source_options=source_options,
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        ),
    )
    type_policy = MssqlClickHouseTypePolicy(
        time_encoding="string",
        temporal=TemporalFidelityPolicy(offset_timestamp_mode="preserve_text"),
    )
    return NativeWireTranscoder(registry=registry).to_clickhouse_binary(
        artifact,
        source_schema,
        clickhouse_schema=target_schema,
        type_policy=type_policy,
    )


def _read_temporal_rows(clickhouse, database: str, table: str) -> list[dict[str, object]]:
    return clickhouse.get_records(
        f"""
        SELECT
            event_id,
            toString(date_as_date) AS date_as_date,
            toString(date_as_date32) AS date_as_date32,
            toString(datetime2_as_datetime) AS datetime2_as_datetime,
            toString(datetime2_as_datetime64) AS datetime2_as_datetime64,
            toString(legacy_datetime) AS legacy_datetime,
            toString(legacy_smalldatetime) AS legacy_smalldatetime,
            time_as_string,
            time_as_seconds,
            toString(offset_as_datetime64) AS offset_as_datetime64,
            offset_as_string,
            ifNull(hex(fixed_ascii), '') AS fixed_ascii_hex,
            isNull(date_as_date) AS date_is_null,
            isNull(datetime2_as_datetime64) AS datetime64_is_null
        FROM `{database}`.`{table}`
        ORDER BY event_id
        """,
        as_dict=True,
    )


def _expected_rows() -> list[dict[str, object]]:
    return [
        {
            "event_id": 1,
            "date_as_date": "1970-01-01",
            "date_as_date32": "1900-01-01",
            "datetime2_as_datetime": "1970-01-01 00:00:00",
            "datetime2_as_datetime64": "1900-01-01 00:00:00.0000000",
            "legacy_datetime": "1970-01-01 00:00:00",
            "legacy_smalldatetime": "1900-01-01 00:00:00",
            "time_as_string": None,
            "time_as_seconds": None,
            "offset_as_datetime64": None,
            "offset_as_string": None,
            "fixed_ascii_hex": "",
            "date_is_null": 0,
            "datetime64_is_null": 0,
        },
        {
            "event_id": 2,
            "date_as_date": "2149-06-06",
            "date_as_date32": "2299-12-31",
            "datetime2_as_datetime": "2106-02-07 06:28:15",
            "datetime2_as_datetime64": "2299-12-31 23:59:59.9999999",
            "legacy_datetime": "2106-02-07 06:28:15",
            "legacy_smalldatetime": "2079-06-06 23:59:00",
            "time_as_string": None,
            "time_as_seconds": None,
            "offset_as_datetime64": None,
            "offset_as_string": None,
            "fixed_ascii_hex": "",
            "date_is_null": 0,
            "datetime64_is_null": 0,
        },
        {
            "event_id": 3,
            "date_as_date": "2026-08-09",
            "date_as_date32": "2026-08-09",
            "datetime2_as_datetime": "2026-08-09 12:34:56",
            "datetime2_as_datetime64": "2026-08-09 12:34:56.1234567",
            "legacy_datetime": "2026-08-09 12:34:56",
            "legacy_smalldatetime": "2026-08-09 12:35:00",
            "time_as_string": "12:34:56.1234567",
            "time_as_seconds": 45296,
            "offset_as_datetime64": "2026-08-09 12:34:56.7654321",
            "offset_as_string": "2026-08-09 15:34:56.7654321 +03:00",
            "fixed_ascii_hex": "78792020",
            "date_is_null": 0,
            "datetime64_is_null": 0,
        },
        {
            "event_id": 4,
            "date_as_date": None,
            "date_as_date32": None,
            "datetime2_as_datetime": None,
            "datetime2_as_datetime64": None,
            "legacy_datetime": None,
            "legacy_smalldatetime": None,
            "time_as_string": None,
            "time_as_seconds": None,
            "offset_as_datetime64": None,
            "offset_as_string": None,
            "fixed_ascii_hex": "",
            "date_is_null": 1,
            "datetime64_is_null": 1,
        },
    ]
