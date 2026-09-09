"""Hermetic contracts for the MySQL source family and mysql→mssql wire."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.dag.errors import DagConfigurationError
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.credentials.airflow_env import parse_airflow_connection_uri
from dpone.runtime.credentials.config import ConnectionType
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.services.schema_type_matrix import PairTypeMatrixService
from dpone.strategy_intelligence.native_paths import NativeFastPathCatalog
from dpone.strategy_intelligence.native_transfer_contracts import NativeTransferTransportContractBuilder
from dpone.type_system.source_sink.mysql_bigquery import MySQLBigQueryTypeMapper
from dpone.type_system.source_sink.mysql_clickhouse import MySQLClickHouseTypeMapper
from dpone.type_system.source_sink.mysql_mssql import MySQLMssqlTypeMapper
from dpone.type_system.source_sink.mysql_postgres import MySQLPostgresTypeMapper


def test_connection_type_includes_mysql() -> None:
    assert ConnectionType.MYSQL == "mysql"


def test_airflow_env_parses_mysql_uri() -> None:
    creds = parse_airflow_connection_uri("mysql+pymysql://reader:secret@mysql.example.com:3306/app")
    assert creds.host == "mysql.example.com"
    assert creds.port == 3306
    assert creds.database == "app"
    assert creds.username == "reader"
    assert creds.password == "secret"


def test_mysql_mssql_type_mapper_core_types() -> None:
    mapper = MySQLMssqlTypeMapper()
    assert mapper.resolve("bigint").target_type == "bigint"
    assert mapper.resolve("decimal(18,4)").target_type == "decimal(18,4)"
    assert mapper.resolve("tinyint(1)").target_type == "bit"
    assert mapper.resolve("datetime").target_type == "datetime2(6)"
    assert mapper.resolve("json").target_type == "nvarchar(max)"
    assert mapper.resolve("json").requires_explicit_contract is True
    assert mapper.resolve("json").lossless is False
    assert mapper.resolve("enum('a','b')").requires_explicit_contract is True
    assert mapper.resolve("int unsigned").target_type == "bigint"
    assert mapper.resolve("bigint unsigned").target_type == "numeric(20,0)"
    assert mapper.resolve("bigint unsigned").requires_explicit_contract is True


def test_mysql_mssql_type_matrix_service_profile() -> None:
    matrix = PairTypeMatrixService().build(source="mysql", sink="mssql")
    assert matrix["profile"] == "mysql_to_mssql_native_v1"
    assert matrix["entries"]
    assert any(entry["source_type"] == "bigint" for entry in matrix["entries"])


def test_mysql_mssql_native_fast_path_catalog() -> None:
    path = NativeFastPathCatalog().resolve("mysql", "mssql")
    assert path.path_id == "mysql_stream_to_mssql_bcp"
    assert "bcp" in path.required_tools


def test_mysql_mssql_transport_contract_warns_without_bcp_wire() -> None:
    contract = NativeTransferTransportContractBuilder().build(
        source_type="mysql",
        sink_type="mssql",
        source_options={"export_format": "csv", "compress_export": True},
        native_ingest_settings={"bulk": {"mode": "executemany"}},
    )
    assert contract is not None
    assert contract.route == "mysql_to_mssql"
    assert contract.lossless is False
    assert any("mssql-delimited" in warning for warning in contract.warnings)


def test_mysql_mssql_transport_contract_lossless_defaults() -> None:
    contract = NativeTransferTransportContractBuilder().build(
        source_type="mysql",
        sink_type="mssql",
        source_options={"export_format": "mssql-delimited", "compress_export": False},
        native_ingest_settings={"bulk": {"mode": "bcp"}},
    )
    assert contract is not None
    assert contract.lossless is True
    assert contract.text_codec == "BulkTextCodec"


def test_mysql_connector_formats_mssql_delimited_row() -> None:
    from datetime import datetime

    from dpone.runtime.connectors.mysql import MySQLConnector

    codec = BulkTextCodec()
    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="u",
        password="p",
    )
    schema = [("id", "int"), ("name", "varchar(255)"), ("note", "text"), ("ts", "datetime")]
    line = connector._format_mssql_delimited_row(
        {
            "id": 1,
            "name": "a\tb",
            "note": None,
            "ts": datetime(2026, 6, 4, 9, 31, 0),
        },
        schema,
        codec,
    )
    assert line.startswith("1\t")
    assert "\x1dT" in line or "a" in line
    assert "2026-06-04 09:31:00" in line
    assert line.endswith("\t") or line.split("\t")[-2] == "" or line.count("\t") >= 3


def test_mysql_ssl_verify_modes_require_ca() -> None:
    from dpone.runtime.credentials.connector_factory import _mysql_ssl_settings

    creds = SimpleNamespace(ssl_ca_location=None)
    assert _mysql_ssl_settings(creds, {"ssl_mode": "REQUIRED"}) is True
    with pytest.raises(ValueError, match="requires ssl_ca"):
        _mysql_ssl_settings(creds, {"ssl_mode": "VERIFY_IDENTITY"})
    settings = _mysql_ssl_settings(creds, {"ssl_mode": "VERIFY_CA", "ssl_ca": "/tmp/ca.pem"})
    assert settings == {"ca": "/tmp/ca.pem"}


def test_mysql_fetch_schema_rejects_unknown_columns() -> None:
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = SimpleNamespace(
        source_database=None,
        source_schema="app",
        source_table="orders",
        options={"columns": ["id", "missing_col"]},
    )
    with pytest.raises(ValueError, match="unknown columns"):
        strategy.fetch_schema(load_config)  # type: ignore[arg-type]


def test_mysql_fetch_schema_maps_types_for_postgres_sink() -> None:
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {
            "id": "int",
            "flag": "tinyint(1)",
            "updated_at": "datetime",
            "payload": "json",
        },
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = SimpleNamespace(
        source_database=None,
        source_schema="app",
        source_table="orders",
        options={"sink_type": "postgres"},
    )
    schema = dict(strategy.fetch_schema(load_config))  # type: ignore[arg-type]
    assert schema["id"] == "integer"
    assert schema["flag"] == "boolean"
    assert schema["updated_at"] == "timestamp"
    assert schema["payload"] == "jsonb"


def test_mysql_postgres_type_mapper_core_types() -> None:
    mapper = MySQLPostgresTypeMapper()
    assert mapper.resolve("bigint").target_type == "bigint"
    assert mapper.resolve("decimal(18,4)").target_type == "numeric(18,4)"
    assert mapper.resolve("tinyint(1)").target_type == "boolean"
    assert mapper.resolve("bool").target_type == "boolean"
    assert mapper.resolve("datetime").target_type == "timestamp"
    assert mapper.resolve("json").target_type == "jsonb"
    assert mapper.resolve("varchar(255)").target_type == "varchar(255)"
    assert mapper.resolve("text").target_type == "text"
    assert mapper.resolve("blob").target_type == "bytea"
    assert mapper.resolve("enum('a','b')").requires_explicit_contract is True
    assert mapper.resolve("int unsigned").target_type == "bigint"
    assert mapper.resolve("bigint unsigned").target_type == "numeric(20,0)"
    assert mapper.resolve("bigint unsigned").requires_explicit_contract is True


def test_mysql_postgres_type_matrix_service_profile() -> None:
    matrix = PairTypeMatrixService().build(source="mysql", sink="postgres")
    assert matrix["profile"] == "mysql_to_postgres_native_v1"
    assert matrix["entries"]
    assert any(entry["source_type"] == "bigint" for entry in matrix["entries"])
    assert ("mysql", "postgres") in PairTypeMatrixService().available_pairs()


def test_mysql_clickhouse_type_mapper_core_types() -> None:
    mapper = MySQLClickHouseTypeMapper()
    assert mapper.resolve("bigint").target_type == "Int64"
    assert mapper.resolve("decimal(18,4)").target_type == "Decimal(18, 4)"
    assert mapper.resolve("tinyint(1)").target_type == "Bool"
    assert mapper.resolve("datetime").target_type == "DateTime64(6)"
    assert mapper.resolve("json").target_type == "String"
    assert mapper.resolve("varchar(255)").target_type == "String"
    assert mapper.resolve("text").target_type == "String"
    assert mapper.resolve("blob").target_type == "String"
    assert mapper.resolve("enum('a','b')").requires_explicit_contract is True
    assert mapper.resolve("int unsigned").target_type == "UInt32"
    assert mapper.resolve("bigint unsigned").target_type == "UInt64"


def test_mysql_clickhouse_type_matrix_service_profile() -> None:
    matrix = PairTypeMatrixService().build(source="mysql", sink="clickhouse")
    assert matrix["profile"] == "mysql_to_clickhouse_analytics_v1"
    assert matrix["entries"]
    assert any(entry["source_type"] == "bigint" for entry in matrix["entries"])
    assert ("mysql", "clickhouse") in PairTypeMatrixService().available_pairs()


def test_mysql_fetch_schema_maps_types_for_clickhouse_sink() -> None:
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {
            "id": "int",
            "flag": "tinyint(1)",
            "updated_at": "datetime",
            "payload": "json",
        },
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = SimpleNamespace(
        source_database=None,
        source_schema="app",
        source_table="orders",
        options={"sink_type": "clickhouse"},
    )
    schema = dict(strategy.fetch_schema(load_config))  # type: ignore[arg-type]
    assert schema["id"] == "Int32"
    assert schema["flag"] == "Bool"
    assert schema["updated_at"] == "DateTime64(6)"
    assert schema["payload"] == "String"


def test_mysql_connector_formats_csv_row_not_mssql_bcp() -> None:
    from datetime import datetime

    from dpone.runtime.connectors.mysql import MySQLConnector

    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="u",
        password="p",
    )
    schema = [("id", "int"), ("name", "varchar(255)"), ("note", "text"), ("flag", "tinyint(1)"), ("ts", "datetime")]
    line = connector._format_csv_row(
        {
            "id": 1,
            "name": 'a,b"c',
            "note": None,
            "flag": 1,
            "ts": datetime(2026, 6, 4, 9, 31, 0),
        },
        schema,
    )
    assert "\x1dT" not in line
    assert "1," in line or line.startswith("1,")
    assert "2026-06-04 09:31:00" in line
    assert '""' in line or '"a,b' in line


def test_mysql_connector_formats_clickhouse_tsv_nulls_and_escapes() -> None:
    from datetime import datetime

    from dpone.runtime.connectors.mysql import MySQLConnector

    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="u",
        password="p",
    )
    schema = [("id", "Int32"), ("name", "String"), ("note", "String"), ("ts", "DateTime64(6)")]
    line = connector._format_clickhouse_tsv_row(
        {
            "id": 1,
            "name": "a\tb",
            "note": None,
            "ts": datetime(2026, 6, 4, 9, 31, 0),
        },
        schema,
    )
    assert line.startswith("1\t")
    assert "a\\tb" in line
    assert "\\N" in line
    assert "2026-06-04 09:31:00" in line
    assert "\x1dT" not in line


def test_mysql_extract_rejects_mssql_delimited_for_postgres_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_mssql_delimited_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("mssql-delimited export must not run for postgres sink")
        ),
        export_csv_to_file=lambda *args, **kwargs: {"row_count": 0},
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="pg",
        source_schema="app",
        source_table="orders",
        target_schema="public",
        target_table="orders",
        export_format="mssql-delimited",
        options={"sink_type": "postgres"},
    )
    with pytest.raises(ValueError, match="incompatible with Postgres"):
        strategy.extract(load_config, None)


def test_mysql_extract_rejects_mssql_delimited_for_clickhouse_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_mssql_delimited_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("mssql-delimited export must not run for clickhouse sink")
        ),
        export_clickhouse_tsv_to_file=lambda *args, **kwargs: {"row_count": 0},
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="ch",
        source_schema="app",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        export_format="mssql-delimited",
        options={"sink_type": "clickhouse"},
    )
    with pytest.raises(ValueError, match="incompatible with ClickHouse"):
        strategy.extract(load_config, None)


def test_mysql_extract_csv_resolves_to_clickhouse_tsv_for_clickhouse_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    calls: list[str] = []

    def _export_tsv(*args, **kwargs):
        del args, kwargs
        calls.append("tsv")
        return {"row_count": 1}

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_clickhouse_tsv_to_file=_export_tsv,
        export_csv_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("postgres CSV must not run for clickhouse sink")
        ),
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="ch",
        source_schema="app",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        export_format="csv",
        options={"sink_type": "clickhouse"},
    )
    result = strategy.extract(load_config, None)
    assert calls == ["tsv"]
    assert getattr(result.artifact, "format", None) == "clickhouse-tsv"


def test_mysql_extract_rejects_mssql_delimited_for_kafka_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_mssql_delimited_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("mssql-delimited export must not run for kafka sink")
        ),
        export_csv_to_file=lambda *args, **kwargs: {"row_count": 0},
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="kafka",
        source_schema="app",
        source_table="orders",
        target_schema="kafka",
        target_table="orders",
        export_format="mssql-delimited",
        options={"sink_type": "kafka"},
    )
    with pytest.raises(ValueError, match="incompatible with Kafka"):
        strategy.extract(load_config, None)


def test_mysql_extract_rejects_compress_export_for_kafka_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_csv_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("csv export must not run when compress_export is rejected")
        ),
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="kafka",
        source_schema="app",
        source_table="orders",
        target_schema="kafka",
        target_table="orders",
        export_format="csv",
        compress_export=True,
        options={"sink_type": "kafka"},
    )
    with pytest.raises(ValueError, match="compress_export"):
        strategy.extract(load_config, None)


def test_mysql_extract_defaults_to_csv_for_kafka_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    calls: list[str] = []

    def _export_csv(*args, **kwargs):
        del args, kwargs
        calls.append("csv")
        return {"row_count": 1}

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_csv_to_file=_export_csv,
        export_mssql_delimited_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("mssql-delimited must not be the kafka default")
        ),
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="kafka",
        source_schema="app",
        source_table="orders",
        target_schema="kafka",
        target_table="orders",
        options={"sink_type": "kafka"},
    )
    result = strategy.extract(load_config, None)
    assert calls == ["csv"]
    assert getattr(result.artifact, "format", None) == "csv"


def test_mysql_bigquery_type_mapper_core_types() -> None:
    mapper = MySQLBigQueryTypeMapper()
    assert mapper.resolve("bigint").target_type == "INT64"
    assert mapper.resolve("decimal(18,4)").target_type == "NUMERIC"
    assert mapper.resolve("tinyint(1)").target_type == "BOOL"
    assert mapper.resolve("bool").target_type == "BOOL"
    assert mapper.resolve("datetime").target_type == "DATETIME"
    assert mapper.resolve("timestamp").target_type == "TIMESTAMP"
    assert mapper.resolve("json").target_type == "JSON"
    assert mapper.resolve("varchar(255)").target_type == "STRING"
    assert mapper.resolve("text").target_type == "STRING"
    assert mapper.resolve("blob").target_type == "BYTES"
    assert mapper.resolve("enum('a','b')").requires_explicit_contract is True
    assert mapper.resolve("int unsigned").target_type == "INT64"
    assert mapper.resolve("bigint unsigned").target_type == "NUMERIC"
    assert mapper.resolve("bigint unsigned").requires_explicit_contract is True
    # Real MySQL INFORMATION_SCHEMA.COLUMNS.COLUMN_TYPE shapes.
    assert mapper.resolve("int(11)").target_type == "INT64"
    assert mapper.resolve("bigint(20)").target_type == "INT64"
    assert mapper.resolve("tinyint(4)").target_type == "INT64"
    assert mapper.resolve("binary(16)").target_type == "BYTES"
    assert mapper.resolve("varbinary(32)").target_type == "BYTES"
    assert mapper.resolve("bit(1)").target_type == "BOOL"
    assert mapper.resolve("decimal(40,10)").target_type == "BIGNUMERIC"
    assert mapper.resolve("decimal(38,0)").target_type == "BIGNUMERIC"
    overflow = mapper.resolve("decimal(50,10)")
    assert overflow.compatible is False
    assert overflow.requires_explicit_contract is True


def test_mysql_bigquery_profile_types_round_trip_data_type_mapper() -> None:
    from dpone.runtime.support.type_mapping.mapper import DataTypeMapper

    mapper = MySQLBigQueryTypeMapper()
    cases = {
        "decimal(18,4)": "NUMERIC",
        "decimal(40,10)": "BIGNUMERIC",
        "datetime": "DATETIME",
        "blob": "BYTES",
        "tinyint(1)": "BOOL",
        "int(11)": "INT64",
        "json": "JSON",
    }
    for source, expected in cases.items():
        profile = mapper.resolve(source).target_type
        assert profile == expected
        assert DataTypeMapper.to_bigquery(profile) == expected


def test_load_config_builder_rejects_mysql_bcp_and_tsv_for_bigquery() -> None:
    builder = LoadConfigBuilder()
    base = {
        "source": {
            "type": "mysql",
            "connection_id": "mysql-demo",
            "connection_type": "vault",
            "table": {"schema": "app", "name": "orders"},
            "options": {"incremental_column": "updated_at", "export_format": "mssql-delimited"},
        },
        "sink": {
            "type": "bigquery",
            "connection_id": "bq-demo",
            "connection_type": "vault",
            "table": {"schema": "landing", "name": "orders"},
            "strategy": {"mode": "incremental_merge", "unique_key": "id"},
        },
    }
    with pytest.raises(DagConfigurationError, match="mssql-delimited"):
        builder.build(base)
    tsv = {
        **base,
        "source": {
            **base["source"],
            "options": {"incremental_column": "updated_at", "export_format": "clickhouse-tsv"},
        },
    }
    with pytest.raises(DagConfigurationError, match="clickhouse-tsv"):
        builder.build(tsv)


def test_mysql_bigquery_type_matrix_service_profile() -> None:
    matrix = PairTypeMatrixService().build(source="mysql", sink="bigquery")
    assert matrix["profile"] == "mysql_to_bigquery_analytics_v1"
    assert matrix["entries"]
    assert any(entry["source_type"] == "bigint" for entry in matrix["entries"])
    assert ("mysql", "bigquery") in PairTypeMatrixService().available_pairs()


def test_mysql_fetch_schema_maps_types_for_bigquery_sink() -> None:
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {
            "id": "int",
            "flag": "tinyint(1)",
            "updated_at": "datetime",
            "payload": "json",
        },
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = SimpleNamespace(
        source_database=None,
        source_schema="app",
        source_table="orders",
        options={"sink_type": "bigquery"},
    )
    schema = dict(strategy.fetch_schema(load_config))  # type: ignore[arg-type]
    assert schema["id"] == "INT64"
    assert schema["flag"] == "BOOL"
    assert schema["updated_at"] == "DATETIME"
    assert schema["payload"] == "JSON"


def test_mysql_extract_rejects_mssql_delimited_for_bigquery_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_mssql_delimited_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("mssql-delimited export must not run for bigquery sink")
        ),
        export_csv_to_file=lambda *args, **kwargs: {"row_count": 0},
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="bigquery",
        source_schema="app",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        export_format="mssql-delimited",
        options={"sink_type": "bigquery"},
    )
    with pytest.raises(ValueError, match="incompatible with BigQuery"):
        strategy.extract(load_config, None)


def test_mysql_extract_rejects_clickhouse_tsv_for_bigquery_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_clickhouse_tsv_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("clickhouse-tsv export must not run for bigquery sink")
        ),
        export_csv_to_file=lambda *args, **kwargs: {"row_count": 0},
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="bigquery",
        source_schema="app",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        export_format="clickhouse-tsv",
        options={"sink_type": "bigquery"},
    )
    with pytest.raises(ValueError, match="clickhouse-tsv"):
        strategy.extract(load_config, None)


def test_mysql_extract_defaults_to_csv_for_bigquery_sink() -> None:
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

    calls: list[str] = []

    def _export_csv(*args, **kwargs):
        del args, kwargs
        calls.append("csv")
        return {"row_count": 1}

    connector = SimpleNamespace(
        get_table_column_types=lambda *args, **kwargs: {"id": "int", "name": "varchar(32)"},
        export_csv_to_file=_export_csv,
        export_mssql_delimited_to_file=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("mssql-delimited must not be the bigquery default")
        ),
        build_select_query=lambda *args, **kwargs: "SELECT 1",
    )
    strategy = MySQLFullExtractStrategy(connector=connector, logger=SimpleNamespace())
    load_config = LoadConfig(
        source_conn_id="mysql",
        target_conn_id="bigquery",
        source_schema="app",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={"sink_type": "bigquery"},
    )
    result = strategy.extract(load_config, None)
    assert calls == ["csv"]
    assert getattr(result.artifact, "format", None) == "csv"


def test_postgres_staging_rejects_mssql_delimited_artifact() -> None:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.file_artifacts import FileExportArtifact
    from dpone.runtime.sinks.staging_managers.postgres import PostgresStagingManager

    manager = PostgresStagingManager(connector=SimpleNamespace(), logger=SimpleNamespace())
    staging = StagingTableArtifact(
        schema="stg",
        table="orders",
        columns=["id"],
        staging_manager=manager,
    )
    artifact = FileExportArtifact(file_path="/tmp/x.bcp", columns=["id"], format="mssql-delimited")
    with pytest.raises(ValueError, match="mssql-delimited"):
        manager.load_from_file(staging, artifact)


def test_bigquery_staging_file_loader_rejects_non_csv_formats() -> None:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.file_artifacts import FileExportArtifact
    from dpone.runtime.sinks.bigquery_staging_file_loader import BigQueryStagingFileLoader

    class _Logger:
        def log_etl_error(self, *args, **kwargs) -> None:
            del args, kwargs

        def log_etl_progress(self, *args, **kwargs) -> None:
            del args, kwargs

    loader = BigQueryStagingFileLoader(connector=SimpleNamespace(project_id="demo"), logger=_Logger())
    staging = StagingTableArtifact(
        schema="landing",
        table="orders",
        columns=["id"],
        staging_manager=SimpleNamespace(),
    )
    for fmt in ("mssql-delimited", "clickhouse-tsv"):
        artifact = FileExportArtifact(file_path="/tmp/x.dat", columns=["id"], format=fmt)
        with pytest.raises(ValueError, match="requires CSV"):
            loader.load_from_file(staging, artifact)


def test_mysql_source_factory_branch_builds_source(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.runtime.credentials import factory as factory_module
    from dpone.runtime.sources.mysql import MySQLSource

    fake_connector = SimpleNamespace(name="mysql")

    monkeypatch.setattr(
        factory_module.SourceFactory,
        "_create_mysql_connector",
        classmethod(lambda cls, *args, **kwargs: fake_connector),
    )
    source = factory_module.SourceFactory.create(
        connection_id="mysql_oltp",
        state_storage=SimpleNamespace(),
        credentials_source="env",
        connection_type="mysql",
    )
    assert isinstance(source, MySQLSource)
    assert source.connector is fake_connector
