from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone._compat import UTC
from dpone.config import LoadConfig, LoadStrategy
from dpone.config.mssql_strategy_contract import MSSQLStrategyContractError
from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
    operation_scope_hash,
)
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.ops.routes.refresh_executors.mssql_clickhouse_config import MssqlClickHouseRefreshConfig
from dpone.ops.routes.refresh_executors.postgres_mssql_config import (
    PostgresMssqlRefreshConfig,
    mssql_qualified_name,
)
from dpone.runtime.artifacts import StagingTableArtifact
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.connectors.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.connectors.mssql_bulk import (
    BcpCredentials,
    BcpDsnOptions,
    BcpOptions,
    BcpRunner,
    DelimitedBulkFile,
    UnsafeBulkValueError,
)
from dpone.runtime.connectors.mssql_sql import MSSQLSqlRenderer
from dpone.runtime.credentials.config import ConnectionType, CredentialsConfig, CredentialsSource
from dpone.runtime.credentials.factory import BaseFactory, SinkFactory, SourceFactory
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink, MSSQLStagingManager
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager as FocusedMSSQLStagingManager
from dpone.runtime.sinks.strategies.mssql import MSSQLFullRefreshStrategy
from dpone.runtime.sources.strategies.mssql.mssql_strategies import MSSQLFullExtractStrategy
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from dpone.runtime.state.factory import StateFactory
from dpone.runtime.state.models import RunState, RunStateStatus
from dpone.runtime.state.mssql import MSSQLLoadAuditStorage, MSSQLRunStateStorage, MSSQLXMinStateStorage
from dpone.runtime.state.mssql_contract import RUN_STATE_CONTRACT
from dpone.runtime.state.xmin_storage import XMinState
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


class FakeRun:
    def __init__(self):
        self.commands = []

    def __call__(self, command, **kwargs):
        self.commands.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="100 rows copied.\n", stderr="")


class FakeMSSQLConnector:
    def __init__(self):
        self.queries = []
        self.records = []

    def quote_identifier(self, name: str) -> str:
        return "[" + name.replace("]", "]]") + "]"

    def qualified_name(self, schema: str, table: str) -> str:
        return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"

    def execute_query(self, query, params=None):
        self.queries.append((str(query), params))
        return 1

    def get_records(self, query, params=None, as_dict=False):
        self.queries.append((str(query), params, as_dict))
        if "__dpone__affected_rows" in str(query):
            return [{"__dpone__affected_rows": 1}]
        if "temporal_type" in str(query) and "sys.tables AS t" in str(query):
            return [
                {
                    "temporal_type": 0,
                    "ledger_type": 0,
                    "is_memory_optimized": 0,
                    "is_filetable": 0,
                    "is_node": 0,
                    "is_edge": 0,
                }
            ]
        if self.records:
            return self.records.pop(0)
        return []


class _GovernedUnitStagingConsumer:
    """Exercise strategy business SQL behind an explicit governed unit port."""

    def __init__(self, strategy):
        self._strategy = strategy

    def consume(self, load_config, payload, handler):
        assert isinstance(payload.mssql_transaction_admission, MssqlTransactionAdmission)
        assert self._strategy.state_storage.atomicity == "target_atomic"
        assert self._strategy.state_storage.provisioning == "external"
        staging = payload.artifact.materialize(
            self._strategy.staging_manager,
            load_config,
            payload.schema,
        )
        resolved_types = {str(name): str(dtype) for name, dtype in payload.schema}
        staging.target_column_types = resolved_types
        staging.column_types = resolved_types
        staging.target_column_nullability = {name: True for name in resolved_types}
        staging.target_column_collations = {}
        result = handler(staging)
        return replace(result, staging_rows=staging.row_count)


def _governed_unit_sink(connector):
    state = SimpleNamespace(atomicity="target_atomic", provisioning="external")
    return MSSQLSink(
        connector,
        state_storage=state,
        staging_consumer_factory=_GovernedUnitStagingConsumer,
    )


def _governed_unit_payload(load_config, artifact, schema):
    invocation = InvocationIdentity("unit-run", "runtime-mssql-contracts", "single")
    request = MssqlAttemptRequest(
        invocation=invocation,
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id="unit-load",
        target_database=str(load_config.target_database or "unit_target"),
        target_schema=str(load_config.target_schema).split(".")[-1],
        target_table=load_config.target_table,
        strategy=load_config.load_strategy.value,
    )
    attempt = MssqlTransactionAttempt(request, 1)
    scope_hash = operation_scope_hash({"kind": "target_wide"})
    operation = MssqlTransactionOperation(
        attempt,
        MssqlOperationRequest(scope_hash, b"o" * 32).operation_key(attempt),
        scope_hash,
        b"o" * 32,
        1,
    )
    return LoadPayload(
        artifact=artifact,
        schema=schema,
        mssql_transaction_admission=MssqlTransactionAdmission(operation=operation),
    )


def _catalog_rows(*, id_nullable: bool, amount: bool = True):
    rows = [
        {
            "column_name": "id",
            "type_name": "bigint",
            "max_length": 8,
            "precision": 19,
            "scale": 0,
            "is_nullable": id_nullable,
            "is_computed": False,
            "collation_name": None,
        }
    ]
    if amount:
        rows.append(
            {
                "column_name": "amount",
                "type_name": "numeric",
                "max_length": 9,
                "precision": 18,
                "scale": 2,
                "is_nullable": True,
                "is_computed": False,
                "collation_name": None,
            }
        )
    return rows


class DummyManager:
    def get_credentials(self, connection_name, source, mount_point=None, path=None):
        assert connection_name == "mssql-demo"
        assert source == CredentialsSource.VAULT
        return CredentialsConfig(
            host="sql.example.com",
            port=1433,
            database="dwh",
            username="etl",
            password="secret",
            driver="ODBC Driver 18 for SQL Server",
            encrypt="yes",
            trust_server_certificate="no",
            bcp_path="/opt/mssql-tools18/bin/bcp",
        )


def test_mssql_staging_manager_imports_from_focused_module() -> None:
    assert MSSQLStagingManager is FocusedMSSQLStagingManager
    assert MSSQLSink is not None


def test_bcp_runner_builds_redacted_import_command() -> None:
    fake_run = FakeRun()
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", user="etl", password="secret"),
        BcpOptions(bcp_path="/opt/mssql-tools18/bin/bcp", field_terminator="\t", row_terminator="\n"),
        run=fake_run,
    )

    result = runner.import_file("landing.orders", "/tmp/orders.tsv")

    assert result.rows_copied == 100
    assert result.command[0] == "/opt/mssql-tools18/bin/bcp"
    assert "secret" not in result.command
    assert fake_run.commands[0][1]["input"] == "secret\n"
    assert "-k" in result.command
    assert "-d" in result.command
    assert "secret" not in result.redacted_command


def test_bcp_runner_omits_database_flag_for_three_part_import_target() -> None:
    fake_run = FakeRun()
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="analytics_staging", user="etl", password="secret"),
        BcpOptions(bcp_path="/opt/mssql-tools18/bin/bcp"),
        run=fake_run,
    )

    runner.import_file("[dwh_example].[DWH_Stage].[stg_orders]", "/tmp/orders.bcp")

    command = fake_run.commands[0][0]
    assert "-d" not in command
    assert command[1] == "[dwh_example].[DWH_Stage].[stg_orders]"


def test_bcp_runner_uses_explicit_format_without_delimiter_flags() -> None:
    fake_run = FakeRun()
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", user="etl", password="secret"),
        BcpOptions(bcp_path="/opt/mssql-tools18/bin/bcp", trust_server_certificate=True),
        run=fake_run,
    )

    result = runner.import_format_file("[dwh].[landing].[orders]", "/tmp/orders.bin", "/tmp/orders.fmt")

    command = list(result.command)
    assert command[:6] == [
        "/opt/mssql-tools18/bin/bcp",
        "[dwh].[landing].[orders]",
        "in",
        "/tmp/orders.bin",
        "-f",
        "/tmp/orders.fmt",
    ]
    assert "-C" in command
    assert "-t" not in command
    assert "-r" not in command
    assert "-u" in command
    assert "-k" in command
    assert "secret" not in command


def test_bcp_runner_caps_packet_size_for_odbc18_ssl() -> None:
    fake_run = FakeRun()
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", user="etl", password="secret"),
        BcpOptions(bcp_path="/opt/mssql-tools18/bin/bcp", packet_size=32_767),
        run=fake_run,
    )

    runner.import_file("landing.orders", "/tmp/orders.tsv")

    command = fake_run.commands[0][0]
    assert "-a" in command
    assert command[command.index("-a") + 1] == "16384"


def test_bcp_runner_uses_private_multisubnet_dsn_for_one_invocation() -> None:
    observed: dict[str, object] = {}

    def fake_run(command, **kwargs):
        dsn_path = Path(kwargs["env"]["ODBCINI"])
        observed.update(
            command=list(command),
            dsn_path=dsn_path,
            dsn_mode=dsn_path.stat().st_mode & 0o777,
            dsn_text=dsn_path.read_text(encoding="utf-8"),
        )
        return SimpleNamespace(returncode=0, stdout="1 row copied.\n", stderr="")

    runner = BcpRunner(
        BcpCredentials(
            host="bi-listener.example.com",
            port=1433,
            database="dwh",
            user="etl",
            password="secret",
        ),
        BcpOptions(
            connection_dsn=BcpDsnOptions(
                multi_subnet_failover=True,
                login_timeout_seconds=60,
                trust_server_certificate="Yes",
            )
        ),
        run=fake_run,
    )

    result = runner.import_file("landing.orders", "/tmp/orders.bcp")

    assert result.rows_copied == 1
    assert observed["command"][0:1] == ["bcp"]
    assert "-D" in observed["command"]
    assert observed["command"][observed["command"].index("-S") + 1] == "dpone_bcp_connection"
    assert observed["dsn_mode"] == 0o600
    assert "Server=tcp:bi-listener.example.com,1433" in observed["dsn_text"]
    assert "MultiSubnetFailover=Yes" in observed["dsn_text"]
    assert "LoginTimeout=60" in observed["dsn_text"]
    assert "secret" not in observed["dsn_text"]
    assert not Path(observed["dsn_path"]).exists()


@pytest.mark.parametrize(
    ("host", "driver"),
    [
        ("listener.example.com;PWD=unsafe", "ODBC Driver 18 for SQL Server"),
        ("listener.example.com", "ODBC Driver 18 for SQL Server\nPWD=unsafe"),
    ],
)
def test_bcp_runner_rejects_dsn_injection_before_process(
    host: str,
    driver: str,
) -> None:
    with pytest.raises(ValueError, match="invalid for an ODBC DSN"):
        runner = BcpRunner(
            BcpCredentials(host=host, port=1433, database="dwh"),
            BcpOptions(connection_dsn=BcpDsnOptions(driver=driver)),
            run=lambda *_args, **_kwargs: pytest.fail("BCP process must not start"),
        )
        runner.import_file("landing.orders", "/tmp/orders.bcp")


def test_delimited_bulk_file_fails_fast_on_unsafe_text(tmp_path: Path) -> None:
    writer = DelimitedBulkFile(directory=str(tmp_path), field_terminator="\t", unsafe_text_policy="fail")

    try:
        writer.write_rows([{"id": 1, "comment": "bad\tvalue"}], ["id", "comment"])
    except UnsafeBulkValueError as exc:
        assert "terminator" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("UnsafeBulkValueError was not raised")

    fallback = DelimitedBulkFile(directory=str(tmp_path), field_terminator="\t", unsafe_text_policy="fallback")
    path, count = fallback.write_rows([{"id": 1, "comment": "bad\tvalue"}], ["id", "comment"])
    assert count == 1
    assert Path(path).read_text(encoding="utf-8") == "1\tbad value\n"


def test_delimited_bulk_file_formats_lineage_iso_timestamps_for_datetime2(tmp_path: Path) -> None:
    codec = BulkTextCodec()
    writer = DelimitedBulkFile(directory=str(tmp_path), field_terminator="\t", text_codec=codec)
    column_types = {
        "id": "int",
        "__dpone__loaded_at": "datetime2(6)",
        "comment": "nvarchar(max)",
    }

    path, count = writer.write_rows(
        [
            {
                "id": 1,
                "__dpone__loaded_at": "2026-06-04T09:31:00+00:00",
                "comment": "line\nwith\ttab",
            }
        ],
        ["id", "__dpone__loaded_at", "comment"],
        column_types=column_types,
    )

    assert count == 1
    content = Path(path).read_text(encoding="utf-8")
    assert content.startswith("1\t2026-06-04 09:31:00.000000\t")


def test_delimited_bulk_file_encodes_empty_string_and_control_chars(tmp_path: Path) -> None:
    codec = BulkTextCodec()
    writer = DelimitedBulkFile(directory=str(tmp_path), field_terminator="\t", text_codec=codec)

    path, count = writer.write_rows(
        [
            {"id": 1, "comment": ""},
            {"id": 2, "comment": "line\nwith\ttab"},
            {"id": 3, "comment": None},
        ],
        ["id", "comment"],
    )

    assert count == 3
    assert Path(path).read_text(encoding="utf-8") == (
        f"1\t{codec.empty_string_marker}\n2\tline{codec.marker_prefix}Nwith{codec.marker_prefix}Ttab\n3\t\n"
    )


def test_bulk_text_codec_renders_mssql_decode_expression() -> None:
    codec = BulkTextCodec()

    expression = codec.mssql_decode_expression("s.[comment]")

    assert "CASE WHEN (s.[comment]) COLLATE Latin1_General_100_BIN2 = NCHAR(29) + N'E' THEN N''" in expression
    assert "WHEN CHARINDEX(NCHAR(29), (s.[comment]) COLLATE Latin1_General_100_BIN2) = 0 THEN s.[comment]" in expression
    assert "NCHAR(10)" in expression
    assert "NCHAR(9)" in expression


@pytest.mark.parametrize(
    "value",
    [
        "",
        "plain",
        "\t\n\r",
        "\x1d\x1e\x1f",
        "\x1fstart",
        "end\x1f",
        "\x1d\x1dE\x1dN\x1dP",
        "\\backslash",
        "\t\x1dN\n\x1f\r\x1e",
        "Ω😀",
    ],
)
def test_bulk_text_codec_control_corpus_is_an_exact_python_inverse(value: str) -> None:
    codec = BulkTextCodec()

    encoded = codec.encode(value)

    codec.assert_file_safe(encoded)
    assert codec.decode(encoded) == value
    assert "\t" not in encoded
    assert "\n" not in encoded
    assert "\r" not in encoded
    assert "\x1f" not in encoded


def test_bulk_text_codec_all_structural_control_combinations_are_invertible() -> None:
    codec = BulkTextCodec()
    alphabet = ("\x1d", "\x1e", "\x1f", "\t", "\n", "\r")

    values = [left + middle + right for left in alphabet for middle in alphabet for right in alphabet]

    assert all(codec.decode(codec.encode(value)) == value for value in values)
    assert all(not any(control in codec.encode(value) for control in ("\t", "\n", "\r", "\x1f")) for value in values)


def test_bulk_text_codec_postgres_encode_compares_empty_on_text_cast() -> None:
    """json/jsonb sources must not compare raw column to '' (PG coerces to json)."""

    codec = BulkTextCodec()
    expression = codec.postgres_encode_expression('dpone_src."c_json"')

    assert "WHEN (dpone_src.\"c_json\")::text = ''" in expression
    assert "WHEN dpone_src.\"c_json\" = ''" not in expression
    assert 'CASE WHEN dpone_src."c_json" IS NOT DISTINCT FROM NULL THEN NULL' in expression


def test_mssql_staging_commit_decodes_encoded_text_columns_only() -> None:
    connector = FakeMSSQLConnector()
    strategy = MSSQLFullRefreshStrategy(connector, logger=None, staging_manager=None)
    staging = StagingTableArtifact(
        schema="staging",
        table="orders_stg",
        columns=["id", "comment"],
        staging_manager=None,
        column_types={"id": "int", "comment": "nvarchar(max)"},
        bulk_text_codec=BulkTextCodec(),
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="dst",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
    )

    strategy._insert_from_staging_to_table(load_config, staging, "orders_shadow")

    query = connector.queries[-1][0]
    assert "s.[id]" in query
    assert "CASE WHEN (s.[comment]) COLLATE Latin1_General_100_BIN2 = NCHAR(29) + N'E' THEN N''" in query
    assert "AS [comment]" in query


def test_mssql_staging_rejects_raw_text_file_without_bulk_codec() -> None:
    manager = MSSQLStagingManager(FakeMSSQLConnector())
    staging = StagingTableArtifact(
        schema="staging",
        table="orders_stg",
        columns=["id", "comment"],
        staging_manager=manager,
        column_types={"id": "int", "comment": "nvarchar(max)"},
    )
    artifact = SimpleNamespace(format="mssql-delimited", bulk_text_codec=None)

    with pytest.raises(ValueError, match="bulk_text_codec"):
        manager._validate_character_bulk_file_safety(staging, artifact)


def test_mssql_staging_rejects_character_bcp_for_unsafe_types() -> None:
    manager = MSSQLStagingManager(FakeMSSQLConnector())
    staging = StagingTableArtifact(
        schema="staging",
        table="orders_stg",
        columns=["id", "payload"],
        staging_manager=manager,
        column_types={"id": "int", "payload": "varbinary(max)"},
    )
    artifact = SimpleNamespace(format="mssql-delimited", bulk_text_codec=BulkTextCodec())

    with pytest.raises(ValueError, match="unsafe"):
        manager._validate_character_bulk_file_safety(staging, artifact)


def test_mssql_staging_load_from_file_uses_canonical_bulk_bcp_options(tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    class BulkConnector(FakeMSSQLConnector):
        bcp_path = "/opt/mssql-tools18/bin/bcp"
        trust_server_certificate = "yes"
        count_calls = 0

        def bcp_import(self, schema: str, table: str, file_path: str, *, options=None) -> int:
            seen["schema"] = schema
            seen["table"] = table
            seen["file_path"] = file_path
            seen["options"] = options
            return 1

        def get_records(self, query, params=None, as_dict=False):
            if "COUNT_BIG" in str(query):
                self.count_calls += 1
                return [(self.count_calls - 1,)]
            return super().get_records(query, params, as_dict)

    file_path = tmp_path / "orders.bcp"
    file_path.write_text("1\talpha\n", encoding="utf-8")
    manager = MSSQLStagingManager(BulkConnector())
    staging = StagingTableArtifact(
        schema="staging",
        table="orders_stg",
        columns=["id", "comment"],
        staging_manager=manager,
        column_types={"id": "int", "comment": "nvarchar(max)"},
        bulk_text_codec=BulkTextCodec(),
        bulk_options=BulkOptionsResolver.resolve(
            {
                "bulk": {
                    "mode": "bcp",
                    "bcp": {
                        "batch_size": 321,
                        "packet_size": 65432,
                        "timeout_seconds": 77,
                        "table_lock": False,
                        "keep_nulls": False,
                    },
                }
            }
        ),
    )
    artifact = FileExportArtifact(
        str(file_path),
        ["id", "comment"],
        format="mssql-delimited",
        rows_exported=1,
        bulk_text_codec=BulkTextCodec(),
    )

    copied = manager.load_from_file(staging, artifact)

    options = seen["options"]
    assert copied == 1
    assert options.bcp_path == "/opt/mssql-tools18/bin/bcp"
    assert options.batch_size == 321
    assert options.packet_size == 16_384
    assert options.timeout_seconds == 77
    assert options.table_lock is False
    assert options.keep_nulls is False
    assert options.field_terminator == "\t"
    assert options.row_terminator == "\n"


def test_mssql_staging_load_from_partition_derives_unique_error_file(tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    class BulkConnector(FakeMSSQLConnector):
        bcp_path = "/opt/mssql-tools18/bin/bcp"
        trust_server_certificate = "yes"
        count_calls = 0

        def bcp_import(self, schema: str, table: str, file_path: str, *, options=None) -> int:
            del schema, table, file_path
            seen["options"] = options
            return 1

        def get_records(self, query, params=None, as_dict=False):
            if "COUNT_BIG" in str(query):
                self.count_calls += 1
                return [(self.count_calls - 1,)]
            return super().get_records(query, params, as_dict)

    file_path = tmp_path / "orders_p0.bcp"
    file_path.write_text("1\talpha\n", encoding="utf-8")
    error_file = tmp_path / "bcp_errors.err"
    manager = MSSQLStagingManager(BulkConnector())
    staging = StagingTableArtifact(
        schema="staging",
        table="orders_stg",
        columns=["id", "comment"],
        staging_manager=manager,
        column_types={"id": "int", "comment": "nvarchar(max)"},
        bulk_options=BulkOptionsResolver.resolve(
            {
                "bulk": {
                    "mode": "bcp",
                    "bcp": {
                        "error_file": str(error_file),
                    },
                }
            }
        ),
    )
    artifact = FileExportArtifact(
        str(file_path),
        ["id", "comment"],
        format="mssql-delimited",
        rows_exported=1,
        bulk_text_codec=BulkTextCodec(),
    )
    artifact.transfer_partition_id = "abcdef1234567890"
    artifact.partition_bounds = {"index": 3, "lower": 100, "upper": 200}

    manager.load_from_file(staging, artifact)

    options = seen["options"]
    assert options.error_file == str(tmp_path / "bcp_errors_p3_abcdef12.err")


def test_clickhouse_tabseparated_codec_renders_mssql_safe_values() -> None:
    expression = ClickHouseTabSeparatedCodec().mssql_select_expression("dpone_src.[comment]")

    assert "CASE WHEN dpone_src.[comment] IS NULL THEN N'\\N'" in expression
    assert "WHEN dpone_src.[comment] = N'' THEN N'__dpone__tsv__empty'" in expression
    assert "N'\\\\'" in expression
    assert "N'\\t'" in expression
    assert "N'\\n'" in expression


def test_clickhouse_tabseparated_codec_does_not_compare_numeric_to_empty_string() -> None:
    expression = ClickHouseTabSeparatedCodec().mssql_select_expression("dpone_src.[amount]", text_column=False)

    assert "CASE WHEN dpone_src.[amount] IS NULL THEN N'\\N'" in expression
    assert "dpone_src.[amount] = N''" not in expression
    assert "CONVERT(VARCHAR(MAX), dpone_src.[amount])" in expression


def test_postgres_mssql_delimited_export_wraps_text_columns_for_bcp_codec() -> None:
    strategy = PostgresFullExtractStrategy(connector=None, logger=None)
    codec = BulkTextCodec()

    query = strategy._wrap_mssql_bulk_text_query(
        "SELECT id, comment FROM public.orders",
        [("id", "integer"), ("comment", "text")],
        codec,
    )

    assert 'dpone_src."id" AS "id"' in query
    assert 'CASE WHEN dpone_src."comment" IS NOT DISTINCT FROM NULL THEN NULL' in query
    assert "E'\\x1dE'" in query


def test_mssql_queryout_export_wraps_text_columns_only_for_mssql_sink() -> None:
    source = FakeMSSQLConnector()
    sink = MSSQLConnector(host="sql", port=1433, database="dwh", user="u", password="p")
    strategy = MSSQLFullExtractStrategy(connector=source, logger=None, sink_connector=sink)
    codec = BulkTextCodec()

    query = strategy._wrap_mssql_bulk_text_query(
        "SELECT [id], [comment] FROM [dbo].[orders]",
        [("id", "int"), ("comment", "nvarchar(max)")],
        codec,
    )

    assert "dpone_src.[id] AS [id]" in query
    assert "CONVERT(NVARCHAR(MAX), dpone_src.[comment])" in query
    assert "NCHAR(29) + N'E'" in query


def test_mssql_queryout_export_wraps_all_columns_for_clickhouse_direct_tsv() -> None:
    class ClickHouseConnector:
        pass

    source = FakeMSSQLConnector()
    sink = ClickHouseConnector()
    strategy = MSSQLFullExtractStrategy(connector=source, logger=None, sink_connector=sink)

    query = strategy._wrap_clickhouse_tabseparated_query(
        "SELECT [id], [comment] FROM [dbo].[orders]",
        [("id", "int"), ("comment", "nvarchar(max)")],
        ClickHouseTabSeparatedCodec(),
    )

    assert "CASE WHEN dpone_src.[id] IS NULL THEN N'\\N'" in query
    assert "CASE WHEN dpone_src.[comment] IS NULL THEN N'\\N'" in query
    assert "CONVERT(VARCHAR(MAX), CONVERT(NVARCHAR(MAX), dpone_src.[comment])" in query


def test_mssql_clickhouse_queryout_projection_view_shortens_wide_bcp_query() -> None:
    source = FakeMSSQLConnector()
    strategy = MSSQLFullExtractStrategy(connector=source, logger=None, sink_connector=object())
    config = LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )

    query, cleanup = strategy._materialize_queryout_projection(
        config,
        "SELECT [id], [comment] FROM [dbo].[orders]",
        [("id", "int"), ("comment", "nvarchar(max)")],
    )

    assert query.startswith("SELECT [id], [comment] FROM [dbo].[__dpone__bcp_")
    assert "CREATE VIEW [dbo].[__dpone__bcp_" in source.queries[0][0]
    cleanup()
    assert "DROP VIEW IF EXISTS [dbo].[__dpone__bcp_" in source.queries[-1][0]


def test_mssql_clickhouse_partition_bounds_use_source_query_before_projection(tmp_path: Path) -> None:
    class ClickHouseConnector:
        pass

    class Logger:
        def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
            del event, payload

        def info(self, message: str) -> None:
            del message

    class BoundsConnector(FakeMSSQLConnector):
        bcp_path = "bcp"
        trust_server_certificate = "yes"

        def __init__(self) -> None:
            super().__init__()
            self.bounds_queries: list[str] = []
            self.bcp_queries: list[str] = []

        def fetch_schema(self, schema: str, table: str):
            assert (schema, table) == ("dbo", "orders")
            return [("order_id", "int"), ("payload", "nvarchar(max)")]

        def build_select_query(self, schema: str, table: str, columns: list[str]) -> str:
            return f"SELECT {', '.join(f'[{column}]' for column in columns)} FROM [{schema}].[{table}]"

        def get_records(self, query: str):
            self.bounds_queries.append(query)
            return [(1, 10000, 10000)]

        def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
            del options
            self.bcp_queries.append(query)
            Path(output_path).write_text("1\talpha\n", encoding="utf-8")
            return 1

    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "clickhouse_bulk": {"mode": "http"},
            "mssql_queryout_projection": "view",
            "partition_tmp_dir": str(tmp_path),
            "type_fidelity": {"binary_encoding": "hex", "time_encoding": "seconds_since_midnight"},
            "partitioning": {
                "strategy": "auto",
                "column": "order_id",
                "bounds": "auto",
                "target_rows_per_partition": 2500,
                "max_partitions": 64,
                "export_workers": 1,
                "load_workers": 1,
            },
        },
    )
    connector = BoundsConnector()

    extract = MSSQLFullExtractStrategy(connector, logger=Logger(), sink_connector=ClickHouseConnector()).extract(
        config,
        None,
    )

    assert "__dpone__bcp_" not in connector.bounds_queries[0]
    assert "FROM [dbo].[orders]" in connector.bounds_queries[0]
    assert any("[order_id] <= 10000" in query for query in connector.bcp_queries)
    extract.artifact.cleanup()


def test_mssql_connector_quotes_identifiers_and_builds_selects() -> None:
    connector = MSSQLConnector(host="sql", port=1433, database="dwh", user="u", password="p")

    assert connector.quote_identifier("bad]name") == "[bad]]name]"
    assert connector.build_select_query("dbo", "orders", ["id", "amount"], limit=10) == (
        "SELECT TOP (10) [id], [amount] FROM [dbo].[orders]"
    )
    assert connector.build_max_query("dbo", "orders", "updated_at") == (
        "SELECT MAX([updated_at]) AS max_val FROM [dbo].[orders]"
    )


def test_mssql_connector_skips_resultless_sets_and_preserves_first_rowset() -> None:
    class Cursor:
        def __init__(self) -> None:
            self._sets = [
                (None, []),
                (None, []),
                ([("inserted_rows",), ("soft_deleted_rows",)], [(3, 2)]),
                ([("later_debug_rowset",)], [(99,)]),
            ]
            self._index = 0
            self.fetch_calls = 0
            self.closed = False

        @property
        def description(self):
            return self._sets[self._index][0]

        def execute(self, _query, _params):
            return self

        def fetchall(self):
            assert self.description is not None
            self.fetch_calls += 1
            return self._sets[self._index][1]

        def nextset(self):
            self._index += 1
            return self._index < len(self._sets)

        def close(self) -> None:
            self.closed = True

    cursor = Cursor()
    connector = MSSQLConnector(host="sql", port=1433, database="dwh", user="u", password="p")
    connector._connection = SimpleNamespace(cursor=lambda: cursor)

    rows = connector.get_records(
        "SET NOCOUNT ON; DECLARE @actions table (...); UPDATE ...; SELECT ...;",
        as_dict=True,
    )

    assert rows == [{"inserted_rows": 3, "soft_deleted_rows": 2}]
    assert cursor.fetch_calls == 1
    assert cursor.closed is True


def test_mssql_object_name_supports_two_part_three_part_and_compact_schema() -> None:
    assert MSSQLObjectName.from_dataset("dbo.orders").quoted() == "[dbo].[orders]"
    assert MSSQLObjectName.from_dataset("analytics_staging.clickhouse.v_dim_example").quoted() == (
        "[analytics_staging].[clickhouse].[v_dim_example]"
    )
    assert MSSQLSqlRenderer().qualified_name("analytics_staging.clickhouse", "v_dim_example") == (
        "[analytics_staging].[clickhouse].[v_dim_example]"
    )

    for unsafe in ("dbo", "a.b.c.d", "dbo.bad-name", "dbo.bad]name"):
        with pytest.raises(ValueError):
            MSSQLObjectName.from_dataset(unsafe)


def test_load_config_builder_normalizes_mssql_database_table_blocks() -> None:
    config = {
        "name": "orders",
        "source": {
            "type": "mssql",
            "connection_id": "mssql_dwh",
            "table": {"database": "analytics_staging", "schema": "clickhouse", "name": "v_dim_example"},
        },
        "sink": {
            "type": "mssql",
            "connection_id": "mssql_dwh",
            "table": {"database": "analytics_staging", "schema": "landing", "name": "orders"},
            "staging": {"schema": "staging"},
        },
    }

    load_config = LoadConfigBuilder().build(config)

    assert load_config.source_database == "analytics_staging"
    assert load_config.source_schema == "analytics_staging.clickhouse"
    assert load_config.target_database == "analytics_staging"
    assert load_config.target_schema == "analytics_staging.landing"
    assert load_config.staging_database == "analytics_staging"
    assert load_config.staging_schema == "analytics_staging.staging"


def test_route_refresh_configs_accept_mssql_three_part_datasets() -> None:
    mssql_clickhouse = MssqlClickHouseRefreshConfig(
        source_dataset="analytics_staging.clickhouse.v_dim_example",
        target_dataset="analytics.v_dim_example",
        boundary_column="id",
        columns=("id",),
    )
    postgres_mssql = PostgresMssqlRefreshConfig(
        source_dataset="public.orders",
        target_dataset="analytics_staging.clickhouse.orders",
        boundary_column="id",
        columns=("id",),
    )

    assert not mssql_clickhouse.blockers()
    assert not postgres_mssql.blockers()
    assert mssql_qualified_name("analytics_staging.clickhouse.orders") == "[analytics_staging].[clickhouse].[orders]"


def test_mssql_format_type_preserves_nullable_metadata() -> None:
    assert (
        MSSQLSqlRenderer().format_type(
            {
                "DATA_TYPE": "decimal",
                "NUMERIC_PRECISION": 18,
                "NUMERIC_SCALE": 2,
                "IS_NULLABLE": "YES",
            }
        )
        == "decimal(18,2) nullable"
    )


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (
            {"DATA_TYPE": "datetime2", "DATETIME_PRECISION": 6, "IS_NULLABLE": "NO"},
            "datetime2(6)",
        ),
        (
            {"DATA_TYPE": "datetimeoffset", "DATETIME_PRECISION": 3, "IS_NULLABLE": "YES"},
            "datetimeoffset(3) nullable",
        ),
        (
            {"DATA_TYPE": "time", "DATETIME_PRECISION": 0, "IS_NULLABLE": "NO"},
            "time(0)",
        ),
        (
            {"DATA_TYPE": "float", "NUMERIC_PRECISION": 53, "IS_NULLABLE": "NO"},
            "float(53)",
        ),
    ],
)
def test_mssql_format_type_preserves_temporal_and_float_precision(row, expected: str) -> None:
    assert MSSQLSqlRenderer().format_type(row) == expected


@pytest.mark.parametrize(
    "row",
    [
        {"DATA_TYPE": "datetime2", "DATETIME_PRECISION": None, "IS_NULLABLE": "NO"},
        {"DATA_TYPE": "float", "NUMERIC_PRECISION": None, "IS_NULLABLE": "NO"},
    ],
)
def test_mssql_format_type_fails_closed_without_exact_precision(row) -> None:
    with pytest.raises(ValueError, match="catalog precision unavailable"):
        MSSQLSqlRenderer().format_type(row)


def test_mssql_connector_uses_bracket_quoted_bcp_import_target() -> None:
    seen: dict[str, object] = {}

    class FakeRunner:
        def __init__(self, credentials, options):
            self.credentials = credentials
            self.options = options

        def import_file(self, table, path):
            seen["table"] = table
            seen["path"] = path
            seen["options"] = self.options
            return SimpleNamespace(rows_copied=7)

    connector = MSSQLConnector(
        host="sql",
        port=1433,
        database="dwh",
        user="u",
        password="p",
        connect_timeout=60,
        odbc_options={"multi_subnet_failover": "yes"},
        bcp_runner_cls=FakeRunner,
    )

    copied = connector.bcp_import("bad]schema", "order detail", "/tmp/orders.bcp")

    assert copied == 7
    assert seen["table"] == "[bad]]schema].[order detail]"
    connection_dsn = seen["options"].connection_dsn
    assert connection_dsn is not None
    assert connection_dsn.multi_subnet_failover is True
    assert connection_dsn.login_timeout_seconds == 60


def test_factories_create_mssql_source_and_sink(monkeypatch) -> None:
    BaseFactory.manager = DummyManager()

    source = SourceFactory.create(
        connection_id="mssql-demo",
        state_storage=None,
        credentials_source="vault",
        connection_type="mssql",
    )
    sink = SinkFactory.create(
        connection_id="mssql-demo",
        state_storage=None,
        credentials_source="vault",
        connection_type="mssql",
    )

    assert source.connector.host == "sql.example.com"
    assert sink.connector.bcp_path == "/opt/mssql-tools18/bin/bcp"
    assert ConnectionType.MSSQL.value == "mssql"


def test_mssql_xmin_state_storage_saves_loads_and_deletes() -> None:
    now = datetime.now(UTC)
    connector = FakeMSSQLConnector()
    connector.records = [
        [{"xmin_value": 55, "__dpone__loaded_at": now, "is_initial": 1, "wraparound_detected": 0, "frozen_xid": None}]
    ]
    storage = MSSQLXMinStateStorage(connector, schema="state", table="xmin")

    storage.save_state("public", "orders", XMinState(55, now, is_initial=True))
    loaded = storage.load_state("public", "orders")
    storage.delete_state("public", "orders")

    assert loaded == XMinState(55, now, is_initial=True, wraparound_detected=False, frozen_xid=None)
    assert any("MERGE [state].[xmin]" in query for query, *_ in connector.queries)
    assert any("DELETE FROM [state].[xmin]" in query for query, *_ in connector.queries)


def test_mssql_run_state_storage_roundtrips_model() -> None:
    started = datetime.now(UTC)
    connector = FakeMSSQLConnector()
    connector.records = [
        [{"column_name": column} for column in RUN_STATE_CONTRACT.columns],
        [
            {
                "id": 1,
                "run_state_key": b"1" * 32,
                "dag_id": "dag",
                "process_name": "dbo.orders->landing.orders",
                "source_schema": "dbo",
                "source_table": "orders",
                "target_schema": "landing",
                "target_table": "orders",
                "load_strategy": "full_refresh",
                "execution_date": started,
                "state": "success",
                "started_at": started,
                "ended_at": started,
                "duration_min": 1.0,
                "error_message": None,
                "rows_read": 10,
                "rows_written": 10,
                "rows_updated": 0,
                "rows_deleted": 0,
            }
        ],
    ]
    storage = MSSQLRunStateStorage(connector, schema="state", table="runs")
    state = RunState(
        dag_id="dag",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy="full_refresh",
        execution_date=started,
        state=RunStateStatus.SUCCESS,
        started_at=started,
        ended_at=started,
        rows_read=10,
        rows_written=10,
    )

    storage.save_run_state(state)
    loaded = storage.get_run_state("dag", started)

    assert loaded is not None
    assert loaded.dag_id == "dag"
    assert loaded.rows_written == 10


def test_mssql_load_audit_storage_uses_canonical_dpone_loads_table() -> None:
    now = datetime.now(UTC)
    connector = FakeMSSQLConnector()
    storage = MSSQLLoadAuditStorage(connector, schema="state", table="__dpone__loads")
    record = LoadAuditRecord(
        run_id="01HY6G9SKW8S7TE4R97X1E2J3K",
        load_id="01HY6G9SKW8S7TE4R97X1E2J3M",
        status="failed",
        process_name="orders",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        strategy="cdc_apply",
        started_at=now,
        failed_at=now,
        error_message="boom",
    )

    storage.record_load_failed(record)

    rendered = "\n".join(query for query, *_ in connector.queries)
    assert "[state].[__dpone__loads]" in rendered
    assert "__dpone__loaded_at" in rendered
    assert "meta__" not in rendered
    assert "MERGE [state].[__dpone__loads]" in rendered
    assert connector.queries[-1][1][0] == record.load_id
    assert connector.queries[-1][1][1] == "failed"


def test_state_factory_creates_mssql_load_audit_storage_with_injected_connector() -> None:
    connector = FakeMSSQLConnector()

    storage = StateFactory.create_mssql_load_audit_storage(mssql_connector=connector, schema="state")

    assert isinstance(storage, MSSQLLoadAuditStorage)
    assert storage.connector is connector
    assert storage.schema == "state"
    assert storage.table == "__dpone__loads"


def test_mssql_sink_incremental_merge_default_uses_delete_insert_pattern() -> None:
    class FakeConnector(FakeMSSQLConnector):
        def __init__(self):
            super().__init__()
            self.began = False

        def begin(self):
            self.began = True

        def commit_transaction(self):
            pass

        def rollback(self):
            pass

        def table_exists(self, schema, table):
            return True

        def get_records(self, query, params=None, as_dict=False):
            rendered = str(query)
            if "sys.columns AS c" in rendered and "sys.types AS ty" in rendered:
                return _catalog_rows(id_nullable=False)
            if "sys.indexes AS i" in rendered:
                return [{"index_id": 1, "filter_definition": None, "column_name": "id", "key_ordinal": 1}]
            return super().get_records(query, params, as_dict)

    class Artifact:
        estimated_rows = 1

        def materialize(self, staging_manager, load_config, schema):
            return SimpleNamespace(
                schema="stg",
                table="orders_stg",
                columns=["id", "amount", "__dpone__xmin"],
                row_count=1,
                cleanup=lambda: None,
            )

        def cleanup(self):
            pass

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key="id",
    )
    sink = _governed_unit_sink(FakeConnector())
    result = sink.load(
        cfg,
        _governed_unit_payload(
            cfg,
            Artifact(),
            [("id", "bigint"), ("amount", "numeric(18,2)"), ("__dpone__xmin", "bigint")],
        ),
    )

    joined = "\n".join(query for query, *_ in sink.connector.queries)
    assert result.inserted_rows == 1
    assert "DELETE t FROM [landing].[orders] AS t" in joined
    assert "CREATE TABLE [landing].[orders__dpone_shadow_" not in joined
    assert "INSERT INTO [landing].[orders]" in joined
    assert "EXEC sp_rename" not in joined
    target_sql = "\n".join(query for query, *_ in sink.connector.queries if "[landing].[orders]" in query)
    assert "__dpone__xmin" not in target_sql


def test_mssql_full_refresh_truncate_insert_preserves_existing_target_object() -> None:
    class FakeConnector(FakeMSSQLConnector):
        def begin(self):
            pass

        def commit_transaction(self):
            pass

        def rollback(self):
            pass

        def table_exists(self, schema, table):
            return table == "orders"

        def get_records(self, query, params=None, as_dict=False):
            rendered = str(query)
            if "sys.columns AS c" in rendered and "sys.types AS ty" in rendered:
                return _catalog_rows(id_nullable=True)
            return super().get_records(query, params, as_dict)

    class Artifact:
        estimated_rows = 2

        def materialize(self, staging_manager, load_config, schema):
            return SimpleNamespace(
                schema="staging",
                table="orders_stg",
                columns=["id", "amount"],
                row_count=2,
                cleanup=lambda: None,
            )

        def cleanup(self):
            pass

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )
    sink = _governed_unit_sink(FakeConnector())

    result = sink.load(
        cfg,
        _governed_unit_payload(
            cfg,
            Artifact(),
            [("id", "bigint"), ("amount", "numeric(18,2)")],
        ),
    )

    joined = "\n".join(query for query, *_ in sink.connector.queries)
    assert result.staging_rows == 2
    assert "TRUNCATE TABLE [landing].[orders]" in joined
    assert "INSERT INTO [landing].[orders] WITH (TABLOCK)" in joined
    assert "__dpone_shadow_" not in joined
    assert "sp_rename" not in joined


def test_mssql_incremental_merge_shadow_swap_fails_before_materialization() -> None:
    class FakeConnector(FakeMSSQLConnector):
        def begin(self):
            pass

        def commit_transaction(self):
            pass

        def rollback(self):
            pass

        def table_exists(self, schema, table):
            return True

        def get_records(self, query, params=None, as_dict=False):
            rendered = str(query)
            if "sys.columns AS c" in rendered and "sys.types AS ty" in rendered:
                return _catalog_rows(id_nullable=False)
            if "sys.indexes AS i" in rendered:
                return [{"index_id": 1, "filter_definition": None, "column_name": "id", "key_ordinal": 1}]
            return super().get_records(query, params, as_dict)

    class Artifact:
        estimated_rows = 2

        def materialize(self, staging_manager, load_config, schema):
            return SimpleNamespace(
                schema="staging",
                table="orders_stg",
                columns=["id", "amount"],
                row_count=2,
                cleanup=lambda: None,
            )

        def cleanup(self):
            pass

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key="id",
        merge_policy="shadow_swap",
    )
    sink = MSSQLSink(FakeConnector())

    with pytest.raises(MSSQLStrategyContractError) as raised:
        sink.load(cfg, LoadPayload(artifact=Artifact(), schema=[("id", "bigint"), ("amount", "numeric(18,2)")]))

    joined = "\n".join(query for query, *_ in sink.connector.queries)
    assert raised.value.blocker == "mssql.strategy.incremental_merge.shadow_swap_physical_preservation"
    assert joined == ""


def test_mssql_full_refresh_truncate_insert_uses_three_part_target_database() -> None:
    class FakeConnector(FakeMSSQLConnector):
        def begin(self):
            pass

        def commit_transaction(self):
            pass

        def rollback(self):
            pass

        def table_exists(self, schema, table, database=None):
            return table == "sample_web_sync"

        def qualified_name(self, schema: str, table: str, *, database: str | None = None) -> str:
            return MSSQLObjectName.from_parts(schema=schema, table=table, database=database).quoted()

        def get_records(self, query, params=None, as_dict=False):
            rendered = str(query)
            if "sys.columns AS c" in rendered and "sys.types AS ty" in rendered:
                return _catalog_rows(id_nullable=True, amount=False)
            return super().get_records(query, params, as_dict)

    class Artifact:
        estimated_rows = 2

        def materialize(self, staging_manager, load_config, schema):
            return SimpleNamespace(
                schema="DWH_Stage",
                table="sample_web_sync_stg",
                database="dwh_example",
                columns=["id"],
                row_count=2,
                cleanup=lambda: None,
            )

        def cleanup(self):
            pass

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="marketing_datamarts",
        source_table="sample_web_sync",
        target_schema="dwh_example.marketing",
        target_table="sample_web_sync",
        target_database="dwh_example",
        staging_schema="DWH_Stage",
        staging_database="dwh_example",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )
    sink = _governed_unit_sink(FakeConnector())

    sink.load(cfg, _governed_unit_payload(cfg, Artifact(), [("id", "bigint")]))

    joined = "\n".join(query for query, *_ in sink.connector.queries)
    assert "TRUNCATE TABLE [dwh_example].[marketing].[sample_web_sync]" in joined
    assert "INSERT INTO [dwh_example].[marketing].[sample_web_sync] WITH (TABLOCK)" in joined
    assert "__dpone_shadow_" not in joined
    assert "sp_rename" not in joined


def test_mssql_clickhouse_preserve_offset_projection_adds_generated_column() -> None:
    class ClickHouseConnector:
        pass

    source = FakeMSSQLConnector()
    strategy = MSSQLFullExtractStrategy(connector=source, logger=None, sink_connector=ClickHouseConnector())
    codec = ClickHouseTabSeparatedCodec(
        MssqlClickHouseTypePolicy.from_config({"temporal": {"offset_timestamp": {"mode": "preserve_offset"}}})
    )

    query = strategy._wrap_clickhouse_tabseparated_query(
        "SELECT [offset_at] FROM [dbo].[orders]",
        [("offset_at", "datetimeoffset(7)")],
        codec,
    )

    assert "AS [__dpone__tz_offset_minutes__offset_at]" in query
    assert "DATEPART(TZOFFSET" in query
