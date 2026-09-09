from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import FileExportArtifact, PartitionedFileExportArtifact, StagingTableArtifact
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.partitioning import RangePartitioner
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.strategies.mssql.mssql_strategies import MSSQLFullRefreshStrategy
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy


class CapturingLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, message: str) -> None:
        del message

    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        self.events.append((event, payload))


def test_range_partitioner_builds_spark_style_half_open_ranges() -> None:
    partitions = RangePartitioner.from_options(
        {
            "partition_column": "id",
            "lower_bound": 1,
            "upper_bound": 10,
            "num_partitions": 3,
        }
    ).partitions()

    assert [partition.lower_bound for partition in partitions] == [1, 4, 7]
    assert [partition.upper_bound for partition in partitions] == [4, 7, 10]
    assert [partition.include_upper for partition in partitions] == [False, False, True]
    assert partitions[-1].predicate('"id"') == '"id" >= 7 AND "id" <= 10'


def test_range_partitioner_auto_bounds_cover_full_numeric_source_range() -> None:
    partitions = RangePartitioner.from_options(
        {
            "partitioning": {
                "strategy": "auto",
                "column": "order_id",
                "bounds": "auto",
                "target_rows_per_partition": 2500,
                "max_partitions": 64,
                "export_workers": 2,
                "load_workers": 2,
            }
        },
        bounds_resolver=lambda _column: (1, 10000, 10000),
    ).partitions()

    assert [(partition.lower_bound, partition.upper_bound, partition.include_upper) for partition in partitions] == [
        (1, 2501, False),
        (2501, 5001, False),
        (5001, 7501, False),
        (7501, 10000, True),
    ]


def test_mssql_full_extract_uses_partitioned_bcp_queryout(tmp_path: Path) -> None:
    class FakeConnector:
        bcp_path = "bcp"
        trust_server_certificate = "yes"

        def __init__(self) -> None:
            self.queries: list[tuple[str, str]] = []

        def fetch_schema(self, schema: str, table: str):
            assert (schema, table) == ("dbo", "orders")
            return [("id", "int"), ("name", "varchar(20)")]

        def build_select_query(self, schema: str, table: str, columns: list[str]) -> str:
            return f"SELECT {', '.join(f'[{column}]' for column in columns)} FROM [{schema}].[{table}]"

        def quote_identifier(self, name: str) -> str:
            return f"[{name}]"

        def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
            del options
            self.queries.append((query, output_path))
            Path(output_path).write_text("1\talpha\n", encoding="utf-8")
            return 1

    config = LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="dbo",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "partition_column": "id",
            "lower_bound": 1,
            "upper_bound": 10,
            "num_partitions": 3,
            "partition_tmp_dir": str(tmp_path),
        },
    )

    connector = FakeConnector()
    extract = MSSQLFullExtractStrategy(connector, CapturingLogger(), sink_connector=object()).extract(config, None)

    assert isinstance(extract.artifact, PartitionedFileExportArtifact)
    assert extract.artifact.estimated_rows == 3
    assert len(connector.queries) == 3
    queries = [query for query, _path in connector.queries]
    assert any("WHERE [id] >= 1 AND [id] < 4" in query for query in queries)
    assert any("WHERE [id] >= 7 AND [id] <= 10" in query for query in queries)

    extract.artifact.cleanup()


def test_mssql_full_extract_can_return_lazy_pipelined_transfer_plan(tmp_path: Path) -> None:
    class FakeConnector:
        bcp_path = "bcp"
        trust_server_certificate = "yes"

        def __init__(self) -> None:
            self.queries: list[tuple[str, str]] = []

        def fetch_schema(self, schema: str, table: str):
            assert (schema, table) == ("dbo", "orders")
            return [("id", "int"), ("name", "varchar(20)")]

        def build_select_query(self, schema: str, table: str, columns: list[str]) -> str:
            return f"SELECT {', '.join(f'[{column}]' for column in columns)} FROM [{schema}].[{table}]"

        def quote_identifier(self, name: str) -> str:
            return f"[{name}]"

        def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
            del options
            self.queries.append((query, output_path))
            Path(output_path).write_text("1\talpha\n", encoding="utf-8")
            return 1

    config = LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="dbo",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "sink_type": "clickhouse",
            "partition_column": "id",
            "lower_bound": 1,
            "upper_bound": 5,
            "num_partitions": 2,
            "runtime_storage": {"work_dir": str(tmp_path)},
            "native_transfer": {
                "execution": {
                    "mode": "pipelined",
                    "resource_policy": {
                        "target_file_bytes": "1B",
                        "max_file_bytes": "1MiB",
                        "min_slice_rows": 1,
                        "max_slice_rows": 1,
                    },
                }
            },
        },
    )

    connector = FakeConnector()
    extract = MSSQLFullExtractStrategy(connector, CapturingLogger(), sink_connector=object()).extract(config, None)

    assert isinstance(extract.artifact, PartitionedTransferPlanArtifact)
    assert connector.queries == []
    assert extract.artifact.load_with(lambda file_artifact: Path(file_artifact.file_path).exists() and 1) == 4
    assert len(connector.queries) == 4
    assert not list(tmp_path.glob("*.bcp"))


def test_mssql_lazy_transfer_plan_uses_configured_object_transfer_store(tmp_path: Path) -> None:
    class FakeConnector:
        bcp_path = "bcp"
        trust_server_certificate = "yes"

        def fetch_schema(self, schema: str, table: str):
            assert (schema, table) == ("dbo", "orders")
            return [("id", "int"), ("name", "varchar(20)")]

        def build_select_query(self, schema: str, table: str, columns: list[str]) -> str:
            return f"SELECT {', '.join(f'[{column}]' for column in columns)} FROM [{schema}].[{table}]"

        def quote_identifier(self, name: str) -> str:
            return f"[{name}]"

        def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
            del query, options
            Path(output_path).write_text("1\talpha\n", encoding="utf-8")
            return 1

    config = LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="dbo",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "sink_type": "clickhouse",
            "partition_column": "id",
            "lower_bound": 1,
            "upper_bound": 5,
            "num_partitions": 2,
            "runtime_storage": {
                "work_dir": str(tmp_path / "scratch"),
                "transfer_store": {
                    "type": "local",
                    "uri": "s3://dpone-stage/native-transfer",
                    "local_root_dir": str(tmp_path / "object-store"),
                },
            },
            "native_transfer": {"execution": {"mode": "pipelined"}},
        },
    )

    extract = MSSQLFullExtractStrategy(FakeConnector(), CapturingLogger(), sink_connector=object()).extract(
        config, None
    )

    assert isinstance(extract.artifact, PartitionedTransferPlanArtifact)
    assert extract.artifact.transfer_store is not None
    assert extract.artifact.load_with(lambda file_artifact: Path(file_artifact.file_path).exists() and 1) == 2
    assert [item["transfer_object"]["uri"] for item in extract.artifact.slice_evidence] == [
        "s3://dpone-stage/native-transfer/dpone/dbo.orders/p0000_s0000.bcp",
        "s3://dpone-stage/native-transfer/dpone/dbo.orders/p0001_s0000.bcp",
    ]
    assert not list((tmp_path / "scratch").glob("*.bcp"))


def test_mssql_full_refresh_direct_bcp_loads_partitioned_artifacts(tmp_path: Path) -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def table_exists(self, schema: str, table: str) -> bool:
            assert (schema, table) == ("dbo", "orders")
            return True

        def execute_query(self, query: str, params=None) -> int:
            del params
            self.queries.append(query)
            if str(query).startswith("INSERT INTO [dbo].[orders__dpone_shadow_"):
                return 6
            return 0

        def quote_identifier(self, name: str) -> str:
            return f"[{name}]"

        def qualified_name(self, schema: str, table: str) -> str:
            return f"[{schema}].[{table}]"

        def get_records(self, query: str, params=None, as_dict: bool = False):
            del params
            self.queries.append(query)
            if "@@ROWCOUNT" in query:
                assert as_dict is True
                return [{"__dpone__affected_rows": 6}]
            return [(6,)]

        def begin(self) -> None:
            self.queries.append("BEGIN")

        def commit_transaction(self) -> None:
            self.queries.append("COMMIT")

        def rollback(self) -> None:
            self.queries.append("ROLLBACK")

    class FakeStagingManager:
        def __init__(self) -> None:
            self.loaded: list[tuple[str, str, str]] = []

        def create(self, load_config, schema):
            return StagingTableArtifact("staging", "tmp_orders", [name for name, _ in schema], self)

        def load_from_file(self, artifact: StagingTableArtifact, file_artifact: FileExportArtifact) -> int:
            self.loaded.append((artifact.schema, artifact.table, file_artifact.file_path))
            return 3

        def load_file_into_table(self, schema: str, table: str, file_artifact: FileExportArtifact) -> int:
            self.loaded.append((schema, table, file_artifact.file_path))
            return 3

        def drop(self, artifact: StagingTableArtifact) -> None:
            del artifact

    class DirectStagingConsumer:
        """Exercise staging mechanics without the higher-level governance service."""

        def __init__(self, strategy: MSSQLFullRefreshStrategy) -> None:
            self._strategy = strategy

        def consume(self, load_config, payload, handler):
            staging_artifact = self._strategy._materialize(load_config, payload)  # noqa: SLF001
            try:
                return handler(staging_artifact)
            finally:
                staging_artifact.cleanup()

    files = [tmp_path / "part1.bcp", tmp_path / "part2.bcp"]
    for file_path in files:
        file_path.write_text("1\talpha\n", encoding="utf-8")
    artifact = PartitionedFileExportArtifact(
        [FileExportArtifact(str(file_path), ["id", "name"], format="mssql-delimited") for file_path in files],
        columns=["id", "name"],
        max_workers=1,
    )
    config = LoadConfig(
        source_conn_id="postgres",
        target_conn_id="mssql",
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )
    staging = FakeStagingManager()

    result = MSSQLFullRefreshStrategy(
        FakeConnector(),
        CapturingLogger(),
        staging,
        staging_consumer_factory=DirectStagingConsumer,
    ).load(
        config,
        LoadPayload(artifact=artifact, schema=[("id", "int"), ("name", "varchar(20)")]),
    )

    assert result.inserted_rows == 6
    assert result.total_rows == 6
    assert staging.loaded == [("staging", "tmp_orders", str(files[0])), ("staging", "tmp_orders", str(files[1]))]
    assert all(file_path.exists() for file_path in files)
    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)
    assert all(not file_path.exists() for file_path in files)
