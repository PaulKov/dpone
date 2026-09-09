from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from dpone.runtime.artifacts import (
    BatchedFileExportArtifact,
    FileExportArtifact,
    GCSExportArtifact,
    InMemoryRowsArtifact,
    InternalQueryArtifact,
    PartitionedFileExportArtifact,
    StagingManager,
    StagingTableArtifact,
    StreamingRowsArtifact,
)
from dpone.runtime.etl_logging.etl_logger import ETLLogger, ETLMetrics, LogLevel
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome


class FakeStagingManager(StagingManager):
    def __init__(self, *, fail_on_file: bool = False) -> None:
        self.fail_on_file = fail_on_file
        self.created: list[tuple[Any, Sequence[tuple[str, str]]]] = []
        self.inserted_rows: list[list[Mapping[str, object]]] = []
        self.loaded_files: list[str] = []
        self.loaded_gcs: list[str] = []
        self.queries: list[tuple[str, Sequence[Any] | None]] = []
        self.dropped: list[str] = []

    def create(self, load_config: Any, schema: Sequence[tuple[str, str]]) -> StagingTableArtifact:
        self.created.append((load_config, schema))
        return StagingTableArtifact(
            schema="staging",
            table=f"tmp_{len(self.created)}",
            columns=[name for name, _type in schema],
            staging_manager=self,
            target_schema="target",
        )

    def insert_rows(self, artifact: StagingTableArtifact, rows: Iterable[Mapping[str, object]]) -> int:
        materialized = list(rows)
        self.inserted_rows.append(materialized)
        return len(materialized)

    def insert_from_query(
        self,
        artifact: StagingTableArtifact,
        query: str,
        schema: Sequence[tuple[str, str]],
        params: Sequence[Any] | None = None,
    ) -> int:
        self.queries.append((query, params))
        return 7

    def load_from_file(self, artifact: StagingTableArtifact, file_artifact: FileExportArtifact) -> int:
        if self.fail_on_file:
            raise RuntimeError("load failed")
        self.loaded_files.append(file_artifact.file_path)
        return 3

    def load_from_file_batched(self, artifact: StagingTableArtifact, file_artifact: FileExportArtifact) -> int:
        if self.fail_on_file:
            raise RuntimeError("batched load failed")
        self.loaded_files.append(file_artifact.file_path)
        return 5

    def load_from_gcs_artifact(self, artifact: StagingTableArtifact, gcs_artifact: GCSExportArtifact) -> int:
        self.loaded_gcs.append(gcs_artifact.gcs_uri)
        return 11

    def drop(self, artifact: StagingTableArtifact) -> None:
        self.dropped.append(artifact.qualified_name())


def test_staging_table_artifact_qualifies_and_cleans_up() -> None:
    manager = FakeStagingManager()
    artifact = StagingTableArtifact("staging", "tmp_orders", ["id"], manager)

    assert artifact.qualified_name() == "staging.tmp_orders"

    artifact.cleanup()

    assert manager.dropped == ["staging.tmp_orders"]


def test_in_memory_streaming_and_query_artifacts_materialize_to_staging() -> None:
    manager = FakeStagingManager()
    schema = [("id", "STRING")]
    load_config = object()

    memory = InMemoryRowsArtifact([{"id": "1"}, {"id": "2"}])
    memory_handle = memory.materialize(manager, load_config, schema)

    assert memory.estimated_rows == 2
    assert memory_handle.row_count == 2
    assert manager.inserted_rows[0] == [{"id": "1"}, {"id": "2"}]

    cleanup_calls: list[str] = []
    streaming = StreamingRowsArtifact(
        iter([{"id": "3"}, {"id": "4"}, {"id": "5"}]),
        batch_size=2,
        estimated_rows=3,
        cleanup_callback=lambda: cleanup_calls.append("stream"),
    )
    streaming_handle = streaming.materialize(manager, load_config, schema)

    assert streaming.estimated_rows == 3
    assert streaming_handle.row_count == 3
    assert manager.inserted_rows[1:] == [[{"id": "3"}, {"id": "4"}], [{"id": "5"}]]
    assert cleanup_calls == []
    streaming.terminate(ArtifactTerminalOutcome.SUCCESS)
    assert cleanup_calls == ["stream"]

    query = InternalQueryArtifact("select * from source", estimated_rows=7, params=("p1",))
    query_handle = query.materialize(manager, load_config, schema)

    assert query.query == "select * from source"
    assert query.params == ("p1",)
    assert query.estimated_rows == 7
    assert query_handle.row_count == 7
    assert manager.queries == [("select * from source", ("p1",))]


def test_file_and_gcs_artifacts_materialize_and_cleanup(tmp_path: Path) -> None:
    manager = FakeStagingManager()
    schema = [("id", "STRING")]
    source_file = tmp_path / "orders.csv"
    source_file.write_text("id\n1\n", encoding="utf-8")

    file_artifact = FileExportArtifact(str(source_file), ["id"], compressed=True, estimated_rows=3)
    file_handle = file_artifact.materialize(manager, object(), schema)

    assert file_artifact.compressed is True
    assert file_artifact.format == "csv"
    assert file_artifact.estimated_rows == 3
    assert file_handle.row_count == 3
    assert manager.loaded_files == [str(source_file)]

    file_artifact.cleanup()

    assert not source_file.exists()

    gcs_artifact = GCSExportArtifact(
        "gs://demo-bucket/export/orders/",
        ["id"],
        format="PARQUET",
        pattern="*.parquet",
        hive_partitioning=True,
        new_partitions=["2026-01-01"],
        cleanup_gcs=False,
        estimated_rows=11,
    )
    gcs_handle = gcs_artifact.materialize(manager, object(), schema)
    gcs_artifact.cleanup()

    assert gcs_artifact.format == "parquet"
    assert gcs_artifact.pattern == "*.parquet"
    assert gcs_artifact.hive_partitioning is True
    assert gcs_artifact.new_partitions == ["2026-01-01"]
    assert gcs_artifact.estimated_rows == 11
    assert gcs_handle.row_count == 11
    assert manager.loaded_gcs == ["gs://demo-bucket/export/orders/"]


def test_batched_file_export_artifact_retains_batches_until_terminal_success(tmp_path: Path) -> None:
    manager = FakeStagingManager()
    files = [tmp_path / "batch1.csv", tmp_path / "batch2.csv"]
    for file_path in files:
        file_path.write_text("id\n1\n", encoding="utf-8")

    def batches() -> Iterable[FileExportArtifact]:
        for file_path in files:
            yield FileExportArtifact(str(file_path), ["id"])

    artifact = BatchedFileExportArtifact(
        batch_generator=batches,
        columns=["id"],
        batch_size=100,
        compressed=False,
        estimated_rows=10,
    )

    handle = artifact.materialize(manager, object(), [("id", "STRING")])

    assert artifact.columns == ["id"]
    assert artifact.batch_size == 100
    assert artifact.estimated_rows == 10
    assert handle.row_count == 10
    assert manager.loaded_files == [str(files[0]), str(files[1])]
    assert all(file_path.exists() for file_path in files)

    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)

    assert all(not file_path.exists() for file_path in files)


def test_batched_file_export_artifact_drops_staging_on_failure(tmp_path: Path) -> None:
    manager = FakeStagingManager(fail_on_file=True)
    source_file = tmp_path / "batch.csv"
    source_file.write_text("id\n1\n", encoding="utf-8")

    artifact = BatchedFileExportArtifact(
        batch_generator=lambda: iter([FileExportArtifact(str(source_file), ["id"])]),
        columns=["id"],
        batch_size=100,
    )

    with pytest.raises(RuntimeError, match="batched load failed"):
        artifact.materialize(manager, object(), [("id", "STRING")])

    assert manager.dropped == ["staging.tmp_1"]
    assert source_file.exists()

    artifact.terminate(ArtifactTerminalOutcome.ABORT)

    assert not source_file.exists()


def test_partitioned_file_export_artifact_retains_parts_until_terminal_success(tmp_path: Path) -> None:
    manager = FakeStagingManager()
    files = [tmp_path / "part1.bcp", tmp_path / "part2.bcp", tmp_path / "part3.bcp"]
    for file_path in files:
        file_path.write_text("1\talpha\n", encoding="utf-8")

    artifact = PartitionedFileExportArtifact(
        partitions=[
            FileExportArtifact(str(file_path), ["id", "name"], format="mssql-delimited") for file_path in files
        ],
        columns=["id", "name"],
        max_workers=2,
        estimated_rows=9,
    )

    handle = artifact.materialize(manager, object(), [("id", "int"), ("name", "varchar(20)")])

    assert artifact.estimated_rows == 9
    assert handle.row_count == 9
    assert manager.loaded_files == [str(file_path) for file_path in files]
    assert all(file_path.exists() for file_path in files)

    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)

    assert all(not file_path.exists() for file_path in files)


def test_etl_metrics_duration_and_rate_contracts() -> None:
    start = datetime(2026, 1, 1, 0, 0, 0)
    metrics = ETLMetrics(start_time=start, end_time=start + timedelta(seconds=4), loaded_rows=20)

    assert metrics.duration == timedelta(seconds=4)
    assert metrics.rows_per_second == 5

    open_metrics = ETLMetrics(start_time=start, loaded_rows=20)

    assert open_metrics.duration is None
    assert open_metrics.rows_per_second is None


def test_etl_logger_formats_strategy_messages_tables_and_details() -> None:
    logger = ETLLogger(show_colors=False)

    assert logger._build_strategy_string("full_refresh") == "FULL_REFRESH"
    assert logger._build_strategy_string("incremental_merge") == "INCREMENTAL_MERGE"
    assert logger._build_strategy_string("incremental_append") == "INCREMENTAL_APPEND"
    assert logger._build_strategy_string("replace") == "REPLACE"
    assert logger._build_strategy_string("custom") == "CUSTOM"
    assert logger._format_strategy_with_color("incremental") == "INCREMENTAL"

    table_lines = logger._format_table(
        [{"Name": "orders", "Rows": 10}, {"Name": "users", "Extra": "late"}],
        title="TABLE",
    )

    assert any("TABLE" in line for line in table_lines)
    assert any("orders" in line for line in table_lines)
    assert any("Extra" in line for line in table_lines)
    assert logger._format_table([]) == []

    message_lines = logger._format_message_lines(
        LogLevel.INFO,
        "hello",
        {
            "First": "value",
            "Nested": {"a": 1},
            "Items": ["x", "y"],
            "EMPTY_LINE": "",
            "TABLE_LINE_2": "second",
            "TABLE_LINE_1": "first",
        },
    )

    assert "ℹ️ hello" in message_lines
    assert "   First: value" in message_lines
    assert "      • a: 1" in message_lines
    assert "   Items: x, y" in message_lines
    assert message_lines.index("first") < message_lines.index("second")


def test_etl_logger_public_methods_update_metrics_and_emit_records(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = ETLLogger(show_colors=False)
    calls: list[tuple[LogLevel, str, dict[str, Any] | None]] = []

    monkeypatch.setattr(
        logger, "_log_with_format", lambda level, message, details=None: calls.append((level, message, details))
    )

    logger.log_etl_start(
        {
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dwh",
            "target_table": "orders",
            "load_strategy": "incremental",
            "batch_size": 1000,
        }
    )
    logger.warning("warn")
    logger.log_batch_progress(batch_num=2, total_batches=4, batch_size=100, processed_rows=200)
    logger.log_etl_error("boom", {"step": "load"})
    logger.log_etl_progress("load", {"rows": 10})
    logger.log_data_sample("rows", [{"id": 1}, {"id": 2}], max_rows=1)
    logger.log_connection_info("postgres", "localhost", 5432, "demo")
    logger.log_connection_info("postgres", "localhost", 5432, "demo")
    logger.log_proxy_initialization("secret/proxy", "corp-proxy")
    logger.log_performance_metrics({"duration": 1.2345, "rows": 1000, "rate": 12.345, "label": "ok"})
    logger.log_sql_query(" select 1 ")
    logger.log_data_quality_check("not_null", {"status": "passed"})
    logger.log_data_quality_check("freshness", {"status": "failed"})
    logger.log_state_change("xmin", 1, 2)
    logger.log_xmin_state_info("saved", {"value": 2})
    logger.log_data_loader_info("loaded", {"rows": 10})
    logger.log_run_state_info("updated", {"state": "success"})
    logger.log_etl_end(
        {
            "status": "failed",
            "extracted_rows": 10,
            "staging_rows": 10,
            "loaded_rows": 9,
            "final_rows": 9,
            "inserted_rows": 9,
            "errors": ["bad row"],
        },
        additional_info={
            "data_flow_explanation": {
                "extracted_from": "source",
                "loaded_to_staging": "staging",
                "final_target": "target",
                "strategy": "incremental",
                "deduplication": "unique key",
            }
        },
    )

    assert logger.metrics is not None
    assert logger.metrics.warnings_count == 1
    assert logger.metrics.error_count == 1
    assert logger.metrics.batch_count == 2
    assert logger.metrics.loaded_rows == 9
    assert [call[1] for call in calls].count("CONNECTION ESTABLISHED: POSTGRES") == 1
    assert any(call[1] == "ETL PROCESS FAILED" for call in calls)
    assert any(call[1] == "DATA QUALITY CHECK: NOT_NULL" and call[0] == LogLevel.SUCCESS for call in calls)
    assert any(call[1] == "DATA QUALITY CHECK: FRESHNESS" and call[0] == LogLevel.WARNING for call in calls)


class FakeClickHouseCounter:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts

    def count_rows_by_date(
        self,
        *,
        schema: str,
        table: str,
        date_column: str,
        partition_date: str,
        partition_by: str,
    ) -> int:
        return self.counts[partition_date]


def test_etl_logger_clickhouse_validation_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = ETLLogger(show_colors=False)
    calls: list[tuple[LogLevel, str, dict[str, Any] | None]] = []
    monkeypatch.setattr(
        logger, "_log_with_format", lambda level, message, details=None: calls.append((level, message, details))
    )
    logger.metrics = ETLMetrics(start_time=datetime.now())

    assert (
        logger._validate_single_partition(
            "2026-01-01",
            FakeClickHouseCounter({"2026-01-01": 10}),
            "src",
            "orders",
            "created_at",
            "day",
            10,
        )
        is True
    )

    logger._validate_clickhouse_rows(
        {"status": "success"},
        {
            "source_connector": FakeClickHouseCounter({"2026-01-01": 10, "2026-01-02": 8}),
            "source_schema": "src",
            "source_table": "orders",
            "date_column": "created_at",
            "partition_by": "day",
            "partitions": ["2026-01-01", "2026-01-02"],
            "final_count": 18,
            "partition_validation_results": {
                "2026-01-01": {"target_count": 10, "match": True},
                "2026-01-02": {"target_count": 8, "match": True},
            },
        },
        {},
    )

    assert any(call[1] == "PARTITION VALIDATION: 2026-01-01" for call in calls)
    assert any(call[1] == "VALIDATION SUMMARY BY DATE" for call in calls)
