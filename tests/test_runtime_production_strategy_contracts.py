from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload, LoadResult
from dpone.runtime.sinks.bigquery import BigQuerySink
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sinks.postgres import PostgresSink
from dpone.runtime.sinks.strategies.backfill import BackfillStrategy
from dpone.runtime.sinks.strategies.base import SinkStrategy
from dpone.runtime.sources.base import ExtractResult


def _cfg(strategy: LoadStrategy = LoadStrategy.SNAPSHOT_DIFF, **options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=strategy,
        unique_key=["id"],
        options=options,
    )


def test_strategy_metadata_enricher_adds_row_hash_for_snapshot_diff() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}]),
        schema=[("id", "bigint"), ("name", "text")],
    )

    enriched = StrategyMetadataEnricher().enrich_payload(
        payload,
        load_config=_cfg(),
        effective_at=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
    )

    row = list(enriched.artifact._rows)[0]
    assert len(row["__dpone__row_hash"]) == 64
    assert ("__dpone__row_hash", "varchar(64)") in enriched.schema


def test_strategy_metadata_enricher_adds_scd2_validity_columns() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}]),
        schema=[("id", "bigint"), ("name", "text")],
    )

    enriched = StrategyMetadataEnricher().enrich_payload(
        payload,
        load_config=_cfg(LoadStrategy.SCD2),
        effective_at=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
    )

    row = list(enriched.artifact._rows)[0]
    assert len(row["__dpone__row_hash"]) == 64
    assert row["__dpone__valid_from_at"] == "2026-06-04T12:00:00+00:00"
    assert row["__dpone__valid_to_at"] is None
    assert row["__dpone__is_current"] is True
    assert ("__dpone__valid_from_at", "timestamp") in enriched.schema
    assert ("__dpone__valid_to_at", "timestamp") in enriched.schema
    assert ("__dpone__is_current", "boolean") in enriched.schema


@pytest.mark.parametrize(
    "strategy",
    [LoadStrategy.FULL_REFRESH, LoadStrategy.INCREMENTAL_MERGE, LoadStrategy.BACKFILL],
)
def test_strategy_metadata_enricher_is_noop_for_non_diff_history_strategies(strategy: LoadStrategy) -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 1}]),
        schema=[("id", "bigint")],
    )

    enriched = StrategyMetadataEnricher().enrich_payload(payload, load_config=_cfg(strategy))

    assert enriched is payload


def test_strategy_metadata_enricher_rewrites_csv_file_export_for_scd2(tmp_path: Path) -> None:
    path = tmp_path / "orders.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["1", "Ada"])
    payload = LoadPayload(
        artifact=FileExportArtifact(file_path=str(path), columns=["id", "name"], format="csv"),
        schema=[("id", "bigint"), ("name", "text")],
    )

    enriched = StrategyMetadataEnricher().enrich_payload(
        payload,
        load_config=_cfg(LoadStrategy.SCD2),
        effective_at=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
    )

    assert isinstance(enriched.artifact, FileExportArtifact)
    assert "__dpone__row_hash" in enriched.artifact.columns
    assert "__dpone__is_current" in enriched.artifact.columns
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    assert len(rows) == 1
    assert len(rows[0]) == len(enriched.artifact.columns)
    by_name = dict(zip(enriched.artifact.columns, rows[0], strict=True))
    assert len(by_name["__dpone__row_hash"]) == 64
    assert by_name["__dpone__is_current"] == "true"
    assert by_name["__dpone__valid_from_at"] == "2026-06-04T12:00:00+00:00"
    assert by_name["__dpone__valid_to_at"] == ""


def test_strategy_metadata_enricher_never_guesses_header_from_first_data_row(tmp_path: Path) -> None:
    path = tmp_path / "header-shaped-data.csv"
    path.write_text("id,name\n2,Ada\n", encoding="utf-8")
    artifact = FileExportArtifact(
        file_path=str(path),
        columns=["id", "name"],
        format="csv",
        rows_exported=2,
    )

    enriched = StrategyMetadataEnricher().enrich_payload(
        LoadPayload(artifact=artifact, schema=[("id", "text"), ("name", "text")]),
        load_config=_cfg(),
        effective_at=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
    )

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    assert [row[:2] for row in rows] == [["id", "name"], ["2", "Ada"]]
    assert enriched.artifact.rows_exported == 2


def test_strategy_metadata_enricher_preserves_blank_first_tab_record(tmp_path: Path) -> None:
    path = tmp_path / "blank-first.tsv"
    path.write_text("\nvalue\n", encoding="utf-8")
    artifact = FileExportArtifact(
        file_path=str(path),
        columns=["value"],
        format="mssql-delimited",
        rows_exported=2,
    )

    enriched = StrategyMetadataEnricher().enrich_payload(
        LoadPayload(artifact=artifact, schema=[("value", "text")]),
        load_config=_cfg(),
        effective_at=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
    )

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert lines[0].startswith("\t")
    assert enriched.artifact.rows_exported == 2


def test_strategy_metadata_enricher_rewrites_mssql_delimited_partitions(tmp_path: Path) -> None:
    part_a = tmp_path / "a.tsv"
    part_b = tmp_path / "b.tsv"
    # Business fields must remain unquoted tab text (BCP wire), including commas.
    part_a.write_text("1\tAda,comma\n", encoding="utf-8")
    part_b.write_text("2\tBob\n", encoding="utf-8")
    first_artifact = FileExportArtifact(file_path=str(part_a), columns=["id", "name"], format="mssql-delimited")
    first_artifact.transfer_partition_id = "a" * 64
    first_artifact.query_hash = "sha256:query"
    first_artifact.schema_hash = "sha256:schema"
    first_artifact.partition_bounds = {"index": 0, "lower": 1, "upper": 2}
    first_artifact.source_table = "public.orders"
    first_artifact.target_table = "dbo.orders"
    first_artifact.strategy = "scd2"
    payload = LoadPayload(
        artifact=PartitionedFileExportArtifact(
            partitions=[
                first_artifact,
                FileExportArtifact(file_path=str(part_b), columns=["id", "name"], format="mssql-delimited"),
            ],
            columns=["id", "name"],
        ),
        schema=[("id", "bigint"), ("name", "text")],
    )

    enriched = StrategyMetadataEnricher().enrich_payload(
        payload,
        load_config=_cfg(LoadStrategy.SCD2),
        effective_at=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
    )

    assert isinstance(enriched.artifact, PartitionedFileExportArtifact)
    assert "__dpone__row_hash" in enriched.artifact.columns
    first = Path(enriched.artifact.partitions[0].file_path).read_text(encoding="utf-8").strip().split("\t")
    by_name = dict(zip(enriched.artifact.partitions[0].columns, first, strict=True))
    assert by_name["name"] == "Ada,comma"
    assert '"' not in by_name["name"]
    assert len(by_name["__dpone__row_hash"]) == 64
    assert by_name["__dpone__is_current"] == "1"
    assert by_name["__dpone__valid_from_at"] == "2026-06-04 12:00:00.0000000"
    assert by_name["__dpone__valid_to_at"] == ""
    enriched_first = enriched.artifact.partitions[0]
    assert enriched_first.transfer_partition_id == "a" * 64
    assert enriched_first.query_hash == "sha256:query"
    assert enriched_first.schema_hash == "sha256:schema"
    assert enriched_first.partition_bounds == {"index": 0, "lower": 1, "upper": 2}
    assert enriched_first.source_table == "public.orders"
    assert enriched_first.target_table == "dbo.orders"
    assert enriched_first.strategy == "scd2"


def test_strategy_metadata_enricher_rewrites_clickhouse_tsv_preserving_nulls(tmp_path: Path) -> None:
    path = tmp_path / "orders.tsv"
    path.write_text("1\t\\N\n", encoding="utf-8")
    payload = LoadPayload(
        artifact=FileExportArtifact(file_path=str(path), columns=["id", "name"], format="clickhouse-tsv"),
        schema=[("id", "bigint"), ("name", "text")],
    )

    enriched = StrategyMetadataEnricher().enrich_payload(
        payload,
        load_config=_cfg(LoadStrategy.SCD2),
        effective_at=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
    )

    row = Path(enriched.artifact.file_path).read_text(encoding="utf-8").strip().split("\t")
    by_name = dict(zip(enriched.artifact.columns, row, strict=True))
    assert by_name["name"] == "\\N"
    assert by_name["__dpone__is_current"] == "1"
    assert by_name["__dpone__valid_to_at"] == "\\N"
    assert "T" not in by_name["__dpone__valid_from_at"]


def test_strategy_metadata_enricher_fails_closed_for_unsupported_file_format(tmp_path: Path) -> None:
    path = tmp_path / "orders.bin"
    path.write_bytes(b"\x00\x01")
    payload = LoadPayload(
        artifact=FileExportArtifact(file_path=str(path), columns=["id"], format="postgres-binary"),
        schema=[("id", "bigint")],
    )

    with pytest.raises(ValueError, match="delimited file formats"):
        StrategyMetadataEnricher().enrich_payload(payload, load_config=_cfg(LoadStrategy.SCD2))


def test_etl_processor_enriches_scd2_strategy_metadata_before_sink_load() -> None:
    class Source:
        def get_incremental_state(self, load_config):
            return None

        def extract(self, load_config, last_state):
            return ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}]),
                schema=[("id", "bigint"), ("name", "text")],
                state=None,
            )

    class Sink:
        def __init__(self) -> None:
            self.rows = []
            self.schema = []

        def get_target_schema(self, load_config):
            return []

        def apply_schema_plan(self, load_config, plan):
            return None

        def load(self, load_config, payload):
            self.rows = list(payload.artifact._rows)
            self.schema = list(payload.schema)
            return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1)

    sink = Sink()
    result = ETLProcessor(Source(), sink).run(_cfg(LoadStrategy.SCD2))

    assert result["status"] == "success"
    assert "__dpone__row_hash" in sink.rows[0]
    assert sink.rows[0]["__dpone__is_current"] is True
    assert ("__dpone__valid_from_at", "timestamp") in sink.schema


class _FakePostgresConnector:
    def get_records(self, *args, **kwargs):
        return []


def test_postgres_sink_exposes_exact_catalog_nullability() -> None:
    class Connector(_FakePostgresConnector):
        def get_records(self, query, params=None, as_dict=False):
            assert "pg_catalog.pg_attribute" in str(query)
            assert as_dict is True
            return [
                {
                    "column_name": "id",
                    "declared_type": "integer",
                    "is_nullable": False,
                    "collation_name": None,
                }
            ]

    columns = PostgresSink(Connector(), state_storage=None).get_target_columns(_cfg(LoadStrategy.FULL_REFRESH))

    assert columns == [ColumnDef("id", "integer", nullable=False)]


class _FakeBigQueryConnector:
    project_id = "demo"


class _FakeMSSQLConnector:
    def quote_identifier(self, name: str) -> str:
        return f"[{name}]"

    def qualified_name(self, schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"


@pytest.mark.parametrize(
    ("sink", "strategies"),
    [
        (
            PostgresSink(_FakePostgresConnector(), state_storage=None),
            {LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2, LoadStrategy.BACKFILL},
        ),
        (
            MSSQLSink(_FakeMSSQLConnector(), state_storage=None),
            {LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2, LoadStrategy.BACKFILL},
        ),
        (
            BigQuerySink(_FakeBigQueryConnector(), state_storage=None),
            {LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2, LoadStrategy.BACKFILL},
        ),
    ],
)
def test_database_sinks_register_production_strategy_handlers(sink, strategies) -> None:
    assert strategies <= set(sink._strategy_map)


class _RecordingStrategy(SinkStrategy):
    def __init__(self) -> None:
        self.seen_strategy = None

    def load(self, load_config, payload):
        self.seen_strategy = load_config.load_strategy
        return LoadResult(inserted_rows=3, updated_rows=0, total_rows=3)


def test_backfill_strategy_delegates_to_configured_inner_mode() -> None:
    inner = _RecordingStrategy()
    strategy = BackfillStrategy({LoadStrategy.REPLACE: inner})
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])

    result = strategy.load(_cfg(LoadStrategy.BACKFILL, backfill={"inner_mode": "replace"}), payload)

    assert result.inserted_rows == 3
    assert inner.seen_strategy == LoadStrategy.REPLACE


def test_backfill_strategy_rejects_unsupported_inner_mode() -> None:
    strategy = BackfillStrategy({})
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])

    with pytest.raises(ValueError, match="backfill.inner_mode"):
        strategy.load(_cfg(LoadStrategy.BACKFILL, backfill={"inner_mode": "snapshot_diff"}), payload)
