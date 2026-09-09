"""StreamingRowsArtifact publishes authoritative row counts for quality gates."""

from __future__ import annotations

from types import SimpleNamespace

import dpone.runtime.streaming_rows as streaming_rows_module
from dpone.runtime.governance.quality_probe_snapshots import source_snapshot
from dpone.runtime.lineage import LineageIdentityService, LineageOptions, RowLineageEnricher
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class _FakeStagingManager:
    def __init__(self) -> None:
        self.insert_sizes: list[int] = []

    def create(self, load_config, schema):  # noqa: ANN001
        return SimpleNamespace(
            row_count=0,
            qualified_name=lambda: "db.schema.stg",
            columns=[name for name, _ in schema],
        )

    def insert_rows(self, handle, chunk):  # noqa: ANN001
        size = len(chunk)
        self.insert_sizes.append(size)
        return size


def test_streaming_materialize_sets_rows_exported_for_source_quality_probe() -> None:
    artifact = StreamingRowsArtifact(iter([{"id": 1}, {"id": 2}, {"id": 3}]), batch_size=2)
    handle = artifact.materialize(
        _FakeStagingManager(),
        load_config=SimpleNamespace(),
        schema=[("id", "Int64")],
    )
    assert handle.row_count == 3
    assert artifact.rows_exported == 3
    assert artifact.row_count == 3
    probe = source_snapshot(SimpleNamespace(artifact=artifact, typed_hash=None))
    assert probe.row_count == 3


def test_empty_stream_materializes_one_explicit_zero_row_receipt_boundary() -> None:
    manager = _FakeStagingManager()
    artifact = StreamingRowsArtifact(iter(()), batch_size=2)

    handle = artifact.materialize(
        manager,
        load_config=SimpleNamespace(),
        schema=[("id", "Int64")],
    )

    assert manager.insert_sizes == [0]
    assert handle.row_count == artifact.rows_exported == artifact.row_count == 0


def test_streaming_materialize_uses_one_sink_owned_bulk_stream_without_python_batch_lists() -> None:
    class _BulkStreamingManager(_FakeStagingManager):
        def __init__(self) -> None:
            super().__init__()
            self.bulk_stream_calls = 0
            self.received_container_types: list[type[object]] = []

        def insert_streaming_rows(self, handle, rows):  # noqa: ANN001
            self.bulk_stream_calls += 1
            self.received_container_types.append(type(rows))
            materialized = tuple(rows)
            handle.row_count += len(materialized)
            return len(materialized)

        def insert_rows(self, handle, chunk):  # noqa: ANN001
            raise AssertionError("the sink-owned bulk stream must not fall back to per-source-batch inserts")

    manager = _BulkStreamingManager()
    iterator = iter({"id": value} for value in range(5))
    artifact = StreamingRowsArtifact(iterator, batch_size=2)

    handle = artifact.materialize(
        manager,
        load_config=SimpleNamespace(),
        schema=[("id", "Int64")],
    )

    assert manager.bulk_stream_calls == 1
    assert manager.received_container_types == [type(iterator)]
    assert handle.row_count == artifact.rows_exported == artifact.row_count == 5


def test_sink_owned_stream_reports_one_observed_materialization_not_estimated_batches(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    logger = SimpleNamespace(log_etl_progress=lambda _event, metrics: events.append(metrics))
    monkeypatch.setattr(streaming_rows_module, "etl_logger", logger)

    class _BulkStreamingManager(_FakeStagingManager):
        @staticmethod
        def insert_streaming_rows(handle, rows):  # noqa: ANN001
            total = sum(1 for _row in rows)
            handle.row_count = total
            return total

    StreamingRowsArtifact(iter({"id": value} for value in range(5)), batch_size=2).materialize(
        _BulkStreamingManager(),
        load_config=SimpleNamespace(),
        schema=[("id", "Int64")],
    )

    assert events == [
        {
            "Total_Rows": 5,
            "Staging": "db.schema.stg",
            "Sink_Stream_Materializations": 1,
        }
    ]
    assert "Batches" not in events[0]


def test_lineage_enrichment_preserves_streaming_identity_for_source_quality_probe() -> None:
    """Marketing CH→MSSQL enables lineage; quality reads extract_result.artifact.

    Enrichment must not replace the StreamingRowsArtifact instance, otherwise
    materialize publishes rows_exported on a different object than source_snapshot.
    """

    original = StreamingRowsArtifact(iter([{"id": 1}, {"id": 2}, {"id": 3}]), batch_size=2)
    extract_result = SimpleNamespace(artifact=original, typed_hash=None)
    enricher = RowLineageEnricher(identity_service=LineageIdentityService())
    enriched = enricher.enrich_payload(
        LoadPayload(artifact=original, schema=[("id", "bigint")]),
        options=LineageOptions.from_config({"preset": "standard"}),
        run_id="01TEST_RUN",
        load_id="01TEST_LOAD",
        source_type="clickhouse",
        source_schema="marketing_datamarts",
        source_table="sample_web_sync",
        unique_key=["id"],
    )
    assert enriched.artifact is original

    handle = enriched.artifact.materialize(
        _FakeStagingManager(),
        load_config=SimpleNamespace(),
        schema=list(enriched.schema),
    )
    assert handle.row_count == 3
    probe = source_snapshot(extract_result)
    assert probe.row_count == 3


def test_clickhouse_streaming_insert_sets_rows_exported_for_source_quality_probe() -> None:
    artifact = StreamingRowsArtifact(iter([{"id": 1}, {"id": 2}]), batch_size=1)
    extract_result = SimpleNamespace(artifact=artifact, typed_hash=None)

    class _Sink:
        def _count(self, load_config):  # noqa: ANN001
            return 0

        def _table(self, load_config):  # noqa: ANN001
            return "db.stg"

        @property
        def connector(self):
            return SimpleNamespace(connection=SimpleNamespace(execute=lambda *a, **k: None))

    service = ClickHousePayloadIngestionService(_Sink(), sink_factory=lambda connector: None)
    inserted = service.insert_payload(
        load_config=SimpleNamespace(options={}),
        payload=LoadPayload(artifact=artifact, schema=[("id", "Int64")]),
    )
    assert inserted == 2
    assert artifact.rows_exported == 2
    assert source_snapshot(extract_result).row_count == 2
