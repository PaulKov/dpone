from __future__ import annotations

from pathlib import Path

from dpone.readiness.cdc import (
    CDCBackend,
    CDCConfig,
    CDCOffset,
    build_mssql_cdc_enable_sql,
    build_postgres_slot_sql,
)
from dpone.readiness.certification import CertificationMatrix, CertificationResult, CertificationStatus
from dpone.readiness.observability import ErrorClassifier, PipelineRunMetrics
from dpone.readiness.resumability import JsonPartitionManifestStore, PartitionRunStatus
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.runtime.partitioning import RangePartitioner


def test_resumable_partition_manifest_round_trips_and_retries_failed_partitions(tmp_path: Path) -> None:
    partitioner = RangePartitioner.from_options(
        {"partition_column": "id", "lower_bound": 1, "upper_bound": 10, "num_partitions": 3}
    )
    store = JsonPartitionManifestStore(tmp_path)
    manifest = store.create_from_partitioner(
        run_id="run-1",
        process_name="orders",
        source="postgres.public.orders",
        target="mssql.dbo.orders",
        partitioner=partitioner,
    )

    manifest.mark_running(0)
    manifest.mark_success(0, rows_extracted=3, rows_loaded=3, artifact_path="/tmp/part0.bcp")
    manifest.mark_failed(1, "network timeout")
    store.save(manifest)

    loaded = store.load("run-1")

    assert loaded.checkpoints[0].status == PartitionRunStatus.SUCCESS
    assert loaded.checkpoints[1].status == PartitionRunStatus.FAILED
    assert [item.partition_index for item in loaded.retryable_partitions()] == [1]
    assert loaded.is_complete is False
    assert loaded.completion_ratio == 1 / 3


def test_schema_evolution_plans_additive_and_breaking_changes() -> None:
    source = [
        ColumnDef("id", "bigint", nullable=False),
        ColumnDef("amount", "numeric(18,2)"),
        ColumnDef("status", "varchar(64)"),
        ColumnDef("created_at", "datetime2"),
    ]
    target = [
        ColumnDef("id", "int", nullable=False),
        ColumnDef("amount", "numeric(10,2)"),
        ColumnDef("legacy", "varchar(20)"),
    ]

    plan = SchemaComparator(SchemaEvolutionPolicy(mode="widening")).compare(source, target)

    assert [change.change_type for change in plan.changes] == [
        "type_widen",
        "type_widen",
        "add_column",
        "add_column",
        "drop_column",
    ]
    assert plan.has_breaking_changes is True
    assert "ALTER TABLE [dbo].[orders] ADD [status] varchar(64) NULL" in plan.ddl_sql("mssql", "dbo.orders")
    assert "ALTER TABLE [dbo].[orders] ALTER COLUMN [id] bigint NOT NULL" in plan.ddl_sql("mssql", "dbo.orders")


def test_observability_metrics_render_json_and_prometheus() -> None:
    metrics = PipelineRunMetrics(run_id="run-1", pipeline="orders")
    metrics.record_stage("extract", rows=100, bytes_processed=1024, duration_seconds=2.0, status="success")
    metrics.record_stage("load", rows=100, bytes_processed=2048, duration_seconds=1.0, status="success")

    payload = metrics.to_dict()
    prometheus = metrics.to_prometheus()

    assert payload["total_rows"] == 200
    assert payload["stages"][0]["rows_per_second"] == 50.0
    assert 'dpone_stage_rows{pipeline="orders",run_id="run-1",stage="extract",status="success"} 100' in prometheus
    assert ErrorClassifier.classify(RuntimeError("login timeout expired")).category == "transient"
    assert ErrorClassifier.classify(RuntimeError("permission denied for table")).category == "authorization"


def test_certification_matrix_reports_missing_required_capabilities() -> None:
    matrix = CertificationMatrix.default()
    results = [
        CertificationResult("postgres", "full_refresh", CertificationStatus.PASS, evidence="15M smoke"),
        CertificationResult("postgres", "incremental_append", CertificationStatus.FAIL, evidence="duplicate row"),
    ]

    report = matrix.evaluate(results)

    assert report.by_connector["postgres"]["full_refresh"].status == CertificationStatus.PASS
    assert report.by_connector["postgres"]["incremental_append"].status == CertificationStatus.FAIL
    assert "postgres.incremental_merge" in report.missing_required
    assert "| postgres | full_refresh | pass | 15M smoke |" in report.to_markdown()


def test_cdc_contracts_validate_offsets_and_generate_setup_sql() -> None:
    pg = CDCConfig(
        backend=CDCBackend.POSTGRES_LOGICAL,
        source_schema="public",
        source_table="orders",
        slot_name="dpone_orders",
        publication_name="dpone_publication",
    )
    mssql = CDCConfig(
        backend=CDCBackend.MSSQL_CDC,
        source_schema="dbo",
        source_table="orders",
        capture_instance="dbo_orders",
    )
    offset = CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/16B6C50", snapshot_complete=True)

    assert pg.validate() == []
    assert mssql.validate() == []
    assert offset.to_state()["snapshot_complete"] is True
    assert "pg_create_logical_replication_slot" in build_postgres_slot_sql(pg)
    assert "sys.sp_cdc_enable_table" in build_mssql_cdc_enable_sql(mssql)


def test_partitioned_file_artifact_updates_resumability_manifest(tmp_path: Path) -> None:
    from dpone.runtime.artifacts import FileExportArtifact, PartitionedFileExportArtifact

    class Store:
        def __init__(self) -> None:
            self.saved = 0

        def save(self, manifest) -> None:
            del manifest
            self.saved += 1

    partitioner = RangePartitioner.from_options(
        {"partition_column": "id", "lower_bound": 1, "upper_bound": 10, "num_partitions": 2}
    )
    store = JsonPartitionManifestStore(tmp_path / "manifests")
    manifest = store.create_from_partitioner(
        run_id="artifact-run",
        process_name="orders",
        source="postgres.public.orders",
        target="mssql.dbo.orders",
        partitioner=partitioner,
    )
    memory_store = Store()
    files = [tmp_path / "p0.bcp", tmp_path / "p1.bcp"]
    for file_path in files:
        file_path.write_text("1\talpha\n", encoding="utf-8")
    artifact = PartitionedFileExportArtifact(
        [FileExportArtifact(str(file_path), ["id", "name"], format="mssql-delimited") for file_path in files],
        ["id", "name"],
        manifest=manifest,
        manifest_store=memory_store,
    )

    loaded = artifact.load_with(lambda file_artifact: 1)

    assert loaded == 2
    assert manifest.is_complete is True
    assert memory_store.saved >= 4
