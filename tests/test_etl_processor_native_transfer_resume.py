from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.commit_unknown import CommitUnknownError
from dpone.runtime.etl.lifecycle import RuntimeLifecycleService
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricSnapshot
from dpone.runtime.governance.service import LoadGovernanceService, QualityGateFailure
from dpone.runtime.kafka.offsets import KafkaOffsetState
from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.runtime.lineage.partition_checkpoint_store import JsonlPartitionCheckpointStore
from dpone.runtime.lineage.partition_resume import NativeTransferTargetCommitGuardError
from dpone.runtime.native_transfer import NativeTransferRuntimeService
from dpone.runtime.native_transfer_row_authority import ROW_COUNT_AUTHORITY
from dpone.runtime.sinks.base import LoadPayload, LoadResult
from dpone.runtime.sources.base import ExtractResult


class _Logger:
    def log_etl_start(self, payload):
        del payload

    def log_etl_progress(self, event, payload):
        del event, payload

    def log_etl_error(self, message, payload):
        del message, payload

    def log_etl_end(self, payload):
        del payload


class _Source:
    def __init__(self, artifact):
        self.artifact = artifact
        self.saved = False

    def get_incremental_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, last_state):
        del load_config, last_state
        return ExtractResult(
            artifact=self.artifact,
            schema=[("id", "int"), ("name", "text")],
            state=KafkaOffsetState(
                topic="orders",
                group_id="dpone.orders",
                partition_offsets={0: 1},
                high_watermarks={0: 1},
                read_mode="offsets",
            ),
        )

    def save_state(self, load_config, saved_state):
        del load_config, saved_state
        self.saved = True


class _Sink:
    def __init__(self, *, fail: bool = False) -> None:
        self.loaded_partition_names: list[str] = []
        self.load_calls = 0
        self.fail = fail

    def load(self, load_config, payload):
        del load_config
        self.load_calls += 1
        if self.fail:
            raise RuntimeError("target unavailable")
        self.loaded_partition_names = [Path(part.file_path).name for part in payload.artifact.partitions]
        return LoadResult(
            inserted_rows=len(self.loaded_partition_names),
            updated_rows=0,
            total_rows=len(self.loaded_partition_names),
            staging_rows=len(self.loaded_partition_names),
        )


class _SchemaEvolutionShouldNotRun:
    def prepare_payload(self, *_args, **_kwargs):
        raise AssertionError("schema evolution must not run for fully skipped native transfers")


def _cfg(tmp_path: Path) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={
            "lineage": False,
            "native_transfer": {"require_artifact_checksum": True},
            "runtime_evidence": {"output_dir": str(tmp_path / ".dpone" / "runs")},
        },
    )


def _part(tmp_path: Path, index: int) -> FileExportArtifact:
    file_path = tmp_path / f"part_{index}.tsv"
    file_path.write_text(f"{index}\tname-{index}\n", encoding="utf-8")
    artifact = FileExportArtifact(str(file_path), ["id", "name"], format="mssql-delimited", estimated_rows=1)
    bounds = {"index": index, "lower": index, "upper": index + 1}
    artifact.partition_bounds = bounds
    artifact.query_hash = "query-a"
    artifact.schema_hash = "schema-a"
    artifact.source_table = "dbo.orders"
    artifact.target_table = "analytics.orders"
    artifact.strategy = "incremental_append"
    artifact.rows_exported = 1
    artifact.transfer_partition_id = build_transfer_partition_id(
        source_table=artifact.source_table,
        target_table=artifact.target_table,
        strategy=artifact.strategy,
        query_hash=artifact.query_hash,
        schema_hash=artifact.schema_hash,
        partition_bounds=bounds,
    )
    return artifact


def _committed_checkpoint(artifact: FileExportArtifact, checksum: str) -> PartitionCheckpoint:
    return PartitionCheckpoint(
        transfer_partition_id=artifact.transfer_partition_id,
        status=PartitionCheckpointStatus.COMMITTED,
        query_hash=artifact.query_hash,
        schema_hash=artifact.schema_hash,
        source_table=artifact.source_table,
        target_table=artifact.target_table,
        partition_bounds=artifact.partition_bounds,
        rows_exported=1,
        bytes_exported=Path(artifact.file_path).stat().st_size,
        diagnostics={
            "artifact_sha256": checksum,
            "rows_loaded": 1,
            "row_count_authority": ROW_COUNT_AUTHORITY,
        },
    )


def test_etl_processor_loads_only_retry_partitions_and_commits_state_after_sink_success(tmp_path: Path) -> None:
    first = _part(tmp_path, 0)
    second = _part(tmp_path, 1)
    artifact = PartitionedFileExportArtifact([first, second], ["id", "name"], max_workers=2)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / ".dpone" / "runs")
    store.upsert(_committed_checkpoint(first, service.checksum_service.checksum(first)))
    source = _Source(artifact)
    sink = _Sink()

    result = ETLProcessor(
        source,
        sink,
        etl_logger=_Logger(),
        native_transfer_runtime_service=service,
    ).run(_cfg(tmp_path))

    assert result["status"] == "success"
    assert sink.loaded_partition_names == ["part_1.tsv"]
    assert source.saved is True
    assert store.summary()["committed"] == 2
    assert result["reconciliation_metrics"]["native_transfer_resume"]["summary"] == {"skip": 1, "retry": 1}


def test_etl_processor_skips_schema_evolution_and_sink_when_all_native_partitions_committed(tmp_path: Path) -> None:
    first = _part(tmp_path, 0)
    second = _part(tmp_path, 1)
    artifact = PartitionedFileExportArtifact([first, second], ["id", "name"], max_workers=2)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / ".dpone" / "runs")
    store.upsert(_committed_checkpoint(first, service.checksum_service.checksum(first)))
    store.upsert(_committed_checkpoint(second, service.checksum_service.checksum(second)))
    source = _Source(artifact)
    sink = _Sink()

    result = ETLProcessor(
        source,
        sink,
        etl_logger=_Logger(),
        schema_evolution_service=_SchemaEvolutionShouldNotRun(),
        native_transfer_runtime_service=service,
    ).run(_cfg(tmp_path))

    assert result["status"] == "success"
    assert sink.loaded_partition_names == []
    assert source.saved is True
    assert result["inserted_rows"] == 0
    assert result["reconciliation_metrics"]["native_transfer_resume"]["summary"] == {"skip": 2, "retry": 0}


def test_partial_native_retry_quality_gates_use_active_and_committed_rows(tmp_path: Path) -> None:
    committed = _part(tmp_path, 0)
    active = _part(tmp_path, 1)
    artifact = PartitionedFileExportArtifact([committed, active], ["id", "name"], max_workers=2)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    store.upsert(_committed_checkpoint(committed, service.checksum_service.checksum(committed)))
    source = _Source(artifact)
    sink = _Sink()
    governance = _CountingGovernanceService()
    config = _cfg(tmp_path)
    config.options["quality"] = {"gates": [_row_count_gate()]}

    result = ETLProcessor(
        source,
        sink,
        etl_logger=_Logger(),
        native_transfer_runtime_service=service,
        load_governance_service=governance,
    ).run(config)

    assert result["status"] == "success"
    assert sink.loaded_partition_names == ["part_1.tsv"]
    [gate] = result["reconciliation_metrics"]["quality_gates"]["results"]
    assert gate["metrics"]["source_row_count"] == 2
    assert gate["metrics"]["target_row_count"] == 2
    assert governance.quality_calls == 1


def test_all_skipped_native_resume_quality_gates_use_matching_committed_rows(tmp_path: Path) -> None:
    parts = [_part(tmp_path, 0), _part(tmp_path, 1)]
    artifact = PartitionedFileExportArtifact(parts, ["id", "name"], max_workers=2)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    for part in parts:
        store.upsert(_committed_checkpoint(part, service.checksum_service.checksum(part)))
    source = _Source(artifact)
    sink = _Sink()
    governance = _CountingGovernanceService()
    config = _cfg(tmp_path)
    config.options["quality"] = {"gates": [_row_count_gate()]}

    result = ETLProcessor(
        source,
        sink,
        etl_logger=_Logger(),
        native_transfer_runtime_service=service,
        load_governance_service=governance,
    ).run(config)

    assert result["status"] == "success"
    assert sink.loaded_partition_names == []
    [gate] = result["reconciliation_metrics"]["quality_gates"]["results"]
    assert gate["metrics"]["source_row_count"] == 2
    assert gate["metrics"]["target_row_count"] == 2
    assert governance.quality_calls == 1


def test_native_post_commit_quality_failure_requires_manual_reconciliation(tmp_path: Path) -> None:
    part = _part(tmp_path, 0)
    artifact = PartitionedFileExportArtifact([part], ["id", "name"])
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    first_source = _Source(artifact)
    first_sink = _Sink()
    config = _cfg(tmp_path)
    config.options["quality"] = {
        "gates": [
            {
                "id": "target_minimum",
                "type": "min_rows",
                "side": "target",
                "threshold": 2,
                "severity": "error",
            }
        ]
    }

    with pytest.raises(CommitUnknownError) as raised:
        ETLProcessor(
            first_source,
            first_sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(config)

    assert isinstance(raised.value.__cause__, QualityGateFailure)
    assert raised.value.outcome.failure_boundary == "checkpoint_persistence"
    assert raised.value.outcome.checkpoint_state == "not_advanced"
    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.EXPORTED
    assert checkpoint.diagnostics["target_commit_guard"]["phase"] == "armed"
    assert first_sink.loaded_partition_names == ["part_0.tsv"]
    assert first_source.saved is False

    config.options["quality"]["gates"][0]["threshold"] = 1
    retry_source = _Source(artifact)
    retry_sink = _Sink()
    with pytest.raises(NativeTransferTargetCommitGuardError):
        ETLProcessor(
            retry_source,
            retry_sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(config)

    assert retry_sink.loaded_partition_names == []
    assert retry_source.saved is False
    assert store.list_latest()[0].status is PartitionCheckpointStatus.EXPORTED


def test_etl_processor_does_not_save_state_when_native_transfer_load_fails(tmp_path: Path) -> None:
    artifact = PartitionedFileExportArtifact([_part(tmp_path, 0)], ["id", "name"])
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / ".dpone" / "runs")
    source = _Source(artifact)

    with pytest.raises(CommitUnknownError) as raised:
        ETLProcessor(
            source,
            _Sink(fail=True),
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(_cfg(tmp_path))

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert str(raised.value.__cause__) == "target unavailable"
    assert raised.value.code == "COMMIT_UNKNOWN"
    assert raised.value.safe_to_retry is False
    assert raised.value.operator_verification_required is True
    assert raised.value.outcome.to_jsonable() == {
        "status": "COMMIT_UNKNOWN",
        "failure_boundary": "target_invocation",
        "target_state": "unknown",
        "checkpoint_state": "not_advanced",
        "source_state": "not_advanced",
        "safe_to_retry": False,
        "operator_verification_required": True,
        "recovery_action": "operator_verification_required",
    }
    assert source.saved is False
    assert store.summary()["failed"] == 1


def test_target_failure_marker_error_preserves_primary_exception_as_cause(
    tmp_path: Path,
) -> None:
    artifact = PartitionedFileExportArtifact([_part(tmp_path, 0)], ["id", "name"])
    store = _FailureMarkerCheckpointStore()
    service = NativeTransferRuntimeService(checkpoint_store=store)
    source = _Source(artifact)
    sink = _PrimaryFailingSink()

    with pytest.raises(CommitUnknownError) as raised:
        ETLProcessor(
            source,
            sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(_cfg(tmp_path))

    assert raised.value.__cause__ is sink.error
    assert raised.value.outcome.failure_boundary == "target_invocation"
    assert sink.load_calls == 1
    assert source.saved is False
    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.EXPORTED
    assert checkpoint.diagnostics["target_commit_guard"]["phase"] == "armed"


def test_incremental_append_retry_after_partial_commit_stops_before_sink(
    tmp_path: Path,
) -> None:
    artifact = PartitionedFileExportArtifact(
        [_part(tmp_path, 0), _part(tmp_path, 1)],
        ["id", "name"],
    )
    store = _PartialCommitCheckpointStore()
    artifact_dir = tmp_path / ".dpone" / "runs"
    service = _ObservedSuccessNativeTransferService(checkpoint_store=store, artifact_dir=artifact_dir)
    first_source = _Source(artifact)
    first_sink = _Sink()

    with pytest.raises(CommitUnknownError) as first_failure:
        ETLProcessor(
            first_source,
            first_sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(_cfg(tmp_path))

    assert first_failure.value.__cause__ is store.error
    assert first_failure.value.outcome.failure_boundary == "checkpoint_persistence"
    assert first_failure.value.outcome.checkpoint_state == "incomplete"
    assert first_failure.value.safe_to_retry is False
    assert first_sink.loaded_partition_names == ["part_0.tsv", "part_1.tsv"]
    assert first_source.saved is False
    [terminal_report] = artifact_dir.glob("native_transfer_runtime_*.json")
    terminal_payload = json.loads(terminal_report.read_text(encoding="utf-8"))
    assert terminal_payload["passed"] is False
    assert terminal_payload["code"] == "COMMIT_UNKNOWN"
    assert terminal_payload["checkpoint_state"] == "incomplete"
    assert terminal_payload["safe_to_retry"] is False
    assert service.observed_passed_values == []

    retry_source = _Source(artifact)
    retry_sink = _Sink()
    with pytest.raises(NativeTransferTargetCommitGuardError) as retry_failure:
        ETLProcessor(
            retry_source,
            retry_sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(_cfg(tmp_path))

    assert retry_failure.value.code == "native_transfer_manual_reconciliation_required"
    assert retry_sink.loaded_partition_names == []
    assert retry_source.saved is False


def test_pre_mutation_failure_is_not_translated_to_commit_unknown(tmp_path: Path) -> None:
    artifact = PartitionedFileExportArtifact([_part(tmp_path, 0)], ["id", "name"])
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    source = _Source(artifact)
    sink = _Sink()
    schema_evolution = _PreMutationFailureSchemaEvolution()

    with pytest.raises(RuntimeError) as raised:
        ETLProcessor(
            source,
            sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
            schema_evolution_service=schema_evolution,
        ).run(_cfg(tmp_path))

    assert raised.value is schema_evolution.error
    assert sink.loaded_partition_names == []
    assert source.saved is False
    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.FAILED
    assert checkpoint.diagnostics["target_commit_guard"]["phase"] == "failed_before_target"


def test_native_transfer_commit_reuses_planned_checksum_after_temp_file_cleanup(tmp_path: Path) -> None:
    part = _part(tmp_path, 0)
    artifact = PartitionedFileExportArtifact([part], ["id", "name"])
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / ".dpone" / "runs")
    context = service.prepare_before_load(
        load_config=_cfg(tmp_path),
        payload=LoadPayload(artifact=artifact, schema=[("id", "int"), ("name", "text")]),
        run_id="run-temp-cleanup",
    )
    Path(part.file_path).unlink()

    service.mark_committed(
        context,
        LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1),
    )

    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.COMMITTED
    assert checkpoint.diagnostics["artifact_sha256"].startswith("sha256:")


def test_post_commit_evidence_failure_is_commit_unknown_and_retry_stays_blocked(tmp_path: Path) -> None:
    part = _part(tmp_path, 0)
    artifact = PartitionedFileExportArtifact([part], ["id", "name"])
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / ".dpone" / "runs")
    first_source = _Source(artifact)
    first_sink = _Sink()

    with pytest.raises(CommitUnknownError) as raised:
        ETLProcessor(
            first_source,
            first_sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
            runtime_lifecycle_service=_EvidenceFailureLifecycle(),
        ).run(_cfg(tmp_path))

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert str(raised.value.__cause__) == "runtime evidence unavailable token=do-not-record"
    assert raised.value.outcome.failure_boundary == "checkpoint_persistence"
    assert raised.value.outcome.checkpoint_state == "not_advanced"
    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.EXPORTED
    assert checkpoint.diagnostics["target_commit_guard"]["phase"] == "armed"
    assert first_sink.loaded_partition_names == ["part_0.tsv"]
    assert first_source.saved is False
    [terminal_report] = (tmp_path / ".dpone" / "runs").glob("native_transfer_runtime_*.json")
    terminal_payload = json.loads(terminal_report.read_text(encoding="utf-8"))
    assert terminal_payload["passed"] is False
    assert terminal_payload["code"] == "COMMIT_UNKNOWN"
    assert terminal_payload["safe_to_retry"] is False

    retry_source = _Source(artifact)
    retry_sink = _Sink()
    with pytest.raises(NativeTransferTargetCommitGuardError):
        ETLProcessor(
            retry_source,
            retry_sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(_cfg(tmp_path))

    assert retry_sink.loaded_partition_names == []
    assert retry_source.saved is False
    assert store.list_latest()[0].status is PartitionCheckpointStatus.EXPORTED
    assert list((tmp_path / ".dpone" / "runs").glob("native_transfer_runtime_*.json")) == [terminal_report]


def test_native_success_report_failure_recovers_after_committed_checkpoint_without_target_retry(tmp_path: Path) -> None:
    part = _part(tmp_path, 0)
    artifact = PartitionedFileExportArtifact([part], ["id", "name"])
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    artifact_dir = tmp_path / ".dpone" / "runs"
    service = _ReportFailureNativeTransferService(checkpoint_store=store, artifact_dir=artifact_dir)
    source = _Source(artifact)
    sink = _Sink()

    with pytest.raises(RuntimeError) as raised:
        ETLProcessor(
            source,
            sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(_cfg(tmp_path))

    assert raised.value is service.error
    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.COMMITTED
    assert checkpoint.diagnostics["target_commit_guard"]["phase"] == "checkpoint_committed"
    assert sink.loaded_partition_names == ["part_0.tsv"]
    assert sink.load_calls == 1
    assert source.saved is False
    assert not list(artifact_dir.glob("native_transfer_runtime_*.json"))

    retry_source = _Source(artifact)
    retry_sink = _Sink()
    result = ETLProcessor(
        retry_source,
        retry_sink,
        etl_logger=_Logger(),
        native_transfer_runtime_service=service,
    ).run(_cfg(tmp_path))

    assert result["status"] == "success"
    assert retry_sink.load_calls == 0
    assert retry_source.saved is True
    assert service.publish_calls == 2
    [terminal_report] = artifact_dir.glob("native_transfer_runtime_*.json")
    assert json.loads(terminal_report.read_text(encoding="utf-8"))["passed"] is True


def test_resume_acceptance_failure_does_not_publish_success_or_rewrite_checkpoint(tmp_path: Path) -> None:
    part = _part(tmp_path, 0)
    artifact = PartitionedFileExportArtifact([part], ["id", "name"])
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / ".dpone" / "runs")
    store.upsert(_committed_checkpoint(part, service.checksum_service.checksum(part)))
    source = _AcceptanceSource(artifact, fail=True)
    sink = _AcceptanceSink()
    config = _cfg(tmp_path)
    config.options["quality"] = {
        "acceptance": {
            "enabled": True,
            "mode": "required",
            "capture": {"source": True, "staged": False, "target": True},
            "checks": {"row_count": True},
        }
    }

    with pytest.raises(RuntimeError, match="metric probe failed"):
        ETLProcessor(
            source,
            sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(config)

    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.COMMITTED
    assert sink.loaded_partition_names == []
    assert source.saved is False
    assert not list((tmp_path / ".dpone" / "runs").glob("native_transfer_runtime_*.json"))


def test_post_commit_acceptance_failure_is_commit_unknown_and_retry_stays_blocked(tmp_path: Path) -> None:
    part = _part(tmp_path, 0)
    artifact = PartitionedFileExportArtifact([part], ["id", "name"])
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / ".dpone" / "runs")
    source = _AcceptanceSource(artifact, fail=False)
    sink = _AcceptanceSink(fail=True)
    config = _cfg(tmp_path)
    config.options["quality"] = {
        "acceptance": {
            "enabled": True,
            "mode": "required",
            "capture": {"source": True, "staged": False, "target": True},
            "checks": {"row_count": True},
        }
    }

    with pytest.raises(CommitUnknownError) as raised:
        ETLProcessor(
            source,
            sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(config)

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "metric probe failed" in str(raised.value.__cause__)
    assert raised.value.outcome.failure_boundary == "checkpoint_persistence"
    assert raised.value.outcome.checkpoint_state == "not_advanced"
    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.EXPORTED
    assert checkpoint.diagnostics["target_commit_guard"]["phase"] == "armed"
    assert sink.loaded_partition_names == ["part_0.tsv"]
    assert source.saved is False
    [terminal_report] = (tmp_path / ".dpone" / "runs").glob("native_transfer_runtime_*.json")
    terminal_payload = json.loads(terminal_report.read_text(encoding="utf-8"))
    assert terminal_payload["passed"] is False
    assert terminal_payload["code"] == "COMMIT_UNKNOWN"
    assert terminal_payload["safe_to_retry"] is False

    retry_source = _AcceptanceSource(artifact, fail=False)
    retry_sink = _AcceptanceSink()
    with pytest.raises(NativeTransferTargetCommitGuardError):
        ETLProcessor(
            retry_source,
            retry_sink,
            etl_logger=_Logger(),
            native_transfer_runtime_service=service,
        ).run(config)

    assert retry_sink.loaded_partition_names == []
    assert retry_source.saved is False


class _EvidenceFailureLifecycle(RuntimeLifecycleService):
    def write_evidence(self, *, load_config, run_id, context):  # noqa: ANN001
        del load_config, run_id, context
        raise RuntimeError("runtime evidence unavailable token=do-not-record")


class _ReportFailureNativeTransferService(NativeTransferRuntimeService):
    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        super().__init__(**kwargs)
        self.error = RuntimeError("native success report unavailable")
        self.publish_calls = 0

    def publish_success_report(self, context, load_result):  # noqa: ANN001
        self.publish_calls += 1
        if self.publish_calls == 1:
            raise self.error
        return super().publish_success_report(context, load_result)


class _ObservedSuccessNativeTransferService(NativeTransferRuntimeService):
    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        super().__init__(**kwargs)
        self.observed_passed_values: list[object] = []

    def publish_success_report(self, context, load_result):  # noqa: ANN001
        result = super().publish_success_report(context, load_result)
        [report] = context.artifact_dir.glob("native_transfer_runtime_*.json")
        self.observed_passed_values.append(json.loads(report.read_text(encoding="utf-8"))["passed"])
        return result


class _PreMutationFailureSchemaEvolution:
    def __init__(self) -> None:
        self.error = RuntimeError("schema evolution failed before target mutation")

    def prepare_payload(self, load_config, sink, payload, logger):  # noqa: ANN001
        del load_config, sink, payload, logger
        raise self.error


class _CheckpointBatchError(RuntimeError):
    pass


class _PrimaryFailingSink:
    def __init__(self) -> None:
        self.error = RuntimeError("target primary failure")
        self.load_calls = 0

    def load(self, load_config, payload):  # noqa: ANN001
        del load_config, payload
        self.load_calls += 1
        raise self.error


class _PartialCommitCheckpointStore:
    def __init__(self) -> None:
        self._latest: dict[str, PartitionCheckpoint] = {}
        self._failed = False
        self.error = _CheckpointBatchError("checkpoint batch failed")

    def upsert(self, checkpoint: PartitionCheckpoint) -> None:
        self._latest[checkpoint.transfer_partition_id] = checkpoint

    def upsert_many(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        is_commit = bool(checkpoints) and all(
            checkpoint.status is PartitionCheckpointStatus.COMMITTED for checkpoint in checkpoints
        )
        if is_commit and not self._failed:
            self._failed = True
            self.upsert(checkpoints[0])
            raise self.error
        for checkpoint in checkpoints:
            self.upsert(checkpoint)

    def list_latest(self) -> tuple[PartitionCheckpoint, ...]:
        return tuple(self._latest.values())

    def safe_to_skip(
        self,
        *,
        query_hash: str,
        schema_hash: str,
    ) -> tuple[PartitionCheckpoint, ...]:
        return tuple(
            checkpoint
            for checkpoint in self._latest.values()
            if checkpoint.can_skip(query_hash=query_hash, schema_hash=schema_hash)
        )

    def summary(self) -> dict[str, int]:
        counts = {status.value: 0 for status in PartitionCheckpointStatus}
        for checkpoint in self._latest.values():
            counts[checkpoint.status.value] += 1
        return counts


class _FailureMarkerCheckpointStore(_PartialCommitCheckpointStore):
    def __init__(self) -> None:
        super().__init__()
        self.secondary = RuntimeError("unknown-outcome marker failed")

    def upsert_many(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        is_unknown_outcome = bool(checkpoints) and all(
            (checkpoint.diagnostics.get("target_commit_guard") or {}).get("phase") == "target_outcome_unknown"
            for checkpoint in checkpoints
        )
        if is_unknown_outcome:
            raise self.secondary
        super().upsert_many(checkpoints)


class _AcceptanceProbe:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    def collect(self, request):  # noqa: ANN001
        if self.fail:
            raise RuntimeError("metric probe failed password=do-not-record")
        return AcceptanceMetricSnapshot(
            side=request.side,
            row_count=1,
            columns=request.columns,
            dataset=request.dataset_identity,
        )


class _AcceptanceSource(_Source):
    def __init__(self, artifact, *, fail: bool) -> None:  # noqa: ANN001
        super().__init__(artifact)
        self.metric_probe = _AcceptanceProbe(fail=fail)


class _AcceptanceSink(_Sink):
    def __init__(self, *, fail: bool = False) -> None:
        super().__init__()
        self.metric_probe = _AcceptanceProbe(fail=fail)


class _CountingGovernanceService(LoadGovernanceService):
    def __init__(self) -> None:
        super().__init__()
        self.quality_calls = 0

    def run_quality_gates(self, **kwargs):  # noqa: ANN003
        self.quality_calls += 1
        return super().run_quality_gates(**kwargs)


def _row_count_gate() -> dict[str, object]:
    return {
        "id": "row_count_reconciliation",
        "type": "row_count_reconciliation",
        "severity": "error",
        "tolerance": {"mode": "absolute", "value": 0},
    }
