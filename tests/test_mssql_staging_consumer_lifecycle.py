"""Lifecycle ordering for lazy row streams entering governed MSSQL staging."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.etl.source_extraction_lifecycle import SourceExtractionLifecycleService
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.physical_chunk_policy import PhysicalChunkPolicy
from dpone.runtime.physical_chunking import PhysicalChunkedFileExportArtifact, RowBoundaryChunkWriter
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.mssql import mssql_staging_consumer as consumer_module
from dpone.runtime.sinks.strategies.mssql.mssql_staging_consumer import MssqlStagingConsumer
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class _StagingManager:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def create(self, _load_config, schema) -> StagingTableArtifact:
        columns = tuple(name for name, _dtype in schema)
        return StagingTableArtifact("stage", "payload", columns, self)  # type: ignore[arg-type]

    def insert_rows(self, _artifact, rows) -> int:
        self.events.append("materialize")
        return len(list(rows))

    def load_from_file(self, _artifact, file_artifact) -> int:
        self.events.append("materialize")
        return len(Path(file_artifact.file_path).read_bytes().splitlines())

    def drop(self, _artifact) -> None:
        self.events.append("drop")


class _ResolvedSchema:
    target_schema = (("id", "int"),)

    def with_lineage(self, _lineage):
        return self


class _Normalizer:
    def __init__(self, _strategy) -> None:
        pass

    def resolve_schema(self, *_args, **_kwargs) -> _ResolvedSchema:
        return _ResolvedSchema()

    def normalize(self, _load_config, raw_staging, *_args, **_kwargs):
        raw_staging.staging_manager.events.append("normalize")
        return raw_staging


class _Finalizer:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def finalize(self, _load_config, admission, handler, staging, **kwargs) -> LoadResult:
        assert admission.operation is not None
        assert kwargs["source_lifecycle_receipt"].complete is True
        self.events.append("finalize")
        return handler(staging)


def test_lazy_stream_is_materialized_before_completed_lifecycle_is_required(monkeypatch) -> None:
    events: list[str] = []
    lifecycle = ExtractionLifecycleAuthority()
    lifecycle.acquire()
    artifact = StreamingRowsArtifact(iter(({"id": 1}, {"id": 2})), extraction_lifecycle=lifecycle)
    payload = LoadPayload(
        artifact=artifact,
        schema=(("id", "int"),),
        extraction_lifecycle=lifecycle,
        mssql_transaction_admission=MssqlTransactionAdmission(
            operation=SimpleNamespace(attempt=SimpleNamespace(request=SimpleNamespace(load_id="load-1")))
        ),
    )
    staging_manager = _StagingManager(events)
    strategy = SimpleNamespace(
        state_storage=SimpleNamespace(atomicity="target_atomic", provisioning="external"),
        transaction_finalizer_factory=lambda *_args: _Finalizer(events),
        _materialize=lambda load_config, current_payload, staging_schema: current_payload.artifact.materialize(
            staging_manager,
            load_config,
            staging_schema,
        ),
    )
    monkeypatch.setattr(consumer_module, "MssqlNativeStagingNormalizer", _Normalizer)
    monkeypatch.setattr(consumer_module, "plan_direct_xmin_initial_staging", lambda *_args: None)
    monkeypatch.setattr(
        consumer_module,
        "MssqlNativeLineageProjection",
        SimpleNamespace(resolve=lambda _config, receipt: events.append("lineage") or receipt),
    )
    config = SimpleNamespace(options={}, reconciliation=False)

    result = MssqlStagingConsumer(strategy).consume(
        config,
        payload,
        lambda staging: LoadResult(inserted_rows=staging.row_count, updated_rows=0, total_rows=staging.row_count),
    )

    assert result.total_rows == 2
    assert events[:4] == ["materialize", "lineage", "normalize", "finalize"]


def test_unstarted_lazy_file_is_materialized_before_completed_lifecycle_is_required(
    monkeypatch, tmp_path: Path
) -> None:
    events: list[str] = []
    writer = RowBoundaryChunkWriter(
        policy=PhysicalChunkPolicy(target_chunk_bytes=16, max_chunk_bytes=32),
        columns=("id",),
        directory=tmp_path,
        format="mssql-delimited",
    )
    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=lambda: writer.write([b"1\n2\n"]),
        columns=("id",),
        evidence_path=tmp_path / "physical_chunks.json",
    )
    extract = SourceExtractionLifecycleService().capture(lambda: SimpleNamespace(artifact=artifact))
    lifecycle = extract.extraction_lifecycle
    assert lifecycle.receipt is None
    payload = LoadPayload(
        artifact=artifact,
        schema=(("id", "int"),),
        extraction_lifecycle=lifecycle,
        mssql_transaction_admission=MssqlTransactionAdmission(
            operation=SimpleNamespace(attempt=SimpleNamespace(request=SimpleNamespace(load_id="load-2")))
        ),
    )
    staging_manager = _StagingManager(events)
    strategy = SimpleNamespace(
        state_storage=SimpleNamespace(atomicity="target_atomic", provisioning="external"),
        transaction_finalizer_factory=lambda *_args: _Finalizer(events),
        _materialize=lambda load_config, current_payload, staging_schema: current_payload.artifact.materialize(
            staging_manager,
            load_config,
            staging_schema,
        ),
    )
    monkeypatch.setattr(consumer_module, "MssqlNativeStagingNormalizer", _Normalizer)
    monkeypatch.setattr(consumer_module, "plan_direct_xmin_initial_staging", lambda *_args: None)
    monkeypatch.setattr(
        consumer_module,
        "MssqlNativeLineageProjection",
        SimpleNamespace(resolve=lambda _config, receipt: events.append("lineage") or receipt),
    )
    config = SimpleNamespace(options={}, reconciliation=False)

    result = MssqlStagingConsumer(strategy).consume(
        config,
        payload,
        lambda staging: LoadResult(inserted_rows=staging.row_count, updated_rows=0, total_rows=staging.row_count),
    )

    assert result.total_rows == 2
    assert lifecycle.require_completed().complete is True
    assert events[:4] == ["materialize", "lineage", "normalize", "finalize"]


def test_acquired_file_snapshot_plans_direct_staging_before_materialization(monkeypatch, tmp_path: Path) -> None:
    """An immutable eager file must not fall back while its snapshot lease stays open."""

    events: list[str] = []
    lifecycle = ExtractionLifecycleAuthority()
    acquired = lifecycle.acquire()
    file_path = tmp_path / "events.bcp"
    file_path.write_text("1\n", encoding="utf-8")
    artifact = SimpleNamespace(file_path=str(file_path))
    payload = LoadPayload(
        artifact=artifact,
        schema=(("id", "int"),),
        extraction_lifecycle=lifecycle,
        mssql_transaction_admission=MssqlTransactionAdmission(
            operation=SimpleNamespace(attempt=SimpleNamespace(request=SimpleNamespace(load_id="load-3")))
        ),
    )
    staging_manager = _StagingManager(events)
    config = SimpleNamespace(options={}, reconciliation=False)

    def materialize(load_config, _payload, staging_schema):
        assert load_config is direct_config
        assert staging_schema == _ResolvedSchema.target_schema
        events.append("materialize")
        lifecycle.complete()
        return StagingTableArtifact(
            "stage",
            "payload",
            tuple(name for name, _dtype in staging_schema),
            staging_manager,  # type: ignore[arg-type]
            row_count=1,
        )

    strategy = SimpleNamespace(
        state_storage=SimpleNamespace(atomicity="target_atomic", provisioning="external"),
        transaction_finalizer_factory=lambda *_args: _Finalizer(events),
        _materialize=materialize,
    )
    direct_config = SimpleNamespace(options={"direct": True}, reconciliation=False)

    monkeypatch.setattr(consumer_module, "MssqlNativeStagingNormalizer", _Normalizer)
    monkeypatch.setattr(consumer_module, "is_direct_xmin_initial_staging_candidate", lambda *_args: True)

    def plan(_load_config, current_artifact, _schema, resolved):
        assert current_artifact is artifact
        assert resolved.target_schema == _ResolvedSchema.target_schema
        events.append("plan")
        return SimpleNamespace(load_config=direct_config, staging_schema=resolved.target_schema)

    monkeypatch.setattr(consumer_module, "plan_direct_xmin_initial_staging", plan)

    def resolve(_config, receipt):
        assert receipt.extraction_started_at == acquired.extraction_started_at
        events.append("lineage_complete" if receipt.complete else "lineage_acquired")
        return SimpleNamespace(columns=(), extracted_at=receipt.extraction_started_at)

    monkeypatch.setattr(
        consumer_module,
        "MssqlNativeLineageProjection",
        SimpleNamespace(resolve=resolve),
    )

    result = MssqlStagingConsumer(strategy).consume(
        config,
        payload,
        lambda staging: LoadResult(inserted_rows=staging.row_count, updated_rows=0, total_rows=staging.row_count),
    )

    assert result.total_rows == 1
    assert events[:6] == [
        "lineage_acquired",
        "plan",
        "materialize",
        "lineage_complete",
        "normalize",
        "finalize",
    ]
