"""Focused contracts for truthful extraction timing and artifact ownership."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.extraction_lifecycle import (
    ArtifactTerminalOutcome,
    ExtractionLifecycleAuthority,
    ExtractionLifecycleStateError,
)
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.sources.strategies.postgres import postgres_whole_file_export_service
from dpone.runtime.sources.strategies.postgres.postgres_base import PostgresBaseStrategy
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)
from dpone.runtime.streaming_rows import StreamingRowsArtifact

_UTC = timezone.utc  # noqa: UP017 - package type checking includes Python 3.11 stubs.


class _Clock:
    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def __call__(self) -> datetime:
        return next(self._values)


class _StagingManager:
    def __init__(
        self,
        *,
        fail_insert: bool = False,
        fail_insert_at: int | None = None,
        primary_error: BaseException | None = None,
        fail_drop: bool = False,
    ) -> None:
        self.fail_insert = fail_insert
        self.fail_insert_at = fail_insert_at
        self.primary_error = primary_error or RuntimeError("target unavailable")
        self.fail_drop = fail_drop
        self.rows: list[dict[str, object]] = []
        self.insert_calls = 0
        self.dropped: list[str] = []

    def create(self, _load_config, _schema) -> StagingTableArtifact:
        return StagingTableArtifact("stage", "rows", ("id",), self)  # type: ignore[arg-type]

    def insert_rows(self, _handle, rows) -> int:
        self.insert_calls += 1
        if self.fail_insert or self.insert_calls == self.fail_insert_at:
            raise self.primary_error
        materialized = list(rows)
        self.rows.extend(materialized)
        return len(materialized)

    def drop(self, handle) -> None:
        self.dropped.append(handle.qualified_name())
        if self.fail_drop:
            raise OSError("drop unavailable")

    def insert_from_query(self, _handle, _query, _schema, _params) -> int:
        if self.fail_insert:
            raise self.primary_error
        return 1

    def load_from_file(self, _handle, _artifact) -> int:
        if self.fail_insert:
            raise self.primary_error
        return 1


class _PostgresLifecycleConnector:
    def __init__(self) -> None:
        self.connection = object()
        self.events: list[str] = []
        self.copy_calls = 0

    def begin(self) -> None:
        self.events.append("begin")

    def execute_query(self, query, params=None) -> int:
        del params
        self.events.append(str(query))
        return 0

    def get_records(self, query, params=None, as_dict=False):
        del params
        rendered = str(query)
        if "txid_current_snapshot" in rendered:
            self.events.append("snapshot")
            return [{"snapshot_token": "10:20:", "extraction_horizon": 20}] if as_dict else [("10:20:", 20)]
        if "reltuples" in rendered:
            return [(1,)]
        return []

    def get_records_iterator(self, query, params=None):
        del query, params
        self.events.append("iterator")
        return iter(({"id": 1}, {"id": 2}))

    def copy_to_file(self, **kwargs):
        self.events.append("copy")
        self.copy_calls += 1
        Path(str(kwargs["output_path"])).write_text("1\n" if self.copy_calls == 1 else "", encoding="utf-8")
        return {"total_bytes": 2, "chunk_count": 1, "elapsed": 0.1, "throughput": 1.0}

    def commit_transaction(self) -> None:
        self.events.append("commit")

    def rollback(self) -> None:
        self.events.append("rollback")


class _PostgresLifecycleStrategy(PostgresBaseStrategy):
    def get_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, last_state):
        del load_config, last_state
        raise NotImplementedError

    def _render_query(self, connector, query) -> str:
        del connector
        return str(query)


class _Logger:
    def log_etl_progress(self, event, payload) -> None:
        del event, payload


def _postgres_file_config(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        source_schema="public",
        source_table="orders",
        options={"sink_type": "postgres", "partition_tmp_dir": str(tmp_path)},
        export_format="csv",
        compress_export=False,
    )


def test_authority_replaces_frozen_receipt_only_after_real_completion() -> None:
    acquired = datetime(2026, 8, 15, 10, 0, tzinfo=_UTC)
    completed = datetime(2026, 8, 15, 10, 5, tzinfo=_UTC)
    authority = ExtractionLifecycleAuthority(clock=_Clock(acquired, completed))

    assert authority.receipt is None
    in_progress = authority.acquire_snapshot(
        snapshot_authority="postgresql.repeatable_read",
        source_token="sha256:snapshot",
    )
    assert in_progress.extraction_completed_at is None

    frozen = authority.complete()
    assert frozen is authority.receipt
    assert frozen is authority.complete()
    assert frozen.extraction_started_at == acquired
    assert frozen.snapshot_acquired_at == acquired
    assert frozen.snapshot_authority == "postgresql.repeatable_read"
    assert frozen.extraction_completed_at == completed
    with pytest.raises(FrozenInstanceError):
        frozen.source_token = "changed"  # type: ignore[misc]


def test_authority_fails_closed_before_acquisition_and_for_naive_time() -> None:
    authority = ExtractionLifecycleAuthority()
    with pytest.raises(ExtractionLifecycleStateError, match="extraction_not_started"):
        authority.complete()
    with pytest.raises(ExtractionLifecycleStateError, match="extraction_not_started"):
        authority.require_in_progress()
    with pytest.raises(ExtractionLifecycleStateError, match="extraction_not_started"):
        authority.require_acquired()
    with pytest.raises(ValueError, match="timestamp_must_be_timezone_aware"):
        authority.acquire(started_at=datetime(2026, 8, 15, 10, 0))

    authority.acquire(started_at=datetime(2026, 8, 15, 10, 0, tzinfo=_UTC))
    assert authority.require_acquired().complete is False
    assert authority.require_in_progress().complete is False
    authority.complete(completed_at=datetime(2026, 8, 15, 10, 1, tzinfo=_UTC))
    assert authority.require_acquired().complete is True
    with pytest.raises(ExtractionLifecycleStateError, match="extraction_already_completed"):
        authority.require_in_progress()


def test_stream_completion_is_delayed_until_iterator_exhaustion_and_row_receipt() -> None:
    acquired = datetime(2026, 8, 15, 10, 0, tzinfo=_UTC)
    completed = datetime(2026, 8, 15, 10, 7, tzinfo=_UTC)
    lifecycle = ExtractionLifecycleAuthority(clock=_Clock(acquired, completed))
    lifecycle.acquire_snapshot(
        snapshot_authority="postgresql.repeatable_read",
        source_token="snapshot-1",
    )
    artifact = StreamingRowsArtifact(
        iter(({"id": 1}, {"id": 2})),
        batch_size=1,
        extraction_lifecycle=lifecycle,
    )

    assert lifecycle.receipt is not None
    assert lifecycle.receipt.extraction_completed_at is None
    handle = artifact.materialize(_StagingManager(), object(), (("id", "int"),))

    assert handle.row_count == 2
    assert artifact.rows_exported == 2
    assert lifecycle.require_completed().extraction_completed_at == completed


def test_failed_stream_does_not_claim_extraction_completion() -> None:
    lifecycle = ExtractionLifecycleAuthority()
    lifecycle.acquire(started_at=datetime(2026, 8, 15, 10, 0, tzinfo=_UTC))
    artifact = StreamingRowsArtifact(iter(({"id": 1},)), extraction_lifecycle=lifecycle)

    with pytest.raises(RuntimeError, match="target unavailable"):
        artifact.materialize(_StagingManager(fail_insert=True), object(), (("id", "int"),))

    assert lifecycle.receipt is not None
    assert lifecycle.receipt.extraction_completed_at is None


def test_stream_batch_failure_drops_unreturned_stage_then_abort_rolls_back_source() -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    artifact = strategy._open_repeatable_read_stream("SELECT id FROM source", batch_size=1)  # noqa: SLF001
    primary = RuntimeError("batch two rejected")
    staging = _StagingManager(fail_insert_at=2, primary_error=primary)

    with pytest.raises(RuntimeError) as raised:
        artifact.materialize(staging, object(), (("id", "int"),))

    assert raised.value is primary
    assert staging.insert_calls == 2
    assert staging.dropped == ["stage.rows"]
    assert artifact.extraction_lifecycle is not None
    assert artifact.extraction_lifecycle.receipt is not None
    assert artifact.extraction_lifecycle.receipt.complete is False
    assert "rollback" not in connector.events

    artifact.terminate(ArtifactTerminalOutcome.ABORT)
    assert connector.events[-1] == "rollback"


def test_staging_cleanup_failure_annotates_but_never_replaces_primary() -> None:
    primary = RuntimeError("insert rejected")
    staging = _StagingManager(fail_insert=True, primary_error=primary, fail_drop=True)
    artifact = InMemoryRowsArtifact(({"id": 1},))

    with pytest.raises(RuntimeError) as raised:
        artifact.materialize(staging, object(), (("id", "int"),))

    assert raised.value is primary
    assert staging.dropped == ["stage.rows"]
    assert getattr(primary, "__notes__", []) == ["staging handle cleanup failed: OSError"]


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (ArtifactTerminalOutcome.SUCCESS, ["commit"]),
        (ArtifactTerminalOutcome.ABORT, ["rollback"]),
        (ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN, ["rollback"]),
    ],
)
def test_streaming_terminal_outcomes_close_session_once(
    outcome: ArtifactTerminalOutcome,
    expected: list[str],
) -> None:
    calls: list[str] = []
    artifact = StreamingRowsArtifact(
        iter(()),
        on_success=lambda: calls.append("commit"),
        on_abort=lambda: calls.append("rollback"),
    )

    first = artifact.terminate(outcome)
    second = artifact.terminate(ArtifactTerminalOutcome.ABORT)

    assert calls == expected
    assert second is first
    assert first.outcome is outcome
    assert first.cleanup_attempted is True
    assert first.cleanup_succeeded is True


def test_stream_materialization_does_not_commit_source_transaction() -> None:
    calls: list[str] = []
    artifact = StreamingRowsArtifact(
        iter(({"id": 1},)),
        on_success=lambda: calls.append("commit"),
        on_abort=lambda: calls.append("rollback"),
    )

    artifact.materialize(_StagingManager(), object(), (("id", "int"),))
    assert calls == []

    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)
    assert calls == ["commit"]


def test_retain_unknown_preserves_file_evidence(tmp_path: Path) -> None:
    path = tmp_path / "evidence.tsv"
    path.write_bytes(b"1\n")
    artifact = FileExportArtifact(str(path), ("id",), format="mssql-delimited", rows_exported=1)

    receipt = artifact.terminate(ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN)

    assert path.exists()
    assert receipt.cleanup_attempted is False
    assert receipt.outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN


def test_cleanup_failure_is_receipted_and_never_masks_primary_failure() -> None:
    class _BrokenArtifact(BaseExtractionArtifact):
        def cleanup(self) -> None:
            raise OSError("cannot release")

    receipt = _BrokenArtifact().terminate(ArtifactTerminalOutcome.ABORT)

    assert receipt.cleanup_succeeded is False
    assert receipt.cleanup_error_code == "builtins.OSError"


def test_file_terminal_receipt_reports_real_filesystem_release_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "leaked.tsv"
    path.write_bytes(b"1\n")
    artifact = FileExportArtifact(str(path), ("id",), rows_exported=1)

    def fail_remove(_path: str) -> None:
        raise OSError("permission denied")

    monkeypatch.setattr("dpone.runtime.file_artifacts.os.remove", fail_remove)
    receipt = artifact.terminate(ArtifactTerminalOutcome.ABORT)

    assert path.exists()
    assert receipt.cleanup_attempted is True
    assert receipt.cleanup_succeeded is False
    assert receipt.cleanup_error_code == "builtins.OSError"


def test_partition_rebind_shares_terminal_owner_and_retain_cannot_be_reversed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "partition.tsv"
    path.write_bytes(b"1\n")
    lifecycle = ExtractionLifecycleAuthority()
    lifecycle.acquire(started_at=datetime(2026, 8, 15, 10, 0, tzinfo=_UTC))
    lifecycle.complete(completed_at=datetime(2026, 8, 15, 10, 1, tzinfo=_UTC))
    original = PartitionedFileExportArtifact(
        [FileExportArtifact(str(path), ("source_id",), rows_exported=1)],
        ("source_id",),
    )
    original.bind_extraction_lifecycle(lifecycle)
    rebound = original.rebind_columns(("target_id",))
    remove_calls: list[str] = []
    real_remove = Path.unlink

    def counted_remove(value: str) -> None:
        remove_calls.append(value)
        real_remove(Path(value))

    monkeypatch.setattr("dpone.runtime.file_artifacts.os.remove", counted_remove)

    original_receipt = original.terminate(ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN)
    rebound_receipt = rebound.terminate(ArtifactTerminalOutcome.ABORT)
    original.partitions[0].cleanup()

    assert original_receipt.outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN
    assert rebound_receipt is original_receipt
    assert rebound.extraction_lifecycle is lifecycle
    assert path.exists()
    assert remove_calls == []

    success_path = tmp_path / "committed.tsv"
    success_path.write_bytes(b"2\n")
    committed = PartitionedFileExportArtifact(
        [FileExportArtifact(str(success_path), ("source_id",), rows_exported=1)],
        ("source_id",),
    )
    committed_view = committed.rebind_columns(("target_id",))
    committed_view.terminate(ArtifactTerminalOutcome.SUCCESS)
    committed.cleanup()

    assert not success_path.exists()
    assert remove_calls == [str(success_path)]


def test_postgres_stream_acquires_snapshot_before_iterator_and_retain_rolls_back() -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())

    artifact = strategy._open_repeatable_read_stream("SELECT id FROM source", batch_size=1)  # noqa: SLF001

    assert connector.events[:4] == [
        "begin",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "snapshot",
        "iterator",
    ]
    assert artifact.extraction_lifecycle is not None
    assert artifact.extraction_lifecycle.receipt is not None
    assert artifact.extraction_lifecycle.receipt.complete is False

    artifact.materialize(_StagingManager(), object(), (("id", "int"),))
    assert artifact.extraction_lifecycle.require_completed().complete is True
    assert "commit" not in connector.events

    artifact.terminate(ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN)
    assert connector.events[-1] == "rollback"


def test_postgres_batched_lifecycle_is_pending_at_handoff_and_completes_on_exhaustion(tmp_path: Path) -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    config = SimpleNamespace(
        source_schema="public",
        source_table="orders",
        options={"sink_type": "postgres", "partition_tmp_dir": str(tmp_path)},
        export_format="csv",
        compress_export=False,
    )

    artifact = strategy._export_to_file_batched(  # noqa: SLF001
        "SELECT id FROM public.orders",
        [("id", "integer")],
        config,
        batch_size=1,
    )
    assert artifact.extraction_lifecycle is not None
    assert artifact.extraction_lifecycle.receipt is None

    batches = artifact.batch_generator()
    first = next(batches)
    assert artifact.extraction_lifecycle.receipt is not None
    assert artifact.extraction_lifecycle.receipt.complete is False
    with pytest.raises(StopIteration):
        next(batches)
    assert artifact.extraction_lifecycle.require_completed().complete is True
    assert "commit" not in connector.events

    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)
    assert connector.events[-1] == "commit"
    first.cleanup()


def test_postgres_batched_materialization_lets_generator_issue_native_snapshot(tmp_path: Path) -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    config = SimpleNamespace(
        source_schema="public",
        source_table="orders",
        options={"sink_type": "postgres", "partition_tmp_dir": str(tmp_path)},
        export_format="csv",
        compress_export=False,
    )

    artifact = strategy._export_to_file_batched(  # noqa: SLF001
        "SELECT id FROM public.orders",
        [("id", "integer")],
        config,
        batch_size=1,
    )

    artifact.materialize(_StagingManager(), config, (("id", "integer"),))

    receipt = artifact.extraction_lifecycle.require_completed()
    assert receipt.snapshot_authority == "postgresql.repeatable_read"
    assert connector.events.count("snapshot") == 1
    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)


def test_postgres_whole_exports_delegate_one_external_snapshot_transaction(tmp_path: Path) -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    config = SimpleNamespace(
        source_schema="public",
        source_table="orders",
        options={"sink_type": "postgres", "partition_tmp_dir": str(tmp_path)},
        export_format="csv",
        compress_export=False,
    )
    lifecycle = ExtractionLifecycleAuthority()
    snapshot_lease = strategy._begin_repeatable_read_snapshot(lifecycle)  # noqa: SLF001

    first = strategy._export_to_file_whole(  # noqa: SLF001
        "SELECT id FROM public.orders",
        [("id", "integer")],
        config,
        snapshot_lease=snapshot_lease,
    )
    second = strategy._export_to_file_whole(  # noqa: SLF001
        "SELECT id FROM public.orders WHERE false",
        [("id", "integer")],
        config,
        snapshot_lease=snapshot_lease,
    )

    assert connector.events.count("begin") == 1
    assert connector.events.count("copy") == 2
    assert connector.events.count("commit") == 0
    assert connector.events.count("rollback") == 0
    assert lifecycle.require_in_progress().complete is False
    assert first.extraction_lifecycle is lifecycle
    assert second.extraction_lifecycle is lifecycle

    lifecycle.complete()
    connector.commit_transaction()
    assert connector.events.count("commit") == 1
    first.cleanup()
    second.cleanup()
    assert list(tmp_path.iterdir()) == []


def test_postgres_copy_progress_distinguishes_rows_from_driver_reads() -> None:
    progress = postgres_whole_file_export_service._copy_completion_progress(  # noqa: SLF001
        {
            "total_bytes": 16,
            "rows_exported": 2,
            "copy_read_count": 7,
            "chunk_count": 7,
            "elapsed": 0.25,
            "throughput": 1.0,
        }
    )

    assert progress["Rows Exported"] == 2
    assert progress["Total COPY Reads"] == 7
    assert "Total Chunks" not in progress


def test_postgres_whole_export_can_release_external_snapshot_before_artifact_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    lifecycle = ExtractionLifecycleAuthority()
    lease = strategy._begin_repeatable_read_snapshot(lifecycle)  # noqa: SLF001
    completion = strategy._repeatable_read_snapshot_completion(lifecycle)  # noqa: SLF001
    artifact_type = postgres_whole_file_export_service.FileExportArtifact
    events: list[str] = []

    def observed_artifact(*args, **kwargs):
        assert completion.active is False
        assert lifecycle.require_completed().complete is True
        assert connector.events[-1] == "commit"
        events.append("artifact-receipt")
        return artifact_type(*args, **kwargs)

    monkeypatch.setattr(postgres_whole_file_export_service, "FileExportArtifact", observed_artifact)
    artifact = strategy._export_to_file_whole(  # noqa: SLF001
        "SELECT id FROM public.orders",
        [("id", "integer")],
        _postgres_file_config(tmp_path),
        snapshot_lease=lease,
        after_copy=completion.complete,
    )

    assert connector.events.index("copy") < connector.events.index("commit")
    assert events == ["artifact-receipt"]
    artifact.cleanup()
    assert list(tmp_path.iterdir()) == []


def test_postgres_external_export_rejects_generic_lifecycle_before_copy(tmp_path: Path) -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    lifecycle = ExtractionLifecycleAuthority()
    lifecycle.acquire()

    with pytest.raises(TypeError, match="postgres_file_export.snapshot_lease_required"):
        strategy._export_to_file_whole(  # type: ignore[arg-type]  # noqa: SLF001
            "SELECT id FROM public.orders",
            [("id", "integer")],
            _postgres_file_config(tmp_path),
            snapshot_lease=lifecycle,
        )

    assert connector.copy_calls == 0
    assert list(tmp_path.iterdir()) == []


def test_postgres_external_export_rejects_cross_connector_and_session_lease_before_copy(
    tmp_path: Path,
) -> None:
    issuer_connector = _PostgresLifecycleConnector()
    issuer = _PostgresLifecycleStrategy(issuer_connector, _Logger())
    lifecycle = ExtractionLifecycleAuthority()
    lease = issuer._begin_repeatable_read_snapshot(lifecycle)  # noqa: SLF001
    assert isinstance(lease, PostgresRepeatableReadSnapshotLease)

    consumer_connector = _PostgresLifecycleConnector()
    consumer = _PostgresLifecycleStrategy(consumer_connector, _Logger())
    with pytest.raises(ExtractionLifecycleStateError, match="connector_mismatch"):
        consumer._export_to_file_whole(  # noqa: SLF001
            "SELECT id FROM public.orders",
            [("id", "integer")],
            _postgres_file_config(tmp_path),
            snapshot_lease=lease,
        )
    assert consumer_connector.copy_calls == 0

    issuer_connector.connection = object()
    with pytest.raises(ExtractionLifecycleStateError, match="session_mismatch"):
        issuer._export_to_file_whole(  # noqa: SLF001
            "SELECT id FROM public.orders",
            [("id", "integer")],
            _postgres_file_config(tmp_path),
            snapshot_lease=lease,
        )
    assert issuer_connector.copy_calls == 0
    issuer_connector.rollback()
    assert list(tmp_path.iterdir()) == []


def test_postgres_external_export_revalidates_exact_snapshot_token_before_copy(tmp_path: Path) -> None:
    class _ChangedTokenLifecycle(ExtractionLifecycleAuthority):
        changed = False

        def require_in_progress(self):
            receipt = super().require_in_progress()
            return replace(receipt, source_token="sha256:changed") if self.changed else receipt

    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    lifecycle = _ChangedTokenLifecycle()
    lease = strategy._begin_repeatable_read_snapshot(lifecycle)  # noqa: SLF001
    lifecycle.changed = True

    with pytest.raises(ExtractionLifecycleStateError, match="token_mismatch"):
        strategy._export_to_file_whole(  # noqa: SLF001
            "SELECT id FROM public.orders",
            [("id", "integer")],
            _postgres_file_config(tmp_path),
            snapshot_lease=lease,
        )

    assert connector.copy_calls == 0
    connector.rollback()
    assert list(tmp_path.iterdir()) == []


def test_postgres_whole_export_has_no_partial_schema_contract_override_surface(tmp_path: Path) -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    lifecycle = ExtractionLifecycleAuthority()
    lease = strategy._begin_repeatable_read_snapshot(lifecycle)  # noqa: SLF001

    with pytest.raises(TypeError, match="schema_contract"):
        strategy._export_to_file_whole(  # type: ignore[call-arg]  # noqa: SLF001
            "SELECT id FROM public.orders",
            [("id", "integer")],
            _postgres_file_config(tmp_path),
            snapshot_lease=lease,
            schema_contract=object(),
        )

    assert connector.copy_calls == 0
    connector.rollback()
    assert list(tmp_path.iterdir()) == []


def test_postgres_key_export_derives_configured_key_and_rejects_arbitrary_or_unknown_subset(
    tmp_path: Path,
) -> None:
    connector = _PostgresLifecycleConnector()
    strategy = _PostgresLifecycleStrategy(connector, _Logger())
    strategy.fetch_schema_projection = lambda _config: SimpleNamespace(  # type: ignore[method-assign]
        projected_schema=(("id", "integer"), ("value", "text")),
    )
    config = _postgres_file_config(tmp_path)
    config.unique_key = ["id"]
    lifecycle = ExtractionLifecycleAuthority()
    lease = strategy._begin_repeatable_read_snapshot(lifecycle)  # noqa: SLF001

    with pytest.raises(TypeError, match="key_columns"):
        strategy._export_key_snapshot_file(  # type: ignore[call-arg]  # noqa: SLF001
            "SELECT id FROM public.orders",
            config,
            snapshot_lease=lease,
            key_columns=("value",),
        )

    config.unique_key = ["missing"]
    with pytest.raises(ValueError, match="unique_key is absent from source schema"):
        strategy._export_key_snapshot_file(  # noqa: SLF001
            "SELECT missing FROM public.orders",
            config,
            snapshot_lease=lease,
        )

    assert connector.copy_calls == 0
    connector.rollback()
    assert list(tmp_path.iterdir()) == []


def test_internal_query_acquires_and_completes_at_target_execution_boundary() -> None:
    acquired = datetime(2026, 8, 15, 10, 0, tzinfo=_UTC)
    completed = datetime(2026, 8, 15, 10, 1, tzinfo=_UTC)
    lifecycle = ExtractionLifecycleAuthority(clock=_Clock(acquired, completed))
    artifact = InternalQueryArtifact("SELECT id FROM source", extraction_lifecycle=lifecycle)

    assert lifecycle.receipt is None
    handle = artifact.materialize(_StagingManager(), object(), (("id", "int"),))

    assert handle.row_count == 1
    assert lifecycle.require_completed().extraction_started_at == acquired
    assert lifecycle.require_completed().snapshot_acquired_at is None
    assert lifecycle.require_completed().extraction_completed_at == completed
