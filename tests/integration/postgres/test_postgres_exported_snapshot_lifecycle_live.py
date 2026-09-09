"""Vendor-live PostgreSQL shared-snapshot and lifecycle certification."""

from __future__ import annotations

import csv
import uuid
from pathlib import Path
from typing import Any

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.file_artifacts import PartitionedFileExportArtifact
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from tests.integration.postgres.postgres_live_support import (
    NoopLogger,
    ensure_postgres_source_schema,
    postgres_connector,
    postgres_enabled,
)

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.integration_postgres]


class _RecordingConnector:
    """Transparent recorder around real, distinct psycopg sessions."""

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self.worker_backend_pids: list[int] = []
        self.imported_snapshots: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def clone_for_partition(self, partition_index: int) -> _RecordingWorkerConnector:
        worker = self._delegate.clone_for_partition(partition_index)
        pid = int(worker.get_records("SELECT pg_backend_pid()")[0][0])
        self.worker_backend_pids.append(pid)
        return _RecordingWorkerConnector(worker, self.imported_snapshots)


class _RecordingWorkerConnector:
    def __init__(self, delegate: Any, imported_snapshots: list[str]) -> None:
        self._delegate = delegate
        self._imported_snapshots = imported_snapshots

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def execute_query(self, query: Any, params: Any = None) -> int:
        rendered = query.as_string(self._delegate.connection) if hasattr(query, "as_string") else str(query)
        if rendered.startswith("SET TRANSACTION SNAPSHOT"):
            self._imported_snapshots.append(rendered)
        return self._delegate.execute_query(query, params)


class _MutatingSnapshotStrategy(PostgresFullExtractStrategy):
    def __init__(self, *args: Any, mutator: Any, table: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._mutator = mutator
        self._table = table
        self.exported_snapshot: str | None = None

    def _begin_exported_snapshot(self, lifecycle):
        token = super()._begin_exported_snapshot(lifecycle)
        self.exported_snapshot = token
        # These commits happen after the coordinator exported its RR snapshot
        # and before either worker starts COPY.
        self._mutator.execute_query(f'UPDATE "dpone_src"."{self._table}" SET payload = %s WHERE id = 1', ("new",))
        self._mutator.execute_query(
            f'INSERT INTO "dpone_src"."{self._table}" (id, payload) VALUES (%s, %s)',
            (5, "late"),
        )
        return token


class _MutatingWholeCopyStrategy(PostgresFullExtractStrategy):
    def __init__(self, *args: Any, mutator: Any, table: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._mutator = mutator
        self._table = table

    def _begin_repeatable_read_snapshot(self, lifecycle):
        token = super()._begin_repeatable_read_snapshot(lifecycle)
        self._mutator.execute_query(f'UPDATE "dpone_src"."{self._table}" SET payload = %s WHERE id = 1', ("new",))
        self._mutator.execute_query(
            f'INSERT INTO "dpone_src"."{self._table}" (id, payload) VALUES (%s, %s)',
            (5, "late"),
        )
        return token


class _MutatingBoundsStrategy(PostgresFullExtractStrategy):
    def __init__(self, *args: Any, mutator: Any, table: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._mutator = mutator
        self._table = table

    def _resolve_partition_bounds(self, query: Any, column: str):
        bounds = super()._resolve_partition_bounds(query, column)
        # The row is outside the just-observed maximum. It must also be outside
        # the already-exported snapshot, otherwise range planning would omit it.
        self._mutator.execute_query(
            f'INSERT INTO "dpone_src"."{self._table}" (id, payload) VALUES (%s, %s)',
            (6, "after-bounds"),
        )
        return bounds


def _create_source_table(postgres: Any, table: str) -> None:
    ensure_postgres_source_schema(postgres, schema="dpone_src")
    postgres.execute_query(f'DROP TABLE IF EXISTS "dpone_src"."{table}"')
    postgres.execute_query(f'CREATE TABLE "dpone_src"."{table}" (id integer PRIMARY KEY, payload text NOT NULL)')
    postgres.execute_query(
        f"INSERT INTO \"dpone_src\".\"{table}\" (id, payload) VALUES (1, 'old'), (2, 'two'), (3, 'three'), (4, 'four')"
    )


def _config(table: str, tmp_path: Path, *, partitioning: dict[str, Any] | None = None) -> LoadConfig:
    options: dict[str, Any] = {
        "sink_type": "postgres",
        "runtime_storage": {"work_dir": str(tmp_path)},
        "batch_commit_mode": "whole",
    }
    if partitioning is not None:
        options["partitioning"] = partitioning
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="postgres_target",
        source_schema="dpone_src",
        source_table=table,
        target_schema="dpone_it",
        target_table=table,
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options=options,
    )


def _read_rows(artifact: Any) -> list[tuple[int, str]]:
    files = artifact.partitions if isinstance(artifact, PartitionedFileExportArtifact) else (artifact,)
    rows: list[tuple[int, str]] = []
    for file_artifact in files:
        with open(file_artifact.file_path, encoding="utf-8", newline="") as handle:
            rows.extend((int(row[0]), row[1]) for row in csv.reader(handle))
    return sorted(rows)


def test_partition_workers_import_one_real_snapshot_and_exclude_concurrent_commits(tmp_path: Path) -> None:
    if not postgres_enabled():
        pytest.skip("PostgreSQL vendor service is disabled")

    coordinator_raw = postgres_connector()
    mutator = postgres_connector()
    table = f"lifecycle_{uuid.uuid4().hex[:10]}"
    _create_source_table(coordinator_raw, table)
    coordinator = _RecordingConnector(coordinator_raw)
    coordinator_pid = int(coordinator.get_records("SELECT pg_backend_pid()")[0][0])
    strategy = _MutatingSnapshotStrategy(
        coordinator,
        NoopLogger(),
        mutator=mutator,
        table=table,
    )
    config = _config(
        table,
        tmp_path,
        partitioning={
            "column": "id",
            "bounds": {"lower": 1, "upper": 6},
            "num_partitions": 2,
            "export_workers": 2,
            "load_workers": 1,
        },
    )

    artifact = None
    try:
        extracted = strategy.extract(config, None)
        artifact = extracted.artifact
        assert isinstance(artifact, PartitionedFileExportArtifact)
        assert _read_rows(artifact) == [(1, "old"), (2, "two"), (3, "three"), (4, "four")]
        assert strategy.exported_snapshot is not None
        assert len(coordinator.imported_snapshots) == 2
        assert all(strategy.exported_snapshot in statement for statement in coordinator.imported_snapshots)
        assert len(set(coordinator.worker_backend_pids)) == 2
        assert coordinator_pid not in coordinator.worker_backend_pids
        assert extracted.extraction_receipt is not None
        assert extracted.extraction_receipt.complete is True
        assert extracted.extraction_receipt.source_token is not None
    finally:
        if artifact is not None:
            artifact.cleanup()
        try:
            mutator.rollback()
        except Exception:
            pass
        coordinator_raw.execute_query(f'DROP TABLE IF EXISTS "dpone_src"."{table}"')
        mutator.close()
        coordinator_raw.close()


def test_whole_copy_uses_explicit_rr_snapshot_and_truthful_source_token(tmp_path: Path) -> None:
    if not postgres_enabled():
        pytest.skip("PostgreSQL vendor service is disabled")

    source = postgres_connector()
    mutator = postgres_connector()
    table = f"whole_lifecycle_{uuid.uuid4().hex[:10]}"
    _create_source_table(source, table)
    artifact = None
    try:
        extracted = _MutatingWholeCopyStrategy(
            source,
            NoopLogger(),
            mutator=mutator,
            table=table,
        ).extract(_config(table, tmp_path), None)
        artifact = extracted.artifact

        assert _read_rows(artifact) == [(1, "old"), (2, "two"), (3, "three"), (4, "four")]
        assert extracted.extraction_receipt is not None
        assert extracted.extraction_receipt.complete is True
        assert extracted.extraction_receipt.source_token is not None
        assert source.get_records(f'SELECT payload FROM "dpone_src"."{table}" WHERE id = 1')[0][0] == "new"
        assert source.get_records(f'SELECT COUNT(*) FROM "dpone_src"."{table}"')[0][0] == 5
    finally:
        if artifact is not None:
            artifact.cleanup()
        source.execute_query(f'DROP TABLE IF EXISTS "dpone_src"."{table}"')
        mutator.close()
        source.close()


def test_auto_bounds_and_worker_files_share_one_pre_mutation_snapshot(tmp_path: Path) -> None:
    if not postgres_enabled():
        pytest.skip("PostgreSQL vendor service is disabled")

    source_raw = postgres_connector()
    mutator = postgres_connector()
    table = f"bounds_lifecycle_{uuid.uuid4().hex[:10]}"
    _create_source_table(source_raw, table)
    source = _RecordingConnector(source_raw)
    artifact = None
    try:
        extracted = _MutatingBoundsStrategy(
            source,
            NoopLogger(),
            mutator=mutator,
            table=table,
        ).extract(
            _config(
                table,
                tmp_path,
                partitioning={
                    "column": "id",
                    "bounds": "auto",
                    "target_rows_per_partition": 2,
                    "max_partitions": 4,
                    "export_workers": 2,
                    "load_workers": 1,
                },
            ),
            None,
        )
        artifact = extracted.artifact
        assert isinstance(artifact, PartitionedFileExportArtifact)

        assert _read_rows(artifact) == [(1, "old"), (2, "two"), (3, "three"), (4, "four")]
        assert source_raw.get_records(f'SELECT COUNT(*) FROM "dpone_src"."{table}"')[0][0] == 5
        assert len(source.imported_snapshots) == 2
        assert extracted.extraction_receipt is not None
        assert extracted.extraction_receipt.complete is True
    finally:
        if artifact is not None:
            artifact.cleanup()
        source_raw.execute_query(f'DROP TABLE IF EXISTS "dpone_src"."{table}"')
        mutator.close()
        source_raw.close()
