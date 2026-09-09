from __future__ import annotations

import builtins
from pathlib import Path

import pytest

import dpone.runtime.artifact_integrity as artifact_integrity_module
from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.incremental_snapshot import DELTA_HASH_COLUMN, KEY_HASH_COLUMN
from dpone.runtime.sources.strategies.postgres.postgres_baseline_snapshot_artifacts import (
    build_baseline_snapshot_files,
)
from dpone.runtime.sources.strategies.postgres.postgres_delta_snapshot import (
    PostgresDeltaSnapshotFileArtifact,
)
from dpone.runtime.sources.strategies.postgres.postgres_key_snapshot import (
    PostgresKeySnapshotFileArtifact,
)
from dpone.runtime.support.bulk_text_codec import BulkTextCodec


def _raw(path: Path, payload: bytes) -> FileExportArtifact:
    path.write_bytes(payload)
    return FileExportArtifact(
        str(path),
        ("guid", "metric_code", "metric_value"),
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=payload.count(b"\n"),
    )


def _snapshot(
    raw: FileExportArtifact,
) -> tuple[PostgresDeltaSnapshotFileArtifact, PostgresKeySnapshotFileArtifact]:
    delta_file, key_file = build_baseline_snapshot_files(
        raw,
        delta_schema=(("guid", "uuid"), ("metric_code", "text"), ("metric_value", "numeric")),
        key_columns=("guid",),
        delta_hash_column=DELTA_HASH_COLUMN,
        key_hash_column=KEY_HASH_COLUMN,
    )
    return (
        PostgresDeltaSnapshotFileArtifact.from_hashed_artifact(
            delta_file,
            snapshot_token="sha256:snapshot",
            scope_hash="sha256:scope",
            columns=("guid", "metric_code", "metric_value"),
        ),
        PostgresKeySnapshotFileArtifact.from_hashed_artifact(
            key_file,
            snapshot_token="sha256:snapshot",
            scope_hash="sha256:scope",
            key_columns=("guid",),
        ),
    )


def test_baseline_projection_reads_raw_file_once_and_creates_no_sqlite_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_path = tmp_path / "raw.bcp"
    raw = _raw(
        raw_path,
        b"00000000-0000-0000-0000-000000000001\tmetric-a\t1.25\n00000000-0000-0000-0000-000000000002\tmetric-b\t2.50\n",
    )
    raw_open_count = 0
    real_open = builtins.open

    def observed_open(file, *args, **kwargs):
        nonlocal raw_open_count
        if Path(file) == raw_path:
            raw_open_count += 1
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", observed_open)

    delta, keys = _snapshot(raw)

    assert raw_open_count == 1
    assert not raw_path.exists()
    assert list(tmp_path.glob("*.sqlite")) == []
    assert delta.receipt.row_count == 2
    assert keys.receipt.row_count == 2
    assert tuple(delta.columns) == ("guid", "metric_code", "metric_value", DELTA_HASH_COLUMN)
    assert tuple(keys.columns) == ("guid", KEY_HASH_COLUMN)
    assert len(Path(delta.file_path).read_bytes().splitlines()[0].split(b"\t")[-1]) == 64
    assert len(Path(keys.file_path).read_bytes().splitlines()[0].split(b"\t")[-1]) == 64
    delta.cleanup()
    keys.cleanup()


def test_baseline_projection_reuses_writer_digests_without_post_write_rescan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _raw(
        tmp_path / "raw.bcp",
        b"00000000-0000-0000-0000-000000000001\tmetric-a\t1.25\n",
    )

    def unexpected_rescan(_path: str):
        raise AssertionError("completed projection files must not be rescanned at construction")

    monkeypatch.setattr(artifact_integrity_module, "_file_identity", unexpected_rescan)

    delta, keys = _snapshot(raw)

    assert delta.receipt.row_count == keys.receipt.row_count == 1
    delta.cleanup()
    keys.cleanup()


def test_baseline_projection_leaves_duplicate_detection_to_set_based_sink_validation(tmp_path: Path) -> None:
    raw = _raw(
        tmp_path / "raw.bcp",
        b"00000000-0000-0000-0000-000000000001\tmetric-a\t1\n00000000-0000-0000-0000-000000000001\tmetric-b\t2\n",
    )

    delta, keys = _snapshot(raw)

    assert delta.receipt.row_count == keys.receipt.row_count == 2
    delta.cleanup()
    keys.cleanup()


def test_baseline_projection_artifacts_still_fail_closed_on_byte_tamper(tmp_path: Path) -> None:
    raw = _raw(
        tmp_path / "raw.bcp",
        b"00000000-0000-0000-0000-000000000001\tmetric-a\t1\n",
    )
    delta, keys = _snapshot(raw)
    Path(delta.file_path).write_bytes(Path(delta.file_path).read_bytes() + b"tamper")

    with pytest.raises(ArtifactIntegrityError):
        delta.materialize(object(), object(), ())

    delta.cleanup()
    keys.cleanup()
