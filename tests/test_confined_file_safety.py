from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import pytest

from dpone.manifest import confined_files
from dpone.manifest.authoring_migration_io import AuthoringMigrationFileSystem, AuthoringMigrationIoError
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file, sha256_confined_file

_requires_fifo = pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX FIFO support is unavailable")


@_requires_fifo
def test_confined_reader_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    os.mkfifo(source)

    with pytest.raises(ConfinedFileError, match="not a regular file") as caught:
        read_confined_file(tmp_path, "pipeline.yaml", max_bytes=1024)

    assert caught.value.code == "not_regular_file"


@_requires_fifo
def test_authoring_migration_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    os.mkfifo(source)

    with pytest.raises(AuthoringMigrationIoError) as caught:
        AuthoringMigrationFileSystem(tmp_path).read_source("pipeline.yaml")

    assert caught.value.code == "DPONE_AUTHORING_MIGRATION_PATH_INVALID"


@pytest.mark.parametrize(
    "operation",
    [
        lambda root: read_confined_file(root, "pipeline.yaml", max_bytes=256 * 1024),
        lambda root: sha256_confined_file(root, "pipeline.yaml"),
    ],
    ids=["read", "sha256"],
)
def test_confined_snapshot_rejects_in_place_source_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: Callable[[Path], object],
) -> None:
    source = tmp_path / "pipeline.yaml"
    original = b"a" * (128 * 1024)
    source.write_bytes(original)
    original_read = confined_files._read_chunk
    mutated = False

    def read_then_mutate(descriptor: int, size: int) -> bytes:
        nonlocal mutated
        chunk = original_read(descriptor, size)
        if chunk and not mutated:
            mutated = True
            with source.open("r+b", buffering=0) as writer:
                writer.seek(0)
                writer.write(b"b" * len(original))
                os.fsync(writer.fileno())
        return chunk

    monkeypatch.setattr(confined_files, "_read_chunk", read_then_mutate)

    with pytest.raises(ConfinedFileError) as caught:
        operation(tmp_path)

    assert caught.value.code == "source_changed"
