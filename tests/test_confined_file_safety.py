from __future__ import annotations

import hashlib
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


@pytest.mark.parametrize("hash_only", [False, True], ids=["read", "sha256"])
@pytest.mark.parametrize("size,limit", [(0, 0), (65535, 65536), (65536, 65536), (65537, 65536)])
def test_confined_content_verification_preserves_byte_limits(
    tmp_path: Path, hash_only: bool, size: int, limit: int
) -> None:
    content = b"a" * size
    (tmp_path / "input").write_bytes(content)

    def read() -> bytes | str:
        if hash_only:
            return sha256_confined_file(tmp_path, "input", max_bytes=limit)
        return read_confined_file(tmp_path, "input", max_bytes=limit)

    if size > limit:
        with pytest.raises(ConfinedFileError) as caught:
            read()
        assert caught.value.code == "file_too_large"
    else:
        expected = "sha256:" + hashlib.sha256(content).hexdigest() if hash_only else content
        assert read() == expected


@pytest.mark.parametrize("failure", ["seek", "read", "stat"])
@pytest.mark.parametrize("hash_only", [False, True], ids=["read", "sha256"])
def test_confined_content_verification_io_failure_is_not_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str, hash_only: bool
) -> None:
    (tmp_path / "input").write_bytes(b"a" * (128 * 1024))
    original_seek = confined_files.os.lseek

    def unavailable(*args: object) -> int:
        raise OSError("verification unavailable")

    def seek(descriptor: int, offset: int, whence: int) -> int:
        if failure == "seek":
            return unavailable()
        result = original_seek(descriptor, offset, whence)
        if failure == "read":
            monkeypatch.setattr(confined_files, "_read_chunk", unavailable)
        else:
            monkeypatch.setattr(confined_files.os, "fstat", unavailable)
        return result

    monkeypatch.setattr(confined_files.os, "lseek", seek)
    with pytest.raises(ConfinedFileError) as caught:
        if hash_only:
            sha256_confined_file(tmp_path, "input")
        else:
            read_confined_file(tmp_path, "input", max_bytes=256 * 1024)
    assert caught.value.code == "file_unavailable"


@pytest.mark.parametrize("hash_only", [False, True], ids=["read", "sha256"])
@pytest.mark.parametrize("mutate_on_read", [1, 4], ids=["capture", "verification"])
def test_confined_snapshot_rejects_content_change_with_identical_stat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, hash_only: bool, mutate_on_read: int
) -> None:
    """Coarse filesystem timestamps cannot make a torn snapshot trustworthy."""
    source = tmp_path / "pipeline.yaml"
    source.write_bytes(b"a" * (128 * 1024))
    frozen_stat = source.stat()
    original_read = confined_files._read_chunk
    reads = 0

    def read_then_mutate(descriptor: int, size: int) -> bytes:
        nonlocal reads
        chunk = original_read(descriptor, size)
        reads += 1
        if reads == mutate_on_read:
            with source.open("r+b", buffering=0) as writer:
                writer.write(b"b" * frozen_stat.st_size)
                os.fsync(writer.fileno())
        return chunk

    monkeypatch.setattr(confined_files.os, "fstat", lambda _: frozen_stat)
    monkeypatch.setattr(confined_files, "_read_chunk", read_then_mutate)
    with pytest.raises(ConfinedFileError) as caught:
        if hash_only:
            sha256_confined_file(tmp_path, source.name)
        else:
            read_confined_file(tmp_path, source.name, max_bytes=frozen_stat.st_size)
    assert caught.value.code == "source_changed"


@pytest.mark.parametrize("new_size", [0, 256 * 1024], ids=["truncate", "grow"])
def test_confined_verification_rejects_size_change_with_bounded_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, new_size: int
) -> None:
    source = tmp_path / "input"
    source.write_bytes(b"a" * (128 * 1024))
    original_read = confined_files._read_chunk
    reads: list[int] = []

    def read_then_resize(descriptor: int, size: int) -> bytes:
        reads.append(size)
        chunk = original_read(descriptor, size)
        if len(reads) == 4:
            with source.open("r+b") as writer:
                writer.truncate(new_size)
        return chunk

    monkeypatch.setattr(confined_files, "_read_chunk", read_then_resize)
    with pytest.raises(ConfinedFileError) as caught:
        sha256_confined_file(tmp_path, source.name)
    assert caught.value.code == "source_changed"
    assert len(reads) <= 6
    assert max(reads) <= 64 * 1024
