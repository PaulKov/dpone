"""Bounded IPC frames and sealed native files; no source or target connections.

Typed digest is a constant-space probabilistic SHA-256 multiset sum. It retains
multiplicity but is not an exact equality proof. Native framing fixes row bytes.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Generator, Iterator
from contextlib import closing
from pathlib import Path

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkLimits
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.mssql_native_sized_frames import NativeRow as NativeRow
from dpone.runtime.mssql_native_sized_frames import sized_native_frames
from dpone.runtime.native_wire_models import SourceNativeWireContract


def native_multiset_digest(count: int, total: int) -> str:
    """Bind multiplicity and an order-independent sum of framed native row hashes."""
    return hashlib.sha256(json.dumps(["mssql-native-sha256-sum-v1", count, total]).encode()).hexdigest()


def native_frames(
    rows: Iterator[NativeRow],
    contract: SourceNativeWireContract,
    limits: NativeChunkLimits,
    check: Callable[[], None] | None = None,
    ipc_overhead: int = 0,
) -> Generator[tuple[NativeRow, ...], None, None]:
    """Compatibility tuple adapter over the single bounded framing algorithm.

    Closing this adapter also releases its sized iterator. The caller retains
    ownership of the source iterator, as with :func:`sized_native_frames`.
    """
    with closing(sized_native_frames(rows, contract, limits, check, ipc_overhead)) as frames:
        for frame in frames:
            yield frame.rows


def encode_native_frame(
    contract: SourceNativeWireContract,
    rows: tuple[NativeRow, ...],
    path: Path,
    ordinal: int,
    max_row_bytes: int,
    max_bytes: int,
) -> EncodedNativeFile:
    """Spawn-safe worker: validate, encode, fsync and seal one bounded owned file."""
    encoder = MssqlNativeEncoder(contract, max_row_bytes=max_row_bytes)
    count, size, total = 0, 0, 0
    digest = hashlib.sha256()
    # Exclusive creation never follows or overwrites an existing path.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            for row in rows:
                encoded = encoder.encode_row(row)
                if size + len(encoded) > max_bytes:
                    raise WindowContractError("mssql_native.chunk_bytes_exceeded")
                stream.write(encoded)
                digest.update(encoded)
                total = (total + int.from_bytes(hashlib.sha256(encoded).digest(), "big")) % (1 << 256)
                size += len(encoded)
                count += 1
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(path, 0o400)
        return EncodedNativeFile(path, ordinal, count, size, digest.hexdigest(), native_multiset_digest(count, total))
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def verify_native_file(file: EncodedNativeFile) -> None:
    """Retries use only the retained immutable bytes bound before first import."""
    if file.path.is_symlink() or not file.path.is_file() or file.path.stat().st_size != file.encoded_bytes:
        raise WindowContractError("mssql_native.retained_file_changed")
    digest = hashlib.sha256()
    with file.path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != file.file_sha256:
        raise WindowContractError("mssql_native.retained_file_changed")


def discard_native_files(directory: Path) -> None:
    """After fenced writer settlement, remove only this invocation's known files."""
    if directory.is_symlink():
        raise WindowContractError("mssql_native.owned_directory_changed")
    if not directory.exists():
        return
    for path in directory.iterdir():
        if path.suffix == ".native" and path.stem.isdecimal():
            path.unlink()
    if not any(directory.iterdir()):
        directory.rmdir()
