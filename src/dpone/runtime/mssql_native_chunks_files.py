"""Bounded IPC frames and sealed native files; no source or target connections.

Typed digest is a constant-space probabilistic SHA-256 multiset sum. It retains
multiplicity but is not an exact equality proof. Native framing fixes row bytes.
"""

from __future__ import annotations

import hashlib
import json
import os
import pickle
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from pathlib import Path
from typing import TypeAlias

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkLimits
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_models import SourceNativeWireContract

NativeRow: TypeAlias = Sequence[object] | Mapping[str, object]


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
    """One producer bounds native and serialized IPC bytes independently.

    Individual pickle lengths plus conservative framing overhead avoid repeatedly
    serializing an ever-growing frame. Exact aggregate IPC size is checked before
    submission. Sizing is not value-validation evidence; workers check all values.
    """
    encoder = MssqlNativeEncoder(contract, max_row_bytes=limits.max_row_bytes)
    frame: list[NativeRow] = []
    native_bytes, ipc_bytes = 0, 64 + ipc_overhead
    emitted = False
    for index, source_row in enumerate(rows):
        if check is not None and index % 1024 == 0:
            check()

        # Drivers may reuse a row container or binary buffer between fetches.
        def freeze(value: object) -> object:
            return bytes(value) if isinstance(value, (bytearray, memoryview)) else value

        row: NativeRow = (
            {key: freeze(value) for key, value in source_row.items()}
            if isinstance(source_row, Mapping)
            else tuple(freeze(value) for value in source_row)
        )
        size = encoder.encoded_row_size(row)
        ipc_size = len(pickle.dumps(row, protocol=5)) + 16
        if size > limits.max_row_bytes or ipc_size + 64 + ipc_overhead > limits.max_bytes:
            raise WindowContractError("mssql_native.row_exceeds_frame_limit")
        if frame and (
            native_bytes + size > limits.max_bytes
            or ipc_bytes + ipc_size > limits.max_bytes
            or len(frame) >= limits.max_rows
        ):
            yield tuple(frame)
            emitted = True
            frame, native_bytes, ipc_bytes = [], 0, 64 + ipc_overhead
        frame.append(row)
        native_bytes += size
        ipc_bytes += ipc_size
    if frame or not emitted:
        yield tuple(frame)


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
