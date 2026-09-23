"""One-shot bounded native input with completion authority outside the TDS SDK.

The worker must call ``require_complete`` after the SDK returns, even on an SDK
success result. A partial, abandoned, corrupted or failed iterator cannot issue a
receipt. SQL verification is still independently required by the coordinator.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Generator, Iterator
from typing import Any, BinaryIO

from dpone.contracts.mssql_tds_api import EncodedNativeFile, TdsInputReceipt
from dpone.ports.mssql_tds_input import TdsRowDecoder


class NativeTdsInput:
    """Decode one sealed file with bounded rows and explicit Arrow batch schemas.

    ``max_row_bytes`` bounds native row reads before allocation; ``batch_rows``
    bounds retained decoded rows. The enclosing worker's OS address-space limit
    additionally bounds Python, Arrow and SDK overhead. This iterable alone is
    not an RSS limit. Call exactly the iterator matching the selected input mode.
    """

    def __init__(
        self,
        file: EncodedNativeFile,
        decoder: TdsRowDecoder,
        *,
        input_mode: str,
        batch_rows: int,
        max_row_bytes: int,
    ) -> None:
        if type(input_mode) is not str or input_mode not in {"rows", "arrow"} or decoder.input_mode != input_mode:
            raise ValueError("mssql_native.tds_input_mode_mismatch")
        if type(batch_rows) is not int or not 1 <= batch_rows <= 65536:
            raise ValueError("mssql_native.tds_invalid_batch_rows")
        if type(max_row_bytes) is not int or max_row_bytes <= 0:
            raise ValueError("mssql_native.tds_invalid_max_row_bytes")
        self._file, self._decoder = file, decoder
        self._mode, self._batch_rows, self._max_row_bytes = input_mode, batch_rows, max_row_bytes
        self._started = False
        self._receipt: TdsInputReceipt | None = None
        self._failed = False
        self._finished = False

    def require_complete(self) -> TdsInputReceipt:
        """Fail closed when the SDK swallowed an error or stopped consuming early."""
        if self._failed or not self._finished or self._receipt is None:
            raise ValueError("mssql_native.tds_input_incomplete")
        return self._receipt

    def iter_rows(self) -> Iterator[tuple[Any, ...]]:
        """Yield exact typed tuples once; EOF must actually be requested."""
        self._begin("rows")
        return self._deliver_rows()

    def iter_arrow_batches(self) -> Iterator[Any]:
        """Lazily import Arrow and yield explicitly typed, bounded RecordBatches."""
        self._begin("arrow")
        return self._arrow_batches()

    def _begin(self, mode: str) -> None:
        if mode != self._mode:
            raise ValueError("mssql_native.tds_input_mode_mismatch")
        if self._started:
            raise ValueError("mssql_native.tds_input_already_consumed")
        self._started = True

    def _deliver_rows(self) -> Iterator[tuple[Any, ...]]:
        yield from self._rows()
        self._finished = True

    def _rows(self) -> Generator[tuple[Any, ...], None, None]:
        rows, consumed = 0, 0
        digest = hashlib.sha256()

        def observe(data: bytes) -> None:
            nonlocal consumed
            consumed += len(data)
            digest.update(data)

        try:
            fd = os.open(self._file.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as handle:
                identity = os.fstat(handle.fileno())
                if not stat.S_ISREG(identity.st_mode):
                    raise ValueError("mssql_native.tds_input_file_changed")
                self._verify(handle, identity)
                for row in self._decoder.iter_rows(handle, max_row_bytes=self._max_row_bytes, on_bytes=observe):
                    rows += 1
                    if rows > self._file.rows:
                        raise ValueError("mssql_native.tds_input_row_count_mismatch")
                    yield row
                if (rows, consumed, digest.hexdigest()) != (
                    self._file.rows,
                    self._file.encoded_bytes,
                    self._file.file_sha256,
                ):
                    raise ValueError("mssql_native.tds_input_identity_mismatch")
                self._verify(handle, identity)
                self._receipt = TdsInputReceipt(rows, consumed, digest.hexdigest())
        except BaseException:
            self._failed = True
            raise

    def _verify(self, handle: BinaryIO, identity: os.stat_result) -> None:
        actual = os.fstat(handle.fileno())
        named = self._file.path.lstat()
        if (
            not stat.S_ISREG(actual.st_mode)
            or not stat.S_ISREG(named.st_mode)
            or (actual.st_dev, actual.st_ino) != (named.st_dev, named.st_ino)
            or (actual.st_dev, actual.st_ino, actual.st_size, actual.st_mtime_ns, actual.st_ctime_ns)
            != (identity.st_dev, identity.st_ino, identity.st_size, identity.st_mtime_ns, identity.st_ctime_ns)
            or actual.st_size != self._file.encoded_bytes
            or actual.st_mode & 0o222
        ):
            raise ValueError("mssql_native.tds_input_file_changed")
        handle.seek(0)
        digest = hashlib.sha256()
        while data := handle.read(65536):
            digest.update(data)
        if digest.hexdigest() != self._file.file_sha256:
            raise ValueError("mssql_native.tds_input_file_changed")
        handle.seek(0)

    def _arrow_batches(self) -> Iterator[Any]:
        # Optional SDK imports must not leak onto base import/help paths.
        try:
            import pyarrow as pa

            types = {
                "bigint": pa.int64(),
                "float": pa.float64(),
                "nvarchar": pa.string(),
                "datetime2": pa.timestamp("us"),
            }
            schema = pa.schema(
                [pa.field(c.name, types[c.storage_type], nullable=c.nullable) for c in self._decoder.columns]
            )
            iterator = self._rows()
            try:
                batch: list[tuple[Any, ...]] = []
                for row in iterator:
                    batch.append(row)
                    if len(batch) == self._batch_rows:
                        yield _record_batch(pa, schema, batch)
                        batch = []
                if batch:
                    yield _record_batch(pa, schema, batch)
                self._finished = True
            finally:
                iterator.close()
        except BaseException:
            self._failed = True
            raise


def _record_batch(pa: Any, schema: Any, rows: list[tuple[Any, ...]]) -> Any:
    arrays = [pa.array([row[i] for row in rows], type=field.type, from_pandas=False) for i, field in enumerate(schema)]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
