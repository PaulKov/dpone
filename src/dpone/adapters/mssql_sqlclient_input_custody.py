"""Durable, confined custody for immutable SqlClient input bytes."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from tempfile import mkstemp
from typing import BinaryIO

from dpone.adapters import mssql_native_publication_journal as publication
from dpone.adapters.mssql_sqlclient_native_receipt_contracts import native
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import SqlClientInputCustodyReceipt

_SCHEMA = "dpone.sqlclient.input-custody.v1"
_ERROR = "mssql_sqlclient.input_custody_invalid"
_COPY_BLOCK_BYTES = 1024 * 1024


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


class FileSqlClientInputCustody:
    """Create-only custody whose deletion requires the exact parent retirement."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def retain(
        self,
        data: bytes,
        *,
        plan_sha256: str,
        target_id: str,
        run_id: str,
        window_fingerprint: str,
        attempt_id: str,
        ordinal: int,
        rows: int,
        typed_digest: str,
    ) -> SqlClientInputCustodyReceipt:
        """Persist exact bytes durably and return their self-binding receipt."""
        if type(data) is not bytes:
            raise ValueError(_ERROR)
        file_sha256 = sha256(data).hexdigest()
        object_identity = {
            "plan_sha256": plan_sha256,
            "target_id": target_id,
            "run_id": run_id,
            "window_fingerprint": window_fingerprint,
            "attempt_id": attempt_id,
            "ordinal": ordinal,
            "rows": rows,
            "encoded_bytes": len(data),
            "file_sha256": file_sha256,
            "typed_digest": typed_digest,
        }
        durable_object_id = sha256(_canonical_bytes(object_identity)).hexdigest()
        path = self._path(durable_object_id)
        location_digest = sha256(str(path.relative_to(self._root)).encode()).hexdigest()
        facts = dict(
            schema=_SCHEMA,
            **object_identity,
            durable_object_id=durable_object_id,
            durable_location_sha256=location_digest,
        )
        digest = sha256(_SCHEMA.encode() + b"\0" + _canonical_bytes(facts)).hexdigest()
        receipt = SqlClientInputCustodyReceipt(
            schema=_SCHEMA,
            plan_sha256=plan_sha256,
            target_id=target_id,
            run_id=run_id,
            window_fingerprint=window_fingerprint,
            attempt_id=attempt_id,
            ordinal=ordinal,
            rows=rows,
            encoded_bytes=len(data),
            file_sha256=file_sha256,
            typed_digest=typed_digest,
            durable_object_id=durable_object_id,
            durable_location_sha256=location_digest,
            custody_sha256=digest,
        )
        if os.path.lexists(path):
            if self._read_regular(path, len(data)) != data:
                raise ValueError("mssql_sqlclient.input_custody_conflict")
        else:
            temporary = path.with_suffix(f".{os.getpid()}.tmp")
            try:
                with temporary.open("xb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
                self._fsync_root()
            finally:
                temporary.unlink(missing_ok=True)
        return receipt

    def retain_file(
        self,
        source: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        plan_sha256: str,
        target_id: str,
        run_id: str,
        window_fingerprint: str,
        attempt_id: str,
        ordinal: int,
        rows: int,
        typed_digest: str,
    ) -> SqlClientInputCustodyReceipt:
        """Stream one sealed regular file into durable custody with bounded memory."""
        receipt = self._receipt(
            encoded_bytes=expected_size,
            file_sha256=expected_sha256,
            plan_sha256=plan_sha256,
            target_id=target_id,
            run_id=run_id,
            window_fingerprint=window_fingerprint,
            attempt_id=attempt_id,
            ordinal=ordinal,
            rows=rows,
            typed_digest=typed_digest,
        )
        if not isinstance(source, Path):
            raise ValueError(_ERROR)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(source, flags)
        except OSError as error:
            raise RuntimeError("mssql_sqlclient.input_custody_unavailable") from error
        destination = self._path(receipt.durable_object_id)
        try:
            observed = os.fstat(descriptor)
            if not stat.S_ISREG(observed.st_mode) or observed.st_size != expected_size:
                raise RuntimeError("mssql_sqlclient.input_custody_drift")
            if os.path.lexists(destination):
                actual_size, actual_digest = self._stream_digest(descriptor)
                if actual_size != expected_size or actual_digest != expected_sha256:
                    raise RuntimeError("mssql_sqlclient.input_custody_drift")
                if self._regular_digest(destination, expected_size) != expected_sha256:
                    raise ValueError("mssql_sqlclient.input_custody_conflict")
                return receipt
            temporary_descriptor, temporary_name = mkstemp(
                prefix=f".{receipt.durable_object_id}.", suffix=".tmp", dir=self._root
            )
            temporary = Path(temporary_name)
            try:
                with os.fdopen(temporary_descriptor, "wb") as stream:
                    actual_size, actual_digest = self._copy_and_digest(descriptor, stream)
                    if actual_size != expected_size or actual_digest != expected_sha256:
                        raise RuntimeError("mssql_sqlclient.input_custody_drift")
                    stream.flush()
                    os.fsync(stream.fileno())
                try:
                    os.link(temporary, destination)
                except FileExistsError:
                    if self._regular_digest(destination, expected_size) != expected_sha256:
                        raise ValueError("mssql_sqlclient.input_custody_conflict") from None
            finally:
                temporary.unlink(missing_ok=True)
                self._fsync_root()
            return receipt
        finally:
            os.close(descriptor)

    def observe(self, receipt: SqlClientInputCustodyReceipt) -> bytes:
        """Read and verify the exact retained bytes; never repair drift."""
        self._validate_location(receipt)
        try:
            data = self._read_regular(self._path(receipt.durable_object_id), receipt.encoded_bytes)
        except OSError as error:
            raise RuntimeError("mssql_sqlclient.input_custody_unavailable") from error
        if len(data) != receipt.encoded_bytes or sha256(data).hexdigest() != receipt.file_sha256:
            raise RuntimeError("mssql_sqlclient.input_custody_drift")
        return data

    @contextmanager
    def open_pinned(self, receipt: SqlClientInputCustodyReceipt) -> Iterator[int]:
        """Pin and verify the retained regular file for one preparation call."""
        self._validate_location(receipt)
        path = self._path(receipt.durable_object_id)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise RuntimeError("mssql_sqlclient.input_custody_unavailable") from error
        try:
            observed = os.fstat(descriptor)
            if not stat.S_ISREG(observed.st_mode) or observed.st_size != receipt.encoded_bytes:
                raise RuntimeError("mssql_sqlclient.input_custody_drift")
            digest = sha256()
            while block := os.read(descriptor, 1024 * 1024):
                digest.update(block)
            if digest.hexdigest() != receipt.file_sha256:
                raise RuntimeError("mssql_sqlclient.input_custody_drift")
            os.lseek(descriptor, 0, os.SEEK_SET)
            yield descriptor
        finally:
            os.close(descriptor)

    def release(
        self,
        receipt: SqlClientInputCustodyReceipt,
        parent_chunk: native.NativeChunkReceipt,
        retirement: publication.NativeParentRetirementReceipt,
    ) -> None:
        """Idempotently delete only after an exact parent retirement receipt."""
        self._validate_location(receipt)
        if (
            type(parent_chunk) is not native.NativeChunkReceipt
            or type(retirement) is not publication.NativeParentRetirementReceipt
            or parent_chunk.ordinal != receipt.ordinal
            or parent_chunk.attempt_id != receipt.attempt_id
            or parent_chunk.rows != receipt.rows
            or parent_chunk.encoded_bytes != receipt.encoded_bytes
            or parent_chunk.file_sha256 != receipt.file_sha256
            or parent_chunk.typed_digest != receipt.typed_digest
            or parent_chunk.consumed_part_evidence.get("input_custody") != asdict(receipt)
        ):
            raise ValueError(_ERROR)
        matching = tuple(chunk for chunk in retirement.chunks if chunk.ordinal == receipt.ordinal)
        if (
            len(matching) != 1
            or matching[0].attempt_id != receipt.attempt_id
            or matching[0].verification_receipt_sha256 != publication.canonical_digest(asdict(parent_chunk))
        ):
            raise ValueError("mssql_sqlclient.input_custody_retirement_mismatch")
        path = self._path(receipt.durable_object_id)
        if not os.path.lexists(path):
            return
        self.observe(receipt)
        path.unlink()
        self._fsync_root()

    def _validate_location(self, receipt: SqlClientInputCustodyReceipt) -> None:
        if type(receipt) is not SqlClientInputCustodyReceipt:
            raise ValueError(_ERROR)
        receipt.__post_init__()
        relative = self._path(receipt.durable_object_id).relative_to(self._root)
        if sha256(str(relative).encode()).hexdigest() != receipt.durable_location_sha256:
            raise ValueError("mssql_sqlclient.input_custody_location_mismatch")

    def _path(self, object_id: str) -> Path:
        if len(object_id) != 64 or any(char not in "0123456789abcdef" for char in object_id):
            raise ValueError(_ERROR)
        path = self._root / f"{object_id}.bin"
        if path.parent != self._root:
            raise ValueError(_ERROR)
        return path

    def _fsync_root(self) -> None:
        descriptor = os.open(self._root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _receipt(
        self,
        *,
        encoded_bytes: int,
        file_sha256: str,
        plan_sha256: str,
        target_id: str,
        run_id: str,
        window_fingerprint: str,
        attempt_id: str,
        ordinal: int,
        rows: int,
        typed_digest: str,
    ) -> SqlClientInputCustodyReceipt:
        object_identity = {
            "plan_sha256": plan_sha256,
            "target_id": target_id,
            "run_id": run_id,
            "window_fingerprint": window_fingerprint,
            "attempt_id": attempt_id,
            "ordinal": ordinal,
            "rows": rows,
            "encoded_bytes": encoded_bytes,
            "file_sha256": file_sha256,
            "typed_digest": typed_digest,
        }
        durable_object_id = sha256(_canonical_bytes(object_identity)).hexdigest()
        path = self._path(durable_object_id)
        location_digest = sha256(str(path.relative_to(self._root)).encode()).hexdigest()
        return SqlClientInputCustodyReceipt.bind(
            **object_identity,
            durable_object_id=durable_object_id,
            durable_location_sha256=location_digest,
        )

    @staticmethod
    def _stream_digest(descriptor: int) -> tuple[int, str]:
        digest = sha256()
        size = 0
        while block := os.read(descriptor, _COPY_BLOCK_BYTES):
            size += len(block)
            digest.update(block)
        return size, digest.hexdigest()

    @staticmethod
    def _copy_and_digest(descriptor: int, stream: BinaryIO) -> tuple[int, str]:
        digest = sha256()
        size = 0
        while block := os.read(descriptor, _COPY_BLOCK_BYTES):
            size += len(block)
            digest.update(block)
            stream.write(block)
        return size, digest.hexdigest()

    @classmethod
    def _regular_digest(cls, path: Path, expected_size: int) -> str:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise RuntimeError("mssql_sqlclient.input_custody_unavailable") from error
        try:
            observed = os.fstat(descriptor)
            if not stat.S_ISREG(observed.st_mode) or observed.st_size != expected_size:
                raise ValueError("mssql_sqlclient.input_custody_conflict")
            return cls._stream_digest(descriptor)[1]
        finally:
            os.close(descriptor)

    @staticmethod
    def _read_regular(path: Path, expected_size: int) -> bytes:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise RuntimeError("mssql_sqlclient.input_custody_unavailable") from error
        try:
            observed = os.fstat(descriptor)
            if not stat.S_ISREG(observed.st_mode) or observed.st_size != expected_size:
                raise RuntimeError("mssql_sqlclient.input_custody_drift")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                return stream.read(expected_size + 1)
        finally:
            os.close(descriptor)


__all__ = ("FileSqlClientInputCustody",)
