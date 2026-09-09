from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, BinaryIO

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.process_io import add_cleanup_failure_note
from dpone.runtime.staging import StagingManager


@dataclass(frozen=True, slots=True)
class ByteStreamStats:
    size_bytes: int = 0
    chunks: int = 0
    sha256: str = "sha256:" + "0" * 64

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ByteStreamArtifact(BaseExtractionArtifact):
    """Bounded byte-stream extraction artifact for zero-file native transfer."""

    extraction_completion_mode = "lazy"

    def __init__(
        self,
        chunk_factory: Callable[[], Iterable[bytes]],
        *,
        columns: Sequence[str],
        format: str,
        estimated_rows: int | None = None,
        cleanup_callback: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(estimated_rows=estimated_rows)
        self.columns = tuple(columns)
        self.format = format
        self.bulk_text_codec: Any | None = None
        self.bulk_wire_contract: Any | None = None
        self.source_export_provider: str | None = None
        self.streaming_policy: Any | None = None
        self.streaming_route_decision: Any | None = None
        self._chunk_factory = chunk_factory
        self._cleanup_callback = cleanup_callback
        self._closed = False
        self._size_bytes = 0
        self._chunks = 0
        self._digest = sha256()

    @classmethod
    def from_binary_reader(
        cls,
        reader: BinaryIO,
        *,
        columns: Sequence[str],
        format: str,
        chunk_size: int,
        estimated_rows: int | None = None,
        cleanup_callback: Callable[[], None] | None = None,
    ) -> ByteStreamArtifact:
        def chunks() -> Iterator[bytes]:
            while chunk := reader.read(chunk_size):
                yield chunk

        def cleanup() -> None:
            try:
                reader.close()
            finally:
                if cleanup_callback is not None:
                    cleanup_callback()

        return cls(
            chunks,
            columns=columns,
            format=format,
            estimated_rows=estimated_rows,
            cleanup_callback=cleanup,
        )

    @property
    def stats(self) -> ByteStreamStats:
        return ByteStreamStats(
            size_bytes=self._size_bytes,
            chunks=self._chunks,
            sha256="sha256:" + self._digest.hexdigest(),
        )

    def iter_bytes(self) -> Iterator[bytes]:
        lifecycle = self.extraction_lifecycle
        if lifecycle is not None and lifecycle.receipt is None:
            lifecycle.acquire()
        chunks: Iterator[bytes] | None = None
        try:
            chunks = iter(self._chunk_factory())
            for chunk in chunks:
                if not isinstance(chunk, bytes):
                    chunk = bytes(chunk)
                self._size_bytes += len(chunk)
                self._chunks += 1
                self._digest.update(chunk)
                yield chunk
        except GeneratorExit:
            try:
                self._close_chunks(chunks)
            except BaseException as cleanup_error:
                self._cleanup_after(cleanup_error)
                raise
            self.cleanup()
            raise
        except BaseException as error:
            self._close_chunks(chunks, parent_error=error)
            self._cleanup_after(error)
            raise
        else:
            try:
                self._close_chunks(chunks)
            except BaseException as error:
                self._cleanup_after(error)
                raise
            if lifecycle is not None:
                lifecycle.complete()
            self.cleanup()

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        del staging_manager, load_config, schema
        raise RuntimeError("byte_stream_artifact_requires_streaming_sink_loader")

    def cleanup(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._cleanup_callback is not None:
            self._cleanup_callback()

    @staticmethod
    def _close_chunks(chunks: Iterator[bytes] | None, *, parent_error: BaseException | None = None) -> None:
        close_chunks = getattr(chunks, "close", None)
        if not callable(close_chunks):
            return
        try:
            close_chunks()
        except Exception as cleanup_error:
            if parent_error is None:
                raise
            add_cleanup_failure_note(
                parent_error,
                context="byte stream iterator cleanup",
                cleanup_error=cleanup_error,
            )

    def _cleanup_after(self, parent_error: BaseException) -> None:
        try:
            self.cleanup()
        except Exception as cleanup_error:
            add_cleanup_failure_note(
                parent_error,
                context="byte stream artifact cleanup",
                cleanup_error=cleanup_error,
            )


__all__ = ["ByteStreamArtifact", "ByteStreamStats"]
