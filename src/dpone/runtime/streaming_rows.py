"""Streaming row extraction artifact."""

from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator, Mapping, Sequence

from dpone.runtime.artifact_logging import etl_logger
from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.extraction_lifecycle import (
    ArtifactTerminalOutcome,
    ExtractionLifecycleAuthority,
)
from dpone.runtime.staging import StagingManager, owned_staging_handle


class StreamingRowsArtifact(BaseExtractionArtifact):
    """Extraction artifact backed by a streaming row iterator."""

    extraction_completion_mode = "stream_acquired"

    def __init__(
        self,
        iterator: Iterator[Mapping[str, object]],
        *,
        batch_size: int = 10000,
        estimated_rows: int | None = None,
        extraction_lifecycle: ExtractionLifecycleAuthority | None = None,
        on_success: Callable[[], None] | None = None,
        on_abort: Callable[[], None] | None = None,
        cleanup_callback: Callable[[], None] | None = None,
    ):
        super().__init__(estimated_rows=estimated_rows)
        self._iterator = iterator
        self._batch_size = batch_size
        if cleanup_callback is not None and (on_success is not None or on_abort is not None):
            raise ValueError("streaming_rows.explicit_and_legacy_terminal_callbacks")
        # Compatibility callbacks describe ordinary resource closure and run
        # for either known terminal outcome. Transactional sources must use
        # explicit success/abort handlers instead.
        self._on_success = on_success or cleanup_callback
        self._on_abort = on_abort or cleanup_callback
        if extraction_lifecycle is not None:
            self.bind_extraction_lifecycle(extraction_lifecycle)

    def rebind_iterator(self, iterator: Iterator[Mapping[str, object]]) -> StreamingRowsArtifact:
        """Replace the backing iterator without changing artifact identity.

        Payload transformers (lineage, schema projection, temporal fidelity) must
        keep the same instance so ``materialize`` / streaming insert can publish
        ``rows_exported`` onto the artifact still referenced by ``extract_result``.
        """

        self._iterator = iterator
        return self

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            return self._materialize_into(handle, staging_manager)

    def _materialize_into(
        self,
        handle: StagingTableArtifact,
        staging_manager: StagingManager,
    ) -> StagingTableArtifact:
        """Consume the iterator and publish completion onto an owned staging table."""

        iterator = self._iterator
        batch_size = self._batch_size

        insert_streaming_rows = getattr(staging_manager, "insert_streaming_rows", None)
        if callable(insert_streaming_rows):
            total_inserted = insert_streaming_rows(handle, iterator)
            if isinstance(total_inserted, bool) or not isinstance(total_inserted, int) or total_inserted < 0:
                raise RuntimeError("streaming_rows.invalid_bulk_stream_row_count")
            return self._complete_materialization(
                handle,
                total_inserted=total_inserted,
                materialization_batches=None,
            )

        def batched_rows() -> Iterator[list[Mapping[str, object]]]:
            while True:
                chunk = list(itertools.islice(iterator, batch_size))
                if not chunk:
                    break
                yield chunk

        total_inserted = 0
        batch_num = 0
        for chunk in batched_rows():
            batch_num += 1
            inserted = staging_manager.insert_rows(handle, chunk)
            total_inserted += inserted

            if batch_num == 1 or batch_num % 10 == 0:
                etl_logger.info(f"📦 Streaming batch {batch_num}: +{inserted:,} rows, total: {total_inserted:,}")

        if batch_num == 0:
            # Materialize an immutable zero-row receipt.  MSSQL uses it to
            # bind wire/schema/provenance evidence without invoking BCP;
            # other managers preserve their ordinary empty-insert semantics.
            total_inserted = staging_manager.insert_rows(handle, ())

        return self._complete_materialization(
            handle,
            total_inserted=total_inserted,
            materialization_batches=batch_num,
        )

    def _complete_materialization(
        self,
        handle: StagingTableArtifact,
        *,
        total_inserted: int,
        materialization_batches: int | None,
    ) -> StagingTableArtifact:
        """Publish one authoritative terminal row count for either insert path."""

        metrics: dict[str, object] = {
            "Total_Rows": total_inserted,
            "Staging": handle.qualified_name(),
        }
        if materialization_batches is None:
            metrics["Sink_Stream_Materializations"] = 1
        else:
            metrics["Materialization_Batches"] = materialization_batches
        etl_logger.log_etl_progress("STREAMING_MATERIALIZE_COMPLETE", metrics)

        handle.row_count = total_inserted
        # Authoritative export count for quality gates (source_target_count /
        # min_rows on source). estimated_rows must not certify gates.
        self.rows_exported = total_inserted
        self.row_count = total_inserted
        lifecycle = self.extraction_lifecycle
        if lifecycle is not None:
            lifecycle.complete()
        return handle

    def set_cleanup(self, callback: Callable[[], None]) -> None:
        """Bind legacy resource closure for either known terminal outcome."""

        self.set_terminal_callbacks(on_success=callback, on_abort=callback)

    def set_terminal_callbacks(
        self,
        *,
        on_success: Callable[[], None] | None,
        on_abort: Callable[[], None] | None,
    ) -> None:
        """Bind separate commit and rollback handlers before termination."""

        with self._terminal_lock:
            if self.terminal_receipt is not None:
                raise ValueError("streaming_rows.already_terminated")
            self._on_success = on_success
            self._on_abort = on_abort

    def cleanup(self) -> None:
        """Fail-closed compatibility entrypoint: abort and roll back source."""

        self.terminate(ArtifactTerminalOutcome.ABORT)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        callback = self._on_success if outcome is ArtifactTerminalOutcome.SUCCESS else self._on_abort
        self._on_success = None
        self._on_abort = None
        errors: list[BaseException] = []
        close = getattr(self._iterator, "close", None)
        if callable(close):
            try:
                close()
            except BaseException as exc:  # still attempt the transaction/resource handler
                errors.append(exc)
        if callback is not None:
            try:
                callback()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise errors[0]

    def _should_release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> bool:
        """A cursor/session is never durable evidence and must never be retained."""

        del outcome
        return True


__all__ = ["StreamingRowsArtifact"]
