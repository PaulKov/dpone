"""MSSQL single-scan physical chunk export adapter."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

from dpone.runtime.physical_chunking import (
    PhysicalChunkedFileExportArtifact,
    PhysicalChunkPolicy,
    RowBoundaryChunkWriter,
)
from dpone.runtime.process_io import abort_after_failure, iter_fifo_bytes


class BcpSingleScanChunkExporter:
    """Run one ``bcp queryout`` and spool its output into row-safe chunks."""

    def __init__(self, connector: Any, logger: Any) -> None:
        self.connector = connector
        self.logger = logger

    def artifact(
        self,
        *,
        query: str,
        columns: tuple[str, ...],
        directory: Path,
        bcp_options: Any,
        policy: PhysicalChunkPolicy,
        source_table: str,
        artifact_format: str,
        bulk_text_codec: Any | None,
        bulk_wire_contract: Any | None,
        source_scan_decision: Any,
    ) -> PhysicalChunkedFileExportArtifact:
        evidence_path = directory / f"dpone_physical_chunks_{uuid.uuid4().hex}.json"

        def chunks():
            yield from self._export_chunks(
                query=query,
                columns=columns,
                directory=directory,
                bcp_options=bcp_options,
                policy=policy,
                source_table=source_table,
                artifact_format=artifact_format,
            )

        return PhysicalChunkedFileExportArtifact(
            chunk_generator=chunks,
            columns=columns,
            evidence_path=evidence_path,
            format=artifact_format,
            bulk_text_codec=bulk_text_codec,
            bulk_wire_contract=bulk_wire_contract,
            source_scan_decision=source_scan_decision,
        )

    def _export_chunks(
        self,
        *,
        query: str,
        columns: tuple[str, ...],
        directory: Path,
        bcp_options: Any,
        policy: PhysicalChunkPolicy,
        source_table: str,
        artifact_format: str,
    ):
        directory.mkdir(parents=True, exist_ok=True)
        fifo_path = directory / f"dpone_mssql_single_scan_{uuid.uuid4().hex}.fifo"
        os.mkfifo(fifo_path)
        process = None
        try:
            runner = self.connector._bcp_runner(bcp_options)
            process = runner.queryout_process(query, str(fifo_path))
            writer = RowBoundaryChunkWriter(
                policy=policy,
                columns=columns,
                directory=directory,
                format=artifact_format,
                row_terminator=str(bcp_options.row_terminator or "\n").encode("utf-8"),
            )
            yield from writer.write(iter_fifo_bytes(fifo_path, process, read_buffer_bytes=1024 * 1024))
            result = process.wait()
            self.logger.log_etl_progress(
                "MSSQL_BCP_SINGLE_SCAN_CHUNKS",
                {"Rows": result.rows_copied, "Source": source_table, "Chunks": "physical"},
            )
        except BaseException as error:
            if process is not None:
                abort_after_failure(process, error, operation="bcp_abort")
            raise
        finally:
            fifo_path.unlink(missing_ok=True)


__all__ = ["BcpSingleScanChunkExporter"]
