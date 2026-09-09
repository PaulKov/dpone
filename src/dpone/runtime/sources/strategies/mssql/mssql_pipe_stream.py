"""MSSQL BCP pipe streaming adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.streaming_transfer import StreamingRouteDecision, StreamingTransferPolicy


import os
import uuid
from pathlib import Path
from typing import Any

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.process_io import abort_after_failure, iter_fifo_bytes


class BcpPipeStreamExporter:
    """Run one ``bcp queryout`` into a FIFO and expose it as a byte stream."""

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
        policy: StreamingTransferPolicy,
        route_decision: StreamingRouteDecision,
        source_table: str,
        bulk_text_codec: Any | None,
        bulk_wire_contract: Any | None,
    ) -> ByteStreamArtifact:
        artifact_ref: dict[str, ByteStreamArtifact] = {}

        def chunks():
            yield from self._stream_fifo(
                query=query,
                directory=directory,
                bcp_options=bcp_options,
                source_table=source_table,
                read_buffer_bytes=policy.read_buffer_bytes,
                artifact=artifact_ref["artifact"],
            )

        artifact = ByteStreamArtifact(
            chunks,
            columns=columns,
            format="mssql-delimited",
            cleanup_callback=None,
        )
        artifact_ref["artifact"] = artifact
        artifact.bulk_text_codec = bulk_text_codec
        if bulk_wire_contract is not None:
            artifact.bulk_wire_contract = bulk_wire_contract
        artifact.source_export_provider = "mssql_bcp_pipe"
        artifact.streaming_policy = policy
        artifact.streaming_route_decision = route_decision
        return artifact

    def _stream_fifo(
        self,
        *,
        query: str,
        directory: Path,
        bcp_options: Any,
        source_table: str,
        read_buffer_bytes: int,
        artifact: ByteStreamArtifact,
    ):
        directory.mkdir(parents=True, exist_ok=True)
        fifo_path = directory / f"dpone_mssql_bcp_pipe_{uuid.uuid4().hex}.fifo"
        os.mkfifo(fifo_path)
        process = None
        try:
            runner = self.connector._bcp_runner(bcp_options)
            process = runner.queryout_process(
                query,
                str(fifo_path),
                progress_callback=lambda line: self._publish_progress(line, source_table),
            )
            publish_runtime_decision(
                {
                    "requested": "bcp_pipe",
                    "selected": "bcp_started",
                    "release_gate": "green",
                },
                decision_id="mssql.bcp_pipe.started",
                phase="extract",
                component="mssql_source",
                category="stream_progress",
                provider="mssql_bcp_pipe",
                details={"source_table": source_table},
            )
            yield from iter_fifo_bytes(fifo_path, process, read_buffer_bytes=read_buffer_bytes)
            result = process.wait()
            rows_copied = result.rows_copied
            if isinstance(rows_copied, int) and not isinstance(rows_copied, bool) and rows_copied >= 0:
                artifact.rows_exported = rows_copied
            publish_runtime_decision(
                {
                    "requested": "bcp_pipe",
                    "selected": "bcp_completed",
                    "release_gate": "green",
                    "rows_copied": rows_copied,
                },
                decision_id="mssql.bcp_pipe.completed",
                phase="extract",
                component="mssql_source",
                category="stream_progress",
                provider="mssql_bcp_pipe",
                details={"source_table": source_table, "rows_copied": rows_copied},
            )
            self.logger.log_etl_progress(
                "MSSQL_BCP_PIPE_STREAM",
                {"Rows": rows_copied, "Source": source_table, "Mode": "fifo"},
            )
        except BaseException as error:
            if process is not None:
                abort_after_failure(process, error, operation="bcp_abort")
            raise
        finally:
            fifo_path.unlink(missing_ok=True)

    @staticmethod
    def _publish_progress(line: str, source_table: str) -> None:
        publish_runtime_decision(
            {
                "requested": "bcp_pipe",
                "selected": "bcp_progress",
                "release_gate": "green",
                "progress_line": line,
            },
            decision_id="mssql.bcp_pipe.progress",
            phase="extract",
            component="mssql_source",
            category="stream_progress",
            provider="mssql_bcp_pipe",
            details={"source_table": source_table, "progress_line": line},
        )


__all__ = ["BcpPipeStreamExporter"]
