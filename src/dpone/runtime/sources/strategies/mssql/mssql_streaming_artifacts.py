"""MSSQL streaming artifact selection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.sources.strategies.mssql.mssql_pipe_stream import BcpPipeStreamExporter
from dpone.runtime.streaming_transfer import StreamingTransferPolicy, decide_streaming_route


def build_mssql_bcp_pipe_stream_artifact(
    *,
    connector: Any,
    logger: Any,
    query: str,
    schema: list[tuple[str, str]],
    directory: Path,
    bcp_options: Any,
    source_table: str,
    artifact_format: str,
    policy: StreamingTransferPolicy,
    bulk_text_codec: Any | None,
    bulk_wire_contract: Any | None,
    sink_connector: Any | None,
) -> Any | None:
    """Return a FIFO streaming artifact when the route is selected."""

    decision = decide_streaming_route(
        policy=policy,
        artifact_format=artifact_format,
        bulk_wire_contract=bulk_wire_contract,
        sink_type=sink_connector.__class__.__name__ if sink_connector is not None else "",
    )
    if decision is None:
        return None
    publish_runtime_decision(
        decision,
        decision_id="mssql.bcp_pipe.route",
        phase="extract",
        component="mssql_source",
        category="streaming_route_selection",
        fallback_allowed=policy.mode == "auto" and bool(decision.blockers),
        provider="mssql_bcp_pipe",
        details={
            "source_table": source_table,
            "configured_read_buffer_bytes": policy.configured_read_buffer_bytes,
            "effective_read_buffer_bytes": policy.read_buffer_bytes,
            "deprecated_aliases": list(policy.deprecated_aliases),
        },
    )
    if decision.blockers:
        if policy.required:
            raise ValueError(", ".join(decision.blockers))
        return None
    return BcpPipeStreamExporter(connector, logger).artifact(
        query=query,
        columns=tuple(column for column, _ in schema),
        directory=directory,
        bcp_options=bcp_options,
        policy=policy,
        route_decision=decision,
        source_table=source_table,
        bulk_text_codec=bulk_text_codec,
        bulk_wire_contract=bulk_wire_contract,
    )


__all__ = ["build_mssql_bcp_pipe_stream_artifact"]
