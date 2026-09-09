"""MSSQL ODBC row-stream artifact builders."""

from __future__ import annotations

from typing import Any

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.clickhouse_rowbinary import ClickHouseRowBinaryEncoder
from dpone.runtime.sources.strategies.mssql.mssql_queryout_bulk_wire import resolve_bulk_wire_contract
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


def build_typed_binary_row_stream_artifact(
    *,
    connector: Any,
    load_config: Any,
    query: str,
    schema: list[tuple[str, str]],
    sink_connector: Any,
) -> ByteStreamArtifact | None:
    """Build safe ODBC row-stream -> ClickHouse RowBinary artifact if selected."""

    contract = resolve_bulk_wire_contract(load_config, schema, sink_connector)
    if contract is None or contract.selected_route != "typed_binary_row_stream":
        return None
    if not hasattr(connector, "get_records_streaming"):
        raise RuntimeError("mssql_typed_binary_requires_streaming_records")

    encoder = ClickHouseRowBinaryEncoder(
        schema,
        chunk_rows=max(1, load_config.batch_size),
        type_policy=MssqlClickHouseTypePolicy.from_config((load_config.options or {}).get("type_fidelity")),
    )

    def chunks():
        batches = connector.get_records_streaming(
            query,
            batch_size=load_config.batch_size,
            as_dict=True,
        )
        for batch in batches:
            yield from encoder.iter_batches(batch)

    artifact = ByteStreamArtifact(
        chunks,
        columns=[column for column, _ in schema],
        format="clickhouse-rowbinary",
        estimated_rows=None,
    )
    setattr(artifact, "bulk_wire_contract", contract)
    return artifact


__all__ = ["build_typed_binary_row_stream_artifact"]
