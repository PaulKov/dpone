"""MSSQL ODBC array fetch artifacts for ClickHouse binary staging."""

from __future__ import annotations

from typing import Any

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.clickhouse_rowbinary import ClickHouseRowBinaryEncoder
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy
from dpone.runtime.typed_stream_limits import TypedStreamLimits


def build_odbc_array_rowbinary_artifact(
    *,
    connector: Any,
    load_config: Any,
    query: str,
    schema: list[tuple[str, str]],
    provider_id: str,
    bulk_wire_contract: Any,
) -> ByteStreamArtifact:
    """Build ODBC array fetch -> ClickHouse RowBinary byte stream."""

    if bulk_wire_contract is not None and bulk_wire_contract.binary_format != "rowbinary":
        raise ValueError("mssql_row_stream_requires_rowbinary_format")
    limits = TypedStreamLimits.from_options(load_config.options or {}, load_config.batch_size)
    encoder = ClickHouseRowBinaryEncoder(
        schema,
        chunk_rows=limits.chunk_rows,
        max_batch_bytes=limits.max_batch_bytes,
        type_policy=MssqlClickHouseTypePolicy.from_config((load_config.options or {}).get("type_fidelity")),
    )

    def chunks():
        batches = connector.get_records_streaming(
            query,
            batch_size=max(1, load_config.batch_size),
            as_dict=True,
        )
        try:
            for batch in batches:
                yield from encoder.iter_batches(batch)
        finally:
            close = getattr(batches, "close", None)
            if callable(close):
                close()

    artifact = ByteStreamArtifact(
        chunks,
        columns=[column for column, _ in schema],
        format="clickhouse-rowbinary",
        estimated_rows=None,
    )
    setattr(artifact, "source_export_provider", provider_id)
    setattr(artifact, "bulk_wire_contract", bulk_wire_contract)
    return artifact


__all__ = ["build_odbc_array_rowbinary_artifact"]
