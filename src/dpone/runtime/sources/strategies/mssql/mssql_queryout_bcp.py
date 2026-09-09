"""BCP artifact assembly for MSSQL queryout extraction."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


import tempfile
from collections.abc import Callable
from contextlib import nullcontext
from typing import Any

from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.physical_chunking import PhysicalChunkPolicy
from dpone.runtime.source_materialization_preparation import guard_source_materialization_preparation
from dpone.runtime.sources.strategies.mssql.mssql_bcp_native_artifacts import build_mssql_bcp_native_artifact
from dpone.runtime.sources.strategies.mssql.mssql_queryout_bulk_wire import (
    bulk_wire_field_terminator,
    bulk_wire_row_terminator,
    resolve_bulk_wire_contract,
)
from dpone.runtime.sources.strategies.mssql.mssql_queryout_export_optimizer import artifact_from_export_optimizer
from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import (
    output_schema as _output_schema,
)
from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import (
    table_label as _table_label,
)
from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import (
    type_policy as _type_policy,
)
from dpone.runtime.sources.strategies.mssql.mssql_queryout_materialization import (
    maybe_materialize_query,
    wrap_prepared_source_artifact,
)
from dpone.runtime.sources.strategies.mssql.mssql_queryout_scan import resolve_mssql_source_scan_decision
from dpone.runtime.sources.strategies.mssql.mssql_single_scan_chunks import BcpSingleScanChunkExporter
from dpone.runtime.sources.strategies.mssql.mssql_streaming_artifacts import build_mssql_bcp_pipe_stream_artifact
from dpone.runtime.storage_policy import RuntimeStoragePolicy
from dpone.runtime.streaming_transfer import StreamingTransferPolicy
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec


def build_bcp_queryout_artifact(
    factory: Any,
    load_config: LoadConfig,
    query: str,
    schema: list[tuple[str, str]],
) -> Any:
    """Build the BCP-backed queryout artifact for a factory instance."""

    physical_chunk_policy = PhysicalChunkPolicy.from_source_options(load_config.options)
    streaming_policy = StreamingTransferPolicy.from_options(load_config.options)
    bounds_query = query
    materialized = maybe_materialize_query(
        connector=factory.connector,
        load_config=load_config,
        query=query,
        schema=schema,
    )
    preparation_guard = (
        guard_source_materialization_preparation(
            cleanup=materialized.snapshot.cleanup,
            provider=materialized.decision.provider,
            cleanup_policy=materialized.policy.cleanup_policy,
        )
        if materialized is not None
        else nullcontext()
    )
    with preparation_guard:
        if materialized is not None:
            query = materialized.query
            bounds_query = query
        artifact_schema, query, artifact_text_codec, can_materialize_projection = _encoded_query(
            factory,
            load_config,
            query,
            schema,
        )
        cleanup_projection: Callable[[], None] | None = None
        if can_materialize_projection and factory.should_materialize_queryout_projection(load_config, query):
            query, cleanup_projection = factory.materialize_queryout_projection(load_config, query, artifact_schema)

        source_table = _table_label(load_config.source_schema, load_config.source_table, load_config.source_database)
        bulk_options = BulkOptionsResolver.resolve(load_config.options, default_batch_size=load_config.batch_size)
        bulk_wire_contract = resolve_bulk_wire_contract(load_config, artifact_schema, factory.sink_connector)
        bcp_native_wire = (
            bulk_wire_contract is not None and bulk_wire_contract.selected_route == "typed_binary_bcp_native"
        )
        file_format = "native" if bcp_native_wire else bulk_options.bcp.file_format.lower()
        artifact_format = _artifact_format(bcp_native_wire=bcp_native_wire, file_format=file_format)
        bcp_options = bulk_options.bcp.to_bcp_options(
            bcp_path=factory.connector.bcp_path,
            trust_server_certificate=factory.connector.trust_server_certificate == "yes",
            file_format=file_format,
            field_terminator=bulk_wire_field_terminator(bulk_wire_contract),
            row_terminator=bulk_wire_row_terminator(bulk_wire_contract),
        )
        optimizer_artifact = artifact_from_export_optimizer(
            connector=factory.connector,
            sink_connector=factory.sink_connector,
            load_config=load_config,
            query=query,
            schema=artifact_schema,
            current_default="mssql_bcp_native" if bcp_native_wire else "mssql_bcp_character_raw",
        )
        if optimizer_artifact is not None:
            return wrap_prepared_source_artifact(optimizer_artifact, materialized)
        try:
            return _build_queryout_transport(
                factory,
                load_config,
                query=query,
                bounds_query=bounds_query,
                artifact_schema=artifact_schema,
                artifact_format=artifact_format,
                artifact_text_codec=artifact_text_codec,
                bulk_wire_contract=bulk_wire_contract,
                bcp_options=bcp_options,
                source_table=source_table,
                bcp_native_wire=bcp_native_wire,
                materialized=materialized,
                physical_chunk_policy=physical_chunk_policy,
                streaming_policy=streaming_policy,
            )
        finally:
            if cleanup_projection is not None:
                cleanup_projection()


def _encoded_query(
    factory: Any,
    load_config: LoadConfig,
    query: str,
    schema: list[tuple[str, str]],
) -> tuple[list[tuple[str, str]], str, Any | None, bool]:
    bulk_text_codec = BulkTextCodec() if factory.should_encode_for_mssql_sink() else None
    clickhouse_tsv_codec = (
        ClickHouseTabSeparatedCodec(type_policy=_type_policy(load_config))
        if factory.should_encode_for_clickhouse_direct(load_config)
        else None
    )
    if bulk_text_codec is not None:
        return schema, factory.wrap_mssql_bulk_text_query(query, schema, bulk_text_codec), bulk_text_codec, False
    if clickhouse_tsv_codec is None:
        return schema, query, None, False
    artifact_schema = _output_schema(load_config, schema)
    return (
        artifact_schema,
        factory.wrap_clickhouse_tabseparated_query(query, schema, clickhouse_tsv_codec),
        clickhouse_tsv_codec,
        True,
    )


def _build_queryout_transport(
    factory: Any,
    load_config: LoadConfig,
    *,
    query: str,
    bounds_query: str,
    artifact_schema: list[tuple[str, str]],
    artifact_format: str,
    artifact_text_codec: Any | None,
    bulk_wire_contract: Any | None,
    bcp_options: Any,
    source_table: str,
    bcp_native_wire: bool,
    materialized: Any,
    physical_chunk_policy: PhysicalChunkPolicy,
    streaming_policy: StreamingTransferPolicy,
) -> Any:
    tmp_dir = RuntimeStoragePolicy.from_options(load_config.options).work_dir
    streaming_artifact = build_mssql_bcp_pipe_stream_artifact(
        connector=factory.connector,
        logger=factory.logger,
        query=query,
        schema=artifact_schema,
        directory=tmp_dir,
        bcp_options=bcp_options,
        source_table=source_table,
        artifact_format=artifact_format,
        policy=streaming_policy,
        bulk_text_codec=artifact_text_codec,
        bulk_wire_contract=bulk_wire_contract,
        sink_connector=factory.sink_connector,
    )
    if streaming_artifact is not None:
        return wrap_prepared_source_artifact(streaming_artifact, materialized)
    scan_decision = resolve_mssql_source_scan_decision(factory.connector, load_config)
    if scan_decision is not None and scan_decision.blockers:
        raise ValueError(", ".join(scan_decision.blockers))
    tmp_dir.mkdir(parents=True, exist_ok=True)
    chunked_artifact = _single_scan_artifact(
        factory,
        load_config,
        query=query,
        artifact_schema=artifact_schema,
        tmp_dir=tmp_dir,
        bcp_options=bcp_options,
        source_table=source_table,
        artifact_format=artifact_format,
        artifact_text_codec=artifact_text_codec,
        bulk_wire_contract=bulk_wire_contract,
        scan_decision=scan_decision,
        policy=physical_chunk_policy,
    )
    if chunked_artifact is not None:
        return wrap_prepared_source_artifact(chunked_artifact, materialized)
    partitioned_artifact = _partitioned_artifact(
        factory,
        load_config,
        query=query,
        bounds_query=bounds_query,
        artifact_schema=artifact_schema,
        bcp_options=bcp_options,
        artifact_format=artifact_format,
        artifact_text_codec=artifact_text_codec,
        bulk_wire_contract=bulk_wire_contract,
        scan_decision=scan_decision,
    )
    if partitioned_artifact is not None:
        return wrap_prepared_source_artifact(partitioned_artifact, materialized)
    file_artifact = _single_file_artifact(
        factory,
        load_config,
        query=query,
        artifact_schema=artifact_schema,
        artifact_format=artifact_format,
        artifact_text_codec=artifact_text_codec,
        bulk_wire_contract=bulk_wire_contract,
        bcp_options=bcp_options,
        source_table=source_table,
        bcp_native_wire=bcp_native_wire,
        tmp_dir=tmp_dir,
    )
    return wrap_prepared_source_artifact(file_artifact, materialized)


def _single_scan_artifact(
    factory: Any,
    load_config: LoadConfig,
    *,
    query: str,
    artifact_schema: list[tuple[str, str]],
    tmp_dir: Any,
    bcp_options: Any,
    source_table: str,
    artifact_format: str,
    artifact_text_codec: Any | None,
    bulk_wire_contract: Any | None,
    scan_decision: Any,
    policy: PhysicalChunkPolicy,
) -> Any | None:
    if scan_decision is None or scan_decision.selected_scan != "single_scan_chunks":
        return None
    if artifact_format == "mssql-delimited":
        return BcpSingleScanChunkExporter(factory.connector, factory.logger).artifact(
            query=query,
            columns=tuple(column for column, _ in artifact_schema),
            directory=tmp_dir,
            bcp_options=bcp_options,
            policy=policy,
            source_table=source_table,
            artifact_format=artifact_format,
            bulk_text_codec=artifact_text_codec,
            bulk_wire_contract=bulk_wire_contract,
            source_scan_decision=scan_decision,
        )
    if policy.mode == "required":
        raise ValueError("physical_chunking_binary_row_boundary_unsupported")
    return None


def _partitioned_artifact(
    factory: Any,
    load_config: LoadConfig,
    *,
    query: str,
    bounds_query: str,
    artifact_schema: list[tuple[str, str]],
    bcp_options: Any,
    artifact_format: str,
    artifact_text_codec: Any | None,
    bulk_wire_contract: Any | None,
    scan_decision: Any,
) -> Any | None:
    if scan_decision is not None and scan_decision.selected_scan != "range_partitioned":
        return None
    partitioner = factory.range_partitioner(load_config, bounds_query)
    if partitioner is None or not partitioner.enabled:
        return None
    return factory.partitioned_queryout(
        load_config,
        query,
        artifact_schema,
        partitioner,
        bcp_options,
        artifact_format=artifact_format,
        bulk_text_codec=artifact_text_codec,
        bulk_wire_contract=bulk_wire_contract,
    )


def _single_file_artifact(
    factory: Any,
    load_config: LoadConfig,
    *,
    query: str,
    artifact_schema: list[tuple[str, str]],
    artifact_format: str,
    artifact_text_codec: Any | None,
    bulk_wire_contract: Any | None,
    bcp_options: Any,
    source_table: str,
    bcp_native_wire: bool,
    tmp_dir: Any,
) -> Any:
    file_path = tempfile.NamedTemporaryFile(
        prefix="dpone_mssql_queryout_",
        suffix=".bcp",
        dir=tmp_dir,
        delete=False,
    ).name
    rows = factory.connector.bcp_queryout(query, file_path, options=bcp_options)
    factory.logger.log_etl_progress("MSSQL_BCP_QUERYOUT", {"Rows": rows, "File": file_path, "Source": source_table})
    if bcp_native_wire:
        artifact = build_mssql_bcp_native_artifact(
            file_path,
            artifact_schema,
            query=query,
            bcp_version=bcp_options.bcp_path,
            type_policy=_type_policy(load_config),
            estimated_rows=rows or None,
            bulk_wire_contract=bulk_wire_contract,
        )
    else:
        artifact = FileExportArtifact(
            file_path=file_path,
            columns=[column for column, _ in artifact_schema],
            compressed=False,
            format=artifact_format,
            estimated_rows=rows or None,
            rows_exported=rows if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0 else None,
            bulk_text_codec=artifact_text_codec,
        )
        if bulk_wire_contract is not None:
            setattr(artifact, "bulk_wire_contract", bulk_wire_contract)
    if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0 and artifact.rows_exported is None:
        artifact.rows_exported = rows
    return artifact


def _artifact_format(*, bcp_native_wire: bool, file_format: str) -> str:
    if bcp_native_wire:
        return "mssql-bcp-native"
    return "mssql-native" if file_format in {"native", "n"} else "mssql-delimited"
