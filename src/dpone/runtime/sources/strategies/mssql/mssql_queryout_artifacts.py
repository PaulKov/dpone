"""MSSQL queryout artifact construction for source extract strategies."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dpone.runtime.bulk_wire import should_use_source_encoded_tsv
from dpone.runtime.clickhouse_bulk_path import (
    clickhouse_path_supports_native_tsv,
    resolve_clickhouse_bulk_path,
)
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.internal_query_capability import (
    INTERNAL_QUERY_NOT_ISSUED,
    InternalQueryCapabilityDecision,
    log_internal_query_capability,
)
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.partitioning import RangePartitioner
from dpone.runtime.physical_chunking import PhysicalChunkedFileExportArtifact
from dpone.runtime.sources.strategies.mssql.mssql_columnar_queryout_bridge import columnar_snapshot_artifact
from dpone.runtime.sources.strategies.mssql.mssql_csv_artifacts import build_csv_file_artifact
from dpone.runtime.sources.strategies.mssql.mssql_partitioned_queryout import build_partitioned_queryout
from dpone.runtime.sources.strategies.mssql.mssql_query_projection import (
    materialize_queryout_projection,
    should_materialize_queryout_projection,
    wrap_clickhouse_tabseparated_query,
    wrap_mssql_bulk_text_query,
)
from dpone.runtime.sources.strategies.mssql.mssql_queryout_bcp import build_bcp_queryout_artifact
from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import (
    output_schema as _output_schema,
)
from dpone.runtime.sources.strategies.mssql.mssql_row_stream_artifacts import build_typed_binary_row_stream_artifact
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec
from dpone.runtime.support.mssql_bulk import BcpOptions
from dpone.type_system.source_sink.mssql_postgres import MSSQLPostgresTypeMapper

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

QueryoutArtifact = (
    FileExportArtifact
    | PartitionedFileExportArtifact
    | PartitionedTransferPlanArtifact
    | PhysicalChunkedFileExportArtifact
)


class MSSQLQueryoutArtifactFactory:
    """Build artifacts for MSSQL queryout and direct streaming extract paths."""

    def __init__(
        self,
        connector: Any,
        logger: Any,
        sink_connector: Any = None,
        columnar_snapshot_provider: Any | None = None,
        internal_query_capability: InternalQueryCapabilityDecision | None = None,
    ) -> None:
        self.connector = connector
        self.logger = logger
        self.sink_connector = sink_connector
        self.columnar_snapshot_provider = columnar_snapshot_provider
        self.internal_query_capability = internal_query_capability or InternalQueryCapabilityDecision.not_issued(
            source_dialect="mssql"
        )

    def bind_internal_query_capability(self, decision: InternalQueryCapabilityDecision) -> None:
        """Replace the fail-closed default with a composition-root decision."""

        self.internal_query_capability = decision

    def artifact_for_query(
        self,
        load_config: LoadConfig,
        query: str,
        schema: list[tuple[str, str]],
    ) -> Any:
        """Return the best extraction artifact for a rendered MSSQL query."""

        decision = self.internal_query_capability
        if decision.diagnostic.code == INTERNAL_QUERY_NOT_ISSUED:
            decision = InternalQueryCapabilityDecision.not_issued(
                source_dialect="mssql",
                target_dialect=str((getattr(load_config, "options", {}) or {}).get("sink_type", "")),
            )
        same_mssql_connection = decision.authorizes(
            source_connector=self.connector,
            dialect="mssql",
            source_database=getattr(load_config, "source_database", None),
            source_schema=str(getattr(load_config, "source_schema", "")),
            source_table=str(getattr(load_config, "source_table", "")),
        )
        log_internal_query_capability(self.logger, decision)
        if same_mssql_connection:
            return InternalQueryArtifact(query=query)

        # Postgres / Kafka / BigQuery reject mssql-delimited for public CSV wire.
        if self.uses_csv_file_export(load_config):
            return build_csv_file_artifact(
                self.connector,
                load_config,
                query,
                schema,
                binary_encoding="base64" if self.targets_bigquery(load_config) else "postgres_hex",
            )

        export_mode = str(load_config.options.get("mssql_export_mode", "bcp")).lower()
        if export_mode in {"row_stream", "odbc_row_stream"}:
            artifact = build_typed_binary_row_stream_artifact(
                connector=self.connector,
                load_config=load_config,
                query=query,
                schema=schema,
                sink_connector=self.sink_connector,
            )
            if artifact is not None:
                return artifact
        if export_mode == "streaming":
            return self.streaming_artifact(query, batch_size=load_config.batch_size)

        columnar_artifact = columnar_snapshot_artifact(
            load_config=load_config,
            query=query,
            schema=schema,
            provider=self.columnar_snapshot_provider,
            logger=self.logger,
        )
        if columnar_artifact is not None:
            return columnar_artifact

        return self._bcp_queryout_artifact(load_config, query, schema)

    def streaming_artifact(
        self,
        query: str,
        *,
        batch_size: int,
        params: tuple[Any, ...] | None = None,
    ) -> StreamingRowsArtifact:
        """Create a streaming rows artifact from a connector iterator."""

        if params is None:
            iterator = self.connector.get_records_iterator(query)
        else:
            iterator = self.connector.get_records_iterator(query, params=params)
        return StreamingRowsArtifact(iterator, batch_size=batch_size)

    def output_schema(self, load_config: LoadConfig, schema: list[tuple[str, str]]) -> list[tuple[str, str]]:
        """Return the schema that downstream sinks will see for this extract."""

        if self.targets_postgres(load_config):
            postgres_mapper = MSSQLPostgresTypeMapper()
            return [(name, postgres_mapper.resolve(dtype).target_type) for name, dtype in schema]
        if self.targets_bigquery(load_config):
            from dpone.type_system.source_sink.mssql_bigquery import MSSQLBigQueryTypeMapper

            bigquery_mapper = MSSQLBigQueryTypeMapper()
            return [(name, bigquery_mapper.resolve(dtype).target_type) for name, dtype in schema]
        if not self.should_encode_for_clickhouse_direct(load_config):
            return schema
        return _output_schema(load_config, schema)

    def uses_csv_file_export(self, load_config: LoadConfig) -> bool:
        """Return whether this extract should emit a CSV FileExportArtifact."""

        return (
            self.targets_postgres(load_config) or self.targets_kafka(load_config) or self.targets_bigquery(load_config)
        )

    def targets_postgres(self, load_config: LoadConfig) -> bool:
        """Return whether this extract targets a Postgres sink (CSV + pair mapper)."""

        return self._sink_matches(
            load_config, hints=("postgres", "pg", "postgresql"), type_tokens=("postgres", "psycopg")
        )

    def targets_kafka(self, load_config: LoadConfig) -> bool:
        """Return whether this extract targets a Kafka sink (CSV JSON publish)."""

        return self._sink_matches(load_config, hints=("kafka",), type_tokens=("kafka",))

    def targets_bigquery(self, load_config: LoadConfig) -> bool:
        """Return whether this extract targets a BigQuery sink (CSV + pair mapper)."""

        return self._sink_matches(load_config, hints=("bigquery", "bq"), type_tokens=("bigquery",))

    def _sink_matches(
        self,
        load_config: LoadConfig,
        *,
        hints: tuple[str, ...],
        type_tokens: tuple[str, ...],
    ) -> bool:
        hint = str(
            (getattr(load_config, "options", {}) or {}).get("sink_type")
            or (getattr(load_config, "options", {}) or {}).get("target_type")
            or ""
        ).lower()
        if hint in hints or any(token in hint for token in hints if len(token) > 2):
            return True
        sink = self.sink_connector
        if sink is None:
            return False
        type_name = type(sink).__name__.lower()
        module_name = type(sink).__module__.lower()
        return any(token in type_name or token in module_name for token in type_tokens)

    def should_encode_for_mssql_sink(self) -> bool:
        """Return whether queryout should encode text for MSSQL bcp ingestion."""

        return self.sink_connector is not None and self.sink_connector.__class__.__name__ == "MSSQLConnector"

    def should_encode_for_clickhouse_direct(self, load_config: LoadConfig) -> bool:
        """Return whether queryout should emit ClickHouse-ready TSV values."""

        if self.sink_connector is None or self.sink_connector.__class__.__name__ != "ClickHouseConnector":
            return False
        if not should_use_source_encoded_tsv(getattr(load_config, "options", {}) or {}):
            return False
        path = resolve_clickhouse_bulk_path(getattr(load_config, "options", {}) or {})
        return clickhouse_path_supports_native_tsv(path)

    def wrap_mssql_bulk_text_query(
        self,
        query: str,
        schema: list[tuple[str, str]],
        codec: BulkTextCodec,
    ) -> str:
        """Wrap a SELECT so text values are safe for MSSQL bcp character files."""

        return wrap_mssql_bulk_text_query(self.connector, query, schema, codec)

    def wrap_clickhouse_tabseparated_query(
        self,
        query: str,
        schema: list[tuple[str, str]],
        codec: ClickHouseTabSeparatedCodec,
    ) -> str:
        """Wrap a SELECT so values are encoded for direct ClickHouse TSV loading."""

        return wrap_clickhouse_tabseparated_query(self.connector, query, schema, codec)

    def should_materialize_queryout_projection(self, load_config: LoadConfig, query: str) -> bool:
        """Return whether a wide ClickHouse queryout projection should use a temporary view."""

        return should_materialize_queryout_projection(load_config, query)

    def materialize_queryout_projection(
        self,
        load_config: LoadConfig,
        query: str,
        schema: list[tuple[str, str]],
    ) -> tuple[str, Callable[[], None]]:
        """Create a temporary projection view and return a shorter query plus cleanup hook."""

        return materialize_queryout_projection(self.connector, load_config, query, schema)

    def resolve_partition_bounds(self, query: str, column: str) -> tuple[Any, Any, int | None, int]:
        """Resolve MIN/MAX/COUNT bounds for partitioned MSSQL queryout."""

        quoted_column = self.connector.quote_identifier(column)
        bounds_query = (
            "SELECT "
            f"MIN({quoted_column}) AS dpone_min_value, "
            f"MAX({quoted_column}) AS dpone_max_value, "
            "COUNT_BIG(1) AS dpone_row_count, "
            f"SUM(CASE WHEN {quoted_column} IS NULL THEN 1 ELSE 0 END) AS dpone_null_count "
            f"FROM ({query}) AS dpone_bounds"
        )
        rows = self.connector.get_records(bounds_query)
        if not rows:
            raise ValueError(f"Unable to resolve MSSQL partition bounds for column {column!r}.")
        lower, upper, row_count, *rest = rows[0]
        null_count = rest[0] if rest else None
        if lower is None or upper is None:
            raise ValueError(f"MSSQL partition column {column!r} has no non-null bounds.")
        return lower, upper, int(row_count) if row_count is not None else None, int(null_count or 0)

    def range_partitioner(self, load_config: LoadConfig, bounds_query: str) -> RangePartitioner | None:
        """Return the configured range partitioner for BCP queryout."""

        return RangePartitioner.from_options(
            load_config.options,
            bounds_resolver=lambda column: self.resolve_partition_bounds(bounds_query, column),
        )

    def partitioned_queryout(
        self,
        load_config: LoadConfig,
        query: str,
        schema: list[tuple[str, str]],
        partitioner: RangePartitioner,
        bcp_options: BcpOptions,
        *,
        artifact_format: str = "mssql-delimited",
        bulk_text_codec: Any | None = None,
        bulk_wire_contract: Any | None = None,
    ) -> QueryoutArtifact:
        """Export a query into partitioned BCP files."""
        return build_partitioned_queryout(
            self,
            load_config,
            query,
            schema,
            partitioner,
            bcp_options,
            artifact_format=artifact_format,
            bulk_text_codec=bulk_text_codec,
            bulk_wire_contract=bulk_wire_contract,
        )

    def _bcp_queryout_artifact(
        self,
        load_config: LoadConfig,
        query: str,
        schema: list[tuple[str, str]],
    ) -> QueryoutArtifact:
        return build_bcp_queryout_artifact(self, load_config, query, schema)


__all__ = ["MSSQLQueryoutArtifactFactory"]
