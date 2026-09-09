"""Executor orchestration for Postgres -> MSSQL refresh chunks."""

from __future__ import annotations

from collections.abc import Sequence

from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.refresh_execution_models import (
    RouteRefreshChunkExecutionRequest,
    RouteRefreshChunkExecutionResult,
)

from .native_pipeline import NativeChunkExporter, NativeChunkLoader, normalize_export, normalize_load, normalize_prepare
from .postgres_mssql_adapters import (
    MssqlBcpChunkLoader,
    PostgresCopyChunkExporter,
    build_bcp_runner,
    build_mssql_connector,
    build_postgres_connector,
)
from .postgres_mssql_artifacts import transfer_path, write_failure_artifact, write_success_artifact
from .postgres_mssql_config import (
    PostgresMssqlRefreshConfig,
    mssql_qualified_name,
    postgres_qualified_name,
    quote_postgres_identifier,
    split_dataset,
)

SUPPORTED_ROUTE = RouteKey.of("postgres", "mssql", "incremental_merge")


class ConfigurationBlockedPostgresMssqlRouteRefreshExecutor:
    """Executor placeholder that records configuration blockers in the report."""

    def __init__(self, *, blockers: Sequence[str], summary: str) -> None:
        self._blockers = tuple(dict.fromkeys(str(item) for item in blockers if str(item)))
        self._summary = summary

    def execute_chunk(self, request: RouteRefreshChunkExecutionRequest) -> RouteRefreshChunkExecutionResult:
        return RouteRefreshChunkExecutionResult(
            ordinal=request.ordinal,
            idempotency_key=request.idempotency_key,
            status="failed",
            passed=False,
            rows_read=0,
            rows_written=0,
            artifact_path="",
            summary=self._summary,
            blockers=self._blockers or ("postgres_mssql_refresh_executor.config_invalid",),
        )


class PostgresMssqlRouteRefreshExecutor:
    """Apply Postgres -> MSSQL route refresh chunks through native bulk tools."""

    def __init__(
        self,
        *,
        config: PostgresMssqlRefreshConfig,
        exporter: NativeChunkExporter,
        loader: NativeChunkLoader,
    ) -> None:
        self._config = config
        self._exporter = exporter
        self._loader = loader

    @classmethod
    def from_config(cls, config: PostgresMssqlRefreshConfig) -> PostgresMssqlRouteRefreshExecutor:
        mssql_connector = build_mssql_connector(config.mssql)
        return cls(
            config=config,
            exporter=PostgresCopyChunkExporter(build_postgres_connector(config.postgres)),
            loader=MssqlBcpChunkLoader(
                statement_runner=mssql_connector,
                bcp_runner=build_bcp_runner(config.mssql),
            ),
        )

    def execute_chunk(self, request: RouteRefreshChunkExecutionRequest) -> RouteRefreshChunkExecutionResult:
        if request.route != SUPPORTED_ROUTE:
            return _failed_result(
                request,
                summary="unsupported route for postgres_mssql executor",
                blockers=("postgres_mssql_refresh_executor.route_unsupported",),
            )
        blockers = self._config.blockers()
        if blockers:
            return _failed_result(request, summary="invalid postgres_mssql executor config", blockers=blockers)
        try:
            output_path = transfer_path(request)
            query = self._query(request)
            target_table = mssql_qualified_name(self._config.target_dataset)
            export_result = normalize_export(self._exporter.export_chunk(query=query, output_path=output_path))
            prepare_result = normalize_prepare(
                self._loader.prepare_chunk(
                    table=target_table,
                    boundary_column=self._config.boundary_column,
                    start=request.start,
                    end=request.end,
                    idempotency_key=request.idempotency_key,
                )
            )
            load_result = normalize_load(
                self._loader.load_chunk(
                    table=target_table,
                    columns=self._config.columns,
                    input_path=output_path,
                    idempotency_key=request.idempotency_key,
                )
            )
            artifact_path = write_success_artifact(
                request=request,
                config=self._config,
                query=query,
                transfer_file=output_path,
                export_result=export_result,
                prepare_result=prepare_result,
                load_result=load_result,
            )
            return RouteRefreshChunkExecutionResult(
                ordinal=request.ordinal,
                idempotency_key=request.idempotency_key,
                status="succeeded",
                passed=True,
                rows_read=export_result.rows_read,
                rows_written=load_result.rows_written,
                artifact_path=str(artifact_path),
                summary="postgres copy loaded into mssql bcp",
            )
        except Exception as exc:
            artifact_path = write_failure_artifact(request=request, config=self._config, exc=exc)
            return RouteRefreshChunkExecutionResult(
                ordinal=request.ordinal,
                idempotency_key=request.idempotency_key,
                status="failed",
                passed=False,
                rows_read=0,
                rows_written=0,
                artifact_path=str(artifact_path),
                summary="postgres_mssql route refresh chunk failed",
                blockers=("postgres_mssql_refresh_executor.chunk_failed",),
                warnings=(str(exc),),
            )

    def _query(self, request: RouteRefreshChunkExecutionRequest) -> str:
        start = _integer_boundary(request.start, "start")
        end = _integer_boundary(request.end, "end")
        source_schema, source_table = split_dataset(self._config.source_dataset)
        column_sql = ", ".join(quote_postgres_identifier(column) for column in self._config.columns)
        source_sql = postgres_qualified_name(f"{source_schema}.{source_table}")
        boundary_sql = quote_postgres_identifier(self._config.boundary_column)
        if self._config.query_template:
            return self._config.query_template.format(
                columns=column_sql,
                source_table=source_sql,
                boundary_column=boundary_sql,
                start=start,
                end=end,
                partition=request.partition,
            )
        return (
            f"SELECT {column_sql} FROM {source_sql} "
            f"WHERE {boundary_sql} BETWEEN {start} AND {end} ORDER BY {boundary_sql}"
        )


def _failed_result(
    request: RouteRefreshChunkExecutionRequest,
    *,
    summary: str,
    blockers: Sequence[str],
) -> RouteRefreshChunkExecutionResult:
    return RouteRefreshChunkExecutionResult(
        ordinal=request.ordinal,
        idempotency_key=request.idempotency_key,
        status="failed",
        passed=False,
        rows_read=0,
        rows_written=0,
        artifact_path="",
        summary=summary,
        blockers=tuple(dict.fromkeys(str(item) for item in blockers if str(item))),
    )


def _integer_boundary(value: str, name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer chunk {name}: {value}") from exc


__all__ = [
    "ConfigurationBlockedPostgresMssqlRouteRefreshExecutor",
    "PostgresMssqlRouteRefreshExecutor",
    "SUPPORTED_ROUTE",
]
