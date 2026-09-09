"""Executor orchestration for MSSQL -> ClickHouse refresh chunks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.refresh_execution_models import (
    RouteRefreshChunkExecutionRequest,
    RouteRefreshChunkExecutionResult,
)
from dpone.runtime.connectors.mssql_sql import MSSQLSqlRenderer

from .mssql_clickhouse_adapters import (
    ClickHouseChunkLoader,
    ClickHouseChunkLoadResult,
    ClickHouseChunkPrepareResult,
    ClickHouseClientChunkLoader,
    MssqlBcpChunkExporter,
    MssqlChunkExporter,
    MssqlChunkExportResult,
    build_bcp_runner,
    build_clickhouse_runner,
)
from .mssql_clickhouse_artifacts import transfer_path, write_failure_artifact, write_success_artifact
from .mssql_clickhouse_config import (
    MssqlClickHouseRefreshConfig,
    clickhouse_qualified_name,
    mssql_qualified_name,
    string_tuple,
)

SUPPORTED_ROUTE = RouteKey.of("mssql", "clickhouse", "incremental_merge")


class ConfigurationBlockedRouteRefreshExecutor:
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
            blockers=self._blockers or ("mssql_clickhouse_refresh_executor.config_invalid",),
        )


class MssqlClickHouseRouteRefreshExecutor:
    """Apply MSSQL -> ClickHouse route refresh chunks through native bulk tools."""

    def __init__(
        self,
        *,
        config: MssqlClickHouseRefreshConfig,
        exporter: MssqlChunkExporter,
        loader: ClickHouseChunkLoader,
        sql_renderer: MSSQLSqlRenderer | None = None,
    ) -> None:
        self._config = config
        self._exporter = exporter
        self._loader = loader
        self._sql = sql_renderer or MSSQLSqlRenderer()

    @classmethod
    def from_config(cls, config: MssqlClickHouseRefreshConfig) -> MssqlClickHouseRouteRefreshExecutor:
        """Build a live executor from declarative connection config."""

        return cls(
            config=config,
            exporter=MssqlBcpChunkExporter(build_bcp_runner(config.mssql)),
            loader=ClickHouseClientChunkLoader(build_clickhouse_runner(config.clickhouse, config.target_dataset)),
        )

    def execute_chunk(self, request: RouteRefreshChunkExecutionRequest) -> RouteRefreshChunkExecutionResult:
        if request.route != SUPPORTED_ROUTE:
            return _failed_result(
                request,
                summary="unsupported route for mssql_clickhouse executor",
                blockers=("mssql_clickhouse_refresh_executor.route_unsupported",),
            )
        blockers = self._config.blockers()
        if blockers:
            return _failed_result(request, summary="invalid mssql_clickhouse executor config", blockers=blockers)
        try:
            output_path = transfer_path(request)
            query = self._query(request)
            target_table = clickhouse_qualified_name(self._config.target_dataset)
            export_result = _export_result(self._exporter.export_chunk(query=query, output_path=output_path))
            prepare_result = _prepare_result(
                self._loader.prepare_chunk(
                    table=target_table,
                    boundary_column=self._config.boundary_column,
                    start=request.start,
                    end=request.end,
                    idempotency_key=request.idempotency_key,
                )
            )
            load_result = _load_result(
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
                summary="mssql bcp queryout loaded into clickhouse",
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
                summary="mssql_clickhouse route refresh chunk failed",
                blockers=("mssql_clickhouse_refresh_executor.chunk_failed",),
                warnings=(str(exc),),
            )

    def _query(self, request: RouteRefreshChunkExecutionRequest) -> str:
        start = _integer_boundary(request.start, "start")
        end = _integer_boundary(request.end, "end")
        column_sql = ", ".join(self._sql.quote_identifier(column) for column in self._config.columns)
        source_sql = mssql_qualified_name(self._config.source_dataset)
        boundary_sql = self._sql.quote_identifier(self._config.boundary_column)
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


def _export_result(value: object) -> MssqlChunkExportResult:
    if isinstance(value, MssqlChunkExportResult):
        return value
    return MssqlChunkExportResult(
        rows_read=_int(_read(value, "rows_read"), default=0),
        redacted_command=string_tuple(_read(value, "redacted_command")),
        stdout_tail=str(_read(value, "stdout_tail", "")),
        stderr_tail=str(_read(value, "stderr_tail", "")),
    )


def _load_result(value: object) -> ClickHouseChunkLoadResult:
    if isinstance(value, ClickHouseChunkLoadResult):
        return value
    return ClickHouseChunkLoadResult(
        rows_written=_int(_read(value, "rows_written"), default=0),
        redacted_command=string_tuple(_read(value, "redacted_command")),
        stdout_tail=str(_read(value, "stdout_tail", "")),
        stderr_tail=str(_read(value, "stderr_tail", "")),
    )


def _prepare_result(value: object) -> ClickHouseChunkPrepareResult:
    if isinstance(value, ClickHouseChunkPrepareResult):
        return value
    return ClickHouseChunkPrepareResult(
        rows_deleted=_int(_read(value, "rows_deleted"), default=0),
        redacted_command=string_tuple(_read(value, "redacted_command")),
        stdout_tail=str(_read(value, "stdout_tail", "")),
        stderr_tail=str(_read(value, "stderr_tail", "")),
    )


def _read(value: object, name: str, default: object = None) -> object:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _integer_boundary(value: str, name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer chunk {name}: {value}") from exc


def _int(value: object, *, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return default
    return default


__all__ = [
    "ConfigurationBlockedRouteRefreshExecutor",
    "MssqlClickHouseRouteRefreshExecutor",
    "SUPPORTED_ROUTE",
]
