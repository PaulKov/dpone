"""Route refresh service factories."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dpone.ops.route_refresh_execute import RouteRefreshExecutionService
from dpone.ops.route_refresh_plan import RouteRefreshPlanService
from dpone.ops.route_refresh_snapshot_capture import RouteRefreshSnapshotCaptureService
from dpone.ops.route_refresh_verify import RouteRefreshVerificationService
from dpone.ops.routes.refresh_execution_executor import RouteRefreshExecutor
from dpone.ops.routes.refresh_executors import RouteRefreshExecutorRegistry
from dpone.ops.routes.refresh_snapshot_capture_reader import JsonRouteRefreshRowsReader
from dpone.ops.routes.refresh_snapshot_capture_registry import RouteRefreshSnapshotCaptureReaderRegistry
from dpone.ops.routes.refresh_verification_verifier import JsonRouteRefreshSnapshotReader


@dataclass(frozen=True, slots=True)
class ReadinessRefreshCatalog:
    """Factories for route refresh planning, execution, capture and verification."""

    @classmethod
    def default(cls) -> ReadinessRefreshCatalog:
        return cls()

    def route_refresh_plan(self) -> RouteRefreshPlanService:
        return RouteRefreshPlanService()

    def route_refresh_execute(
        self,
        *,
        executor: RouteRefreshExecutor | None = None,
        executor_backend: str | None = None,
        executor_config_json: str | Path | None = None,
    ) -> RouteRefreshExecutionService:
        selected_executor = executor
        if selected_executor is None:
            selected_executor = RouteRefreshExecutorRegistry.default().build(
                executor_backend,
                config_json=executor_config_json,
            )
        return RouteRefreshExecutionService(executor=selected_executor)

    def route_refresh_capture_snapshots(
        self,
        *,
        source_rows_json: str | Path | None = None,
        sink_rows_json: str | Path | None = None,
        executor_backend: str | None = None,
        executor_config_json: str | Path | None = None,
    ) -> RouteRefreshSnapshotCaptureService:
        source_reader = sink_reader = None
        if source_rows_json is not None or sink_rows_json is not None:
            source_reader = JsonRouteRefreshRowsReader(source_rows_json or "")
            sink_reader = JsonRouteRefreshRowsReader(sink_rows_json or "")
        if executor_backend:
            readers = RouteRefreshSnapshotCaptureReaderRegistry.default().build(
                executor_backend,
                config_json=executor_config_json,
            )
            source_reader = readers.source_reader
            sink_reader = readers.sink_reader
        return RouteRefreshSnapshotCaptureService(source_reader=source_reader, sink_reader=sink_reader)

    def route_refresh_verify(
        self,
        *,
        source_snapshot_json: str | Path | None = None,
        sink_snapshot_json: str | Path | None = None,
    ) -> RouteRefreshVerificationService:
        source_reader = sink_reader = None
        if source_snapshot_json is not None or sink_snapshot_json is not None:
            source_reader = JsonRouteRefreshSnapshotReader(source_snapshot_json or "")
            sink_reader = JsonRouteRefreshSnapshotReader(sink_snapshot_json or "")
        return RouteRefreshVerificationService(source_reader=source_reader, sink_reader=sink_reader)


__all__ = ["ReadinessRefreshCatalog"]
