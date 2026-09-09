"""Route refresh executor protocol and safe dry-run implementation."""

from __future__ import annotations

from typing import Protocol

from dpone.ops.routes.refresh_execution_models import (
    RouteRefreshChunkExecutionRequest,
    RouteRefreshChunkExecutionResult,
)


class RouteRefreshExecutor(Protocol):
    """Concrete backend capable of applying one route refresh chunk."""

    def execute_chunk(self, request: RouteRefreshChunkExecutionRequest) -> RouteRefreshChunkExecutionResult: ...


class DryRunRouteRefreshExecutor:
    """Executor that records planned chunks without mutating a source or sink."""

    def execute_chunk(self, request: RouteRefreshChunkExecutionRequest) -> RouteRefreshChunkExecutionResult:
        return RouteRefreshChunkExecutionResult(
            ordinal=request.ordinal,
            idempotency_key=request.idempotency_key,
            status="planned",
            passed=True,
            rows_read=0,
            rows_written=0,
            artifact_path="",
            summary="dry-run planned chunk; no data movement executed",
        )


__all__ = ["DryRunRouteRefreshExecutor", "RouteRefreshExecutor", "RouteRefreshChunkExecutionResult"]
