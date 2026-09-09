"""Dependency-free route refresh executor placeholder."""

from __future__ import annotations

from collections.abc import Sequence

from dpone.ops.routes.refresh_execution_models import (
    RouteRefreshChunkExecutionRequest,
    RouteRefreshChunkExecutionResult,
)


class ConfigurationBlockedRouteRefreshExecutor:
    """Records configuration blockers without importing live connector dependencies."""

    def __init__(
        self,
        *,
        blockers: Sequence[str],
        summary: str,
        default_blocker: str = "route_refresh_execution.executor_config_invalid",
    ) -> None:
        self._blockers = tuple(dict.fromkeys(str(item) for item in blockers if str(item)))
        self._summary = summary
        self._default_blocker = default_blocker

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
            blockers=self._blockers or (self._default_blocker,),
        )
