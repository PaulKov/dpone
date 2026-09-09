"""Route-level refresh execution receipt service."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file
from dpone.ops.routes import RouteKey, RouteProfileCatalog
from dpone.ops.routes.refresh_execution_executor import DryRunRouteRefreshExecutor, RouteRefreshExecutor
from dpone.ops.routes.refresh_execution_models import (
    RouteRefreshChunkExecutionRequest,
    RouteRefreshChunkExecutionResult,
    RouteRefreshExecutionArtifact,
    RouteRefreshExecutionReport,
)
from dpone.ops.routes.refresh_execution_policy import RouteRefreshExecutionPolicy


class RouteRefreshExecutionService:
    """Execute or dry-run a route refresh plan through a narrow executor port."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        executor: RouteRefreshExecutor | None = None,
        policy: RouteRefreshExecutionPolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._executor = executor
        self._dry_run_executor = DryRunRouteRefreshExecutor()
        self._policy = policy or RouteRefreshExecutionPolicy()

    def execute(
        self,
        *,
        route_refresh_plan_json: str | Path,
        output_dir: str | Path,
        runner_id: str,
        execute: bool = False,
        source: str | None = None,
        sink: str | None = None,
        strategy: str | None = None,
        max_chunks: int | None = None,
        artifacts: Mapping[str, str | Path] | None = None,
    ) -> RouteRefreshExecutionReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        plan_path = Path(route_refresh_plan_json)
        payload = _load_payload(plan_path)
        route = _route(payload)
        profile = self._catalog.get(route)
        chunks_payload = _chunks(payload)
        if max_chunks is not None:
            chunks_payload = chunks_payload[: max(0, max_chunks)]
        route_matched = _route_matched(route=route, source=source, sink=sink, strategy=strategy)
        hard_blockers = _hard_blockers(
            plan_path=plan_path, payload=payload, all_chunks=_chunks(payload), max_chunks=max_chunks
        )
        should_run = (
            execute
            and self._executor is not None
            and not hard_blockers
            and route_matched
            and bool(payload.get("passed"))
            and str(payload.get("status")) == "ready"
        )
        results = self._execute_chunks(
            route=route,
            dataset=str(payload.get("dataset", "")),
            chunks_payload=chunks_payload,
            output_dir=directory,
            runner_id=runner_id,
            plan_path=plan_path,
            execute=execute,
            should_run=should_run,
        )
        decision = self._policy.evaluate(
            execute=execute,
            profile_exists=profile is not None,
            route_matched=route_matched,
            plan_passed=bool(payload.get("passed")),
            plan_status=str(payload.get("status", "blocked")),
            executor_available=self._executor is not None,
            chunks=results,
            hard_blockers=hard_blockers,
        )
        report = RouteRefreshExecutionReport(
            route=route,
            profile=profile,
            dataset=str(payload.get("dataset", "")),
            runner_id=runner_id,
            route_refresh_plan_json=str(plan_path),
            plan_sha256=sha256_file(plan_path) if plan_path.is_file() else "0" * 64,
            mode="execute" if execute else "dry_run",
            executed=execute and should_run,
            status=decision.status,
            passed=decision.passed,
            ready_for_state_promotion=decision.ready_for_state_promotion,
            summary=_summary(results),
            chunks=results,
            artifacts=_artifacts(plan_path=plan_path, artifacts=artifacts or {}),
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            output_dir=str(directory),
            json_path=str(directory / "route_refresh_execution.json"),
            markdown_path=str(directory / "route_refresh_execution.md"),
        )
        report.write()
        return report

    def _execute_chunks(
        self,
        *,
        route: RouteKey,
        dataset: str,
        chunks_payload: tuple[Mapping[str, object], ...],
        output_dir: Path,
        runner_id: str,
        plan_path: Path,
        execute: bool,
        should_run: bool,
    ) -> tuple[RouteRefreshChunkExecutionResult, ...]:
        executor: RouteRefreshExecutor = (
            self._executor if should_run and self._executor is not None else self._dry_run_executor
        )
        results: list[RouteRefreshChunkExecutionResult] = []
        stopped = False
        for chunk in chunks_payload:
            request = _request(
                route=route,
                dataset=dataset,
                chunk=chunk,
                output_dir=output_dir,
                runner_id=runner_id,
                execute=execute,
                plan_path=plan_path,
            )
            if stopped:
                results.append(_skipped_result(request))
                continue
            result = executor.execute_chunk(request)
            results.append(_with_request_boundaries(result=result, request=request))
            stopped = should_run and not result.passed
        return tuple(results)


def _load_payload(path: Path) -> Mapping[str, Any]:
    if not path.is_file():
        return {"passed": False, "status": "blocked", "blockers": ["route_refresh_execution.plan_missing"]}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "status": "blocked", "blockers": ["route_refresh_execution.plan_invalid_json"]}
    return payload if isinstance(payload, Mapping) else {"passed": False, "status": "blocked"}


def _route(payload: Mapping[str, Any]) -> RouteKey:
    route = payload.get("route")
    values = route if isinstance(route, Mapping) else {}
    return RouteKey.of(str(values.get("source", "")), str(values.get("sink", "")), str(values.get("strategy", "")))


def _route_matched(
    *,
    route: RouteKey,
    source: str | None,
    sink: str | None,
    strategy: str | None,
) -> bool:
    expected = tuple(item for item in (source, sink, strategy) if item is not None)
    if not expected:
        return True
    return route == RouteKey.of(source or route.source, sink or route.sink, strategy or route.strategy)


def _chunks(payload: Mapping[str, Any]) -> tuple[Mapping[str, object], ...]:
    chunks = payload.get("chunks", ())
    if not isinstance(chunks, list | tuple):
        return tuple()
    return tuple(item for item in chunks if isinstance(item, Mapping))


def _hard_blockers(
    *,
    plan_path: Path,
    payload: Mapping[str, Any],
    all_chunks: tuple[Mapping[str, object], ...],
    max_chunks: int | None,
) -> tuple[str, ...]:
    blockers = _strings(payload.get("blockers"))
    values: list[str] = []
    if not plan_path.is_file():
        values.append("route_refresh_execution.plan_missing")
    values.extend(item for item in blockers if item.startswith("route_refresh_execution."))
    if max_chunks is not None and len(all_chunks) > max_chunks:
        values.append("route_refresh_execution.chunk_count_exceeded")
    return tuple(dict.fromkeys(values))


def _request(
    *,
    route: RouteKey,
    dataset: str,
    chunk: Mapping[str, object],
    output_dir: Path,
    runner_id: str,
    execute: bool,
    plan_path: Path,
) -> RouteRefreshChunkExecutionRequest:
    return RouteRefreshChunkExecutionRequest(
        route=route,
        dataset=dataset,
        ordinal=_int(chunk.get("ordinal"), default=0),
        start=str(chunk.get("start", "")),
        end=str(chunk.get("end", "")),
        partition=str(chunk.get("partition", "")),
        source_boundary=str(chunk.get("source_boundary", "")),
        sink_boundary=str(chunk.get("sink_boundary", "")),
        idempotency_key=str(chunk.get("idempotency_key", "")),
        runner_id=runner_id,
        execute=execute,
        output_dir=str(output_dir),
        plan_path=str(plan_path),
    )


def _skipped_result(request: RouteRefreshChunkExecutionRequest) -> RouteRefreshChunkExecutionResult:
    return RouteRefreshChunkExecutionResult(
        ordinal=request.ordinal,
        idempotency_key=request.idempotency_key,
        status="skipped",
        passed=True,
        rows_read=0,
        rows_written=0,
        artifact_path="",
        summary="skipped after prior chunk failure",
        start=request.start,
        end=request.end,
        partition=request.partition,
        source_boundary=request.source_boundary,
        sink_boundary=request.sink_boundary,
    )


def _with_request_boundaries(
    *,
    result: RouteRefreshChunkExecutionResult,
    request: RouteRefreshChunkExecutionRequest,
) -> RouteRefreshChunkExecutionResult:
    return replace(
        result,
        start=result.start or request.start,
        end=result.end or request.end,
        partition=result.partition or request.partition,
        source_boundary=result.source_boundary or request.source_boundary,
        sink_boundary=result.sink_boundary or request.sink_boundary,
    )


def _summary(results: tuple[RouteRefreshChunkExecutionResult, ...]) -> dict[str, int]:
    return {
        "chunks_total": len(results),
        "chunks_succeeded": sum(1 for item in results if item.status == "succeeded"),
        "chunks_failed": sum(1 for item in results if item.status == "failed"),
        "chunks_skipped": sum(1 for item in results if item.status == "skipped"),
        "rows_read": sum(item.rows_read for item in results),
        "rows_written": sum(item.rows_written for item in results),
    }


def _artifacts(
    *,
    plan_path: Path,
    artifacts: Mapping[str, str | Path],
) -> tuple[RouteRefreshExecutionArtifact, ...]:
    values = {"route_refresh_plan": plan_path, **artifacts}
    result: list[RouteRefreshExecutionArtifact] = []
    for name, value in sorted(values.items()):
        path = Path(value)
        exists = path.is_file()
        result.append(
            RouteRefreshExecutionArtifact(
                name=str(name),
                path=str(path),
                required=name == "route_refresh_plan",
                exists=exists,
                sha256=sha256_file(path) if exists else "0" * 64,
                summary="artifact attached" if exists else "artifact missing",
            )
        )
    return tuple(result)


def _strings(value: object) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(str(item) for item in value if str(item))
    return (str(value),) if str(value) else tuple()


def _int(value: object, *, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return default
    return default


__all__ = ["RouteRefreshExecutionService"]
