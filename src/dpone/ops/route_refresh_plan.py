"""Route-level refresh, backfill, and resync planning service."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from dpone.backfill import chunk_spec_from_options, plan_chunks
from dpone.ops.routes import (
    RouteKey,
    RouteProfileCatalog,
    RouteRefreshApproval,
    RouteRefreshChunk,
    RouteRefreshEvidenceReader,
    RouteRefreshPlanPolicy,
    RouteRefreshPlanReport,
    RouteRefreshStateRewind,
    RouteRefreshWindow,
)

DEFAULT_ROUTE_REFRESH_EVIDENCE: tuple[str, ...] = ()
DEFAULT_MAX_REFRESH_CHUNKS = 1000


class RouteRefreshPlanService:
    """Build route-scoped refresh plans without executing heavy data movement."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        reader: RouteRefreshEvidenceReader | None = None,
        policy: RouteRefreshPlanPolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._reader = reader or RouteRefreshEvidenceReader()
        self._policy = policy or RouteRefreshPlanPolicy()

    def plan(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        dataset: str,
        reason: str,
        window_kind: str,
        start: str,
        end: str,
        chunk_size: int | None = None,
        max_chunks: int = DEFAULT_MAX_REFRESH_CHUNKS,
        partition: str = "",
        current_state: str = "",
        target_state: str = "",
        destructive: bool = False,
        require_approval: bool = False,
        approval_granted: bool = False,
        artifacts: Mapping[str, str | Path] | None = None,
        required_evidence: Sequence[str] = (),
    ) -> RouteRefreshPlanReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        window = RouteRefreshWindow(
            kind=_normalize_token(window_kind),
            start=str(start),
            end=str(end),
            chunk_size=chunk_size,
            partition=partition,
        )
        chunk_plan = _chunks(
            route=route,
            dataset=dataset,
            window=window,
            max_chunks=max_chunks,
        )
        state_rewind = _state_rewind(current_state=current_state, target_state=target_state)
        approval = _approval(
            require_approval=require_approval,
            approval_granted=approval_granted,
            destructive=destructive,
            state_rewind=state_rewind,
        )
        required = _required_evidence(extra=tuple(required_evidence))
        artifact_map = dict(artifacts or {})
        names = tuple(dict.fromkeys((*required, *sorted(artifact_map))))
        evidence = tuple(
            self._reader.read(
                name=name,
                path_value=artifact_map.get(name),
                required=name in required,
                route=route,
            )
            for name in names
        )
        decision = self._policy.evaluate(
            route_colon_id=route.colon_id,
            profile_exists=profile is not None,
            required_evidence=required,
            evidence=evidence,
            approval=approval,
            hard_blockers=chunk_plan.blockers,
            warnings=chunk_plan.warnings,
        )
        report = RouteRefreshPlanReport(
            route=route,
            profile=profile,
            dataset=dataset,
            reason=_normalize_token(reason),
            status=decision.status,
            passed=decision.passed,
            window=window,
            chunks=chunk_plan.chunks,
            state_rewind=state_rewind,
            approval=approval,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            required_evidence=required,
            evidence=evidence,
            output_dir=str(directory),
            json_path=str(directory / "route_refresh_plan.json"),
            markdown_path=str(directory / "route_refresh_plan.md"),
        )
        report.write()
        return report


class _ChunkPlan:
    __slots__ = ("blockers", "chunks", "warnings")

    def __init__(
        self,
        *,
        chunks: tuple[RouteRefreshChunk, ...],
        blockers: tuple[str, ...] = (),
        warnings: tuple[str, ...] = (),
    ) -> None:
        self.chunks = chunks
        self.blockers = blockers
        self.warnings = warnings


def _required_evidence(*, extra: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*DEFAULT_ROUTE_REFRESH_EVIDENCE, *extra)))


def _chunks(
    *,
    route: RouteKey,
    dataset: str,
    window: RouteRefreshWindow,
    max_chunks: int,
) -> _ChunkPlan:
    if window.kind == "integer":
        return _integer_chunks(route=route, dataset=dataset, window=window, max_chunks=max_chunks)
    if window.kind == "timestamp" and window.chunk_size:
        return _timestamp_chunks(route=route, dataset=dataset, window=window, max_chunks=max_chunks)
    return _single_chunk(route=route, dataset=dataset, window=window)


def _timestamp_chunks(
    *,
    route: RouteKey,
    dataset: str,
    window: RouteRefreshWindow,
    max_chunks: int,
) -> _ChunkPlan:
    """Multi-chunk timestamp windows via the shared backfill planner.

    ``chunk_size`` is interpreted as days per chunk, matching the backfill
    ``Nd`` step contract so route refresh and ``dpone backfill`` produce
    identical deterministic windows for the same boundaries.
    """

    if not window.chunk_size or window.chunk_size <= 0:
        return _ChunkPlan(chunks=tuple(), blockers=("route_refresh.window_invalid",))
    try:
        spec = chunk_spec_from_options(
            {
                "max_chunks": max_chunks,
                "chunk": {
                    "column": "window",
                    "from": window.start,
                    "to": window.end,
                    "step": f"{window.chunk_size}d",
                },
            }
        )
        planned = plan_chunks(spec, run_key=f"{route.case_id}:{dataset}") if spec else ()
    except ValueError as exc:
        blocker = "route_refresh.chunk_count_exceeded" if "max_chunks" in str(exc) else "route_refresh.window_invalid"
        return _ChunkPlan(chunks=tuple(), blockers=(blocker,))
    if not planned:
        return _ChunkPlan(chunks=tuple(), blockers=("route_refresh.window_invalid",))
    chunks = tuple(
        _chunk(
            route=route,
            dataset=dataset,
            ordinal=item.index,
            start=item.start,
            end=item.end,
            partition=window.partition,
        )
        for item in planned
    )
    return _ChunkPlan(chunks=chunks)


def _integer_chunks(
    *,
    route: RouteKey,
    dataset: str,
    window: RouteRefreshWindow,
    max_chunks: int,
) -> _ChunkPlan:
    try:
        start = int(window.start)
        end = int(window.end)
    except ValueError:
        return _ChunkPlan(chunks=tuple(), blockers=("route_refresh.window_invalid",))
    if start > end or not window.chunk_size or window.chunk_size <= 0:
        return _ChunkPlan(chunks=tuple(), blockers=("route_refresh.window_invalid",))
    count = ((end - start) // window.chunk_size) + 1
    if count > max_chunks:
        return _ChunkPlan(chunks=tuple(), blockers=("route_refresh.chunk_count_exceeded",))
    chunks: list[RouteRefreshChunk] = []
    cursor = start
    ordinal = 1
    while cursor <= end:
        chunk_end = min(cursor + window.chunk_size - 1, end)
        chunks.append(
            _chunk(
                route=route,
                dataset=dataset,
                ordinal=ordinal,
                start=str(cursor),
                end=str(chunk_end),
                partition=window.partition,
            )
        )
        cursor = chunk_end + 1
        ordinal += 1
    return _ChunkPlan(chunks=tuple(chunks))


def _single_chunk(
    *,
    route: RouteKey,
    dataset: str,
    window: RouteRefreshWindow,
) -> _ChunkPlan:
    if not window.start and not window.end and not window.partition:
        return _ChunkPlan(chunks=tuple(), blockers=("route_refresh.window_invalid",))
    return _ChunkPlan(
        chunks=(
            _chunk(
                route=route,
                dataset=dataset,
                ordinal=1,
                start=window.start,
                end=window.end,
                partition=window.partition,
            ),
        )
    )


def _chunk(
    *,
    route: RouteKey,
    dataset: str,
    ordinal: int,
    start: str,
    end: str,
    partition: str,
) -> RouteRefreshChunk:
    source_boundary = partition or f"{start}..{end}"
    sink_boundary = source_boundary
    return RouteRefreshChunk(
        ordinal=ordinal,
        start=start,
        end=end,
        partition=partition,
        source_boundary=source_boundary,
        sink_boundary=sink_boundary,
        idempotency_key=f"{route.case_id}:{dataset}:{ordinal}:{source_boundary}",
    )


def _state_rewind(*, current_state: str, target_state: str) -> RouteRefreshStateRewind:
    current = str(current_state or "")
    target = str(target_state or "")
    rewind = bool(current and target and current != target)
    return RouteRefreshStateRewind(
        current_state=current,
        target_state=target,
        safe_to_rewind=not rewind,
        requires_approval=rewind,
        reason="state_rewind" if rewind else "none",
    )


def _approval(
    *,
    require_approval: bool,
    approval_granted: bool,
    destructive: bool,
    state_rewind: RouteRefreshStateRewind,
) -> RouteRefreshApproval:
    reasons: list[str] = []
    if destructive:
        reasons.append("destructive_refresh")
    if state_rewind.requires_approval:
        reasons.append("state_rewind")
    if require_approval:
        reasons.append("policy_required")
    normalized = tuple(dict.fromkeys(reasons))
    return RouteRefreshApproval(
        required=bool(normalized),
        approved=approval_granted,
        reasons=normalized,
    )


def _normalize_token(value: str) -> str:
    return str(value).strip().lower().replace("-", "_")


__all__ = ["DEFAULT_ROUTE_REFRESH_EVIDENCE", "RouteRefreshPlanService"]
