"""Route-level refresh snapshot capture receipt service."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from dpone.ops.checksums import sha256_file
from dpone.ops.route_refresh_snapshot_capture_payloads import (
    base_blockers,
    chunks_from_execution,
    load_snapshot_execution,
    route_from_execution,
    snapshot_request,
)
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.refresh_snapshot_capture_models import (
    RouteRefreshChunkSnapshotCapture,
    RouteRefreshSnapshotCaptureArtifact,
    RouteRefreshSnapshotCaptureReport,
    RouteRefreshSnapshotCaptureRequest,
    RouteRefreshSnapshotChunk,
    RouteRefreshSnapshotDocument,
)
from dpone.ops.routes.refresh_snapshot_capture_policy import RouteRefreshSnapshotCapturePolicy
from dpone.ops.routes.refresh_snapshot_capture_reader import (
    UnavailableRouteRefreshRowsReader,
    empty_side_snapshot,
    snapshot_from_rows,
)


class RouteRefreshSnapshotCaptureService:
    """Capture source and sink snapshots for a succeeded route refresh execution."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        source_reader: _RowsReader | None = None,
        sink_reader: _RowsReader | None = None,
        policy: RouteRefreshSnapshotCapturePolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._source_reader = source_reader or UnavailableRouteRefreshRowsReader()
        self._sink_reader = sink_reader or UnavailableRouteRefreshRowsReader()
        self._policy = policy or RouteRefreshSnapshotCapturePolicy()

    def capture(
        self,
        *,
        route_refresh_execution_json: str | Path,
        output_dir: str | Path,
        runner_id: str,
        key_columns: tuple[str, ...],
        boundary_column: str,
        columns: tuple[str, ...],
        type_hints: Mapping[str, str] | None = None,
    ) -> RouteRefreshSnapshotCaptureReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        execution_path = Path(route_refresh_execution_json)
        execution = load_snapshot_execution(execution_path)
        route = route_from_execution(execution)
        chunks_payload = chunks_from_execution(execution)
        hints = {str(key): str(value) for key, value in (type_hints or {}).items()}
        execution_blockers = base_blockers(execution_path=execution_path, execution=execution)
        chunks = (
            tuple()
            if execution_blockers
            else self._capture_chunks(
                route=route,
                dataset=str(execution.get("dataset", "")),
                chunks_payload=chunks_payload,
                execution_path=execution_path,
                runner_id=runner_id,
                key_columns=key_columns,
                boundary_column=boundary_column,
                columns=columns,
                type_hints=hints,
            )
        )
        execution_sha = sha256_file(execution_path) if execution_path.is_file() else "0" * 64
        source_snapshot_json = directory / "source_route_refresh_snapshot.json"
        sink_snapshot_json = directory / "sink_route_refresh_snapshot.json"
        if chunks:
            _snapshot_document(
                route=route,
                side="source",
                dataset=str(execution.get("dataset", "")),
                runner_id=runner_id,
                execution_path=execution_path,
                execution_sha=execution_sha,
                columns=columns,
                key_columns=key_columns,
                boundary_column=boundary_column,
                type_hints=hints,
                chunks=chunks,
                json_path=source_snapshot_json,
            ).write()
            _snapshot_document(
                route=route,
                side="sink",
                dataset=str(execution.get("dataset", "")),
                runner_id=runner_id,
                execution_path=execution_path,
                execution_sha=execution_sha,
                columns=columns,
                key_columns=key_columns,
                boundary_column=boundary_column,
                type_hints=hints,
                chunks=chunks,
                json_path=sink_snapshot_json,
            ).write()
        decision = self._policy.evaluate(chunks=chunks, base_blockers=execution_blockers)
        report = RouteRefreshSnapshotCaptureReport(
            route=route,
            profile=self._catalog.get(route),
            dataset=str(execution.get("dataset", "")),
            runner_id=runner_id,
            route_refresh_execution_json=str(execution_path),
            execution_sha256=execution_sha,
            status=decision.status,
            passed=decision.passed,
            ready_for_verification=decision.ready_for_verification,
            summary=_summary(chunks=chunks, execution_chunks=chunks_payload, column_count=len(columns)),
            chunks=chunks,
            artifacts=_artifacts(
                execution_path=execution_path,
                source_snapshot_json=source_snapshot_json,
                sink_snapshot_json=sink_snapshot_json,
            ),
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            output_dir=str(directory),
            source_snapshot_json=str(source_snapshot_json),
            sink_snapshot_json=str(sink_snapshot_json),
            json_path=str(directory / "route_refresh_snapshot_capture.json"),
            markdown_path=str(directory / "route_refresh_snapshot_capture.md"),
        )
        report.write()
        return report

    def _capture_chunks(
        self,
        *,
        route: Any,
        dataset: str,
        chunks_payload: tuple[Mapping[str, object], ...],
        execution_path: Path,
        runner_id: str,
        key_columns: tuple[str, ...],
        boundary_column: str,
        columns: tuple[str, ...],
        type_hints: Mapping[str, str],
    ) -> tuple[RouteRefreshChunkSnapshotCapture, ...]:
        chunks: list[RouteRefreshChunkSnapshotCapture] = []
        for chunk in chunks_payload:
            request = snapshot_request(
                route=route,
                dataset=dataset,
                chunk=chunk,
                execution_path=execution_path,
                runner_id=runner_id,
                key_columns=key_columns,
                boundary_column=boundary_column,
                columns=columns,
                type_hints=type_hints,
            )
            chunks.append(self._capture_chunk(request))
        return tuple(chunks)

    def _capture_chunk(self, request: RouteRefreshSnapshotCaptureRequest) -> RouteRefreshChunkSnapshotCapture:
        try:
            source = snapshot_from_rows(
                self._source_reader.read_rows(request),
                columns=request.columns,
                key_columns=request.key_columns,
                boundary_column=request.boundary_column,
                type_hints=request.type_hints,
            )
            sink = snapshot_from_rows(
                self._sink_reader.read_rows(request),
                columns=request.columns,
                key_columns=request.key_columns,
                boundary_column=request.boundary_column,
                type_hints=request.type_hints,
            )
        except Exception as exc:
            empty = empty_side_snapshot()
            return RouteRefreshChunkSnapshotCapture(
                ordinal=request.ordinal,
                idempotency_key=request.idempotency_key,
                start=request.start,
                end=request.end,
                status="failed",
                passed=False,
                source=empty,
                sink=empty,
                summary="snapshot capture reader failed",
                blockers=("route_refresh_snapshot_capture.reader_unavailable", str(exc)),
            )
        return RouteRefreshChunkSnapshotCapture(
            ordinal=request.ordinal,
            idempotency_key=request.idempotency_key,
            start=request.start,
            end=request.end,
            status="captured",
            passed=True,
            source=source,
            sink=sink,
            summary="source and sink snapshots captured",
        )


class _RowsReader(Protocol):
    def read_rows(self, request: RouteRefreshSnapshotCaptureRequest) -> Sequence[Mapping[str, object]]: ...


def _snapshot_document(
    *,
    route: Any,
    side: str,
    dataset: str,
    runner_id: str,
    execution_path: Path,
    execution_sha: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    boundary_column: str,
    type_hints: Mapping[str, str],
    chunks: tuple[RouteRefreshChunkSnapshotCapture, ...],
    json_path: Path,
) -> RouteRefreshSnapshotDocument:
    return RouteRefreshSnapshotDocument(
        route=route,
        side="source" if side == "source" else "sink",
        dataset=dataset,
        runner_id=runner_id,
        route_refresh_execution_json=str(execution_path),
        execution_sha256=execution_sha,
        columns=columns,
        key_columns=key_columns,
        boundary_column=boundary_column,
        type_hints=dict(type_hints),
        chunks=tuple(
            RouteRefreshSnapshotChunk(
                ordinal=chunk.ordinal,
                idempotency_key=chunk.idempotency_key,
                start=chunk.start,
                end=chunk.end,
                snapshot=chunk.source if side == "source" else chunk.sink,
            )
            for chunk in chunks
        ),
        json_path=str(json_path),
    )


def _summary(
    *,
    chunks: tuple[RouteRefreshChunkSnapshotCapture, ...],
    execution_chunks: tuple[Mapping[str, object], ...],
    column_count: int,
) -> dict[str, int]:
    return {
        "chunks_total": len(execution_chunks),
        "chunks_captured": sum(1 for item in chunks if item.status == "captured"),
        "chunks_failed": sum(1 for item in chunks if item.status == "failed"),
        "source_rows": sum(item.source.row_count for item in chunks),
        "sink_rows": sum(item.sink.row_count for item in chunks),
        "column_count": column_count,
    }


def _artifacts(
    *,
    execution_path: Path,
    source_snapshot_json: Path,
    sink_snapshot_json: Path,
) -> tuple[RouteRefreshSnapshotCaptureArtifact, ...]:
    return (
        _artifact("source_route_refresh_snapshot", source_snapshot_json, required=True, summary="captured source rows"),
        _artifact("sink_route_refresh_snapshot", sink_snapshot_json, required=True, summary="captured sink rows"),
        _artifact("route_refresh_execution", execution_path, required=True, summary="input execution receipt"),
    )


def _artifact(
    name: str,
    path: Path,
    *,
    required: bool,
    summary: str,
) -> RouteRefreshSnapshotCaptureArtifact:
    return RouteRefreshSnapshotCaptureArtifact(
        name=name,
        path=str(path),
        required=required,
        exists=path.is_file(),
        sha256=sha256_file(path) if path.is_file() else "0" * 64,
        summary=summary,
    )


__all__ = ["RouteRefreshSnapshotCaptureService"]
