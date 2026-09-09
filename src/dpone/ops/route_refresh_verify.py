"""Route-level refresh verification receipt service."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.ops.checksums import sha256_file
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.refresh_verification_models import (
    RouteRefreshChunkVerification,
    RouteRefreshSideSnapshot,
    RouteRefreshVerificationArtifact,
    RouteRefreshVerificationReport,
    RouteRefreshVerificationRequest,
)
from dpone.ops.routes.refresh_verification_policy import RouteRefreshVerificationPolicy


class RouteRefreshVerificationService:
    """Verify post-load route refresh execution through injected snapshot readers."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        source_reader: _SnapshotReader | None = None,
        sink_reader: _SnapshotReader | None = None,
        policy: RouteRefreshVerificationPolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._source_reader = source_reader or _UnavailableRouteRefreshSnapshotReader()
        self._sink_reader = sink_reader or _UnavailableRouteRefreshSnapshotReader()
        self._policy = policy or RouteRefreshVerificationPolicy()

    def verify(
        self,
        *,
        route_refresh_execution_json: str | Path,
        output_dir: str | Path,
        runner_id: str,
        key_columns: tuple[str, ...],
        boundary_column: str,
        columns: tuple[str, ...],
        type_hints: Mapping[str, str] | None = None,
    ) -> RouteRefreshVerificationReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        execution_path = Path(route_refresh_execution_json)
        execution = _load_payload(execution_path)
        route = _route(execution)
        chunks_payload = _chunks(execution)
        base_blockers = _base_blockers(execution_path=execution_path, execution=execution)
        chunks = (
            tuple()
            if base_blockers
            else self._verify_chunks(
                route=route,
                dataset=str(execution.get("dataset", "")),
                chunks_payload=chunks_payload,
                execution_path=execution_path,
                runner_id=runner_id,
                key_columns=key_columns,
                boundary_column=boundary_column,
                columns=columns,
                type_hints=type_hints or {},
            )
        )
        decision = self._policy.evaluate_report(chunks=chunks, base_blockers=base_blockers)
        report = RouteRefreshVerificationReport(
            route=route,
            profile=self._catalog.get(route),
            dataset=str(execution.get("dataset", "")),
            runner_id=runner_id,
            route_refresh_execution_json=str(execution_path),
            execution_sha256=sha256_file(execution_path) if execution_path.is_file() else "0" * 64,
            status=decision.status,
            passed=decision.passed,
            ready_for_state_promotion=decision.ready_for_state_promotion,
            summary=_summary(chunks=chunks, execution_chunks=chunks_payload),
            chunks=chunks,
            artifacts=_artifacts(execution_path),
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            output_dir=str(directory),
            json_path=str(directory / "route_refresh_verification.json"),
            markdown_path=str(directory / "route_refresh_verification.md"),
        )
        report.write()
        return report

    def _verify_chunks(
        self,
        *,
        route: RouteKey,
        dataset: str,
        chunks_payload: tuple[Mapping[str, object], ...],
        execution_path: Path,
        runner_id: str,
        key_columns: tuple[str, ...],
        boundary_column: str,
        columns: tuple[str, ...],
        type_hints: Mapping[str, str],
    ) -> tuple[RouteRefreshChunkVerification, ...]:
        chunks: list[RouteRefreshChunkVerification] = []
        for chunk in chunks_payload:
            request = _request(
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
            chunks.append(self._verify_chunk(request))
        return tuple(chunks)

    def _verify_chunk(self, request: RouteRefreshVerificationRequest) -> RouteRefreshChunkVerification:
        try:
            source = self._source_reader.read_chunk(request)
            sink = self._sink_reader.read_chunk(request)
        except Exception as exc:
            empty = RouteRefreshSideSnapshot(row_count=0, min_boundary="", max_boundary="", typed_hash="")
            return RouteRefreshChunkVerification(
                ordinal=request.ordinal,
                idempotency_key=request.idempotency_key,
                status="failed",
                passed=False,
                source=empty,
                sink=empty,
                summary="snapshot reader failed",
                blockers=("route_refresh_verification.reader_unavailable", str(exc)),
            )
        decision = self._policy.evaluate_chunk(
            ordinal=request.ordinal,
            idempotency_key=request.idempotency_key,
            source=source,
            sink=sink,
        )
        return RouteRefreshChunkVerification(
            ordinal=decision.ordinal,
            idempotency_key=decision.idempotency_key,
            status=decision.status,
            passed=decision.passed,
            source=source,
            sink=sink,
            summary=decision.summary,
            blockers=decision.blockers,
            warnings=decision.warnings,
        )


def _load_payload(path: Path) -> Mapping[str, Any]:
    if not path.is_file():
        return {"passed": False, "status": "missing", "blockers": ["route_refresh_verification.execution_missing"]}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "passed": False,
            "status": "invalid_json",
            "blockers": ["route_refresh_verification.execution_invalid_json"],
        }
    return payload if isinstance(payload, Mapping) else {"passed": False, "status": "invalid_shape"}


class _UnavailableRouteRefreshSnapshotReader:
    def read_chunk(self, request: RouteRefreshVerificationRequest) -> RouteRefreshSideSnapshot:
        del request
        raise RuntimeError("route_refresh_verification.reader_unavailable")


class _SnapshotReader(Protocol):
    def read_chunk(self, request: RouteRefreshVerificationRequest) -> RouteRefreshSideSnapshot: ...


def _route(payload: Mapping[str, Any]) -> RouteKey:
    route = payload.get("route")
    values = route if isinstance(route, Mapping) else {}
    return RouteKey.of(str(values.get("source", "")), str(values.get("sink", "")), str(values.get("strategy", "")))


def _chunks(payload: Mapping[str, Any]) -> tuple[Mapping[str, object], ...]:
    chunks = payload.get("chunks", ())
    if not isinstance(chunks, list | tuple):
        return tuple()
    return tuple(item for item in chunks if isinstance(item, Mapping))


def _base_blockers(*, execution_path: Path, execution: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    if not execution_path.is_file():
        blockers.append("route_refresh_verification.execution_missing")
    if execution.get("status") != "succeeded" or not bool(execution.get("passed")):
        blockers.append("route_refresh_verification.execution_not_succeeded")
    if not bool(execution.get("executed")):
        blockers.append("route_refresh_verification.execution_not_run")
    return tuple(dict.fromkeys((*blockers, *_strings(execution.get("blockers")))))


def _request(
    *,
    route: RouteKey,
    dataset: str,
    chunk: Mapping[str, object],
    execution_path: Path,
    runner_id: str,
    key_columns: tuple[str, ...],
    boundary_column: str,
    columns: tuple[str, ...],
    type_hints: Mapping[str, str],
) -> RouteRefreshVerificationRequest:
    artifact_payload = _chunk_artifact_payload(str(chunk.get("artifact_path", "")))
    raw_artifact_chunk = artifact_payload.get("chunk")
    artifact_chunk: Mapping[str, object] = raw_artifact_chunk if isinstance(raw_artifact_chunk, Mapping) else {}
    return RouteRefreshVerificationRequest(
        route=route,
        dataset=dataset,
        ordinal=_int(chunk.get("ordinal"), default=0),
        start=str(chunk.get("start", artifact_chunk.get("start", ""))),
        end=str(chunk.get("end", artifact_chunk.get("end", ""))),
        partition=str(chunk.get("partition", artifact_chunk.get("partition", ""))),
        source_boundary=str(chunk.get("source_boundary", artifact_chunk.get("source_boundary", ""))),
        sink_boundary=str(chunk.get("sink_boundary", artifact_chunk.get("sink_boundary", ""))),
        idempotency_key=str(chunk.get("idempotency_key", artifact_chunk.get("idempotency_key", ""))),
        runner_id=runner_id,
        execution_path=str(execution_path),
        chunk_artifact_path=str(chunk.get("artifact_path", "")),
        columns=columns,
        key_columns=key_columns,
        boundary_column=boundary_column,
        type_hints=dict(type_hints),
    )


def _chunk_artifact_payload(path_value: str) -> Mapping[str, Any]:
    path = Path(path_value)
    if not path.is_file() or path.suffix.lower() != ".json":
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _summary(
    *,
    chunks: tuple[RouteRefreshChunkVerification, ...],
    execution_chunks: tuple[Mapping[str, object], ...],
) -> dict[str, int]:
    return {
        "chunks_total": len(execution_chunks),
        "chunks_verified": sum(1 for item in chunks if item.status == "verified"),
        "chunks_failed": sum(1 for item in chunks if item.status == "failed"),
        "source_rows": sum(item.source.row_count for item in chunks),
        "sink_rows": sum(item.sink.row_count for item in chunks),
    }


def _artifacts(execution_path: Path) -> tuple[RouteRefreshVerificationArtifact, ...]:
    exists = execution_path.is_file()
    return (
        RouteRefreshVerificationArtifact(
            name="route_refresh_execution",
            path=str(execution_path),
            required=True,
            exists=exists,
            sha256=sha256_file(execution_path) if exists else "0" * 64,
            summary="execution receipt verified" if exists else "execution receipt missing",
        ),
    )


def _strings(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value else tuple()
    if isinstance(value, list | tuple):
        return tuple(str(item) for item in value if str(item))
    return tuple()


def _int(value: object, *, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return default
    return default


__all__ = ["RouteRefreshVerificationService"]
