"""Manifest fact extraction for load profile advice."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.load_profile.models import ProfileAdviceRequest, SourceShape, WorkerProfile
from dpone.manifest.advisory import AdvisoryManifestProcessReader
from dpone.runtime.columnar_execution_mode import resolve_columnar_execution_policy
from dpone.runtime.storage_policy import parse_byte_size


class ManifestProfileRequestBuilder:
    """Builds profile-advice requests from a manifest plus optional probe facts."""

    def __init__(
        self,
        *,
        process_reader: AdvisoryManifestProcessReader | None = None,
    ) -> None:
        self._process_reader = process_reader or AdvisoryManifestProcessReader()

    def build(
        self,
        path: str | Path,
        *,
        row_count: int | None = None,
        column_count: int | None = None,
        estimated_bytes_per_row: int | None = None,
        worker_profile: str | None = None,
        optimization_goal: str | None = None,
        target_chunk_bytes: str | int | None = None,
        current_max_chunk_rows: int | None = None,
    ) -> ProfileAdviceRequest:
        raw = self._process_reader.read(Path(path))
        source = _mapping(raw.get("source"))
        sink = _mapping(raw.get("sink"))
        columnar = _columnar_options(source)
        execution = _mapping(columnar.get("execution"))
        route_id = _route_id(columnar)
        policy = resolve_columnar_execution_policy(columnar)
        return ProfileAdviceRequest(
            source_type=str(source.get("type") or "unknown"),
            sink_type=str(sink.get("type") or "unknown"),
            route_id=route_id,
            execution_mode=policy.value,
            optimization_goal=str(optimization_goal or "balanced"),
            target_chunk_bytes=parse_byte_size(
                target_chunk_bytes
                or execution.get("target_chunk_bytes")
                or columnar.get("target_chunk_bytes")
                or "512MiB"
            ),
            current_max_chunk_rows=int(
                current_max_chunk_rows or execution.get("max_chunk_rows") or columnar.get("max_chunk_rows") or 1_000_000
            ),
            source_shape=SourceShape(
                row_count=row_count,
                column_count=column_count,
                estimated_bytes_per_row=estimated_bytes_per_row,
                source_kind=str(source.get("kind") or "unknown"),
            ),
            worker_profile=WorkerProfile.from_name(worker_profile),
        )


def _columnar_options(source: Mapping[str, Any]) -> dict[str, Any]:
    options = _mapping(source.get("options"))
    native = _mapping(options.get("native_transfer"))
    snapshot = _mapping(native.get("snapshot"))
    return _mapping(snapshot.get("columnar_fast_path") or options.get("columnar_fast_path"))


def _route_id(columnar: Mapping[str, Any]) -> str:
    provider = str(columnar.get("provider") or "auto").strip().lower()
    if provider == "object_storage_pull":
        object_storage = _mapping(columnar.get("object_storage"))
        pull = "s3cluster" if object_storage.get("enabled", True) else "s3"
        return f"object_storage_pull_{pull}"
    if provider == "direct_push_columnar":
        return "direct_push_columnar"
    return "auto"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = ["ManifestProfileRequestBuilder"]
