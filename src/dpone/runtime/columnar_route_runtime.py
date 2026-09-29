"""Runtime adapters for columnar route capability orchestration."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import Any

from dpone.runtime.columnar_execution_mode import resolve_columnar_execution_policy
from dpone.runtime.columnar_route_capabilities import (
    REQ_SOURCE_COLUMNAR,
    columnar_route_candidates,
    columnar_route_evidence,
)
from dpone.runtime.partitioning_options import PartitioningOptionsResolver
from dpone.runtime.route_runtime import SelectedRouteExecutor

SourceCapabilityProbe = Callable[..., Any]
ObjectStorageAccessProbe = Callable[..., Any]
SinkEvidenceProbe = Callable[..., Mapping[str, Any]]
ColumnarRequestFactory = Callable[..., Any]


class ColumnarRouteCandidateProvider:
    def __init__(
        self,
        *,
        include_streaming_fallback: bool = True,
        include_direct_push: bool = True,
        range_admission_available: Callable[[], bool] | None = None,
    ) -> None:
        self.include_streaming_fallback = include_streaming_fallback
        self.include_direct_push = include_direct_push
        self._range_admission_available = range_admission_available

    def candidates(self, *, load_config: Any, source: Any, sink: Any) -> Sequence[Any]:
        del source, sink
        options = getattr(load_config, "options", {}) or {}
        source_options = options.get("source_options") if isinstance(options, dict) else None
        canonical_options = source_options if isinstance(source_options, dict) else options
        range_mode = PartitioningOptionsResolver.resolve(canonical_options).range_parallelism.mode
        if (
            range_mode == "required"
            and self._range_admission_available is not None
            and not self._range_admission_available()
        ):
            candidate = columnar_route_candidates(False)[0]
            source_requirement = next(item for item in candidate.requirements if item.id == REQ_SOURCE_COLUMNAR)
            return (replace(candidate, requirements=(source_requirement,)),)
        allow_streaming_fallback = self.include_streaming_fallback and range_mode != "required"
        candidates = columnar_route_candidates(allow_streaming_fallback)
        if self.include_direct_push:
            return candidates
        return tuple(candidate for candidate in candidates if candidate.route_id != "direct_push_columnar")


class ColumnarCapabilityProbeRunner:
    def __init__(
        self,
        *,
        source_capability: SourceCapabilityProbe,
        object_storage_access: ObjectStorageAccessProbe,
        sink_evidence: SinkEvidenceProbe,
    ) -> None:
        self._source_capability = source_capability
        self._object_storage_access = object_storage_access
        self._sink_evidence = sink_evidence

    def probe(
        self,
        *,
        candidates: Sequence[Any],
        load_config: Any,
        source: Any,
        sink: Any,
        load_record: Any | None = None,
    ) -> Mapping[str, Any]:
        context = {
            "candidates": candidates,
            "load_config": load_config,
            "source": source,
            "sink": sink,
            "load_record": load_record,
        }
        source_capability = self._source_capability(**context)
        required_ids = {requirement.id for candidate in candidates for requirement in candidate.requirements}
        if required_ids == {REQ_SOURCE_COLUMNAR}:
            return columnar_route_evidence(
                source_capability=source_capability,
                object_storage_access=None,
                sink_evidence={},
            )
        object_storage_access = self._object_storage_access(**context)
        context["object_storage_access"] = object_storage_access
        return columnar_route_evidence(
            source_capability=source_capability,
            object_storage_access=object_storage_access,
            sink_evidence=self._sink_evidence(**context),
        )


class ColumnarObjectStoragePullExecutor(SelectedRouteExecutor):
    def __init__(
        self,
        *,
        route_id: str,
        request_factory: ColumnarRequestFactory,
        snapshot_provider: Any,
    ) -> None:
        self.route_id = route_id
        self._request_factory = request_factory
        self._snapshot_provider = snapshot_provider

    def extract(self, *, load_config: Any, source: Any, sink: Any, state: Any, load_record: Any) -> Any:
        from dpone.runtime.sources.extract_result import ExtractResult

        request = self._request_factory(
            load_config=load_config,
            source=source,
            sink=sink,
            state=state,
            load_record=load_record,
        )
        execution_policy = resolve_columnar_execution_policy(getattr(request, "options", None))
        if execution_policy.is_streaming:
            raise RuntimeError("columnar_streaming_provider_not_certified")
        if execution_policy.is_chunked:
            iterator = getattr(self._snapshot_provider, "iter_object_storage_windows", None)
            if not callable(iterator):
                raise RuntimeError("columnar_object_storage_provider_missing_iter_windows")
            from dpone.runtime.columnar_object_storage_windows import ObjectStorageColumnarChunkedArtifact

            return ExtractResult(
                artifact=ObjectStorageColumnarChunkedArtifact(
                    provider=self._snapshot_provider,
                    request=request,
                    columns=tuple(column for column, _ in request.schema),
                    schema_hash=_schema_hash(request.schema),
                    format=request.format,
                    cleanup_policy=execution_policy.cleanup_policy,
                ),
                schema=request.schema,
                state=None,
            )
        manifest = self._snapshot_provider.snapshot(request)
        return ExtractResult(artifact=manifest, schema=request.schema, state=None)


class ColumnarDirectPushExecutor(SelectedRouteExecutor):
    def __init__(
        self,
        *,
        route_id: str,
        request_factory: ColumnarRequestFactory,
        snapshot_provider: Any,
    ) -> None:
        self.route_id = route_id
        self._request_factory = request_factory
        self._snapshot_provider = snapshot_provider

    def extract(self, *, load_config: Any, source: Any, sink: Any, state: Any, load_record: Any) -> Any:
        from dpone.runtime.sources.extract_result import ExtractResult

        request = self._request_factory(
            load_config=load_config,
            source=source,
            sink=sink,
            state=state,
            load_record=load_record,
        )
        execution_mode = resolve_columnar_execution_policy(getattr(request, "options", None))
        if execution_mode.is_streaming:
            raise RuntimeError("columnar_streaming_provider_not_certified")
        if execution_mode.is_chunked:
            iter_local_chunks = getattr(self._snapshot_provider, "iter_local_chunks", None)
            if not callable(iter_local_chunks):
                raise RuntimeError("columnar_direct_push_provider_missing_iter_local_chunks")
            from dpone.runtime.columnar_fast_path_models import LocalColumnarChunkedArtifact

            return ExtractResult(
                artifact=LocalColumnarChunkedArtifact(
                    provider=self._snapshot_provider,
                    request=request,
                    columns=tuple(column for column, _ in request.schema),
                    schema_hash=_schema_hash(request.schema),
                    format=request.format,
                    cleanup_policy=execution_mode.cleanup_policy,
                ),
                schema=request.schema,
                state=None,
            )
        snapshot_local = getattr(self._snapshot_provider, "snapshot_local", None)
        if not callable(snapshot_local):
            raise RuntimeError("columnar_direct_push_provider_missing_snapshot_local")
        manifest = snapshot_local(request)
        return ExtractResult(artifact=manifest, schema=request.schema, state=None)


def _schema_hash(schema: Sequence[tuple[str, str]]) -> str:
    from hashlib import sha256

    return f"sha256:{sha256(repr(tuple(schema)).encode('utf-8')).hexdigest()}"


__all__ = [
    "ColumnarCapabilityProbeRunner",
    "ColumnarDirectPushExecutor",
    "ColumnarObjectStoragePullExecutor",
    "ColumnarRouteCandidateProvider",
]
