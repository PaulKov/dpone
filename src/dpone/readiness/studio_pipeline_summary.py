"""Bounded project pipeline projection for the read-only Studio API."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.contracts.pipeline_summary import (
    PipelineNextAction,
    PipelineProcessSummary,
    PipelineSummary,
)
from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService
from dpone.readiness.airflow_explain_service import AirflowExplainService
from dpone.readiness.capability_discovery_protocols import (
    CapabilitySnapshotProtocol,
    CapabilitySnapshotProvider,
)
from dpone.readiness.capability_discovery_service import normalize_connector_ref
from dpone.readiness.error_contract import dpone_error, error_docs_url
from dpone.readiness.studio_recovery import (
    explain_next_actions,
    structured_studio_errors,
)

_MAX_PIPELINES = 1000


class StudioPipelineCatalog:
    """List direct pipeline sources without recursive project discovery."""

    def __init__(
        self,
        *,
        root: Path,
        authoring_check: AirflowAuthoringCheckService,
        explain: AirflowExplainService,
        capabilities: CapabilitySnapshotProvider,
    ) -> None:
        self._root = root.resolve(strict=True)
        self._authoring_check = authoring_check
        self._explain = explain
        self._capabilities = capabilities

    def list(self, *, limit: int = 200) -> dict[str, Any]:
        bounded_limit = _bounded_limit(limit)
        pipeline_ids = self._pipeline_ids()
        capabilities = self._capabilities()
        return {
            "schema": "dpone.pipeline-summary-list.v1",
            "items": [
                self._summary(pipeline_id, capabilities=capabilities).to_dict()
                for pipeline_id in pipeline_ids[:bounded_limit]
            ],
            "count": min(len(pipeline_ids), bounded_limit),
            "total": len(pipeline_ids),
        }

    def summary(self, pipeline_id: str) -> PipelineSummary:
        return self._summary(pipeline_id, capabilities=self._capabilities())

    def _summary(
        self,
        pipeline_id: str,
        *,
        capabilities: CapabilitySnapshotProtocol,
    ) -> PipelineSummary:
        checked = self._authoring_check.inspect(pipeline_id)
        explained = self._explain.explain_checked(pipeline_id, checked)
        payload = checked.payload or {}
        process_payloads = (
            checked.compilation.processes if checked.compilation is not None else _declared_processes(payload)
        )
        processes = _process_summaries(process_payloads)
        capability, route_errors = _resolve_process_routes(
            processes,
            capabilities=capabilities,
        )
        metadata = payload.get("metadata")
        details = checked.result.details or {}
        explain_details = explained.details or {}
        route_blocked = bool(route_errors)
        valid = checked.result.passed and not route_blocked
        return PipelineSummary(
            id=pipeline_id,
            source_path=checked.source_label or f"pipelines/{pipeline_id}/pipeline.yaml",
            valid=valid,
            operational_status=(
                "blocked"
                if route_blocked
                else "ready"
                if explained.passed
                else "blocked"
                if checked.result.passed
                else "not_applicable"
            ),
            authoring_mode=_optional_text(details.get("authoring_mode")),
            recipe=(_optional_text(metadata.get("recipe")) if isinstance(metadata, Mapping) else None),
            processes=processes,
            quality_gates=_quality_gates(payload),
            schedule=_optional_text(payload.get("schedule")),
            artifact_state=dict(explain_details.get("artifact_state") or {}),
            support=capability.support.to_dict() if capability is not None else None,
            certification=(capability.certification.to_dict() if capability is not None else None),
            next_actions=_next_actions(
                pipeline_id,
                valid=valid and explained.passed,
                route_blocked=route_blocked,
                artifact_state=explain_details.get("artifact_state"),
            ),
            errors=tuple(
                structured_studio_errors(
                    [*route_errors, *explained.errors],
                )
            ),
        )

    def _pipeline_ids(self) -> tuple[str, ...]:
        pipelines = self._root / "pipelines"
        if not pipelines.exists():
            return ()
        if pipelines.is_symlink() or not pipelines.is_dir():
            raise StudioPipelineCatalogError(
                "DPONE_STUDIO_PIPELINE_ROOT_UNSAFE",
                "The pipelines root must be a regular project directory.",
            )
        ids: list[str] = []
        for child in sorted(pipelines.iterdir(), key=lambda path: path.name):
            if len(ids) >= _MAX_PIPELINES:
                raise StudioPipelineCatalogError(
                    "DPONE_STUDIO_PIPELINE_LIMIT_EXCEEDED",
                    f"Studio supports at most {_MAX_PIPELINES} direct pipeline entries.",
                )
            source = child / "pipeline.yaml"
            if child.is_symlink() or not child.is_dir() or source.is_symlink():
                continue
            if source.is_file():
                ids.append(child.name)
        return tuple(ids)


class StudioPipelineCatalogError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.public_message = message

    def to_error(self) -> dict[str, Any]:
        return dpone_error(
            self.code,
            "Studio could not read the bounded pipeline catalog safely.",
            stage="studio_pipeline_catalog",
        )


def _process_summaries(
    processes: Sequence[Mapping[str, Any]],
) -> tuple[PipelineProcessSummary, ...]:
    result = []
    for index, process in enumerate(processes):
        source = process.get("source")
        sink = process.get("sink")
        if not isinstance(source, Mapping) or not isinstance(sink, Mapping):
            continue
        strategy = sink.get("strategy")
        strategy_mode = strategy.get("mode") if isinstance(strategy, Mapping) else strategy
        result.append(
            PipelineProcessSummary(
                name=str(process.get("name") or f"process_{index + 1}"),
                source=str(source.get("type") or "unknown"),
                sink=str(sink.get("type") or "unknown"),
                strategy=str(strategy_mode or "unknown"),
            )
        )
    return tuple(result)


def _declared_processes(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    processes = payload.get("processes")
    if not isinstance(processes, Sequence) or isinstance(
        processes,
        (str, bytes, bytearray),
    ):
        return ()
    return tuple(item for item in processes if isinstance(item, Mapping))


def _resolve_process_routes(
    processes: tuple[PipelineProcessSummary, ...],
    *,
    capabilities: CapabilitySnapshotProtocol,
) -> tuple[Any | None, tuple[dict[str, Any], ...]]:
    route_by_id = {item.id: item for item in capabilities.routes}
    resolved = []
    errors = []
    for process in processes:
        route_id = (
            f"{normalize_connector_ref(process.source)}:{normalize_connector_ref(process.sink)}:{process.strategy}"
        )
        capability = route_by_id.get(route_id)
        if capability is None or capability.support.status == "not_supported":
            errors.append(
                dpone_error(
                    "DPONE_ROUTE_NOT_SUPPORTED",
                    "The pipeline uses a source, sink, and strategy route that is not supported.",
                    stage="studio_pipeline_summary",
                    entity={"kind": "route", "id": route_id},
                    docs_url=error_docs_url("DPONE_ROUTE_NOT_SUPPORTED"),
                )
            )
            continue
        resolved.append(capability)
    single = resolved[0] if len(processes) == 1 and len(resolved) == 1 else None
    return single, tuple(errors)


def _quality_gates(payload: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    quality = payload.get("quality")
    gates = quality.get("gates") if isinstance(quality, Mapping) else None
    if not isinstance(gates, list):
        return ()
    return tuple(dict(item) for item in gates if isinstance(item, Mapping))


def _next_actions(
    pipeline_id: str,
    *,
    valid: bool,
    route_blocked: bool,
    artifact_state: object,
) -> tuple[PipelineNextAction, ...]:
    if route_blocked:
        return (
            PipelineNextAction(
                id="check_pipeline",
                label="Choose a supported source, sink, and strategy route.",
                argv=("dpone", "check", f"pipelines/{pipeline_id}"),
            ),
        )
    actions = explain_next_actions(
        pipeline_id,
        {
            "passed": valid,
            "artifact_state": (artifact_state if isinstance(artifact_state, Mapping) else {}),
        },
    )
    return tuple(
        PipelineNextAction(
            id=str(action["id"]),
            label=str(action["label"]),
            argv=tuple(str(item) for item in action["argv"]),
        )
        for action in actions
    )


def _bounded_limit(value: int) -> int:
    if not 1 <= value <= 200:
        raise StudioPipelineCatalogError(
            "DPONE_STUDIO_PAGE_LIMIT_INVALID",
            "Pagination limit must be between 1 and 200.",
        )
    return value


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


__all__ = ["StudioPipelineCatalog", "StudioPipelineCatalogError"]
