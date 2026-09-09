"""Application service for CDC replay planning."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.readiness.cdc_replay import CDCReplayPlan, CDCReplayPlanner


@dataclass(frozen=True, slots=True)
class CDCReplayPlanRequest:
    backend: str
    pipeline_name: str
    source_schema: str
    source_table: str
    stored_offset: str | None = None
    replay_from: str = ""
    replay_to: str | None = None
    retention_min: str | None = None
    high_watermark: str | None = None
    artifact_uri: str | None = None
    allow_rewind: bool = False


class CDCReplayPlanService:
    """Build replay plans while keeping CLI commands runtime-free."""

    def __init__(self, *, planner: CDCReplayPlanner | None = None) -> None:
        self._planner = planner or CDCReplayPlanner()

    def plan(self, request: CDCReplayPlanRequest) -> CDCReplayPlan:
        backend = CDCBackend(request.backend)
        return self._planner.plan(
            backend=backend,
            pipeline_name=request.pipeline_name,
            source_schema=request.source_schema,
            source_table=request.source_table,
            stored_offset=self._offset(backend, request.stored_offset),
            replay_from=self._required_offset(backend, request.replay_from),
            replay_to=self._offset(backend, request.replay_to),
            retention_min=self._offset(backend, request.retention_min),
            high_watermark=self._offset(backend, request.high_watermark),
            artifact_uri=request.artifact_uri,
            allow_rewind=request.allow_rewind,
        )

    @staticmethod
    def _required_offset(backend: CDCBackend, token: str) -> CDCOffset:
        return CDCOffset(backend=backend, token=token, snapshot_complete=True)

    @staticmethod
    def _offset(backend: CDCBackend, token: str | None) -> CDCOffset | None:
        if token is None:
            return None
        return CDCOffset(backend=backend, token=token, snapshot_complete=True)
