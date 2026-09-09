"""Generic CDC retention-gap policy and resync planner."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.retention_models import (
    CdcResyncAction,
    CdcResyncPlan,
    CdcResyncPlanReport,
    CdcRetentionBounds,
    CdcRetentionDecision,
    CdcRetentionReport,
    load_retention_report,
    resync_row_key,
)


class CdcRetentionProbe(Protocol):
    """Read the retained CDC interval for a source stream."""

    def read_bounds(self) -> CdcRetentionBounds:
        """Return source-side retention bounds."""


class CdcRetentionPolicy:
    """Default fail-closed retention gap policy."""

    def __init__(self, *, at_risk_margin: int = 1000) -> None:
        self._at_risk_margin = max(0, at_risk_margin)

    def decide(
        self,
        *,
        stream: Any,
        committed_offset: CDCOffset | None,
        bounds: CdcRetentionBounds,
    ) -> CdcRetentionDecision:
        metrics: dict[str, object] = {
            "backend": bounds.backend.value,
            "at_risk_margin": self._at_risk_margin,
            "min_available_offset": bounds.min_available_offset,
            "high_watermark": bounds.high_watermark,
            "current_offset": bounds.current_offset,
            "retention_seconds": bounds.retention_seconds,
        }
        if bounds.backend != stream.backend:
            return CdcRetentionDecision(
                level="gap_detected",
                passed=False,
                blockers=("cdc_retention.backend_mismatch",),
                warnings=tuple(),
                metrics={**metrics, "stream_backend": stream.backend.value},
                next_actions=("verify stream backend configuration before resync",),
            )
        if committed_offset is None:
            return CdcRetentionDecision(
                level="healthy",
                passed=True,
                blockers=tuple(),
                warnings=("cdc_retention.no_committed_offset",),
                metrics={**metrics, "offset_margin": None},
                next_actions=("start initial snapshot before CDC apply",),
            )
        if committed_offset.backend != bounds.backend:
            return CdcRetentionDecision(
                level="gap_detected",
                passed=False,
                blockers=("cdc_retention.offset_backend_mismatch",),
                warnings=tuple(),
                metrics={
                    **metrics,
                    "committed_backend": committed_offset.backend.value,
                    "committed_offset": committed_offset.token,
                },
                next_actions=("rebuild checkpoint using the configured CDC backend",),
            )

        margin = _offset_margin(
            backend=bounds.backend,
            committed=committed_offset.token,
            min_available=bounds.min_available_offset,
        )
        high_watermark_lag = _offset_margin(
            backend=bounds.backend,
            committed=committed_offset.token,
            min_available=bounds.high_watermark,
        )
        metrics.update(
            {
                "committed_offset": committed_offset.token,
                "offset_margin": margin,
                "high_watermark_lag": abs(high_watermark_lag),
            }
        )
        if margin < 0:
            return CdcRetentionDecision(
                level="gap_detected",
                passed=False,
                blockers=("cdc_retention.offset_before_min_available",),
                warnings=tuple(),
                metrics=metrics,
                next_actions=("run cdc-resync-plan and cdc-resync-execute before resuming CDC",),
            )
        if margin < self._at_risk_margin:
            return CdcRetentionDecision(
                level="at_risk",
                passed=True,
                blockers=tuple(),
                warnings=("cdc_retention.offset_near_min_available",),
                metrics=metrics,
                next_actions=("increase retention or run CDC before cleanup reaches the committed offset",),
            )
        return CdcRetentionDecision(
            level="healthy",
            passed=True,
            blockers=tuple(),
            warnings=tuple(),
            metrics=metrics,
            next_actions=tuple(),
        )


class CdcRetentionGapService:
    """Orchestrate probe reading, policy evaluation, and report writing."""

    def __init__(self, *, policy: CdcRetentionPolicy | None = None) -> None:
        self._policy = policy or CdcRetentionPolicy()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        stream: Any,
        committed_offset: CDCOffset | None,
        probe: CdcRetentionProbe,
    ) -> CdcRetentionReport:
        directory = Path(output_dir)
        bounds = probe.read_bounds()
        decision = self._policy.decide(stream=stream, committed_offset=committed_offset, bounds=bounds)
        return CdcRetentionReport(
            stream=stream,
            committed_offset=committed_offset,
            bounds=bounds,
            decision=decision,
            output_dir=str(directory),
            json_path=str(directory / "cdc_retention_check.json"),
            markdown_path=str(directory / "cdc_retention_check.md"),
        ).write()


class CdcResyncPlanner:
    """Build a bounded snapshot resync plan from retention-gap evidence."""

    def plan(
        self,
        *,
        output_dir: str | Path,
        retention_report: CdcRetentionReport,
        rows_json: str | Path,
        max_rows: int | None = None,
    ) -> CdcResyncPlanReport:
        directory = Path(output_dir)
        actions_path = directory / "cdc_resync_actions.json"
        report_path = directory / "cdc_resync_plan.json"
        markdown_path = directory / "cdc_resync_plan.md"
        rows = _rows_from_json(rows_json)
        bounded_rows = rows[:max_rows] if max_rows is not None else rows
        if retention_report.decision.level != "gap_detected":
            actions: tuple[CdcResyncAction, ...] = tuple()
            warnings = ("cdc_resync.no_retention_gap",)
        else:
            actions = tuple(
                CdcResyncAction(
                    action_id=f"resync:{retention_report.stream.stream_id}:{index}",
                    operation="upsert",
                    key=resync_row_key(unique_key=retention_report.stream.unique_key, payload=row),
                    payload=row,
                    reason="cdc_retention_gap",
                )
                for index, row in enumerate(bounded_rows, 1)
            )
            warnings = tuple()
        plan = CdcResyncPlan(
            stream=retention_report.stream,
            actions=actions,
            reason=retention_report.decision.level,
            retention_report_json=retention_report.json_path or None,
        ).write(actions_path)
        return CdcResyncPlanReport(
            stream=retention_report.stream,
            retention_level=retention_report.decision.level,
            plan=plan,
            passed=True,
            blockers=tuple(),
            warnings=warnings,
            metrics={
                "source_rows": len(rows),
                "planned_rows": len(actions),
                "max_rows": max_rows,
                "plan_path": str(actions_path),
            },
            output_dir=str(directory),
            json_path=str(report_path),
            markdown_path=str(markdown_path),
        ).write()

    def plan_from_file(
        self,
        *,
        output_dir: str | Path,
        stream: Any,
        retention_report_json: str | Path,
        rows_json: str | Path,
        max_rows: int | None = None,
    ) -> CdcResyncPlanReport:
        return self.plan(
            output_dir=output_dir,
            retention_report=load_retention_report(retention_report_json, stream=stream),
            rows_json=rows_json,
            max_rows=max_rows,
        )


def _rows_from_json(path: str | Path) -> tuple[Mapping[str, object], ...]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("rows") if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list | tuple):
        raise ValueError("CDC resync row JSON must be a list or an object with rows")
    return tuple(_mapping(row) for row in rows)


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC resync rows must be objects")


def _offset_margin(*, backend: CDCBackend, committed: str, min_available: str) -> int:
    return _offset_value(backend, committed) - _offset_value(backend, min_available)


def _offset_value(backend: CDCBackend, value: str) -> int:
    token = value.strip()
    if backend == CDCBackend.MSSQL_CDC:
        return int(token[2:] if token.lower().startswith("0x") else token, 16)
    return int(token)


__all__ = [
    "CdcResyncPlanner",
    "CdcRetentionGapService",
    "CdcRetentionPolicy",
    "CdcRetentionProbe",
]
