"""Canonical manifest execution service for ``dpone run``."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from dpone.contracts.process_types import ProcessResult
from dpone.contracts.retry_policy import (
    validate_retry_attempts,
    validate_retry_backoff_seconds,
)
from dpone.contracts.run_context import RunContext
from dpone.dag.process import ETLProcess
from dpone.readiness.native_transfer_route_guard import NativeTransferRouteRunGuard
from dpone.services.manifest import ManifestCommandContext, resolve_single_process
from dpone.services.run_invocation_context import RunInvocationContextService as RunInvocationContextService

ProcessFactory = Callable[..., ETLProcess]
SleepFn = Callable[[float], None]


@dataclass(frozen=True, slots=True)
class RunManifestResult:
    manifest: str
    process: str
    selector: str | None
    run_id: str
    passed: bool
    result: ProcessResult
    attempts: int = 1
    max_attempts: int = 1
    retry_backoff_seconds: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest": self.manifest,
            "process": self.process,
            "selector": self.selector,
            "run_id": self.run_id,
            "passed": self.passed,
            "result": self.result.to_dict(),
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "retry_backoff_seconds": self.retry_backoff_seconds,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_text(self) -> str:
        return "\n".join(
            [
                "dpone run",
                f"- manifest: {self.manifest}",
                f"- process: {self.process}",
                f"- selector: {self.selector or ''}",
                f"- run_id: {self.run_id}",
                f"- status: {self.result.status}",
                f"- passed: {self.passed}",
                f"- attempts: {self.attempts}",
                f"- max_attempts: {self.max_attempts}",
                f"- retry_backoff_seconds: {self.retry_backoff_seconds}",
                f"- extracted_rows: {self.result.extracted_rows}",
                f"- inserted_rows: {self.result.inserted_rows}",
                f"- updated_rows: {self.result.updated_rows}",
                f"- final_rows: {self.result.final_rows}",
                f"- duration_seconds: {self.result.duration_seconds}",
                "",
            ]
        )

    def to_markdown(self) -> str:
        return "\n".join(
            [
                "# dpone run",
                "",
                f"- Manifest: `{self.manifest}`",
                f"- Process: `{self.process}`",
                f"- Selector: `{self.selector or ''}`",
                f"- Run ID: `{self.run_id}`",
                f"- Status: `{self.result.status}`",
                f"- Passed: `{self.passed}`",
                f"- Attempts: `{self.attempts}`",
                f"- Max attempts: `{self.max_attempts}`",
                f"- Retry backoff seconds: `{self.retry_backoff_seconds}`",
                "",
                "| metric | value |",
                "|---|---|",
                f"| `extracted_rows` | `{self.result.extracted_rows}` |",
                f"| `inserted_rows` | `{self.result.inserted_rows}` |",
                f"| `updated_rows` | `{self.result.updated_rows}` |",
                f"| `final_rows` | `{self.result.final_rows}` |",
                f"| `duration_seconds` | `{self.result.duration_seconds}` |",
                "",
            ]
        )


class RunManifestService:
    """Loads one manifest process and executes it through the ProcessRunner port."""

    def __init__(
        self,
        *,
        process_factory: ProcessFactory = ETLProcess,
        sleep: SleepFn = time.sleep,
        route_guard: NativeTransferRouteRunGuard | None = None,
        invocation_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._process_factory = process_factory
        self._sleep = sleep
        self._route_guard = route_guard or NativeTransferRouteRunGuard()
        self._invocation_id_factory = invocation_id_factory or (lambda: f"manual-{uuid4().hex}")

    def run(
        self,
        *,
        path: str | Path,
        manifest_ctx: ManifestCommandContext,
        selector: str | None = None,
        run_id: str | None = None,
        dag_id: str | None = None,
        execution_date: Any | None = None,
        retry_attempts: int = 0,
        retry_backoff_seconds: float = 0.0,
        load_config_mutator: Callable[[Any], Any] | None = None,
        run_context_config: Mapping[str, Any] | None = None,
        on_execution_started: Callable[[], None] | None = None,
    ) -> RunManifestResult:
        max_attempts = validate_retry_attempts(retry_attempts) + 1
        backoff_seconds = validate_retry_backoff_seconds(retry_backoff_seconds)
        manifest_path = Path(path)
        if manifest_path.is_file():
            metadata_manifest = manifest_ctx.loader.load(manifest_path, metadata_only=True)
            metadata_spec = resolve_single_process(metadata_manifest, selector=selector)
        else:
            metadata_spec = None
        effective_run_id = run_id or self._invocation_id_factory()
        blocked = (
            self._route_decision_blockers(metadata_spec, manifest_dir=manifest_path.parent)
            if metadata_spec
            else tuple()
        )
        if blocked and metadata_spec is not None:
            return RunManifestResult(
                manifest=str(manifest_path),
                process=metadata_spec.name,
                selector=metadata_spec.selector,
                run_id=effective_run_id,
                passed=False,
                result=ProcessResult(
                    status="error",
                    inserted_rows=0,
                    updated_rows=0,
                    final_rows=0,
                    extracted_rows=0,
                    duration_seconds=0.0,
                    errors=list(blocked),
                ),
                attempts=0,
                max_attempts=max_attempts,
                retry_backoff_seconds=backoff_seconds,
            )
        manifest = manifest_ctx.loader.load(manifest_path, metadata_only=False)
        spec = resolve_single_process(manifest, selector=selector)
        if load_config_mutator is not None:
            spec.config.load_config = load_config_mutator(spec.config.load_config)
        if on_execution_started is not None:
            on_execution_started()
        invocation_config = dict(run_context_config or {})
        invocation_config.setdefault("process", spec.name)
        invocation_config.setdefault("pipeline_id", spec.name)
        invocation_config.setdefault("task_id", spec.selector or spec.name)
        result: ProcessResult | None = None
        attempts = 0
        for attempt in range(1, max_attempts + 1):
            attempts = attempt
            process = self._process_factory(config=spec.config, config_path=str(spec.config_path))
            result = process.run(
                context=RunContext(run_id=effective_run_id, config=invocation_config),
                dag_id=dag_id,
                execution_date=execution_date,
            )
            if self._passed(result) or attempt == max_attempts:
                break
            if backoff_seconds > 0:
                self._sleep(backoff_seconds)
        assert result is not None
        return RunManifestResult(
            manifest=str(manifest_path),
            process=spec.name,
            selector=spec.selector,
            run_id=effective_run_id,
            passed=self._passed(result),
            result=result,
            attempts=attempts,
            max_attempts=max_attempts,
            retry_backoff_seconds=backoff_seconds,
        )

    @staticmethod
    def _passed(result: ProcessResult) -> bool:
        return result.status in {"success", "completed", "succeeded"} and not result.errors

    def _route_decision_blockers(self, spec: Any, *, manifest_dir: Path) -> tuple[str, ...]:
        return self._route_guard.blockers(raw_config=spec.raw_config, manifest_dir=manifest_dir)
