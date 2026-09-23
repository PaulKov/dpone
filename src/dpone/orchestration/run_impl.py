"""Orchestrated execution wrapper around canonical ``dpone run``."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.orchestration.handoff import SchedulerHandoffBuilder
from dpone.orchestration.locks import LocalRunLockManager
from dpone.orchestration.resume import ResumePolicyPlanner
from dpone.orchestration.state import JobStateStore, NoopJobStateStore, OrchestrationJobState
from dpone.services.run_manifest import RunManifestResult

RunExecutor = Callable[["OrchestratedRunRequest"], RunManifestResult]


@dataclass(frozen=True, slots=True)
class OrchestratedRunRequest:
    manifest_path: str | Path
    selector: str | None = None
    run_id: str | None = None
    dag_id: str | None = None
    execution_date: Any | None = None
    retry_attempts: int = 0
    retry_backoff_seconds: float = 0.0
    concurrency_key: str | None = None
    lock_ttl_seconds: int = 3600
    resume_policy: str = "fail"

    @property
    def effective_concurrency_key(self) -> str:
        return self.concurrency_key or self.run_id or self.selector or str(self.manifest_path)


@dataclass(frozen=True, slots=True)
class OrchestratedRunReport:
    passed: bool
    status: str
    run_id: str
    process: str
    lock_key: str
    lock_acquired: bool
    stale_lock_replaced: bool
    blockers: tuple[str, ...]
    run_result: Mapping[str, object]
    scheduler_handoff: Mapping[str, str]
    job_state: Mapping[str, object]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "status": self.status,
            "run_id": self.run_id,
            "process": self.process,
            "lock_key": self.lock_key,
            "lock_acquired": self.lock_acquired,
            "stale_lock_replaced": self.stale_lock_replaced,
            "blockers": list(self.blockers),
            "run_result": dict(self.run_result),
            "scheduler_handoff": dict(self.scheduler_handoff),
            "job_state": dict(self.job_state),
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone orchestrated run",
            "",
            f"- Passed: `{self.passed}`",
            f"- Status: `{self.status}`",
            f"- Run ID: `{self.run_id}`",
            f"- Process: `{self.process}`",
            f"- Lock key: `{self.lock_key}`",
            f"- Lock acquired: `{self.lock_acquired}`",
            f"- Stale lock replaced: `{self.stale_lock_replaced}`",
            f"- Job state: `{self.job_state.get('status', '')}`",
        ]
        if self.blockers:
            lines.extend(["", "Blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Scheduler handoff",
                "",
                "```bash",
                str(self.scheduler_handoff.get("cron_command", "")),
                "```",
                "",
                "## Runbook",
                "",
                "1. If lock acquisition fails, inspect the active lock and running scheduler job.",
                "2. If retry is exhausted, inspect the underlying `run_result` errors.",
                "3. Attach `orchestrated_run.json` to run registry, lineage, or release evidence.",
                "",
            ]
        )
        return "\n".join(lines)


class OrchestratedRunService:
    """Runs a canonical dpone process under lock and writes orchestration evidence."""

    def __init__(
        self,
        *,
        lock_manager: LocalRunLockManager,
        handoff_builder: SchedulerHandoffBuilder,
        run_executor: RunExecutor,
        job_state_store: JobStateStore | None = None,
        resume_policy_planner: ResumePolicyPlanner | None = None,
    ) -> None:
        self._lock_manager = lock_manager
        self._handoff_builder = handoff_builder
        self._run_executor = run_executor
        self._job_state_store = job_state_store or NoopJobStateStore()
        self._resume_policy_planner = resume_policy_planner or ResumePolicyPlanner()

    def run(self, *, output_dir: str | Path, request: OrchestratedRunRequest) -> OrchestratedRunReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        handoff = self._handoff_builder.build(
            manifest_path=str(request.manifest_path),
            selector=request.selector,
            run_id=request.run_id,
            retry_attempts=request.retry_attempts,
            retry_backoff_seconds=request.retry_backoff_seconds,
            concurrency_key=request.concurrency_key,
            lock_dir=str(self._lock_manager.lock_dir),
            state_dir=getattr(self._job_state_store, "state_dir", None),
            output_dir=str(directory),
            lock_ttl_seconds=request.lock_ttl_seconds,
            resume_policy=request.resume_policy,
        )
        state_run_id = request.run_id or request.effective_concurrency_key
        previous_state = self._job_state_store.get(state_run_id)
        resume_decision = self._resume_policy_planner.decide(
            previous=previous_state,
            policy=request.resume_policy,
        )
        if not resume_decision.allowed:
            job_state = self._job_state_store.mark_blocked(run_id=state_run_id, blockers=resume_decision.blockers)
            report = self._preflight_blocked_report(
                directory=directory,
                request=request,
                blockers=resume_decision.blockers,
                handoff=handoff.to_dict(),
            )
            self._write(directory, self._with_job_state(report, job_state))
            return self._with_job_state(report, job_state)
        lock = self._lock_manager.acquire(
            request.effective_concurrency_key,
            ttl_seconds=request.lock_ttl_seconds,
        )
        if not lock.acquired:
            job_state = self._job_state_store.mark_started(
                run_id=state_run_id,
                process=request.selector or "",
                manifest_path=request.manifest_path,
                selector=request.selector,
                lock_key=request.effective_concurrency_key,
                output_dir=directory,
            )
            job_state = self._job_state_store.mark_blocked(
                run_id=job_state.run_id,
                blockers=(lock.blocker or "lock.not_acquired",),
            )
            report = self._blocked_report(directory=directory, request=request, lock=lock, handoff=handoff.to_dict())
            report = self._with_job_state(report, job_state)
            self._write(directory, report)
            return report
        try:
            job_state = self._job_state_store.mark_started(
                run_id=state_run_id,
                process=request.selector or "",
                manifest_path=request.manifest_path,
                selector=request.selector,
                lock_key=request.effective_concurrency_key,
                output_dir=directory,
            )
            self._job_state_store.mark_running(run_id=job_state.run_id, attempt=1)
            result = self._run_executor(request)
            blockers = tuple() if result.passed else ("run.not_passed",)
            final_state = (
                self._job_state_store.mark_committed(run_id=job_state.run_id, run_result=result.to_dict())
                if result.passed
                else self._job_state_store.mark_failed(
                    run_id=job_state.run_id,
                    blockers=blockers,
                    run_result=result.to_dict(),
                )
            )
            report = self._report(
                directory=directory, request=request, result=result, lock=lock, handoff=handoff.to_dict()
            )
            report = self._with_job_state(report, final_state)
            self._write(directory, report)
            return report
        finally:
            self._lock_manager.release(lock)

    def _blocked_report(
        self,
        *,
        directory: Path,
        request: OrchestratedRunRequest,
        lock,
        handoff: Mapping[str, str],
    ) -> OrchestratedRunReport:
        return OrchestratedRunReport(
            passed=False,
            status="blocked",
            run_id=request.run_id or "",
            process=request.selector or "",
            lock_key=request.effective_concurrency_key,
            lock_acquired=False,
            stale_lock_replaced=lock.stale_replaced,
            blockers=(lock.blocker or "lock.not_acquired",),
            run_result={},
            scheduler_handoff=handoff,
            job_state={},
            output_dir=str(directory),
        )

    def _preflight_blocked_report(
        self,
        *,
        directory: Path,
        request: OrchestratedRunRequest,
        blockers: tuple[str, ...],
        handoff: Mapping[str, str],
    ) -> OrchestratedRunReport:
        return OrchestratedRunReport(
            passed=False,
            status="blocked",
            run_id=request.run_id or "",
            process=request.selector or "",
            lock_key=request.effective_concurrency_key,
            lock_acquired=False,
            stale_lock_replaced=False,
            blockers=blockers,
            run_result={},
            scheduler_handoff=handoff,
            job_state={},
            output_dir=str(directory),
        )

    def _report(
        self,
        *,
        directory: Path,
        request: OrchestratedRunRequest,
        result: RunManifestResult,
        lock,
        handoff: Mapping[str, str],
    ) -> OrchestratedRunReport:
        return OrchestratedRunReport(
            passed=result.passed,
            status=str(result.result.status),
            run_id=result.run_id,
            process=result.process,
            lock_key=request.effective_concurrency_key,
            lock_acquired=True,
            stale_lock_replaced=lock.stale_replaced,
            blockers=tuple() if result.passed else ("run.not_passed",),
            run_result=result.to_dict(),
            scheduler_handoff=handoff,
            job_state={},
            output_dir=str(directory),
        )

    @staticmethod
    def _with_job_state(report: OrchestratedRunReport, state: OrchestrationJobState) -> OrchestratedRunReport:
        return OrchestratedRunReport(
            passed=report.passed,
            status=report.status,
            run_id=report.run_id,
            process=report.process,
            lock_key=report.lock_key,
            lock_acquired=report.lock_acquired,
            stale_lock_replaced=report.stale_lock_replaced,
            blockers=report.blockers,
            run_result=report.run_result,
            scheduler_handoff=report.scheduler_handoff,
            job_state=state.to_dict(),
            output_dir=report.output_dir,
        )

    @staticmethod
    def _write(directory: Path, report: OrchestratedRunReport) -> None:
        (directory / "orchestrated_run.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "orchestrated_run.md").write_text(report.to_markdown(), encoding="utf-8")
