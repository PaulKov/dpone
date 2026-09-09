"""Operational recovery planning for runs, locks, and load packages."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.ids import utc_now_iso


@dataclass(frozen=True, slots=True)
class RecoveryAction:
    action: str
    reason: str
    command: str
    destructive: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RecoveryPlanReport:
    passed: bool
    generated_at: str
    failed_runs: int
    active_locks: int
    uncommitted_loads: int
    blockers: tuple[str, ...]
    actions: tuple[RecoveryAction, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "generated_at": self.generated_at,
            "failed_runs": self.failed_runs,
            "active_locks": self.active_locks,
            "uncommitted_loads": self.uncommitted_loads,
            "blockers": list(self.blockers),
            "actions": [item.to_dict() for item in self.actions],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone runtime recovery plan",
            "",
            f"- Passed: `{self.passed}`",
            f"- Failed/resumable runs: `{self.failed_runs}`",
            f"- Active locks: `{self.active_locks}`",
            f"- Uncommitted load packages: `{self.uncommitted_loads}`",
            "",
            "| action | destructive | reason | command |",
            "|---|---|---|---|",
        ]
        if self.actions:
            for action in self.actions:
                lines.append(f"| `{action.action}` | `{action.destructive}` | {action.reason} | `{action.command}` |")
        else:
            lines.append("| `none` | `False` | no recovery action required | `-` |")
        if self.blockers:
            lines.extend(["", "## Blockers", ""])
            lines.extend(f"- `{item}`" for item in self.blockers)
        lines.extend(
            [
                "",
                "## Operator runbook",
                "",
                "1. Prefer resume over restart when the previous job state is marked `resumable`.",
                "2. Inspect active locks before deleting them; a lock can represent a live process.",
                "3. Resolve staged load packages with target-native rollback or commit evidence before state replay.",
                "4. Re-run `dpone ops recovery-plan` after every manual recovery step.",
                "",
            ]
        )
        return "\n".join(lines)


class RuntimeRecoveryPlanner:
    """Builds a non-destructive recovery plan from local operational metadata."""

    _ACTIVE_PACKAGE_STATUSES = {"started", "staged"}

    def plan(
        self,
        *,
        state_dir: str | Path,
        lock_dir: str | Path,
        load_package_dir: str | Path,
        output_dir: str | Path,
    ) -> RecoveryPlanReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        failed_jobs = tuple(_json_files(Path(state_dir), "*.job_state.json"))
        locks = tuple(Path(lock_dir).glob("*.lock.json")) if Path(lock_dir).exists() else tuple()
        packages = tuple(_json_files(Path(load_package_dir), "*.json"))
        failed_runs = tuple(item for item in failed_jobs if _is_failed_resumable(item))
        active_packages = tuple(item for item in packages if str(item.get("status")) in self._ACTIVE_PACKAGE_STATUSES)
        actions = (
            *self._failed_run_actions(failed_runs),
            *self._lock_actions(locks),
            *self._package_actions(active_packages),
        )
        blockers = tuple(action.reason for action in actions)
        json_path = directory / "runtime_recovery_plan.json"
        markdown_path = directory / "runtime_recovery_plan.md"
        report = RecoveryPlanReport(
            passed=not blockers,
            generated_at=utc_now_iso(),
            failed_runs=len(failed_runs),
            active_locks=len(locks),
            uncommitted_loads=len(active_packages),
            blockers=blockers,
            actions=tuple(actions),
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _failed_run_actions(jobs: tuple[Mapping[str, Any], ...]) -> tuple[RecoveryAction, ...]:
        actions: list[RecoveryAction] = []
        for job in jobs:
            manifest = str(job.get("manifest_path") or job.get("manifest") or "manifest.yml")
            selector = str(job.get("selector") or "")
            run_id = str(job.get("run_id") or "unknown_run")
            selector_arg = f" --selector {selector}" if selector else ""
            actions.append(
                RecoveryAction(
                    action="dpone orchestrate run --resume-policy resume",
                    reason=f"failed resumable job `{run_id}`",
                    command=f"dpone orchestrate run --manifest {manifest}{selector_arg} --run-id {run_id} --resume-policy resume",
                )
            )
        return tuple(actions)

    @staticmethod
    def _lock_actions(locks: tuple[Path, ...]) -> tuple[RecoveryAction, ...]:
        return tuple(
            RecoveryAction(
                action="inspect_or_cleanup_lock",
                reason=f"active lock `{path.name}`",
                command=f"cat {path}",
                destructive=False,
            )
            for path in locks
        )

    @staticmethod
    def _package_actions(packages: tuple[Mapping[str, Any], ...]) -> tuple[RecoveryAction, ...]:
        actions: list[RecoveryAction] = []
        for package in packages:
            load_id = str(package.get("load_id") or "unknown_load")
            target = str(package.get("target") or "unknown_target")
            actions.append(
                RecoveryAction(
                    action="rollback_or_commit_load_package",
                    reason=f"load package `{load_id}` for `{target}` is `{package.get('status')}`",
                    command=f"dpone ops rollback-plan --sink <sink> --target {target} --load-id {load_id}",
                    destructive=False,
                )
            )
        return tuple(actions)


def _json_files(directory: Path, pattern: str) -> tuple[Mapping[str, Any], ...]:
    if not directory.exists():
        return tuple()
    payloads: list[Mapping[str, Any]] = []
    for path in sorted(directory.glob(pattern)):
        payload = _read_json(path)
        if payload:
            payloads.append(payload)
    return tuple(payloads)


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _is_failed_resumable(payload: Mapping[str, Any]) -> bool:
    return str(payload.get("status")) == "failed" and bool(payload.get("resumable", True))
