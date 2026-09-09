"""Route release-candidate executor over orchestration receipts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from dpone.ops.checksums import sha256_file
from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.rc_executor_models import (
    RouteRcExecutionArtifact,
    RouteRcExecutionReport,
    RouteRcExecutionStep,
)
from dpone.ops.routes.rc_executor_policy import RouteRcExecutionPolicy
from dpone.ops.routes.rc_executor_redaction import redact_text
from dpone.ops.routes.rc_executor_runner import CommandProcessResult, CommandProcessRunner, SubprocessCommandRunner

DEFAULT_COMMAND_TIMEOUT_SECONDS = 1800
DEFAULT_MAX_ATTEMPTS = 1
TAIL_LIMIT = 4000


class RouteReleaseCandidateExecutorService:
    """Execute an existing route RC command train with explicit opt-in."""

    def __init__(
        self,
        *,
        command_runner: CommandProcessRunner | None = None,
        policy: RouteRcExecutionPolicy | None = None,
    ) -> None:
        self._command_runner = command_runner or SubprocessCommandRunner()
        self._policy = policy or RouteRcExecutionPolicy()

    def execute(
        self,
        *,
        orchestration_json: str | Path,
        output_dir: str | Path,
        execute: bool = False,
        timeout_seconds: int = DEFAULT_COMMAND_TIMEOUT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        extra_redactions: Sequence[str] = (),
    ) -> RouteRcExecutionReport:
        orchestration_path = Path(orchestration_json)
        payload = json.loads(orchestration_path.read_text(encoding="utf-8"))
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = _route_from_payload(payload)
        mode = "execute" if execute else "dry_run"
        timeout = max(1, int(timeout_seconds))
        attempts = max(1, int(max_attempts))
        redactions = tuple(extra_redactions)
        step_payloads = _iter_mappings(payload.get("steps", ()))

        if execute:
            steps = self._execute_steps(
                step_payloads=step_payloads,
                cwd=orchestration_path.parent,
                timeout_seconds=timeout,
                max_attempts=attempts,
                extra_redactions=redactions,
            )
        else:
            steps = tuple(
                _planned_step(
                    step,
                    orchestration_dir=orchestration_path.parent,
                    timeout_seconds=timeout,
                    extra_redactions=redactions,
                )
                for step in step_payloads
            )
        artifacts = _collect_artifacts(
            payload=payload,
            orchestration_dir=orchestration_path.parent,
            execute=execute,
        )
        decision = self._policy.evaluate(
            mode=mode,
            orchestration_passed=bool(payload.get("passed", True)),
            orchestration_blockers=_string_tuple(payload.get("blockers", ())),
            steps=steps,
            artifacts=artifacts,
        )
        report = RouteRcExecutionReport(
            release=str(payload.get("release", "")),
            profile=str(payload.get("profile", "")),
            route=route,
            orchestration_json=str(orchestration_path),
            mode=mode,
            executed=execute,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            steps=steps,
            artifacts=artifacts,
            output_dir=str(directory),
            json_path=str(directory / "route_rc_execution.json"),
            markdown_path=str(directory / "route_rc_execution.md"),
        )
        report.write()
        return report

    def _execute_steps(
        self,
        *,
        step_payloads: Sequence[Mapping[str, object]],
        cwd: Path,
        timeout_seconds: int,
        max_attempts: int,
        extra_redactions: Sequence[str],
    ) -> tuple[RouteRcExecutionStep, ...]:
        executed_steps: list[RouteRcExecutionStep] = []
        blocked = False
        for step in step_payloads:
            if blocked:
                executed_steps.append(
                    _skipped_step(
                        step,
                        orchestration_dir=cwd,
                        timeout_seconds=timeout_seconds,
                        extra_redactions=extra_redactions,
                    )
                )
                continue
            result = self._run_step(
                step,
                cwd=cwd,
                timeout_seconds=timeout_seconds,
                max_attempts=max_attempts,
                extra_redactions=extra_redactions,
            )
            executed_steps.append(result)
            blocked = result.required and not result.passed
        return tuple(executed_steps)

    def _run_step(
        self,
        step: Mapping[str, object],
        *,
        cwd: Path,
        timeout_seconds: int,
        max_attempts: int,
        extra_redactions: Sequence[str],
    ) -> RouteRcExecutionStep:
        command = str(step.get("command", "")).strip()
        if not command:
            return _failed_step(
                step,
                status="failed",
                result=None,
                attempts=0,
                orchestration_dir=cwd,
                timeout_seconds=timeout_seconds,
                extra_redactions=extra_redactions,
                blockers=("command_missing",),
            )
        last_result: CommandProcessResult | None = None
        attempts_used = 0
        for _ in range(max_attempts):
            attempts_used += 1
            last_result = self._command_runner.run(command, cwd=cwd, timeout_seconds=timeout_seconds)
            if last_result.exit_code == 0 and not last_result.timed_out:
                return _result_step(
                    step,
                    status="passed",
                    result=last_result,
                    attempts=attempts_used,
                    orchestration_dir=cwd,
                    timeout_seconds=timeout_seconds,
                    extra_redactions=extra_redactions,
                )
        status = "timed_out" if last_result and last_result.timed_out else "failed"
        return _failed_step(
            step,
            status=status,
            result=last_result,
            attempts=attempts_used,
            orchestration_dir=cwd,
            timeout_seconds=timeout_seconds,
            extra_redactions=extra_redactions,
        )


def _route_from_payload(payload: Mapping[str, object]) -> RouteKey:
    route = _as_mapping(payload.get("route", {}))
    return RouteKey.of(str(route.get("source", "")), str(route.get("sink", "")), str(route.get("strategy", "")))


def _planned_step(
    step: Mapping[str, object],
    *,
    orchestration_dir: Path,
    timeout_seconds: int,
    extra_redactions: Sequence[str],
) -> RouteRcExecutionStep:
    path = _path_from_step(step, orchestration_dir)
    return RouteRcExecutionStep(
        name=_step_name(step),
        command=redact_text(str(step.get("command", "")), extra_secrets=extra_redactions),
        path=str(path),
        required=bool(step.get("required", True)),
        status="planned",
        passed=True,
        attempts=0,
        timeout_seconds=timeout_seconds,
        exit_code=None,
        timed_out=False,
        duration_seconds=0.0,
        stdout_tail="",
        stderr_tail="",
        sha256=sha256_file(path) if path.exists() else "0" * 64,
        artifact_exists=path.exists(),
        blockers=_blockers_from_step(step),
    )


def _skipped_step(
    step: Mapping[str, object],
    *,
    orchestration_dir: Path,
    timeout_seconds: int,
    extra_redactions: Sequence[str],
) -> RouteRcExecutionStep:
    path = _path_from_step(step, orchestration_dir)
    return RouteRcExecutionStep(
        name=_step_name(step),
        command=redact_text(str(step.get("command", "")), extra_secrets=extra_redactions),
        path=str(path),
        required=bool(step.get("required", True)),
        status="skipped",
        passed=False,
        attempts=0,
        timeout_seconds=timeout_seconds,
        exit_code=None,
        timed_out=False,
        duration_seconds=0.0,
        stdout_tail="",
        stderr_tail="",
        sha256=sha256_file(path) if path.exists() else "0" * 64,
        artifact_exists=path.exists(),
        blockers=_blockers_from_step(step),
    )


def _result_step(
    step: Mapping[str, object],
    *,
    status: str,
    result: CommandProcessResult,
    attempts: int,
    orchestration_dir: Path,
    timeout_seconds: int,
    extra_redactions: Sequence[str],
    extra_blockers: Sequence[str] = (),
) -> RouteRcExecutionStep:
    path = _path_from_step(step, orchestration_dir)
    return RouteRcExecutionStep(
        name=_step_name(step),
        command=redact_text(str(step.get("command", "")), extra_secrets=extra_redactions),
        path=str(path),
        required=bool(step.get("required", True)),
        status=status,
        passed=status == "passed",
        attempts=attempts,
        timeout_seconds=timeout_seconds,
        exit_code=result.exit_code,
        timed_out=result.timed_out,
        duration_seconds=result.duration_seconds,
        stdout_tail=_tail(redact_text(result.stdout, extra_secrets=extra_redactions)),
        stderr_tail=_tail(redact_text(result.stderr, extra_secrets=extra_redactions)),
        sha256=sha256_file(path) if path.exists() else "0" * 64,
        artifact_exists=path.exists(),
        blockers=(*_blockers_from_step(step), *_string_tuple(extra_blockers)),
    )


def _failed_step(
    step: Mapping[str, object],
    *,
    status: str,
    result: CommandProcessResult | None,
    attempts: int,
    orchestration_dir: Path,
    timeout_seconds: int,
    extra_redactions: Sequence[str],
    blockers: Sequence[str] = (),
) -> RouteRcExecutionStep:
    fallback = result or CommandProcessResult(exit_code=1)
    return _result_step(
        step,
        status=status,
        result=fallback,
        attempts=attempts,
        orchestration_dir=orchestration_dir,
        timeout_seconds=timeout_seconds,
        extra_redactions=extra_redactions,
        extra_blockers=blockers,
    )


def _collect_artifacts(
    *,
    payload: Mapping[str, object],
    orchestration_dir: Path,
    execute: bool,
) -> tuple[RouteRcExecutionArtifact, ...]:
    expected: dict[str, Path] = {}
    artifact_index = _as_mapping(payload.get("artifact_index", {}))
    for name, raw_path in artifact_index.items():
        expected[str(name)] = _resolve_path(raw_path, orchestration_dir)
    for step in _iter_mappings(payload.get("steps", ())):
        name = _step_name(step)
        expected.setdefault(name, _resolve_path(step.get("path", ""), orchestration_dir))
    artifacts: list[RouteRcExecutionArtifact] = []
    for name, path in expected.items():
        exists = path.exists()
        status = "present" if exists else ("missing" if execute else "planned")
        artifacts.append(
            RouteRcExecutionArtifact(
                name=name,
                path=str(path),
                required=True,
                exists=exists,
                sha256=sha256_file(path) if exists else "0" * 64,
                status=status,
                summary="artifact collected" if exists else "artifact expected after execution",
            )
        )
    return tuple(artifacts)


def _path_from_step(step: Mapping[str, object], base: Path) -> Path:
    return _resolve_path(step.get("path", ""), base)


def _resolve_path(value: object, base: Path) -> Path:
    path = Path(str(value))
    return path if path.is_absolute() else base / path


def _step_name(step: Mapping[str, object]) -> str:
    return str(step.get("name", "route_rc_step"))


def _as_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _iter_mappings(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(_as_mapping(item) for item in value)
    return tuple()


def _blockers_from_step(step: Mapping[str, object]) -> tuple[str, ...]:
    return _string_tuple(step.get("blockers", ()))


def _string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value else tuple()
    if isinstance(value, Sequence) and not isinstance(value, bytes):
        return tuple(str(item) for item in value)
    return tuple()


def _tail(value: str) -> str:
    return value[-TAIL_LIMIT:]


__all__ = [
    "DEFAULT_COMMAND_TIMEOUT_SECONDS",
    "DEFAULT_MAX_ATTEMPTS",
    "RouteReleaseCandidateExecutorService",
]
