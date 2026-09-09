"""Select one shell-free command from a verified Airflow pack."""

from __future__ import annotations

import json
import shlex
from collections.abc import Mapping
from typing import Any

from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import (
    RuntimeExecutionSelection,
    RuntimeInitFetchPlan,
)
from dpone.runtime.verified_pack_hook_policy import (
    exact_verified_process_plan,
    legacy_verified_process_plan,
    verified_runtime_environment,
)

_WORKLOAD_BOOTSTRAP_KEY = "__workload__"
_DEFAULT_PROCESS_BOOTSTRAP_KEY = "__default_process__"


def selected_verified_command(
    pack: Mapping[str, Any],
    plan: RuntimeInitFetchPlan,
) -> tuple[tuple[str, ...], Mapping[str, str]]:
    """Return the command selected by the immutable execution scope."""

    legacy_process_plan: Mapping[str, Any] | None = None
    if plan.execution.hook_execution is None:
        legacy_process_plan = legacy_verified_process_plan(
            pack,
            workload_selector=plan.execution.selector,
        )
    if plan.execution.kind == "runtime":
        bootstrap = _mapping(pack.get("runtime_bootstrap"), "runtime_bootstrap")
        if bootstrap.get("schema") != "dpone.airflow-runtime-bootstrap.v1":
            raise _error("runtime bootstrap schema is invalid")
        commands = _mapping(
            bootstrap.get("commands"),
            "runtime_bootstrap.commands",
        )
        legacy_process_selector: str | None = None
        if legacy_process_plan is not None:
            raw_selector = legacy_process_plan.get("selector")
            if raw_selector is not None and not isinstance(raw_selector, str):
                raise _error("verified runtime process selector is invalid")
            legacy_process_selector = raw_selector
        command, selector = _runtime_bootstrap_command(
            commands,
            execution=plan.execution,
            legacy_process_selector=legacy_process_selector,
        )
        if set(command) != {"argv", "env"}:
            raise _error("runtime bootstrap command contains unknown or missing fields")
        return (
            _runtime_argv(command.get("argv"), selector=selector),
            verified_runtime_environment(
                pack,
                execution=plan.execution,
                value=command.get("env"),
            ),
        )
    return _pre_hook_argv(pack, plan.execution), {}


def _runtime_bootstrap_command(
    commands: Mapping[str, Any],
    *,
    execution: RuntimeExecutionSelection,
    legacy_process_selector: str | None,
) -> tuple[Mapping[str, Any], str | None]:
    if execution.hook_execution is not None:
        selector = execution.process_selector if execution.scope == "process" else None
        command_key = (
            selector or _DEFAULT_PROCESS_BOOTSTRAP_KEY if execution.scope == "process" else _WORKLOAD_BOOTSTRAP_KEY
        )
        raw = commands.get(command_key)
        if not isinstance(raw, Mapping):
            raise _error("runtime bootstrap command does not match execution scope")
        return raw, selector
    return _legacy_runtime_bootstrap_command(
        commands,
        process_selector=legacy_process_selector,
    )


def _legacy_runtime_bootstrap_command(
    commands: Mapping[str, Any],
    *,
    process_selector: object,
) -> tuple[Mapping[str, Any], str | None]:
    if process_selector is not None and not isinstance(process_selector, str):
        raise _error("verified runtime process selector is invalid")
    command_key = process_selector or _DEFAULT_PROCESS_BOOTSTRAP_KEY
    raw = commands.get(command_key)
    if isinstance(raw, Mapping):
        return raw, process_selector
    if raw is not None:
        raise _error("runtime bootstrap command must be an object")
    fallback = commands.get("__default__") if process_selector is None else None
    if isinstance(fallback, Mapping):
        return fallback, None
    raise _error("runtime bootstrap command must be an object")


def _runtime_argv(
    value: object,
    *,
    selector: str | None,
) -> tuple[str, ...]:
    argv = _argv(value)
    control = ("--format", "json") if selector is None else ("--format", "json", "--selector", selector)
    if len(argv) == 3 + len(control) and argv[:2] == ("dpone", "run") and argv[3:] == control:
        return argv
    if len(argv) == 6 and argv[:3] == ("dpone", "dbt", "execute-pack") and argv[4:] == ("--format", "json"):
        return argv
    raise _error("verified runtime command has an unsupported shape")


def _pre_hook_argv(
    pack: Mapping[str, Any],
    execution: RuntimeExecutionSelection,
) -> tuple[str, ...]:
    hook_name = execution.hook_name
    if hook_name is None:
        raise _error("verified pre-hook command is missing")
    matches = _matching_pre_hook_steps(pack, execution=execution, hook_name=hook_name)
    if len(matches) != 1:
        raise _error("verified pre-hook command is ambiguous or missing")
    if execution.hook_execution is not None:
        return _structured_pre_hook_argv(matches[0], execution=execution)
    if isinstance(matches[0].get("runtime_command"), Mapping):
        return _legacy_structured_pre_hook_argv(matches[0])
    return _legacy_pre_hook_argv(matches[0], hook_name=hook_name)


def _matching_pre_hook_steps(
    pack: Mapping[str, Any],
    *,
    execution: RuntimeExecutionSelection,
    hook_name: str,
) -> list[Mapping[str, Any]]:
    if execution.hook_execution is not None and execution.scope == "process":
        inventories = [
            exact_verified_process_plan(
                pack,
                selector=execution.process_selector,
            ).get("steps")
        ]
    elif execution.hook_execution is not None:
        inventories = [pack.get("steps")]
    else:
        inventories = _legacy_step_inventories(pack)
    unique: dict[str, Mapping[str, Any]] = {}
    for raw_steps in inventories:
        if not isinstance(raw_steps, list):
            continue
        for step in raw_steps:
            if isinstance(step, Mapping) and step.get("name") == hook_name and step.get("phase") == "pre_hook":
                unique.setdefault(_hook_step_identity(step), step)
    return list(unique.values())


def _legacy_step_inventories(pack: Mapping[str, Any]) -> list[object]:
    inventories: list[object] = [pack.get("steps")]
    raw_plans = pack.get("process_plans")
    if isinstance(raw_plans, Mapping):
        inventories.extend(plan.get("steps") for plan in raw_plans.values() if isinstance(plan, Mapping))
    return inventories


def _hook_step_identity(step: Mapping[str, Any]) -> str:
    runtime_command = step.get("runtime_command")
    if isinstance(runtime_command, Mapping):
        try:
            return (
                "structured:"
                + json.dumps(
                    dict(runtime_command),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + f"|legacy:{step.get('command')!r}"
            )
        except (TypeError, ValueError):
            return "structured:<invalid>"
    return f"legacy:{step.get('command')!r}"


def _structured_pre_hook_argv(
    step: Mapping[str, Any],
    *,
    execution: RuntimeExecutionSelection,
) -> tuple[str, ...]:
    parsed, process_selector = _validated_structured_pre_hook(step)
    if process_selector != execution.process_selector:
        raise _error("verified pre-hook process selector is inconsistent")
    return parsed


def _legacy_structured_pre_hook_argv(
    step: Mapping[str, Any],
) -> tuple[str, ...]:
    parsed, _ = _validated_structured_pre_hook(step)
    command = step.get("command")
    if not isinstance(command, str):
        raise _error("verified pre-hook command is ambiguous or missing")
    try:
        legacy_argv = _argv(tuple(shlex.split(command, posix=True)))
    except ValueError as exc:
        raise _error("verified pre-hook command cannot be tokenized") from exc
    if legacy_argv != parsed:
        raise _error("legacy and structured pre-hook commands are inconsistent")
    return parsed


def _validated_structured_pre_hook(
    step: Mapping[str, Any],
) -> tuple[tuple[str, ...], str | None]:
    command = _mapping(step.get("runtime_command"), "pre_hook.runtime_command")
    if set(command) != {"schema", "hook_id", "process_selector", "argv"}:
        raise _error("verified pre-hook runtime command contains unknown or missing fields")
    if command.get("schema") != "dpone.airflow-pre-hook-command.v1":
        raise _error("verified pre-hook runtime command schema is invalid")
    hook_id = command.get("hook_id")
    if not isinstance(hook_id, str) or not hook_id:
        raise _error("verified pre-hook hook id is invalid")
    process_selector = command.get("process_selector")
    if process_selector is not None and (not isinstance(process_selector, str) or not process_selector):
        raise _error("verified pre-hook process selector is invalid")
    parsed = _argv(command.get("argv"))
    expected = (
        "dpone",
        "hooks",
        "execute",
        parsed[3] if len(parsed) > 3 else "",
        "--phase",
        "pre_hook",
        "--hook-id",
        hook_id,
        *(("--selector", process_selector) if process_selector is not None else ()),
    )
    if parsed != expected:
        raise _error("verified pre-hook runtime command has an unsupported shape")
    return parsed, process_selector


def _legacy_pre_hook_argv(
    step: Mapping[str, Any],
    *,
    hook_name: str,
) -> tuple[str, ...]:
    command = step.get("command")
    if not isinstance(command, str):
        raise _error("verified pre-hook command is ambiguous or missing")
    try:
        parsed = _argv(tuple(shlex.split(command, posix=True)))
    except ValueError as exc:
        raise _error("verified pre-hook command cannot be tokenized") from exc
    expected = ("--phase", "pre_hook", "--hook-id", hook_name)
    if len(parsed) != 8 or parsed[:3] != ("dpone", "hooks", "execute") or parsed[4:] != expected:
        raise _error("verified pre-hook command has an unsupported shape")
    return parsed


def _argv(value: object) -> tuple[str, ...]:
    if not isinstance(value, list | tuple) or not value:
        raise _error("verified workload argv must be a non-empty list")
    argv = tuple(value)
    if any(
        not isinstance(item, str) or not item or any(control in item for control in ("\x00", "\n", "\r"))
        for item in argv
    ):
        raise _error("verified workload argv contains an unsafe value")
    return argv


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _error(f"{field} must be an object")
    return value


def _error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)


__all__ = ["selected_verified_command"]
