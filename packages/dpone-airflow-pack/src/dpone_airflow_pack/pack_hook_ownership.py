"""Prove that whole-workload Airflow tasks own every externalized hook."""

from __future__ import annotations

import shlex
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError

_HOOK_COMMAND_SCHEMA = "dpone.airflow-pre-hook-command.v1"
_DEFAULT_PROCESS_PLAN_KEY = "__default__"
_UNSPECIFIED_SELECTOR = object()


@dataclass(frozen=True, slots=True)
class PackHookDescriptor:
    """Canonical executable and graph identity for one pack pre-hook."""

    name: str
    hook_id: str
    process_selector: str | None
    argv: tuple[str, ...]
    depends_on: tuple[str, ...]
    required: bool
    credential_required: bool
    produces: tuple[str, ...]
    reason: str


def require_complete_workload_hook_ownership(pack: Mapping[str, Any]) -> None:
    """Fail before DAG installation when process hooks have no Airflow task."""

    externalized = _hook_descriptors(pack.get("steps"))
    duplicate_names = sorted(
        name for name, count in Counter(descriptor.name for descriptor in externalized).items() if count > 1
    )
    if duplicate_names:
        raise _ownership_error("whole-workload externalized hook names are not unique: " + ", ".join(duplicate_names))
    required = _process_hook_descriptors(pack.get("process_plans"))
    if not required:
        return
    missing = Counter(required) - Counter(externalized)
    if not missing:
        return
    names = ", ".join(
        sorted(f"{descriptor.name}[{descriptor.process_selector or 'workload'}]" for descriptor in missing)
    )
    raise _ownership_error(f"whole-workload hook ownership is incomplete: {names}")


def pre_hook_descriptor(
    step: Mapping[str, Any],
    *,
    fallback_selector: str | None | object = _UNSPECIFIED_SELECTOR,
) -> PackHookDescriptor:
    """Return the exact provider-side execution descriptor for one hook."""

    name = _text(step.get("name"), "pre-hook name")
    command = _text(step.get("command"), f"{name} command")
    runtime_command = _mapping(step.get("runtime_command"), f"{name} runtime_command")
    if set(runtime_command) != {"schema", "hook_id", "process_selector", "argv"}:
        raise _ownership_error(f"{name} runtime_command contains unknown or missing fields")
    if runtime_command.get("schema") != _HOOK_COMMAND_SCHEMA:
        raise _ownership_error(f"{name} runtime_command schema is invalid")
    hook_id = _text(runtime_command.get("hook_id"), f"{name} hook_id")
    process_selector = runtime_command.get("process_selector")
    if process_selector is not None:
        process_selector = _text(process_selector, f"{name} process_selector")
    if fallback_selector is not _UNSPECIFIED_SELECTOR and process_selector != fallback_selector:
        raise _ownership_error(f"{name} process selector is inconsistent")
    argv = _texts(runtime_command.get("argv"), f"{name} argv")
    expected = (
        "dpone",
        "hooks",
        "execute",
        argv[3] if len(argv) > 3 else "",
        "--phase",
        "pre_hook",
        "--hook-id",
        hook_id,
        *(("--selector", process_selector) if process_selector is not None else ()),
    )
    if argv != expected:
        raise _ownership_error(f"{name} runtime command has an unsupported shape")
    try:
        display_argv = tuple(shlex.split(command, posix=True))
    except ValueError as exc:
        raise _ownership_error(f"{name} command cannot be tokenized") from exc
    if display_argv != argv:
        raise _ownership_error(f"{name} command differs from runtime argv")
    return PackHookDescriptor(
        name=name,
        hook_id=hook_id,
        process_selector=process_selector,
        argv=argv,
        depends_on=_texts(step.get("depends_on", []), f"{name} depends_on"),
        required=_boolean(step.get("required"), f"{name} required"),
        credential_required=_boolean(
            step.get("credential_required"),
            f"{name} credential_required",
        ),
        produces=_texts(step.get("produces"), f"{name} produces"),
        reason=_text(step.get("reason"), f"{name} reason"),
    )


def pre_hook_execution_selection(
    step: Mapping[str, Any],
    *,
    node_selector: str | None,
    node_scoped: bool,
) -> tuple[str, str | None]:
    """Return the exact v3 scope and selector for one externalized hook."""

    descriptor = pre_hook_descriptor(step)
    if node_scoped and descriptor.process_selector != node_selector:
        raise _ownership_error(f"{descriptor.name} process selector is inconsistent")
    if node_scoped:
        return "process", node_selector
    selector = descriptor.process_selector
    return ("process", selector) if selector is not None else ("workload", None)


def _process_hook_descriptors(value: object) -> tuple[PackHookDescriptor, ...]:
    if value is None or value == {}:
        return ()
    if not isinstance(value, Mapping):
        raise _ownership_error("whole-workload process plan inventory is invalid")
    descriptors: list[PackHookDescriptor] = []
    for raw_key, raw_plan in value.items():
        if not isinstance(raw_key, str) or not raw_key:
            raise _ownership_error("whole-workload process plan key is invalid")
        if not isinstance(raw_plan, Mapping):
            raise _ownership_error("whole-workload process plan is invalid")
        expected_selector = None if raw_key == _DEFAULT_PROCESS_PLAN_KEY else raw_key
        if raw_plan.get("selector") != expected_selector:
            raise _ownership_error("whole-workload process selector is inconsistent")
        descriptors.extend(
            _hook_descriptors(
                raw_plan.get("steps"),
                fallback_selector=expected_selector,
            )
        )
    return tuple(descriptors)


def _hook_descriptors(
    value: object,
    *,
    fallback_selector: str | None | object = _UNSPECIFIED_SELECTOR,
) -> tuple[PackHookDescriptor, ...]:
    if not isinstance(value, list):
        return ()
    descriptors: list[PackHookDescriptor] = []
    for step in value:
        if not isinstance(step, Mapping) or step.get("phase") != "pre_hook":
            continue
        descriptors.append(
            pre_hook_descriptor(
                step,
                fallback_selector=fallback_selector,
            )
        )
    return tuple(descriptors)


def _texts(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        raise _ownership_error(f"{field} must be a list")
    return tuple(_text(item, field) for item in value)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise _ownership_error(f"{field} must be non-empty text")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _ownership_error(f"{field} must be an object")
    return value


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise _ownership_error(f"{field} must be boolean")
    return value


def _ownership_error(message: str) -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_INIT_FETCH_HOOK_OWNERSHIP_INCOMPLETE",
        message,
    )


__all__ = [
    "PackHookDescriptor",
    "pre_hook_descriptor",
    "pre_hook_execution_selection",
    "require_complete_workload_hook_ownership",
]
