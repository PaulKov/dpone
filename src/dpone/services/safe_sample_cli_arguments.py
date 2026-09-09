"""Fail-fast validation for beginner safe-sample CLI arguments."""

from __future__ import annotations

import argparse
import shlex
from collections.abc import Iterable
from pathlib import Path
from typing import Any

_EXPLICIT_OPTIONS_ATTRIBUTE = "_dpone_explicit_run_options"
_ORDINARY_RUN_ONLY_OPTIONS = (
    "--registry",
    "--dag-id",
    "--execution-date",
    "--interval-start",
    "--interval-end",
    "--retry-attempts",
    "--retry-backoff-seconds",
)


class ExplicitOptionAction(argparse.Action):
    """Store an option value while retaining that argv supplied it."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str | None = None,
    ) -> None:
        del parser, option_string
        setattr(namespace, self.dest, values)
        _record_explicit_option(namespace, self.option_strings[0])


class ExplicitAppendOptionAction(argparse.Action):
    """Append an option value while retaining that argv supplied it."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str | None = None,
    ) -> None:
        del parser, option_string
        current = list(getattr(namespace, self.dest, None) or ())
        current.append(values)
        setattr(namespace, self.dest, current)
        _record_explicit_option(namespace, self.option_strings[0])


def safe_sample_incompatible_options(args: argparse.Namespace) -> tuple[str, ...]:
    """Return explicitly supplied ordinary-run options in stable contract order."""

    supplied = getattr(args, _EXPLICIT_OPTIONS_ATTRIBUTE, ())
    if not isinstance(supplied, (list, tuple, set, frozenset)):
        return ()
    supplied_names = frozenset(str(option) for option in supplied)
    return tuple(option for option in _ORDINARY_RUN_ONLY_OPTIONS if option in supplied_names)


def selected_safe_sample_fix_command(args: argparse.Namespace) -> str:
    """Render a copyable bounded selection command from safe argv values."""

    command = [
        "dpone",
        "run",
        _project_relative_pipeline(str(getattr(args, "path", "") or ".")),
    ]
    for option, values in (
        ("--select", getattr(args, "select", ())),
        ("--exclude", getattr(args, "exclude", ())),
    ):
        for value in values or ():
            command.extend((option, str(value)))
    state = getattr(args, "state", None)
    if state:
        command.extend(("--state", str(state)))
    selectors = str(getattr(args, "selectors", "selectors.yaml") or "selectors.yaml")
    if selectors != "selectors.yaml":
        command.extend(("--selectors", selectors))
    max_selected = int(getattr(args, "max_selected", 10) or 10)
    if max_selected != 10:
        command.extend(("--max-selected", str(max_selected)))
    sample = getattr(args, "sample", None)
    command.extend(("--sample", str(sample if isinstance(sample, int) and sample > 0 else 1000)))
    command.extend(("--target", "temporary"))
    environment = str(getattr(args, "environment", "") or "")
    if environment and environment != "development":
        command.extend(("--environment", environment))
    run_id = str(getattr(args, "run_id", "") or "")
    if run_id:
        command.extend(("--run-id", run_id))
    return shlex.join(command)


def build_safe_sample_incompatible_error(
    *,
    path: str,
    incompatible_options: Iterable[str],
    fix_command: str | None = None,
) -> dict[str, Any]:
    """Build the value-free usage error for incompatible safe-sample argv."""

    incompatible = _ordered_incompatible_options(incompatible_options)
    if not incompatible:
        raise ValueError("At least one incompatible safe-sample option is required.")
    option_names = ", ".join(incompatible)
    return {
        "schema": "dpone.error.v1",
        "code": "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID",
        "stage": "safe_sample_cli_arguments",
        "severity": "error",
        "exit_code": 2,
        "message": f"Safe-sample mode does not accept ordinary-run options: {option_names}.",
        "entity": {"kind": "command", "id": "dpone run --sample --target temporary"},
        "incompatible_options": list(incompatible),
        "fixes": [_fix(path, command=fix_command)],
        "docs_url": "docs/errors/DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID.md",
    }


def validate_safe_sample_cli_arguments(
    *,
    path: str,
    sample: int | None,
    target: str | None,
    incompatible_options: Iterable[str] = (),
    fix_command: str | None = None,
) -> dict[str, Any] | None:
    """Return a structured error when the sample-run flag pair is incomplete."""

    incompatible = _ordered_incompatible_options(incompatible_options)
    if incompatible:
        return build_safe_sample_incompatible_error(
            path=path,
            incompatible_options=incompatible,
            fix_command=fix_command,
        )
    has_sample = sample is not None
    has_target = target is not None
    if not has_sample or not has_target:
        missing = "--target temporary" if has_sample else "--sample <rows>"
        return {
            "schema": "dpone.error.v1",
            "code": "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID",
            "stage": "safe_sample_cli_arguments",
            "severity": "error",
            "message": f"Safe sample runs require both --sample and --target temporary; missing {missing}.",
            "entity": {"kind": "command", "id": "dpone run --sample --target temporary"},
            "fixes": [_fix(path, command=fix_command)],
        }
    if sample is not None and sample <= 0:
        return {
            "schema": "dpone.error.v1",
            "code": "DPONE_RUNTIME_SAMPLE_SIZE_INVALID",
            "stage": "safe_sample_cli_arguments",
            "severity": "error",
            "message": "Sample row budget must be greater than zero.",
            "entity": {"kind": "command", "id": "dpone run --sample --target temporary"},
            "fixes": [_fix(path, command=fix_command)],
        }
    return None


def safe_sample_cli_argument_error_lines(error: dict[str, object], *, markdown: bool = False) -> list[str]:
    """Return human-readable error lines with the same fix command as JSON output."""

    code = str(error.get("code") or "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID")
    message = str(error.get("message") or "Safe sample arguments are invalid.")
    command = _first_fix_command(error)
    if markdown:
        lines = [f"- code: `{code}`", f"- message: {message}"]
        if command:
            lines.append(f"- fix: `{command}`")
        return lines
    lines = [f"- code: {code}", f"- message: {message}"]
    if command:
        lines.append(f"- fix: {command}")
    return lines


def _fix(path: str, *, command: str | None = None) -> dict[str, str]:
    return {
        "id": "provide_complete_safe_sample_arguments",
        "safety": "safe",
        "description": "Run with an explicit positive row budget and temporary target.",
        "command": command
        or shlex.join(
            [
                "dpone",
                "run",
                _project_relative_pipeline(path),
                "--sample",
                "1000",
                "--target",
                "temporary",
            ]
        ),
    }


def _record_explicit_option(namespace: argparse.Namespace, option: str) -> None:
    current = tuple(getattr(namespace, _EXPLICIT_OPTIONS_ATTRIBUTE, ()))
    if option not in current:
        setattr(namespace, _EXPLICIT_OPTIONS_ATTRIBUTE, (*current, option))


def _ordered_incompatible_options(options: Iterable[str]) -> tuple[str, ...]:
    supplied = frozenset(str(option) for option in options)
    return tuple(option for option in _ORDINARY_RUN_ONLY_OPTIONS if option in supplied)


def _project_relative_pipeline(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "pipeline.yaml"
    path = Path(text)
    try:
        return path.resolve(strict=False).relative_to(Path.cwd().resolve(strict=False)).as_posix()
    except (OSError, RuntimeError, ValueError):
        return path.name or "pipeline.yaml"


def _first_fix_command(error: dict[str, object]) -> str:
    fixes = error.get("fixes")
    if not isinstance(fixes, list):
        return ""
    for fix in fixes:
        if isinstance(fix, dict) and isinstance(fix.get("command"), str):
            return fix["command"]
    return ""


__all__ = [
    "ExplicitAppendOptionAction",
    "ExplicitOptionAction",
    "build_safe_sample_incompatible_error",
    "safe_sample_cli_argument_error_lines",
    "safe_sample_incompatible_options",
    "selected_safe_sample_fix_command",
    "validate_safe_sample_cli_arguments",
]
