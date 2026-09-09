"""Structured recovery projections for Studio responses."""

from __future__ import annotations

import shlex
from collections.abc import Mapping, Sequence
from typing import Any


def explain_next_actions(
    pipeline_id: str,
    payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Return one executable argv action consistent with explain state."""

    artifact_state = payload.get("artifact_state")
    state = artifact_state if isinstance(artifact_state, Mapping) else {}
    manifest_state = state.get("manifest")
    if manifest_state == "missing":
        return [
            _action(
                "init_pipeline",
                "Create the missing pipeline source.",
                ("dpone", "init", "pipeline", pipeline_id),
            )
        ]
    if manifest_state == "invalid":
        return [
            _action(
                "check_pipeline",
                "Correct the pipeline source and check it again.",
                ("dpone", "check", f"pipelines/{pipeline_id}"),
            )
        ]
    if not payload.get("passed"):
        return [
            _action(
                "refresh_airflow_preview",
                "Rebuild the local Airflow preview after resolving diagnostics.",
                ("dpone", "airflow", "preview", pipeline_id),
            )
        ]
    if state.get("dag_spec") == "materialized":
        return [
            _action(
                "run_hermetic_test",
                "Run the hermetic pipeline test.",
                ("dpone", "test", pipeline_id),
            )
        ]
    return [
        _action(
            "preview_airflow",
            "Materialize a local Airflow preview.",
            ("dpone", "airflow", "preview", pipeline_id),
        )
    ]


def structured_studio_errors(
    errors: object,
) -> list[dict[str, Any]]:
    """Project dpone errors without shell command strings."""

    if not isinstance(errors, Sequence) or isinstance(errors, (str, bytes, bytearray)):
        return []
    return [_structured_error(item) for item in errors if isinstance(item, Mapping)]


def _structured_error(error: Mapping[str, Any]) -> dict[str, Any]:
    payload = {str(key): value for key, value in error.items()}
    fixes = payload.get("fixes")
    payload["fixes"] = (
        [_structured_fix(item) for item in fixes if isinstance(item, Mapping)]
        if isinstance(fixes, Sequence) and not isinstance(fixes, (str, bytes, bytearray))
        else []
    )
    return payload


def _structured_fix(fix: Mapping[str, Any]) -> dict[str, Any]:
    payload = {str(key): value for key, value in fix.items() if key != "command"}
    command = fix.get("command")
    if isinstance(command, str) and command.strip():
        try:
            argv = shlex.split(command)
        except ValueError:
            argv = []
        if argv:
            payload["argv"] = argv
    return payload


def _action(
    action_id: str,
    label: str,
    argv: tuple[str, ...],
) -> dict[str, Any]:
    return {"id": action_id, "label": label, "argv": list(argv)}


__all__ = ["explain_next_actions", "structured_studio_errors"]
