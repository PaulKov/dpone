"""Redacted result assembly for the safe-sample CLI facade."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.commands.run_output import write_json, write_text
from dpone.commands.run_safe_sample_rendering import (
    redact_safe_sample_output_text,
    redact_safe_sample_payload,
    safe_sample_markdown,
    safe_sample_text,
)

if TYPE_CHECKING:
    from dpone.readiness.airflow_authoring_check_service import CheckedPipelineSource
    from dpone.services.safe_sample_cli_result import SafeSampleCliResult


@dataclass(frozen=True, slots=True)
class SafeSampleCommandResult:
    """Programmatic safe-sample result shared by single and selected runs."""

    exit_code: int
    payload: dict[str, object]
    text: str
    markdown: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", redact_safe_sample_payload(self.payload))
        object.__setattr__(self, "text", redact_safe_sample_output_text(self.text))
        object.__setattr__(self, "markdown", redact_safe_sample_output_text(self.markdown))

    def write(self, *, format: str) -> int:
        if format == "json":
            write_json(self.payload)
        elif format == "md":
            write_text(self.markdown)
        else:
            write_text(self.text)
        return self.exit_code


def safe_sample_result(
    args: argparse.Namespace,
    *,
    run_id: str,
    cli_result: SafeSampleCliResult,
    safe_sample: dict[str, object],
    runtime_run: dict[str, object] | None,
) -> SafeSampleCommandResult:
    """Build the stable CLI envelope around one policy/runtime result."""

    payload: dict[str, object] = {
        "manifest": public_pipeline_source(getattr(args, "path", "")),
        "process": "",
        "selector": getattr(args, "selector", None),
        "run_id": run_id,
        "passed": cli_result.passed,
        "result": {
            "status": cli_result.status,
            "inserted_rows": 0,
            "updated_rows": 0,
            "final_rows": 0,
            "extracted_rows": 0,
            "duration_seconds": 0.0,
            "errors": [dict(error) for error in cli_result.errors],
        },
        "sample": getattr(args, "sample", None),
        "target": getattr(args, "target", None),
        "safe_sample": safe_sample,
    }
    runtime_handoff = safe_sample.get("runtime_handoff")
    if not isinstance(runtime_handoff, dict):
        runtime_handoff = None
    execution_mode = str(safe_sample.get("execution_mode") or "blocked")
    return SafeSampleCommandResult(
        exit_code=cli_result.exit_code,
        payload=payload,
        text=safe_sample_text(
            errors=cli_result.errors,
            runtime_run=runtime_run,
            runtime_handoff=runtime_handoff,
            execution_mode=execution_mode,
        ),
        markdown=safe_sample_markdown(
            errors=cli_result.errors,
            runtime_run=runtime_run,
            runtime_handoff=runtime_handoff,
            execution_mode=execution_mode,
        ),
    )


def argument_error_result(
    args: argparse.Namespace,
    error: dict[str, object],
    *,
    text_lines: list[str],
    markdown_lines: list[str],
) -> SafeSampleCommandResult:
    """Return an argument failure without reading the pipeline source."""

    payload: dict[str, object] = {
        "manifest": public_pipeline_source(getattr(args, "path", "")),
        "process": "",
        "selector": getattr(args, "selector", None),
        "run_id": getattr(args, "run_id", "") or "",
        "passed": False,
        "result": {
            "status": "invalid",
            "inserted_rows": 0,
            "updated_rows": 0,
            "final_rows": 0,
            "extracted_rows": 0,
            "duration_seconds": 0.0,
            "errors": [dict(error)],
        },
        "sample": getattr(args, "sample", None),
        "target": getattr(args, "target", None),
        "safe_sample": {
            "schema": "dpone.safe-sample-cli-argument-validation.v1",
            "errors": [dict(error)],
        },
    }
    return SafeSampleCommandResult(
        exit_code=2,
        payload=payload,
        text="\n".join(["dpone safe sample arguments invalid", *text_lines, ""]),
        markdown="\n".join(
            [
                "# dpone safe sample arguments invalid",
                "",
                *markdown_lines,
                "",
            ]
        ),
    )


def source_error_result(
    args: argparse.Namespace,
    checked: CheckedPipelineSource,
) -> SafeSampleCommandResult:
    """Return a fail-closed authoring-reference failure before artifacts exist."""

    errors = [dict(error) for error in checked.result.errors]
    primary = errors[0] if errors else not_implemented_error()
    payload: dict[str, object] = {
        "manifest": public_pipeline_source(getattr(args, "path", "")),
        "process": "",
        "selector": getattr(args, "selector", None),
        "run_id": getattr(args, "run_id", "") or "",
        "passed": False,
        "result": {
            "status": "blocked",
            "inserted_rows": 0,
            "updated_rows": 0,
            "final_rows": 0,
            "extracted_rows": 0,
            "duration_seconds": 0.0,
            "errors": errors,
        },
        "sample": getattr(args, "sample", None),
        "target": getattr(args, "target", None),
        "safe_sample": {
            "local_deployment": {
                "status": "failed",
                "environment": str(getattr(args, "environment", "") or "development"),
                "error": primary,
            },
            "execution_mode": "blocked",
        },
    }
    return SafeSampleCommandResult(
        exit_code=checked.result.exit_code or 4,
        payload=payload,
        text=safe_sample_text(errors=errors, runtime_run=None, runtime_handoff=None, execution_mode="blocked"),
        markdown=safe_sample_markdown(
            errors=errors,
            runtime_run=None,
            runtime_handoff=None,
            execution_mode="blocked",
        ),
    )


def not_implemented_error() -> dict[str, object]:
    """Return the compatibility fallback when no runtime boundary was selected."""

    return {
        "schema": "dpone.error.v1",
        "code": "DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED",
        "stage": "safe_sample_run",
        "severity": "error",
        "message": "Safe temporary sample runs require Phase 1B runtime delivery and policy enforcement.",
        "entity": {"kind": "command", "id": "dpone run --sample --target temporary"},
        "fixes": [
            {
                "id": "implement_certified_source_data_copier",
                "safety": "manual",
                "description": "Inject a certified source data copier and keep the fail-closed boundary until certification passes.",
            }
        ],
    }


def public_pipeline_source(value: object) -> str:
    """Return a project-relative or basename-only source label safe for output."""

    text = str(value or "").strip()
    if not text:
        return ""
    path = Path(text)
    try:
        resolved = path.resolve(strict=False)
        relative = resolved.relative_to(Path.cwd().resolve(strict=False))
        if relative.parts and all(part not in {"", ".", ".."} for part in relative.parts):
            return relative.as_posix()
    except (OSError, RuntimeError, ValueError):
        pass
    return path.name or "pipeline.yaml"


__all__ = [
    "SafeSampleCommandResult",
    "argument_error_result",
    "not_implemented_error",
    "public_pipeline_source",
    "safe_sample_result",
    "source_error_result",
]
