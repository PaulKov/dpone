"""Stable, redacted failure rendering for ``dpone run``."""

from __future__ import annotations

import argparse
from typing import Any

from dpone.commands.run_output import write_json, write_text
from dpone.contracts.commit_unknown import CommitUnknownOutcome
from dpone.contracts.quality_failure import QualityGateFailureOutcome
from dpone.security_redaction import public_path_label, redact_absolute_paths, redact_text, redact_value


def write_run_failure(args: argparse.Namespace, exc: Exception) -> None:
    """Render one runtime failure without exposing paths or credentials."""

    stable_code = getattr(exc, "code", None)
    raw_message = f"{stable_code}: {exc}" if isinstance(stable_code, str) else f"{exc.__class__.__name__}: {exc}"
    message = redact_absolute_paths(redact_text(raw_message))
    if args.format == "json":
        result: dict[str, Any] = {
            "status": "error",
            "inserted_rows": 0,
            "updated_rows": 0,
            "final_rows": 0,
            "extracted_rows": 0,
            "duration_seconds": 0.0,
            "errors": [message],
        }
        quality_report = getattr(exc, "report", None)
        to_jsonable = getattr(quality_report, "to_jsonable", None)
        if callable(to_jsonable):
            result["quality_gates"] = redact_value(to_jsonable())
        if isinstance(stable_code, str):
            result["error_code"] = stable_code
        outcome = getattr(exc, "outcome", None)
        if isinstance(outcome, QualityGateFailureOutcome):
            result.update(outcome.result_fields())
        elif isinstance(outcome, CommitUnknownOutcome):
            result.update(outcome.to_jsonable())
        attempts = outcome.attempts if isinstance(outcome, QualityGateFailureOutcome) else 0
        payload = {
            "manifest": public_path_label(
                str(getattr(args, "path", "")),
                fallback="manifest",
            ),
            "process": "",
            "selector": getattr(args, "selector", None),
            "run_id": getattr(args, "run_id", "") or "",
            "passed": False,
            "result": result,
            "attempts": attempts,
            "max_attempts": max(1, int(getattr(args, "retry_attempts", 0) or 0) + 1),
            "retry_backoff_seconds": float(getattr(args, "retry_backoff_seconds", 0.0) or 0.0),
        }
        write_json(redact_value(payload))
        return
    if args.format == "md":
        write_text("\n".join(["# dpone run failed", "", f"- error: `{message}`", ""]))
        return
    write_text("\n".join(["dpone run failed", f"- error: {message}", ""]))


__all__ = ["write_run_failure"]
