"""Stable, redacted failure rendering for ``dpone run``."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from contextlib import redirect_stdout
from typing import Any

from dpone.commands.run_output import write_json, write_text
from dpone.contracts.commit_unknown import CommitUnknownOutcome
from dpone.contracts.quality_failure import QualityGateFailureOutcome
from dpone.security_redaction import public_path_label, redact_absolute_paths, redact_text, redact_value


def write_run_failure(args: argparse.Namespace, exc: Exception) -> None:
    """Render one runtime failure without exposing paths or credentials."""

    stable_code = getattr(exc, "code", None)
    if isinstance(stable_code, str) and stable_code.startswith("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_"):
        with redirect_stdout(sys.stderr):
            _render_run_failure(args, exc)
        return
    _render_run_failure(args, exc)
    diagnostic = replay_failure_diagnostic(exc)
    if diagnostic is not None and args.format == "json":
        print(diagnostic, file=sys.stderr)


def _render_run_failure(args: argparse.Namespace, exc: Exception) -> None:
    """Render to the caller-selected stream without changing the envelope."""

    stable_code = replay_failure_code(exc) or getattr(exc, "code", None)
    detail = str(exc)
    raw_message = (
        (detail if isinstance(stable_code, str) and detail.startswith(stable_code) else f"{stable_code}: {detail}")
        if isinstance(stable_code, str)
        else f"{exc.__class__.__name__}: {detail}"
    )
    replay_diagnostic = replay_failure_diagnostic(exc)
    message = str(stable_code) if replay_diagnostic is not None else redact_absolute_paths(redact_text(raw_message))
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
        replay_details = replay_failure_details(exc)
        if replay_details is not None:
            result["replay_details"] = replay_details
        quality_report = getattr(exc, "report", None)
        to_jsonable = getattr(quality_report, "to_jsonable", None)
        if callable(to_jsonable):
            result["quality_gates"] = redact_value(to_jsonable())
        if isinstance(stable_code, str):
            result["error_code"] = stable_code
        evidence = getattr(exc, "evidence", None)
        if isinstance(evidence, dict):
            result["evidence"] = redact_value(evidence)
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
    detail_lines = [f"- replay: {replay_diagnostic}"] if replay_diagnostic is not None else []
    if args.format == "md":
        write_text("\n".join(["# dpone run failed", "", f"- error: `{message}`", *detail_lines, ""]))
        return
    write_text("\n".join(["dpone run failed", f"- error: {message}", *detail_lines, ""]))


__all__ = ["write_run_failure"]


_ENUM_FIELDS = {
    "target_commit": {"proven", "unknown", "not_started"},
    "governance": {"blocked", "complete"},
    "quality_state": {"PREPARED", "TARGET_PENDING", "COMPLETE", "FAILED"},
}
_CODES = {
    f"DPONE_REPLAY_QUALITY_EVIDENCE_{reason}"
    for reason in ("REQUIRED", "MISMATCH", "INVALID", "FAILED", "INCOMPLETE", "UNSUPPORTED")
}
_ID_FIELDS = ("original_run_id", "original_load_id", "current_run_id", "current_load_id")


def _registered_replay_code(error: BaseException) -> str | None:
    """Accept runtime classification only from the bounded public code registry."""
    code = getattr(error, "code", None)
    if isinstance(code, str) and code in _CODES:
        return code
    details = getattr(error, "replay_details", None)
    code = details.get("error_code") if isinstance(details, Mapping) else None
    return code if isinstance(code, str) and code in _CODES else None


def _replay_error(exc: Exception) -> BaseException | None:
    """Retain a known safe code through explicit manifest parsing wrappers."""
    current: BaseException | None = exc
    for _ in range(8):
        if current is None:
            break
        if _registered_replay_code(current) is not None:
            return current
        current = current.__cause__
    return None


def replay_failure_code(exc: Exception) -> str | None:
    """Return only a registered code, never an arbitrary causal message."""
    error = _replay_error(exc)
    return _registered_replay_code(error) if error is not None else None


def replay_failure_details(exc: Exception) -> dict[str, str] | None:
    """Project optional runtime metadata, excluding SQL and connection details."""
    error = _replay_error(exc)
    code = replay_failure_code(exc)
    if error is None or code is None:
        return None
    raw = getattr(error, "replay_details", None)
    if not isinstance(raw, Mapping):
        return None
    details = {"error_code": code}
    for key, choices in _ENUM_FIELDS.items():
        value = raw.get(key)
        if isinstance(value, str) and value in choices:
            details[key] = value
    for key in _ID_FIELDS:
        value = raw.get(key)
        if isinstance(value, str) and value:
            safe = redact_absolute_paths(redact_text(value))
            details[key] = " ".join(safe.split())[:256]
    return details if len(details) > 1 else None


def replay_failure_diagnostic(exc: Exception) -> str | None:
    """Return a bounded diagnostic using stable codes and validated enums only."""
    code = replay_failure_code(exc)
    if code is None:
        return None
    details = replay_failure_details(exc) or {}
    fields = [code]
    for key in _ENUM_FIELDS:
        if key in details:
            fields.append(f"{key}={details[key]}")
    fields.append("Preserve the operation identity and follow the committed replay recovery runbook.")
    return "; ".join(fields)
