"""Safe replay failure projections for the command-line boundary.

The runtime owns commit truth. Rendering never derives it from row counters,
exception text, or the mere presence of a replay error.
"""

from __future__ import annotations

from collections.abc import Mapping

from dpone.security_redaction import redact_absolute_paths, redact_text

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
