"""Human-readable rendering helpers for ``dpone run --sample``."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.commands.docs_url_rendering import public_docs_url
from dpone.security_redaction import public_path_label
from dpone.services.safe_sample_redaction import redact_safe_sample_text, redact_safe_sample_value


def safe_sample_text(
    *,
    errors: Sequence[Mapping[str, Any]],
    runtime_run: Mapping[str, Any] | None,
    runtime_handoff: Mapping[str, Any] | None = None,
    execution_mode: str | None = None,
) -> str:
    lines = [_title(runtime_handoff, execution_mode=execution_mode, markdown=False), *_error_lines(errors)]
    lines.extend(
        _runtime_lines(
            runtime_run,
            runtime_handoff=runtime_handoff,
            execution_mode=execution_mode,
            markdown=False,
            has_errors=bool(errors),
        )
    )
    if execution_mode == "blocked":
        lines.extend(_recovery_lines(errors, markdown=False))
    return "\n".join(lines) + "\n"


def safe_sample_markdown(
    *,
    errors: Sequence[Mapping[str, Any]],
    runtime_run: Mapping[str, Any] | None,
    runtime_handoff: Mapping[str, Any] | None = None,
    execution_mode: str | None = None,
) -> str:
    lines = [
        _title(runtime_handoff, execution_mode=execution_mode, markdown=True),
        "",
        *[f"- {line}" for line in _error_lines(errors)],
    ]
    lines.extend(
        _runtime_lines(
            runtime_run,
            runtime_handoff=runtime_handoff,
            execution_mode=execution_mode,
            markdown=True,
            has_errors=bool(errors),
        )
    )
    if execution_mode == "blocked":
        lines.extend(_recovery_lines(errors, markdown=True))
    return "\n".join(lines) + "\n"


def redact_safe_sample_payload(payload: dict[str, object]) -> dict[str, object]:
    """Redact one command payload at the existing rendering boundary."""

    redacted = redact_safe_sample_value(payload)
    if not isinstance(redacted, dict):
        raise TypeError("Safe sample command payload must be a mapping.")
    return redacted


def redact_safe_sample_output_text(value: str) -> str:
    """Redact one already-rendered command response."""

    return redact_safe_sample_text(value)


def safe_sample_recovery_lines(
    errors: Sequence[Mapping[str, Any]],
    *,
    markdown: bool = False,
) -> list[str]:
    """Render one redacted actionable recovery line for blocked output."""

    return _recovery_lines(errors, markdown=markdown)


def _title(
    runtime_handoff: Mapping[str, Any] | None,
    *,
    execution_mode: str | None,
    markdown: bool,
) -> str:
    if execution_mode == "live_copy":
        text = "dpone safe sample live copy"
    elif execution_mode == "blocked":
        text = "dpone safe sample run blocked"
    elif execution_mode == "local_handoff" or isinstance(runtime_handoff, Mapping):
        text = "dpone safe sample handoff prepared"
    else:
        text = "dpone safe sample run unavailable"
    return f"# {text}" if markdown else text


def _error_lines(errors: Sequence[Mapping[str, Any]]) -> list[str]:
    if not errors:
        return ["OK: safe sample run passed"]
    lines: list[str] = []
    for error in errors:
        code = redact_safe_sample_text(str(error.get("code") or "UNKNOWN"))
        message = redact_safe_sample_text(str(error.get("message") or ""))
        lines.append(f"{code}: {message}" if message else code)
    return lines


def _runtime_lines(
    runtime_run: Mapping[str, Any] | None,
    *,
    runtime_handoff: Mapping[str, Any] | None,
    execution_mode: str | None,
    markdown: bool,
    has_errors: bool,
) -> list[str]:
    if execution_mode is None:
        return _legacy_runtime_lines(runtime_run, runtime_handoff=runtime_handoff, markdown=markdown)

    if execution_mode == "local_handoff":
        lines: list[str] = []
        evidence = _evidence_path(runtime_run) if isinstance(runtime_run, Mapping) else None
        evidence = evidence or _handoff_evidence_path(runtime_handoff)
        if evidence:
            lines.append(_runtime_line("evidence", evidence, markdown=markdown))
        lines.append(
            _runtime_line(
                "next",
                "platform prepares the signed authorization overlay; rerun the same dpone run command",
                markdown=markdown,
            )
        )
        return lines

    if execution_mode == "live_copy" and not has_errors:
        evidence = _evidence_path(runtime_run) if isinstance(runtime_run, Mapping) else None
        if evidence:
            return [_runtime_line("runtime evidence", evidence, markdown=markdown)]
        return []

    if has_errors:
        return []

    if not isinstance(runtime_run, Mapping):
        return []
    evidence = _evidence_path(runtime_run)
    if evidence:
        return [_runtime_line("runtime evidence", evidence, markdown=markdown)]
    return []


def _legacy_runtime_lines(
    runtime_run: Mapping[str, Any] | None,
    *,
    runtime_handoff: Mapping[str, Any] | None,
    markdown: bool,
) -> list[str]:
    lines: list[str] = []
    if isinstance(runtime_run, Mapping):
        for label, value in (
            ("runtime status", _optional_string(runtime_run.get("execution_status"))),
            ("data outcome", _optional_string(runtime_run.get("data_outcome"))),
            ("runtime evidence", _evidence_path(runtime_run)),
        ):
            if value:
                lines.append(_runtime_line(label, value, markdown=markdown))
    handoff_command = _handoff_command(runtime_handoff)
    if handoff_command:
        lines.append(_runtime_line("runtime handoff", handoff_command, markdown=markdown))
    live_copy_command = _live_copy_command(runtime_handoff)
    if live_copy_command:
        lines.append(_runtime_line("live copy", "blocked until a certified executor is configured", markdown=markdown))
        lines.append(_runtime_line("live copy handoff", live_copy_command, markdown=markdown))
    return lines


def _runtime_line(label: str, value: str, *, markdown: bool) -> str:
    if markdown:
        return f"- {label}: `{value}`"
    return f"- {label}: {value}"


def _evidence_path(runtime_run: Mapping[str, Any]) -> str | None:
    evidence_write = runtime_run.get("evidence_write")
    if not isinstance(evidence_write, Mapping):
        return None
    return _project_relative_path(evidence_write.get("path"))


def _handoff_evidence_path(runtime_handoff: Mapping[str, Any] | None) -> str | None:
    if not isinstance(runtime_handoff, Mapping):
        return None
    return _project_relative_path(runtime_handoff.get("plan_path"))


def _handoff_command(runtime_handoff: Mapping[str, Any] | None) -> str | None:
    if not isinstance(runtime_handoff, Mapping):
        return None
    return _optional_string(runtime_handoff.get("command"))


def _live_copy_command(runtime_handoff: Mapping[str, Any] | None) -> str | None:
    if not isinstance(runtime_handoff, Mapping):
        return None
    return _optional_string(runtime_handoff.get("live_copy_command"))


def _optional_string(value: Any) -> str | None:
    text = str(value or "")
    return text or None


def _project_relative_path(value: Any) -> str | None:
    text = _optional_string(value)
    if text is None:
        return None
    try:
        relative = Path(text).resolve(strict=False).relative_to(Path.cwd().resolve(strict=False))
        return redact_safe_sample_text(relative.as_posix())
    except (OSError, RuntimeError, ValueError):
        return redact_safe_sample_text(public_path_label(text, fallback="evidence.json"))


def _recovery_lines(
    errors: Sequence[Mapping[str, Any]],
    *,
    markdown: bool,
) -> list[str]:
    for error in errors:
        recovery = _error_recovery(error)
        if recovery:
            return [_runtime_line("recovery", recovery, markdown=markdown)]
    return [
        _runtime_line(
            "recovery",
            "rerun with --format json and follow the structured error guidance",
            markdown=markdown,
        )
    ]


def _error_recovery(error: Mapping[str, Any]) -> str | None:
    fixes = error.get("fixes")
    if isinstance(fixes, list):
        for fix in fixes:
            if not isinstance(fix, Mapping):
                continue
            command = _optional_string(fix.get("command"))
            if fix.get("safety") == "safe" and command:
                return redact_safe_sample_text(command)
        for fix in fixes:
            if not isinstance(fix, Mapping):
                continue
            description = _optional_string(fix.get("description"))
            if description:
                return redact_safe_sample_text(description)
    docs_url = _optional_string(error.get("docs_url"))
    if docs_url:
        return redact_safe_sample_text(public_docs_url(docs_url))
    code = str(error.get("code") or "")
    if code.startswith("DPONE_") and code.replace("_", "").isalnum():
        return public_docs_url(f"docs/errors/{code}.md")
    return None


__all__ = ["safe_sample_markdown", "safe_sample_recovery_lines", "safe_sample_text"]
