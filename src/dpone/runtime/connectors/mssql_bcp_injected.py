"""Injected synchronous BCP execution policy.

This module keeps test-injected command execution separate from both process
lifecycle supervision and command construction.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from typing import Any

from dpone.runtime.connectors.mssql_bcp_process import (
    BcpResult,
    BcpTimeoutError,
    parse_bcp_rows_copied,
)
from dpone.runtime.process_io import add_exception_note


def run_injected_bcp_command(
    command: Sequence[str],
    *,
    redacted_command: Sequence[str],
    stdin_text: str | None,
    timeout_seconds: int,
    environment: dict[str, str] | None,
    cleanup_callback: Callable[[], None] | None,
    run: Callable[..., Any],
    process_options: dict[str, tuple[int, ...]],
    redact_output: Callable[[str], str],
) -> BcpResult:
    """Run injected BCP while preserving input authority and redaction rules."""

    kwargs: dict[str, Any] = {
        "check": False,
        "capture_output": True,
        "text": True,
        "timeout": timeout_seconds,
        "input": stdin_text,
        **process_options,
    }
    if environment is not None:
        kwargs["env"] = environment
    timeout_error: BcpTimeoutError | None = None
    try:
        completed = run(list(command), **kwargs)
    except subprocess.TimeoutExpired:
        timeout_error = BcpTimeoutError(
            redacted_command=tuple(redacted_command),
            timeout_seconds=timeout_seconds,
        )
        if cleanup_callback is not None:
            try:
                cleanup_callback()
            except Exception as cleanup_error:
                add_exception_note(timeout_error, f"bcp timeout cleanup failed: {type(cleanup_error).__name__}")
            cleanup_callback = None
    finally:
        if cleanup_callback is not None:
            cleanup_callback()
    if timeout_error is not None:
        raise timeout_error from None
    stdout = redact_output(completed.stdout or "")
    stderr = redact_output(completed.stderr or "")
    result = BcpResult(
        command=tuple(command),
        redacted_command=tuple(redacted_command),
        returncode=int(completed.returncode),
        stdout=stdout,
        stderr=stderr,
        rows_copied=parse_bcp_rows_copied(stdout + "\n" + stderr),
    )
    if result.returncode != 0:
        redacted = " ".join(result.redacted_command)
        raise RuntimeError(f"bcp failed with exit code {result.returncode}: {redacted}\n{stderr or stdout}")
    return result
