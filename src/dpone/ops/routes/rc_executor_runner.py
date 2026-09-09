"""Shell command runner port for route release-candidate execution."""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True, slots=True)
class CommandProcessResult:
    """Result returned by a command-process runner."""

    exit_code: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0
    timed_out: bool = False


class CommandProcessRunner(Protocol):
    """Thin process runner protocol used by route RC execution."""

    def run(self, command: str, *, cwd: Path | None, timeout_seconds: int) -> CommandProcessResult:
        """Run one shell command and return captured output."""


class SubprocessCommandRunner:
    """Default command runner for explicit opt-in local execution."""

    def run(self, command: str, *, cwd: Path | None, timeout_seconds: int) -> CommandProcessResult:
        started = time.monotonic()
        try:
            completed = subprocess.run(  # noqa: S602 - route RC execution is explicit and opt-in.
                command,
                cwd=str(cwd) if cwd else None,
                shell=True,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            return CommandProcessResult(
                exit_code=124,
                stdout=_decode_timeout_output(exc.stdout),
                stderr=_decode_timeout_output(exc.stderr),
                duration_seconds=round(time.monotonic() - started, 3),
                timed_out=True,
            )
        return CommandProcessResult(
            exit_code=int(completed.returncode),
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
            duration_seconds=round(time.monotonic() - started, 3),
            timed_out=False,
        )


def _decode_timeout_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


__all__ = ["CommandProcessResult", "CommandProcessRunner", "SubprocessCommandRunner"]
