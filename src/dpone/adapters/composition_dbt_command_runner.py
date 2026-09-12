"""Adapt existing dbt runner calls to protected, one-shot build dispatch.

The application derives commands from verified pack, interval, profile and output
paths. This adapter never derives SQL policy or accepts a worker assertion as a
permit. Capture retains ownership of intent-before-launch and original evidence.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Protocol

from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_outcome import DbtCaptureError, DbtChildExit, DbtDispatchIntent
from dpone.ports.dbt_publishing import DbtCommandResult, DbtCommandRunner


@dataclass(frozen=True)
class TrustedDbtCommand:
    """Exact existing-runtime invocation derived at the composition root."""

    phase: str
    args: tuple[str, ...]
    cwd: Path
    timeout_seconds: int

    def __post_init__(self) -> None:
        if self.phase not in {"preflight", "build"} or not self.args or not self.cwd.is_absolute():
            raise DbtCaptureError("capture_trusted_command")
        if type(self.timeout_seconds) is not int or self.timeout_seconds <= 0:
            raise DbtCaptureError("capture_trusted_command")


class RegistrationStore(Protocol):
    def register(self, attempt: CompositionAttemptIdentity) -> DbtDispatchIntent: ...


class BuildCapture(Protocol):
    def dispatch(self, attempt: CompositionAttemptIdentity) -> DbtChildExit: ...


class ProtectedDbtCommandRunner:
    """Delegate exact preflight calls and fence every build invocation once.

    ``trusted_commands`` and ``trusted_intent`` are app-owned derivations, never
    forwarded input arguments. Only the literal runtime executable ``dbt`` may
    normalize to the registered absolute executable. Both successful dispatch
    and uncertain registration/dispatch consume this instance permanently; the
    durable capture store independently rejects dispatch across new instances.
    Final capture is called by the app after the runtime writes final evidence.
    """

    def __init__(
        self,
        *,
        attempt: CompositionAttemptIdentity,
        trusted_commands: Callable[[], tuple[TrustedDbtCommand, ...]],
        trusted_intent: Callable[[], DbtDispatchIntent],
        store: RegistrationStore,
        capture: BuildCapture,
        preflight_runner: DbtCommandRunner,
    ) -> None:
        self._attempt = attempt
        self._commands, self._intent = trusted_commands, trusted_intent
        self._store, self._capture, self._preflight = store, capture, preflight_runner
        self._lock = Lock()
        self._consumed = False

    def run(
        self,
        args: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: int,
        redactions: tuple[str, ...],
    ) -> DbtCommandResult:
        with self._lock:
            if self._consumed:
                raise DbtCaptureError("capture_command_replay")
            matches = tuple(
                command
                for command in self._commands()
                if (command.args, command.cwd, command.timeout_seconds) == (args, cwd, timeout_seconds)
            )
            if len(matches) != 1:
                raise DbtCaptureError("capture_command_mismatch")
            command = matches[0]
            if command.phase == "preflight":
                return self._preflight.run(args, cwd=cwd, timeout_seconds=timeout_seconds, redactions=redactions)
            expected = self._intent()
            expected.__post_init__()
            normalized = (expected.argv[0], *args[1:]) if args[0] == "dbt" else args
            if (
                expected.attempt != self._attempt
                or normalized != expected.argv
                or str(cwd) != expected.working_directory
                or timeout_seconds != expected.timeout_seconds
            ):
                raise DbtCaptureError("capture_command_mismatch")
            # Consume before any persistence call: an exception may be lost ACK.
            self._consumed = True
            if self._store.register(self._attempt) != expected:
                raise DbtCaptureError("capture_registration_mismatch")
            child = self._capture.dispatch(self._attempt)
            return DbtCommandResult(exit_code=child.exit_code)
