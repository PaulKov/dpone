"""Exclusive child-process ownership and unresolved-launch capabilities for TDS."""

from typing import Protocol

from dpone.contracts.mssql_tds_api import TdsInputReceipt, TdsWorkerResult, WindowOutcomeUnknown
from dpone.contracts.mssql_tds_worker import (
    TdsChildExit,
    TdsProcessIdentity,
)


class TdsManagedWorker(Protocol):
    """Exclusive supervisor-thread ownership of a fixed, authenticated child."""

    @property
    def identity(self) -> TdsProcessIdentity:
        """Independently authenticated process incarnation bound to held pidfd."""

    def startup(self, *, deadline: float) -> None:
        """Validate the child startup message against the held process identity."""

    def send(self, body: bytes, *, deadline: float) -> None:
        """Send one private raw job, close its pipe, never retry partial delivery."""

    def receive(
        self, *, expected_attempt_sha256: str, expected_input: TdsInputReceipt, deadline: float
    ) -> TdsWorkerResult:
        """Require actual EOF and strict result identity within the deadline."""

    def wait(self, *, deadline: float) -> TdsChildExit:
        """Observe exit and exclusively reap without signalling a live child."""

    def terminate(self, *, deadline: float) -> TdsChildExit:
        """Kill through authenticated pidfd and reap within one absolute budget."""

    def close(self) -> None:
        """Close descriptors after proven child settlement; not a kill operation."""


class TdsWorkerLauncher(Protocol):
    """Composition supplies admitted implementation and resource configuration."""

    def spawn(self, *, startup_deadline: float, operation_deadline: float) -> TdsManagedWorker:
        """Launch without credentials and acquire process identity; unknown spawn raises.

        On failure the implementation must settle its child or retain unknown
        process authority. The caller validates startup while retaining the
        returned handle for containment even if the startup message is invalid.
        """


class TdsUnresolvedLaunch(Protocol):
    """Retained launch authority when a fully authenticated worker is unavailable.

    Missing process identity is not permission to signal a numeric PID. Keep the
    capability and SQL reservations until explicit containment establishes exit.
    """

    def contain(self, *, deadline: float) -> None:
        """Prove child exit and exclusive reaping, or preserve unknown authority."""

    def close(self) -> None:
        """Release descriptors only after proven containment."""


class TdsLaunchUnknown(WindowOutcomeUnknown):
    """Launch failed with unresolved resources that the caller must retain."""

    def __init__(self, launch: TdsUnresolvedLaunch) -> None:
        self.launch = launch
        super().__init__("mssql_native.tds_spawn_unknown")
