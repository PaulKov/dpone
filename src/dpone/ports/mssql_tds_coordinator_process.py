"""Internal one-shot coordinator transport; bytes confer no SQL authority.

The trusted supervisor validates closed envelopes and durably acknowledges each
phase before calling the next method. The fixed child validates independently.
Exit receipts prove local direct-child reaping only, never remote settlement.
"""

from typing import Protocol

from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity


class TdsManagedCoordinatorProcess(Protocol):
    """Single process/thread ownership; failed phase attempts cannot be retried."""

    @property
    def identity(self) -> TdsProcessIdentity:
        """Process incarnation independently bound to the exclusively owned pidfd."""

    @property
    def startup_receipt(self) -> TdsCoordinatorStartup | None:
        """Authenticated startup retained for the supervisor's digest producer."""

    @property
    def received_result(self) -> bytes | None:
        """Raw bounded result retained before teardown; not validated SQL evidence."""

    def startup(self, *, deadline: float) -> TdsCoordinatorStartup:
        """Authenticate one startup frame through EOF against trusted launch inputs."""

    def deliver_credentials(self, body: bytes, *, deadline: float) -> None:
        """After durable credential intent, send one bounded private frame and EOF."""

    def observe_authority(self, *, deadline: float) -> bytes:
        """Return one bounded authority envelope for independent semantic validation."""

    def deliver_grant(self, body: bytes, *, deadline: float) -> None:
        """After durable grant ACK, send one frame; partial delivery is never retried."""

    def receive_result(self, *, deadline: float) -> bytes:
        """Retain one bounded result through EOF before any descriptor cleanup."""

    def wait(self, *, deadline: float) -> TdsChildExit:
        """Exclusively reap within the inherited operation deadline."""

    def terminate(self, *, deadline: float) -> TdsChildExit:
        """Contain using a separately captured finite absolute cleanup deadline."""

    def close(self) -> None:
        """Release resources only after proven local settlement."""


class TdsCoordinatorProcessLauncher(Protocol):
    """Trusted composition supplies admitted source and binary/profile inputs."""

    @property
    def admission_sha256(self) -> str:
        """Canonical expected binary/profile descriptor digest, not verified binaries."""

    def spawn(self, *, startup_deadline: float, operation_deadline: float) -> TdsManagedCoordinatorProcess:
        """Launch fixed guarded code without credentials; retain uncertain launches."""
